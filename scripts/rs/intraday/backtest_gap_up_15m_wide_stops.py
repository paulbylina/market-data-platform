from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


CANDIDATES_PATH = Path("data/research/intraday_gap_up/gap_up_candidates_top3.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up")

COST_BPS = 10

STOP_PCTS = [None, 1.5, 2.0, 2.5, 3.0]
TARGET_PCTS = [2.0]


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_15m(ticker: str, trade_date: str) -> pd.DataFrame:
    path = build_market_curated_output_path(
        symbol=ticker,
        start_date=trade_date,
        end_date=trade_date,
        timeframe="15m",
    )

    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_parquet(path)


def simulate_stop_target(rth: pd.DataFrame, stop_pct: float | None, target_pct: float) -> dict:
    first_bar = rth.iloc[0]
    entry_price = first_bar["close"]

    stop_price = None
    if stop_pct is not None:
        stop_price = entry_price * (1 - stop_pct / 100)

    target_price = entry_price * (1 + target_pct / 100)

    future_bars = rth.iloc[1:].copy()

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in future_bars.iterrows():
        stop_hit = stop_price is not None and bar["low"] <= stop_price
        target_hit = bar["high"] >= target_price

        # Conservative same-bar assumption: stop first.
        if stop_hit:
            exit_price = stop_price
            exit_reason = "stop"
            exit_time_et = str(bar["bar_start_et"])
            break

        if target_hit:
            exit_price = target_price
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross_return_pct = (exit_price / entry_price - 1) * 100

    return {
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return_pct": gross_return_pct,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
        "stop_pct": stop_pct,
        "target_pct": target_pct,
    }


def summarize(results: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for rule_name in sorted(results["rule_name"].unique()):
        for split in ["WF1", "WF2"]:
            sub = results[
                (results["rule_name"] == rule_name)
                & (results["split"] == split)
            ].copy()

            net = sub["gross_return_pct"] - (COST_BPS / 100)

            rows.append({
                "rule_name": rule_name,
                "split": split,
                "trades": len(net),
                "avg_net_trade": net.mean(),
                "median_net_trade": net.median(),
                "win_rate_net": (net > 0).mean() * 100 if len(net) else float("nan"),
                "total_net_return_sum": net.sum(),
                "best_net_trade": net.max() if len(net) else float("nan"),
                "worst_net_trade": net.min() if len(net) else float("nan"),
                "stop_rate": (sub["exit_reason"] == "stop").mean() * 100 if len(sub) else float("nan"),
                "target_rate": (sub["exit_reason"] == "target").mean() * 100 if len(sub) else float("nan"),
                "eod_rate": (sub["exit_reason"] == "eod").mean() * 100 if len(sub) else float("nan"),
            })

    summary = pd.DataFrame(rows)

    avg = (
        summary
        .groupby("rule_name")
        .agg(
            avg_trades=("trades", "mean"),
            min_trades=("trades", "min"),
            avg_net_trade=("avg_net_trade", "mean"),
            median_net_trade=("median_net_trade", "mean"),
            avg_win_rate_net=("win_rate_net", "mean"),
            min_split_avg_net_trade=("avg_net_trade", "min"),
            avg_total_net_return_sum=("total_net_return_sum", "mean"),
            avg_stop_rate=("stop_rate", "mean"),
            avg_target_rate=("target_rate", "mean"),
            avg_eod_rate=("eod_rate", "mean"),
        )
        .reset_index()
        .sort_values("avg_net_trade", ascending=False)
    )

    return summary, avg


def main() -> None:
    candidates = pd.read_csv(CANDIDATES_PATH)

    rows = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]

        df = load_15m(ticker, trade_date)
        rth = get_rth(df)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]
        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        # Best current entry filter.
        if first_bar_return_pct > -0.50:
            continue

        base = {
            "split": row["split"],
            "ticker": ticker,
            "signal_date": row["signal_date"],
            "trade_date": trade_date,
            "daily_gap_pct": row["next_gap_pct"],
            "first_bar_return_pct": first_bar_return_pct,
        }

        for target_pct in TARGET_PCTS:
            for stop_pct in STOP_PCTS:
                if stop_pct is None:
                    rule_name = f"no_stop_target_{target_pct:g}pct"
                else:
                    rule_name = f"stop_{stop_pct:g}pct_target_{target_pct:g}pct"

                result = simulate_stop_target(
                    rth=rth,
                    stop_pct=stop_pct,
                    target_pct=target_pct,
                )

                rows.append({**base, "rule_name": rule_name, **result})

    results = pd.DataFrame(rows)

    summary, avg = summarize(results)

    results_path = OUTPUT_DIR / "gap_up_15m_wide_stop_results.csv"
    summary_path = OUTPUT_DIR / "gap_up_15m_wide_stop_summary.csv"
    avg_path = OUTPUT_DIR / "gap_up_15m_wide_stop_avg_summary.csv"

    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)
    avg.to_csv(avg_path, index=False)

    print("=== Gap-up 15m wide stop test ===")
    print("Entry filter: first 15m return <= -0.50%")
    print("Entry: first RTH 15m close")
    print("Target: +2%")
    print(f"Cost: {COST_BPS} bps")
    print()
    print(f"Trades per rule: {results.groupby('rule_name').size().iloc[0] if len(results) else 0}")
    print(f"Saved avg summary: {avg_path}")
    print()
    print("=== Average across WF1 + WF2 ===")
    print(avg.round(4).to_string(index=False))
    print()
    print("=== Split detail ===")
    print(summary.round(4).to_string(index=False))


if __name__ == "__main__":
    main()
