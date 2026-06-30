from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


CANDIDATES_PATH = Path("data/research/intraday_gap_up/gap_up_candidates_top3.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up")

ROUND_TRIP_COST_BPS_LIST = [0, 5, 10, 20]

EXIT_RULES = {
    "eod": {},
    "stop_first_bar_low": {"stop_kind": "first_bar_low"},
    "stop_1pct": {"stop_pct": 1.0},
    "target_1pct": {"target_pct": 1.0},
    "target_1_5pct": {"target_pct": 1.5},
    "target_2pct": {"target_pct": 2.0},
    "stop_1pct_target_1pct": {"stop_pct": 1.0, "target_pct": 1.0},
    "stop_1pct_target_1_5pct": {"stop_pct": 1.0, "target_pct": 1.5},
    "stop_first_bar_low_target_1pct": {"stop_kind": "first_bar_low", "target_pct": 1.0},
    "stop_first_bar_low_target_1_5pct": {"stop_kind": "first_bar_low", "target_pct": 1.5},
}


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


def simulate_exit(rth: pd.DataFrame, rule_name: str, rule: dict) -> dict:
    first_bar = rth.iloc[0]
    entry_price = first_bar["close"]

    stop_price = None
    target_price = None

    if rule.get("stop_kind") == "first_bar_low":
        stop_price = first_bar["low"]

    if "stop_pct" in rule:
        stop_price = entry_price * (1 - rule["stop_pct"] / 100)

    if "target_pct" in rule:
        target_price = entry_price * (1 + rule["target_pct"] / 100)

    # Entry occurs after first 15m bar closes, so only future bars can trigger exits.
    future_bars = rth.iloc[1:].copy()

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in future_bars.iterrows():
        stop_hit = stop_price is not None and bar["low"] <= stop_price
        target_hit = target_price is not None and bar["high"] >= target_price

        # Conservative assumption: if stop and target both hit in same 15m bar,
        # assume the stop happened first.
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
        "exit_rule": rule_name,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return_pct": gross_return_pct,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
        "stop_price": stop_price,
        "target_price": target_price,
    }


def add_entry_filter_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    r = out["first_bar_return_pct"]

    out["all_gap_up_1pct"] = True
    out["first_bar_red_or_flat"] = r <= 0
    out["first_bar_le_minus_0_25"] = r <= -0.25
    out["first_bar_le_minus_0_50"] = r <= -0.50

    return out


def summarize(results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    filters = [
        "all_gap_up_1pct",
        "first_bar_red_or_flat",
        "first_bar_le_minus_0_25",
        "first_bar_le_minus_0_50",
    ]

    rows = []

    for entry_filter in filters:
        for exit_rule in sorted(results["exit_rule"].unique()):
            for split in ["WF1", "WF2"]:
                subset = results[
                    (results[entry_filter])
                    & (results["exit_rule"] == exit_rule)
                    & (results["split"] == split)
                ].copy()

                for cost_bps in ROUND_TRIP_COST_BPS_LIST:
                    cost_pct = cost_bps / 100.0
                    net = subset["gross_return_pct"] - cost_pct

                    rows.append(
                        {
                            "entry_filter": entry_filter,
                            "exit_rule": exit_rule,
                            "split": split,
                            "round_trip_cost_bps": cost_bps,
                            "trades": len(net),
                            "avg_net_trade": net.mean(),
                            "median_net_trade": net.median(),
                            "win_rate_net": (net > 0).mean() * 100 if len(net) else float("nan"),
                            "total_net_return_sum": net.sum(),
                            "best_net_trade": net.max() if len(net) else float("nan"),
                            "worst_net_trade": net.min() if len(net) else float("nan"),
                            "stop_rate": (subset["exit_reason"] == "stop").mean() * 100 if len(subset) else float("nan"),
                            "target_rate": (subset["exit_reason"] == "target").mean() * 100 if len(subset) else float("nan"),
                            "eod_rate": (subset["exit_reason"] == "eod").mean() * 100 if len(subset) else float("nan"),
                        }
                    )

    summary = pd.DataFrame(rows)

    avg = (
        summary
        .groupby(["entry_filter", "exit_rule", "round_trip_cost_bps"])
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
    )

    avg["sample_ok"] = avg["min_trades"] >= 10

    return summary, avg


def main() -> None:
    candidates = pd.read_csv(CANDIDATES_PATH)

    result_rows = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]

        df = load_15m(ticker, trade_date)
        rth = get_rth(df)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]

        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        base = {
            "split": row["split"],
            "ticker": ticker,
            "signal_date": row["signal_date"],
            "trade_date": trade_date,
            "daily_signal_rank": row["daily_signal_rank"],
            "daily_gap_pct": row["next_gap_pct"],
            "daily_open_to_close_pct": row["next_open_to_close_return_pct"],
            "rth_bars": len(rth),
            "first_bar_return_pct": first_bar_return_pct,
            "first_bar_open": first_bar["open"],
            "first_bar_high": first_bar["high"],
            "first_bar_low": first_bar["low"],
            "first_bar_close": first_bar["close"],
            "entry_time_et": str(first_bar["bar_start_et"]),
        }

        for rule_name, rule in EXIT_RULES.items():
            exit_result = simulate_exit(rth, rule_name, rule)
            result_rows.append({**base, **exit_result})

    results = pd.DataFrame(result_rows)
    results = add_entry_filter_flags(results)

    summary, avg = summarize(results)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results_path = OUTPUT_DIR / "gap_up_15m_exit_results.csv"
    summary_path = OUTPUT_DIR / "gap_up_15m_exit_summary.csv"
    avg_path = OUTPUT_DIR / "gap_up_15m_exit_avg_summary.csv"

    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)
    avg.to_csv(avg_path, index=False)

    print("=== Gap-up 15m exit test ===")
    print("Setup: daily signal + next-day gap up >= 1%")
    print("Entry: first RTH 15m close")
    print("Exit: tested stops/targets/EOD")
    print()
    print(f"Candidates: {len(candidates)}")
    print(f"Result rows: {len(results)}")
    print(f"Saved results: {results_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Saved avg summary: {avg_path}")
    print()

    for cost_bps in [10, 20]:
        print(f"=== Best sample-ok results | round-trip cost: {cost_bps} bps ===")
        view = avg[
            (avg["round_trip_cost_bps"] == cost_bps)
            & (avg["sample_ok"])
        ].sort_values("avg_net_trade", ascending=False)

        print(view.head(30).round(4).to_string(index=False))
        print()

    print("=== Baseline all_gap_up_1pct at 10 bps ===")
    baseline = avg[
        (avg["round_trip_cost_bps"] == 10)
        & (avg["entry_filter"] == "all_gap_up_1pct")
    ].sort_values("avg_net_trade", ascending=False)

    print(baseline.round(4).to_string(index=False))


if __name__ == "__main__":
    main()
