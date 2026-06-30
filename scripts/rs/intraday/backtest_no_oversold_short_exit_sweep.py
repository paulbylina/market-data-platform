from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


INPUT_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_relative_gap_daily_candidates.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

REL_GAP_THRESHOLDS = [3.0, 3.5, 4.0]
RANK_LIMITS = [3, 5]
FIRST_BAR_FILTERS = [
    ("first_bar_le_0", "<=", 0.0),
    ("first_bar_le_minus_0_25", "<=", -0.25),
    ("first_bar_le_minus_0_50", "<=", -0.50),
    ("first_bar_le_minus_0_75", "<=", -0.75),
]

TARGETS = [1.0, 1.5, 2.0, 2.5, 3.0]
STOPS = [1.0, 1.5, 2.0, 2.5, 3.0]
COST_BPS = 20


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


def simulate_short(rth: pd.DataFrame, target_pct: float, stop_pct: float) -> dict:
    first_bar = rth.iloc[0]
    entry = first_bar["close"]

    target = entry * (1 - target_pct / 100)
    stop = entry * (1 + stop_pct / 100)

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in rth.iloc[1:].iterrows():
        # Conservative for same-bar ambiguity: stop first.
        if bar["high"] >= stop:
            exit_price = stop
            exit_reason = "stop"
            exit_time_et = str(bar["bar_start_et"])
            break

        if bar["low"] <= target:
            exit_price = target
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross = (entry / exit_price - 1) * 100
    net = gross - (COST_BPS / 100)

    return {
        "exit_price": exit_price,
        "gross_return_pct": gross,
        "net_return_pct": net,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
    }


def passes_first_bar_filter(series: pd.Series, operator: str, threshold: float) -> pd.Series:
    if operator == ">=":
        return series >= threshold

    if operator == "<=":
        return series <= threshold

    raise ValueError(operator)


def summarize(sub: pd.DataFrame, label: str, target_pct: float, stop_pct: float) -> dict:
    years = sub["trade_date"].dt.year.nunique()

    return {
        "label": label,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "trades": len(sub),
        "years": years,
        "trades_per_year": len(sub) / years if years else float("nan"),
        "trades_per_day": len(sub) / (years * 252) if years else float("nan"),
        "avg": sub["net_return_pct"].mean(),
        "median": sub["net_return_pct"].median(),
        "win_rate": (sub["net_return_pct"] > 0).mean() * 100 if len(sub) else float("nan"),
        "target_rate": (sub["exit_reason"] == "target").mean() * 100 if len(sub) else float("nan"),
        "stop_rate": (sub["exit_reason"] == "stop").mean() * 100 if len(sub) else float("nan"),
        "worst": sub["net_return_pct"].min() if len(sub) else float("nan"),
        "best": sub["net_return_pct"].max() if len(sub) else float("nan"),
        "total": sub["net_return_pct"].sum(),
    }


def main() -> None:
    candidates = pd.read_csv(INPUT_PATH)
    candidates["trade_date"] = pd.to_datetime(candidates["trade_date"])

    candidates = candidates[
        (candidates["relative_gap_vs_spy_pct"] >= 3.0)
        & (candidates["relative_gap_rank"] <= 5)
    ].copy()

    base_rows = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"].strftime("%Y-%m-%d")

        try:
            bars = load_15m(ticker, trade_date)
        except FileNotFoundError:
            continue

        rth = get_rth(bars)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]
        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        base_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "stock_gap_pct": row["stock_gap_pct"],
                "spy_gap_pct": row["spy_gap_pct"],
                "relative_gap_vs_spy_pct": row["relative_gap_vs_spy_pct"],
                "relative_gap_rank": row["relative_gap_rank"],
                "first_bar_return_pct": first_bar_return_pct,
                "rth": rth,
            }
        )

    summary_rows = []
    trade_rows = []

    for target_pct in TARGETS:
        for stop_pct in STOPS:
            rows = []

            for base in base_rows:
                result = simulate_short(base["rth"], target_pct, stop_pct)

                rows.append(
                    {
                        "ticker": base["ticker"],
                        "trade_date": base["trade_date"],
                        "stock_gap_pct": base["stock_gap_pct"],
                        "spy_gap_pct": base["spy_gap_pct"],
                        "relative_gap_vs_spy_pct": base["relative_gap_vs_spy_pct"],
                        "relative_gap_rank": base["relative_gap_rank"],
                        "first_bar_return_pct": base["first_bar_return_pct"],
                        "target_pct": target_pct,
                        "stop_pct": stop_pct,
                        **result,
                    }
                )

            results = pd.DataFrame(rows)
            results["trade_date"] = pd.to_datetime(results["trade_date"])
            results["year"] = results["trade_date"].dt.year

            for rel_gap in REL_GAP_THRESHOLDS:
                for rank_limit in RANK_LIMITS:
                    for filter_name, operator, threshold in FIRST_BAR_FILTERS:
                        mask = (
                            (results["relative_gap_vs_spy_pct"] >= rel_gap)
                            & (results["relative_gap_rank"] <= rank_limit)
                            & passes_first_bar_filter(
                                results["first_bar_return_pct"],
                                operator,
                                threshold,
                            )
                        )

                        sub = results[mask].copy()
                        label = f"short_relgap_ge_{rel_gap:g}_rank_le_{rank_limit}_{filter_name}"
                        summary_rows.append(summarize(sub, label, target_pct, stop_pct))

                        if (
                            rel_gap == 4.0
                            and rank_limit == 5
                            and filter_name in ["first_bar_le_minus_0_25", "first_bar_le_minus_0_50"]
                        ):
                            trade_rows.append(sub)

    summary = pd.DataFrame(summary_rows).sort_values(
        ["avg", "trades_per_day"],
        ascending=[False, False],
    )

    summary_path = OUTPUT_DIR / "no_oversold_short_exit_sweep_summary.csv"
    summary.to_csv(summary_path, index=False)

    if trade_rows:
        trades = pd.concat(trade_rows).reset_index(drop=True)
        trades_path = OUTPUT_DIR / "no_oversold_short_exit_sweep_selected_trades.csv"
        trades.to_csv(trades_path, index=False)
    else:
        trades_path = None

    print("=== No-oversold SHORT exit sweep ===")
    print("Cost:", COST_BPS, "bps")
    print("Saved summary:", summary_path)
    if trades_path:
        print("Saved selected trades:", trades_path)
    print()

    print("=== Best avg results ===")
    print(summary.head(40).round(4).to_string(index=False))
    print()

    print("=== Results near 1 trade/day or more ===")
    active = summary[summary["trades_per_day"] >= 0.75].copy()
    active = active.sort_values(["avg", "trades_per_day"], ascending=[False, False])
    print(active.head(40).round(4).to_string(index=False))


if __name__ == "__main__":
    main()
