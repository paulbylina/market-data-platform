from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


INPUT_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_relative_gap_daily_candidates.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

REL_GAP_THRESHOLDS = [3.0, 3.5, 4.0]
RANK_LIMITS = [1, 3, 5]
FIRST_BAR_FILTERS = [
    ("no_first_bar_filter", None),
    ("first_bar_le_0", 0.0),
    ("first_bar_le_minus_0_25", -0.25),
    ("first_bar_le_minus_0_50", -0.50),
    ("first_bar_le_minus_0_75", -0.75),
]

TARGET_PCT = 2.0
STOP_PCT = 3.0
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


def simulate_stop_target_eod(rth: pd.DataFrame) -> dict:
    first_bar = rth.iloc[0]
    entry = first_bar["close"]
    target = entry * (1 + TARGET_PCT / 100)
    stop = entry * (1 - STOP_PCT / 100)

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in rth.iloc[1:].iterrows():
        if bar["low"] <= stop:
            exit_price = stop
            exit_reason = "stop"
            exit_time_et = str(bar["bar_start_et"])
            break

        if bar["high"] >= target:
            exit_price = target
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross = (exit_price / entry - 1) * 100
    net = gross - (COST_BPS / 100)

    return {
        "entry_price": entry,
        "exit_price": exit_price,
        "gross_return_pct": gross,
        "net_return_pct": net,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
    }


def summarize(sub: pd.DataFrame, label: str) -> dict:
    years = sub["trade_date"].dt.year.nunique()

    return {
        "label": label,
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

    # Only use the intraday pool we downloaded.
    candidates = candidates[
        (candidates["relative_gap_vs_spy_pct"] >= 3.0)
        & (candidates["relative_gap_rank"] <= 5)
    ].copy()

    rows = []
    missing = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"].strftime("%Y-%m-%d")

        try:
            bars = load_15m(ticker, trade_date)
        except FileNotFoundError:
            missing.append((ticker, trade_date))
            continue

        rth = get_rth(bars)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]
        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        result = simulate_stop_target_eod(rth)

        rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "stock_gap_pct": row["stock_gap_pct"],
                "spy_gap_pct": row["spy_gap_pct"],
                "relative_gap_vs_spy_pct": row["relative_gap_vs_spy_pct"],
                "relative_gap_rank": row["relative_gap_rank"],
                "volume_ratio_20d": row["volume_ratio_20d"],
                "first_bar_return_pct": first_bar_return_pct,
                **result,
            }
        )

    results = pd.DataFrame(rows)
    results["trade_date"] = pd.to_datetime(results["trade_date"])
    results["year"] = results["trade_date"].dt.year

    summary_rows = []

    for rel_gap in REL_GAP_THRESHOLDS:
        for rank_limit in RANK_LIMITS:
            for filter_name, first_bar_threshold in FIRST_BAR_FILTERS:
                mask = (
                    (results["relative_gap_vs_spy_pct"] >= rel_gap)
                    & (results["relative_gap_rank"] <= rank_limit)
                )

                if first_bar_threshold is not None:
                    mask = mask & (results["first_bar_return_pct"] <= first_bar_threshold)

                sub = results[mask].copy()
                label = f"relgap_ge_{rel_gap:g}_rank_le_{rank_limit}_{filter_name}"
                summary_rows.append(summarize(sub, label))

    summary = pd.DataFrame(summary_rows).sort_values(
        ["avg", "trades_per_day"],
        ascending=[False, False],
    )

    results_path = OUTPUT_DIR / "no_oversold_relative_gap_intraday_results.csv"
    summary_path = OUTPUT_DIR / "no_oversold_relative_gap_intraday_summary.csv"

    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("=== No-oversold relative-gap intraday test ===")
    print(f"Input candidates: {len(candidates)}")
    print(f"Missing intraday files: {len(missing)}")
    print(f"Result rows: {len(results)}")
    print(f"Saved results: {results_path}")
    print(f"Saved summary: {summary_path}")
    print()

    if missing:
        print("First missing examples:")
        print(missing[:20])
        print()

    print("=== Best avg results ===")
    print(summary.head(30).round(4).to_string(index=False))
    print()

    print("=== Results near 1 trade/day or more ===")
    active = summary[summary["trades_per_day"] >= 0.75].copy()
    active = active.sort_values(["avg", "trades_per_day"], ascending=[False, False])
    print(active.head(30).round(4).to_string(index=False))


if __name__ == "__main__":
    main()
