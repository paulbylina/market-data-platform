from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_context_scored_daily_best_2024_2026.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

TRADES_OUT = OUT_DIR / "abc_prior_last15_exit_trades_2024_2026.csv"
SUMMARY_OUT = OUT_DIR / "abc_prior_last15_exit_summary_2024_2026.csv"


def simulate_exit(
    row: pd.Series,
    target_pct: float,
    stop_pct: float,
    cost_bps: float = 10.0,
    both_policy: str = "stop_first",
) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    if target_hit and stop_hit:
        if both_policy == "target_first":
            gross = target_pct
            exit_type = "target_ambiguous"
        else:
            gross = -stop_pct
            exit_type = "stop_ambiguous"
    elif target_hit:
        gross = target_pct
        exit_type = "target"
    elif stop_hit:
        gross = -stop_pct
        exit_type = "stop"
    else:
        gross = eod
        exit_type = "eod"

    net = gross - (cost_bps / 100.0)
    return net, exit_type


def summarize(g: pd.DataFrame) -> dict:
    vals = pd.to_numeric(g["net_pct"], errors="coerce").dropna()
    dates = g["trade_date"].nunique()

    return {
        "trades": len(vals),
        "dates": dates,
        "tickers": g["ticker"].nunique(),
        "trades_per_candidate_day": len(vals) / g["candidate_days"].iloc[0],
        "trades_per_active_day": len(vals) / dates if dates else np.nan,
        "sum_return_pct": vals.sum(),
        "avg_return_pct": vals.mean(),
        "median_return_pct": vals.median(),
        "win_rate": (vals > 0).mean() * 100,
        "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
        "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,
        "eod_rate": g["exit_type"].str.contains("eod", na=False).mean() * 100,
        "median_eod_raw": g["long_eod_pct"].median(),
        "median_runup_raw": g["long_max_runup_pct"].median(),
        "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
        "best": vals.max(),
        "worst": vals.min(),
    }


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)

    numeric_cols = [
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    candidate_days = df["trade_date"].nunique()

    pm = df["premarket_dollar_vs_prior_daily_avg"]
    prior = df["prior_day_last15_dollar_rvol_20d"]
    ret = df["first_15m_return_pct"]
    rng = df["first15_range_pct"]
    close_pos = df["first15_close_position_in_range"]

    strict_shape = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.90)
    )

    strong_shape = (
        (pm <= 0.03)
        & (ret >= 2) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.75)
    )

    extended_shape = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 4) & (rng < 6)
        & (close_pos >= 0.75)
    )

    base_shape = strict_shape | strong_shape | extended_shape

    masks = {
        "A_only_prior_ge_3": base_shape & (prior >= 3),
        "B_only_prior_1_5_to_3": base_shape & (prior >= 1.5) & (prior < 3),
        "C_only_prior_lt_1_5_or_missing": base_shape & ((prior < 1.5) | prior.isna()),
        "AB_prior_ge_1_5": base_shape & (prior >= 1.5),
        "ABC_no_prior_requirement": base_shape,
    }

    combos = [
        (1.0, 1.25),
        (1.0, 1.5),
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
    ]

    rows = []

    for bucket, mask in masks.items():
        sub = df[mask].copy()
        sub["abc_bucket"] = bucket
        sub["candidate_days"] = candidate_days

        for target, stop in combos:
            tmp = sub.copy()
            tmp["target_pct"] = target
            tmp["stop_pct"] = stop
            tmp["both_policy"] = "stop_first"

            exits = tmp.apply(
                lambda row: simulate_exit(
                    row=row,
                    target_pct=target,
                    stop_pct=stop,
                    cost_bps=10.0,
                    both_policy="stop_first",
                ),
                axis=1,
                result_type="expand",
            )

            tmp["net_pct"] = exits[0]
            tmp["exit_type"] = exits[1]

            rows.append(tmp)

    trades = pd.concat(rows, ignore_index=True)

    summary_rows = []
    for keys, g in trades.groupby(["abc_bucket", "target_pct", "stop_pct"], observed=True):
        bucket, target, stop = keys
        row = {
            "abc_bucket": bucket,
            "target_pct": target,
            "stop_pct": stop,
        }
        row.update(summarize(g))
        summary_rows.append(row)

    summary = pd.DataFrame(summary_rows)
    summary = summary.sort_values(
        ["abc_bucket", "sum_return_pct", "avg_return_pct", "worst"],
        ascending=[True, False, False, False],
    )

    trades.to_csv(TRADES_OUT, index=False)
    summary.to_csv(SUMMARY_OUT, index=False)

    print("input:", INPUT)
    print("candidate_days:", candidate_days)
    print("total_rows:", len(df))

    print()
    print("=== ABC raw bucket counts ===")
    for bucket, mask in masks.items():
        sub = df[mask]
        print(
            f"{bucket:35s} "
            f"trades={len(sub):4d} "
            f"dates={sub['trade_date'].nunique():4d} "
            f"tickers={sub['ticker'].nunique():4d} "
            f"trades/candidate_day={len(sub)/candidate_days:.3f}"
        )

    print()
    print("=== Top target/stop by ABC bucket ===")

    show_cols = [
        "abc_bucket",
        "target_pct",
        "stop_pct",
        "trades",
        "dates",
        "trades_per_candidate_day",
        "sum_return_pct",
        "avg_return_pct",
        "median_return_pct",
        "win_rate",
        "target_rate",
        "stop_rate",
        "median_eod_raw",
        "median_runup_raw",
        "median_drawdown_raw",
        "best",
        "worst",
    ]

    for bucket, g in summary.groupby("abc_bucket", observed=True):
        print()
        print(f"--- {bucket} ---")
        print(g[show_cols].head(8).to_string(index=False))

    print()
    print("saved trades:", TRADES_OUT)
    print("saved summary:", SUMMARY_OUT)


if __name__ == "__main__":
    main()
