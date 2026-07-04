from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_context_scored_daily_best_2024_2026.csv"
)


def count_block(name: str, df: pd.DataFrame, mask: pd.Series) -> dict:
    sub = df[mask].copy()
    active_days = sub["trade_date"].nunique()
    candidate_days = df["trade_date"].nunique()

    return {
        "bucket": name,
        "trades": len(sub),
        "dates": active_days,
        "tickers": sub["ticker"].nunique(),
        "trades_per_candidate_day": len(sub) / candidate_days if candidate_days else np.nan,
        "trades_per_active_day": len(sub) / active_days if active_days else np.nan,
        "avg_long_eod": sub["long_eod_pct"].mean(),
        "median_long_eod": sub["long_eod_pct"].median(),
        "eod_win_rate": (sub["long_eod_pct"] > 0).mean() * 100,
        "median_runup": sub["long_max_runup_pct"].median(),
        "median_drawdown": sub["long_max_drawdown_pct"].median(),
    }


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input file: {INPUT}")

    df = pd.read_csv(INPUT)

    numeric_cols = [
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "first15_dollar_rvol_20d",
        "first15_dollar_vs_prior_daily_avg",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    pm = df["premarket_dollar_vs_prior_daily_avg"]
    prior = df["prior_day_last15_dollar_rvol_20d"]
    ret = df["first_15m_return_pct"]
    rng = df["first15_range_pct"]
    close_pos = df["first15_close_position_in_range"]

    current_a_bplus = df["context_label"].isin(
        ["A+ strict", "A strong", "B+ extended clean"]
    )

    no_prior_strict = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.90)
    )

    no_prior_strong = (
        (pm <= 0.03)
        & (ret >= 2) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.75)
    )

    no_prior_extended = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 4) & (rng < 6)
        & (close_pos >= 0.75)
    )

    no_prior_a_bplus = no_prior_strict | no_prior_strong | no_prior_extended
    prior_15_a_bplus = no_prior_a_bplus & (prior >= 1.5)
    prior_3_a_bplus = no_prior_a_bplus & (prior >= 3)

    rows = [
        count_block("current_A_Aplus_Bplus_with_prior_rules", df, current_a_bplus),
        count_block("same_structure_prior_ge_3", df, prior_3_a_bplus),
        count_block("same_structure_prior_ge_1_5", df, prior_15_a_bplus),
        count_block("same_structure_no_prior_requirement", df, no_prior_a_bplus),
        count_block("no_prior_strict_shape_only", df, no_prior_strict),
        count_block("no_prior_strong_shape_only", df, no_prior_strong),
        count_block("no_prior_extended_shape_only", df, no_prior_extended),
    ]

    out = pd.DataFrame(rows)

    current_trades = out.loc[
        out["bucket"].eq("current_A_Aplus_Bplus_with_prior_rules"),
        "trades",
    ].iloc[0]

    out["multiple_vs_current"] = out["trades"] / current_trades

    print("input:", INPUT)
    print("candidate dates:", df["trade_date"].nunique())
    print("total rows:", len(df))
    print()
    print("=== Prior-day last15 filter impact ===")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
