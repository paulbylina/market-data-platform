from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_prior_last15_direct_ranked_signals_2026-06-29_to_2026-07-02.csv"
)

OUT = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_abc_gap_ranked_signals_2026-06-29_to_2026-07-02.csv"
)


def classify(row: pd.Series) -> tuple[str, int, str]:
    prior = row.get("prior_day_last15_dollar_rvol_20d", np.nan)
    gap = row.get("gap_pct", np.nan)
    pm = row.get("premarket_dollar_vs_prior_daily_avg", np.nan)
    ret = row.get("first_15m_return_pct", np.nan)
    rng = row.get("first15_range_pct", np.nan)
    close_pos = row.get("first15_close_position_in_range", np.nan)
    f15_rvol = row.get("first15_dollar_rvol_20d", np.nan)

    notes = []
    score = 0

    # Hard avoid rules first.
    if pd.notna(gap) and gap < 0:
        return "AVOID_gap_down", -10, "gap down; green first15 may be bounce"
    if pd.notna(gap) and gap > 5:
        return "AVOID_gap_too_hot", -9, "gap above 5%"
    if pd.notna(pm) and pm > 0.03:
        return "AVOID_active_premarket", -8, "pre-market too active"
    if pd.notna(ret) and ret >= 6:
        return "AVOID_first15_too_hot", -7, "first15 return >= 6%"
    if pd.notna(rng) and rng >= 8:
        return "AVOID_range_too_wide", -7, "first15 range >= 8%"
    if pd.notna(close_pos) and close_pos < 0.75:
        return "AVOID_weak_close_position", -6, "first15 close position < 0.75"

    # Score clean structure.
    if pd.notna(gap) and 0 <= gap <= 5:
        score += 3
        notes.append("gap 0-5")
    if pd.notna(pm) and pm <= 0.01:
        score += 2
        notes.append("quiet pre-market")
    elif pd.notna(pm) and pm <= 0.03:
        score += 1
        notes.append("mild pre-market")

    if pd.notna(ret) and 1 <= ret < 4:
        score += 3
        notes.append("first15 return 1-4")
    elif pd.notna(ret) and 4 <= ret < 6:
        score += 1
        notes.append("first15 return 4-6 extended")

    if pd.notna(rng) and 2 <= rng < 4:
        score += 3
        notes.append("range 2-4 clean")
    elif pd.notna(rng) and 4 <= rng < 6:
        score += 1
        notes.append("range 4-6 extended")

    if pd.notna(close_pos) and close_pos >= 0.90:
        score += 2
        notes.append("closed very near high")
    elif pd.notna(close_pos) and close_pos >= 0.75:
        score += 1
        notes.append("closed near high")

    if pd.notna(f15_rvol) and f15_rvol >= 3:
        score += 2
        notes.append("first15 RVOL >= 3")

    # ABC rank from prior-day last15.
    if pd.notna(prior) and prior >= 3:
        label = "A"
        score += 3
        notes.append("prior last15 >= 3")
    elif pd.notna(prior) and prior >= 1.5:
        label = "B"
        score += 1
        notes.append("prior last15 1.5-3")
    else:
        # C only if structure is very clean.
        if (
            pd.notna(gap) and 1 <= gap <= 5
            and pd.notna(pm) and pm <= 0.01
            and pd.notna(ret) and 1 <= ret < 4
            and pd.notna(rng) and 2 <= rng < 6
            and pd.notna(close_pos) and close_pos >= 0.90
        ):
            label = "C"
            notes.append("no prior confirmation but clean gap/first15")
        else:
            label = "WATCH"
            notes.append("missing/weak prior confirmation")

    return label, score, "; ".join(notes)


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)

    numeric_cols = [
        "prior_day_last15_dollar_rvol_20d",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "first15_dollar_rvol_20d",
        "first15_dollar_vs_prior_daily_avg",
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    labels = df.apply(classify, axis=1, result_type="expand")
    df["abc_gap_rank"] = labels[0]
    df["abc_gap_score"] = labels[1]
    df["abc_gap_notes"] = labels[2]

    rank_order = {
        "A": 1,
        "B": 2,
        "C": 3,
        "WATCH": 4,
        "AVOID_gap_down": 8,
        "AVOID_gap_too_hot": 8,
        "AVOID_active_premarket": 8,
        "AVOID_first15_too_hot": 8,
        "AVOID_range_too_wide": 8,
        "AVOID_weak_close_position": 8,
    }

    df["abc_gap_sort"] = df["abc_gap_rank"].map(rank_order).fillna(9)

    df = df.sort_values(
        ["trade_date", "abc_gap_sort", "abc_gap_score"],
        ascending=[True, True, False],
    )

    df.to_csv(OUT, index=False)

    show_cols = [
        "trade_date",
        "ticker",
        "abc_gap_rank",
        "abc_gap_score",
        "gap_pct",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "abc_gap_notes",
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    print("input:", INPUT)
    print("saved:", OUT)
    print()
    print("=== ABC + gap rank counts ===")
    print(df["abc_gap_rank"].value_counts().to_string())
    print()
    print("=== This week's ABC + gap signals ===")
    print(df[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
