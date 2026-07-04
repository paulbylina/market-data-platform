from __future__ import annotations

from pathlib import Path
import itertools

import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "old_research_prior_day_context_with_prior_last15_2024-01-01_to_2026-07-02.csv"
)

OUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "first15_return_range_cap_sweep_prior_last15_context.csv"
)


def pct(series: pd.Series, cond) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return np.nan
    return cond(s).mean() * 100


def summarize(df: pd.DataFrame) -> dict:
    eod = pd.to_numeric(df["long_eod_pct"], errors="coerce").dropna()
    runup = pd.to_numeric(df["long_max_runup_pct"], errors="coerce").dropna()
    dd = pd.to_numeric(df["long_max_drawdown_pct"], errors="coerce").dropna()

    return {
        "rows": len(df),
        "tickers": df["ticker"].nunique() if "ticker" in df.columns else np.nan,

        "eod_avg": eod.mean(),
        "eod_median": eod.median(),
        "eod_win_rate": (eod > 0).mean() * 100,
        "eod_ge_1": (eod >= 1).mean() * 100,
        "eod_ge_2": (eod >= 2).mean() * 100,
        "eod_le_minus_2": (eod <= -2).mean() * 100,
        "eod_le_minus_3": (eod <= -3).mean() * 100,

        "runup_median": runup.median(),
        "runup_ge_2": (runup >= 2).mean() * 100,
        "runup_ge_3": (runup >= 3).mean() * 100,
        "runup_ge_4": (runup >= 4).mean() * 100,

        "drawdown_median": dd.median(),
        "drawdown_le_minus_2": (dd <= -2).mean() * 100,
        "drawdown_le_minus_3": (dd <= -3).mean() * 100,
        "drawdown_le_minus_4": (dd <= -4).mean() * 100,
    }


def main() -> None:
    df = pd.read_csv(INPUT)

    needed = [
        "ticker",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}")

    for c in df.columns:
        if any(x in c.lower() for x in ["pct", "rvol", "volume", "dollar", "range", "position", "gap"]):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Optional live-safe first15 RVOL filter if column exists.
    has_first15_rvol = "first15_dollar_rvol_20d" in df.columns

    prior_mins = [1.5, 3.0]
    pm_caps = [0.003, 0.01, 0.03]
    ret_mins = [1.0, 2.0]
    ret_caps = [4.0, 5.0, 6.0, 8.0]
    range_mins = [2.0]
    range_caps = [4.0, 5.0, 6.0, 8.0]
    close_pos_mins = [0.75, 0.90]
    first15_rvol_mins = [None, 3.0] if has_first15_rvol else [None]

    rows = []

    for prior_min, pm_cap, ret_min, ret_cap, range_min, range_cap, close_pos_min, first15_rvol_min in itertools.product(
        prior_mins,
        pm_caps,
        ret_mins,
        ret_caps,
        range_mins,
        range_caps,
        close_pos_mins,
        first15_rvol_mins,
    ):
        if ret_min >= ret_cap:
            continue
        if range_min >= range_cap:
            continue

        mask = (
            (df["prior_day_last15_dollar_rvol_20d"] >= prior_min)
            & (df["premarket_dollar_vs_prior_daily_avg"] <= pm_cap)
            & (df["first_15m_return_pct"] >= ret_min)
            & (df["first_15m_return_pct"] < ret_cap)
            & (df["first15_range_pct"] >= range_min)
            & (df["first15_range_pct"] < range_cap)
            & (df["first15_close_position_in_range"] >= close_pos_min)
        )

        if first15_rvol_min is not None:
            mask &= df["first15_dollar_rvol_20d"] >= first15_rvol_min

        g = df[mask].copy()

        if len(g) < 30:
            continue

        row = {
            "prior_last15_min": prior_min,
            "premarket_cap": pm_cap,
            "first15_return_min": ret_min,
            "first15_return_cap": ret_cap,
            "first15_range_min": range_min,
            "first15_range_cap": range_cap,
            "close_position_min": close_pos_min,
            "first15_rvol_min": first15_rvol_min if first15_rvol_min is not None else "none",
        }

        row.update(summarize(g))
        rows.append(row)

    out = pd.DataFrame(rows)

    if out.empty:
        raise SystemExit("No result groups found. Lower min rows or check columns.")

    # Conservative ranking:
    # prioritize median EOD, 2% runup frequency, controlled drawdown, and sample size.
    out["quality_score"] = (
        out["eod_median"] * 2.0
        + out["eod_win_rate"] * 0.03
        + out["runup_ge_2"] * 0.03
        - out["drawdown_le_minus_3"] * 0.04
        + np.log1p(out["rows"]) * 0.15
    )

    out = out.sort_values(
        ["quality_score", "eod_median", "runup_ge_2", "drawdown_le_minus_3", "rows"],
        ascending=[False, False, False, True, False],
    )

    out.to_csv(OUT, index=False)

    print("input:", INPUT)
    print("rows in source:", len(df))
    print("saved:", OUT)
    print()
    print("=== Top 40 first15 return/range cap combos ===")
    show_cols = [
        "quality_score",
        "rows",
        "prior_last15_min",
        "premarket_cap",
        "first15_return_min",
        "first15_return_cap",
        "first15_range_cap",
        "close_position_min",
        "first15_rvol_min",
        "eod_median",
        "eod_win_rate",
        "eod_ge_2",
        "runup_median",
        "runup_ge_2",
        "drawdown_median",
        "drawdown_le_minus_3",
    ]

    print(out[show_cols].head(40).to_string(index=False))


if __name__ == "__main__":
    main()
