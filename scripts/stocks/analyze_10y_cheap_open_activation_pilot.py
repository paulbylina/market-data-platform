from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/extended_hours_features_pilot.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot"
)


def summarize(label, df):
    if df.empty:
        return {"label": label, "rows": 0}

    return {
        "label": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "median_prev_close": df["prev_close"].median(),
        "median_daily_dollar_rvol": df["dollar_volume_rvol_20d"].median(),

        "median_pm_dollar_vs_daily_avg": df["premarket_dollar_vs_prior_daily_avg"].median(),
        "median_first15_dollar_vs_daily_avg": df["first15_dollar_vs_prior_daily_avg"].median(),
        "median_first15_share_of_day": df["first15_share_of_day_dollar_volume"].median(),

        "median_pm_high_vs_prev_close": df["premarket_high_vs_prev_close_pct"].median(),
        "median_open_vs_pm_high": df["regular_open_vs_premarket_high_pct"].median(),
        "median_first15_return": df["first_15m_return_pct"].median(),

        "same_day_open_to_close_median": df["open_to_close_pct"].median(),
        "same_day_green_rate": (df["open_to_close_pct"] > 0).mean() * 100,

        "first15_to_eod_median": df["first15_to_eod_pct"].median(),
        "first15_to_eod_win_rate": (df["first15_to_eod_pct"] > 0).mean() * 100,

        "fwd_1d_median": df["fwd_1d_close_pct"].median(),
        "fwd_1d_win_rate": (df["fwd_1d_close_pct"] > 0).mean() * 100,
        "fwd_5d_median": df["fwd_5d_close_pct"].median(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)

    numeric_cols = [
        "prev_close",
        "dollar_volume",
        "dollar_volume_rvol_20d",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_volume",
        "premarket_dollar_vs_prior_daily_avg",
        "premarket_high_vs_prev_close_pct",
        "regular_open_vs_premarket_high_pct",
        "first_15m_dollar_volume",
        "first_15m_close",
        "regular_close",
        "first_15m_return_pct",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "fwd_5d_close_pct",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["download_status"] == "ok"].copy()

    df["first15_dollar_vs_prior_daily_avg"] = np.where(
        df["avg_dollar_volume_20d_prior"] > 0,
        df["first_15m_dollar_volume"] / df["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    df["first15_share_of_day_dollar_volume"] = np.where(
        df["dollar_volume"] > 0,
        df["first_15m_dollar_volume"] / df["dollar_volume"],
        np.nan,
    )

    df["first15_to_eod_pct"] = np.where(
        df["first_15m_close"] > 0,
        (df["regular_close"] / df["first_15m_close"] - 1) * 100,
        np.nan,
    )

    high_daily_regimes = [
        "extreme_p95_p99",
        "mania_p99_p99_9",
        "super_mania_p99_9_p100",
    ]

    cheap_high_daily = df[
        (df["price_regime"] == "cheap_under_5")
        & (df["dollar_volume_regime"].isin(high_daily_regimes))
    ].copy()

    # Premarket definitions.
    pm_quiet = cheap_high_daily[
        cheap_high_daily["premarket_dollar_vs_prior_daily_avg"] <= 0.1
    ].copy()

    pm_hot = cheap_high_daily[
        cheap_high_daily["premarket_dollar_vs_prior_daily_avg"] > 0.1
    ].copy()

    # First-15m activation definitions.
    first15_active = cheap_high_daily[
        cheap_high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01
    ].copy()

    first15_very_active = cheap_high_daily[
        cheap_high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.1
    ].copy()

    first15_mania = cheap_high_daily[
        cheap_high_daily["first15_dollar_vs_prior_daily_avg"] >= 1.0
    ].copy()

    # Focus setup candidates.
    quiet_pm_first15_active = cheap_high_daily[
        (cheap_high_daily["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
        & (cheap_high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
    ].copy()

    quiet_pm_first15_green = quiet_pm_first15_active[
        quiet_pm_first15_active["first_15m_return_pct"] > 0
    ].copy()

    quiet_pm_first15_strong = quiet_pm_first15_active[
        quiet_pm_first15_active["first_15m_return_pct"] >= 1
    ].copy()

    quiet_pm_first15_red = quiet_pm_first15_active[
        quiet_pm_first15_active["first_15m_return_pct"] < 0
    ].copy()

    hot_pm_first15_active = cheap_high_daily[
        (cheap_high_daily["premarket_dollar_vs_prior_daily_avg"] > 0.1)
        & (cheap_high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
    ].copy()

    rows = [
        summarize("cheap_high_daily_all", cheap_high_daily),
        summarize("cheap_high_daily_pm_quiet_le_0.1x", pm_quiet),
        summarize("cheap_high_daily_pm_hot_gt_0.1x", pm_hot),
        summarize("cheap_high_daily_first15_active_ge_0.01x", first15_active),
        summarize("cheap_high_daily_first15_very_active_ge_0.1x", first15_very_active),
        summarize("cheap_high_daily_first15_mania_ge_1x", first15_mania),
        summarize("SETUP_quiet_pm_first15_active", quiet_pm_first15_active),
        summarize("SETUP_quiet_pm_first15_green", quiet_pm_first15_green),
        summarize("SETUP_quiet_pm_first15_strong_ge_1pct", quiet_pm_first15_strong),
        summarize("SETUP_quiet_pm_first15_red", quiet_pm_first15_red),
        summarize("CONTROL_hot_pm_first15_active", hot_pm_first15_active),
    ]

    summary = pd.DataFrame(rows)

    summary_path = OUTPUT_DIR / "cheap_open_activation_pilot_summary.csv"
    candidates_path = OUTPUT_DIR / "cheap_quiet_pm_first15_activation_candidates.csv"

    summary.to_csv(summary_path, index=False)
    quiet_pm_first15_active.to_csv(candidates_path, index=False)

    print("saved summary:", summary_path)
    print("saved candidates:", candidates_path)

    print()
    print("=== Cheap Open Activation Pilot Summary ===")
    display_cols = [
        "label",
        "rows",
        "tickers",
        "median_prev_close",
        "median_daily_dollar_rvol",
        "median_pm_dollar_vs_daily_avg",
        "median_first15_dollar_vs_daily_avg",
        "median_first15_share_of_day",
        "median_pm_high_vs_prev_close",
        "median_open_vs_pm_high",
        "median_first15_return",
        "same_day_open_to_close_median",
        "same_day_green_rate",
        "first15_to_eod_median",
        "first15_to_eod_win_rate",
        "fwd_1d_median",
        "fwd_5d_median",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
