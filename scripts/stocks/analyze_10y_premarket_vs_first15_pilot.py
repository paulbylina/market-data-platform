from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/extended_hours_features_pilot.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot"
)


def bucket_activity(x):
    if pd.isna(x):
        return "unknown"
    if x == 0:
        return "none"
    if x < 0.001:
        return "tiny_lt_0_001x"
    if x < 0.01:
        return "low_0_001x_0_01x"
    if x < 0.1:
        return "active_0_01x_0_1x"
    if x < 1:
        return "very_active_0_1x_1x"
    if x < 10:
        return "mania_1x_10x"
    if x < 100:
        return "super_mania_10x_100x"
    return "insane_100x_plus"


def bucket_first15_return(x):
    if pd.isna(x):
        return "unknown"
    if x < -10:
        return "crash_lt_-10"
    if x < -5:
        return "weak_-10_to_-5"
    if x < -1:
        return "pullback_-5_to_-1"
    if x < 1:
        return "flat_-1_to_1"
    if x < 5:
        return "green_1_to_5"
    if x < 10:
        return "strong_5_to_10"
    return "explosive_10_plus"


def bucket_pm_fade(x):
    if pd.isna(x):
        return "unknown"
    if x >= -1:
        return "opens_near_pm_high"
    if x >= -5:
        return "mild_fade_1_5"
    if x >= -15:
        return "big_fade_5_15"
    if x >= -30:
        return "crash_fade_15_30"
    return "collapse_30_plus"


def summarize(group):
    return pd.Series(
        {
            "rows": len(group),
            "tickers": group["ticker"].nunique(),

            "median_prev_close": group["prev_close"].median(),
            "median_daily_dollar_rvol": group["dollar_volume_rvol_20d"].median(),

            "median_premarket_dollar_vs_daily_avg": group["premarket_dollar_vs_prior_daily_avg"].median(),
            "median_first15_dollar_vs_daily_avg": group["first15_dollar_vs_prior_daily_avg"].median(),
            "median_first15_share_of_day": group["first15_share_of_day_dollar_volume"].median(),

            "median_pm_high_vs_prev_close": group["premarket_high_vs_prev_close_pct"].median(),
            "median_open_vs_pm_high": group["regular_open_vs_premarket_high_pct"].median(),
            "median_first15_return": group["first_15m_return_pct"].median(),

            "same_day_median": group["open_to_close_pct"].median(),
            "same_day_green_rate": (group["open_to_close_pct"] > 0).mean() * 100,

            "fwd_1d_median": group["fwd_1d_close_pct"].median(),
            "fwd_1d_win_rate": (group["fwd_1d_close_pct"] > 0).mean() * 100,
            "fwd_5d_median": group["fwd_5d_close_pct"].median(),
            "drawdown_5d_median": group["drawdown_5d_from_close_pct"].median(),
        }
    )


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
        "first_15m_return_pct",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "fwd_5d_close_pct",
        "drawdown_5d_from_close_pct",
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

    df["premarket_activity_bucket"] = df["premarket_dollar_vs_prior_daily_avg"].apply(bucket_activity)
    df["first15_activity_bucket"] = df["first15_dollar_vs_prior_daily_avg"].apply(bucket_activity)
    df["first15_return_bucket"] = df["first_15m_return_pct"].apply(bucket_first15_return)
    df["open_vs_pm_high_bucket"] = df["regular_open_vs_premarket_high_pct"].apply(bucket_pm_fade)

    # High daily-volume buckets only.
    high_daily = df[
        df["dollar_volume_regime"].isin(
            [
                "extreme_p95_p99",
                "mania_p99_p99_9",
                "super_mania_p99_9_p100",
            ]
        )
    ].copy()

    cheap_high_daily = high_daily[high_daily["price_regime"] == "cheap_under_5"].copy()

    groupings = {
        "cheap_high_daily_by_premarket_activity": [
            "premarket_activity_bucket",
        ],
        "cheap_high_daily_by_first15_activity": [
            "first15_activity_bucket",
        ],
        "cheap_high_daily_by_pm_activity_x_first15_activity": [
            "premarket_activity_bucket",
            "first15_activity_bucket",
        ],
        "cheap_high_daily_by_pm_fade_x_first15_return": [
            "open_vs_pm_high_bucket",
            "first15_return_bucket",
        ],
        "all_price_high_daily_by_price_x_pm_activity_x_first15_activity": [
            "price_regime",
            "premarket_activity_bucket",
            "first15_activity_bucket",
        ],
    }

    for name, cols in groupings.items():
        source = cheap_high_daily if name.startswith("cheap") else high_daily

        summary = (
            source.groupby(cols, observed=True)
            .apply(summarize, include_groups=False)
            .reset_index()
            .sort_values(cols)
        )

        out_path = OUTPUT_DIR / f"{name}.csv"
        summary.to_csv(out_path, index=False)

        print()
        print(f"=== {name} ===")
        print(summary.to_string(index=False))
        print("saved:", out_path)


if __name__ == "__main__":
    main()
