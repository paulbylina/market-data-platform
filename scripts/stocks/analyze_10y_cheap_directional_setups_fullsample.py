from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/extended_hours_features_pilot.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


def safe_short_return(long_return_pct):
    if pd.isna(long_return_pct):
        return np.nan
    r = long_return_pct / 100.0
    if r <= -0.999:
        return np.nan
    return (1.0 / (1.0 + r) - 1.0) * 100.0


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
        "median_pm_high_vs_prev_close": df["premarket_high_vs_prev_close_pct"].median(),
        "median_open_vs_pm_high": df["regular_open_vs_premarket_high_pct"].median(),
        "median_first15_return": df["first_15m_return_pct"].median(),

        "long_first15_to_eod_median": df["long_first15_to_eod_pct"].median(),
        "long_first15_to_eod_win_rate": (df["long_first15_to_eod_pct"] > 0).mean() * 100,

        "short_first15_to_eod_median": df["short_first15_to_eod_pct"].median(),
        "short_first15_to_eod_win_rate": (df["short_first15_to_eod_pct"] > 0).mean() * 100,

        "long_fwd_1d_median": df["fwd_1d_close_pct"].median(),
        "long_fwd_1d_win_rate": (df["fwd_1d_close_pct"] > 0).mean() * 100,
        "short_fwd_1d_median": df["short_fwd_1d_close_pct"].median(),
        "short_fwd_1d_win_rate": (df["short_fwd_1d_close_pct"] > 0).mean() * 100,

        "long_fwd_5d_median": df["fwd_5d_close_pct"].median(),
        "short_fwd_5d_median": df["short_fwd_5d_close_pct"].median(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)

    numeric_cols = [
        "prev_close",
        "dollar_volume",
        "dollar_volume_rvol_20d",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_vs_prior_daily_avg",
        "premarket_high_vs_prev_close_pct",
        "regular_open_vs_premarket_high_pct",
        "first_15m_dollar_volume",
        "first_15m_close",
        "regular_close",
        "first_15m_return_pct",
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

    df["long_first15_to_eod_pct"] = np.where(
        df["first_15m_close"] > 0,
        (df["regular_close"] / df["first_15m_close"] - 1.0) * 100.0,
        np.nan,
    )

    df["short_first15_to_eod_pct"] = df["long_first15_to_eod_pct"].apply(safe_short_return)
    df["short_fwd_1d_close_pct"] = df["fwd_1d_close_pct"].apply(safe_short_return)
    df["short_fwd_5d_close_pct"] = df["fwd_5d_close_pct"].apply(safe_short_return)

    high_daily = df[
        df["dollar_volume_regime"].isin(
            ["extreme_p95_p99", "mania_p99_p99_9", "super_mania_p99_9_p100"]
        )
    ].copy()

    rows = []

    rows.append(summarize(
        "LONG_quiet_pm_first15_active_green",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
            & (high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
            & (high_daily["first_15m_return_pct"] > 0)
        ],
    ))

    rows.append(summarize(
        "LONG_quiet_pm_first15_active_strong_ge_1pct",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
            & (high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
            & (high_daily["first_15m_return_pct"] >= 1)
        ],
    ))

    rows.append(summarize(
        "SHORT_hot_pm_big_fade_first15_red",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] > 0.1)
            & (high_daily["regular_open_vs_premarket_high_pct"] <= -5)
            & (high_daily["first_15m_return_pct"] < 0)
        ],
    ))

    rows.append(summarize(
        "SHORT_mania_pm_big_fade_first15_weak_le_-1pct",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] > 1)
            & (high_daily["regular_open_vs_premarket_high_pct"] <= -5)
            & (high_daily["first_15m_return_pct"] <= -1)
        ],
    ))

    rows.append(summarize(
        "SHORT_super_mania_pm_collapse",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] > 10)
            & (high_daily["regular_open_vs_premarket_high_pct"] <= -15)
        ],
    ))

    rows.append(summarize("CONTROL_all_high_daily", high_daily))

    rows.append(summarize(
        "CONTROL_hot_pm_first15_active",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] > 0.1)
            & (high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        ],
    ))

    rows.append(summarize(
        "CONTROL_quiet_pm_first15_active",
        high_daily[
            (high_daily["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
            & (high_daily["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        ],
    ))

    summary = pd.DataFrame(rows)

    summary_path = OUTPUT_DIR / "cheap_directional_setups_fullsample_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("saved:", summary_path)

    print()
    print("=== Cheap Directional Setups Full-Sample Summary ===")

    display_cols = [
        "label",
        "rows",
        "tickers",
        "median_prev_close",
        "median_daily_dollar_rvol",
        "median_pm_dollar_vs_daily_avg",
        "median_first15_dollar_vs_daily_avg",
        "median_pm_high_vs_prev_close",
        "median_open_vs_pm_high",
        "median_first15_return",
        "long_first15_to_eod_median",
        "long_first15_to_eod_win_rate",
        "short_first15_to_eod_median",
        "short_first15_to_eod_win_rate",
        "long_fwd_1d_median",
        "short_fwd_1d_median",
        "long_fwd_5d_median",
        "short_fwd_5d_median",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
