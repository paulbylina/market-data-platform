from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research/from_scratch_base_rows_with_forward_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/price_regime_local_volume_percentiles"
)


def price_regime(price):
    if price < 5:
        return "cheap_under_5"
    if price < 50:
        return "mid_5_to_50"
    return "high_50_plus"


def summarize(group):
    return pd.Series(
        {
            "rows": len(group),
            "tickers": group["ticker"].nunique(),
            "median_prev_close": group["prev_close"].median(),
            "median_dollar_volume": group["dollar_volume"].median(),
            "median_volume_rvol": group["volume_rvol_20d"].median(),
            "median_dollar_volume_rvol": group["dollar_volume_rvol_20d"].median(),

            "same_day_median": group["open_to_close_pct"].median(),
            "same_day_green_rate": (group["open_to_close_pct"] > 0).mean() * 100,

            "fwd_1d_median": group["fwd_1d_close_pct"].median(),
            "fwd_1d_win_rate": (group["fwd_1d_close_pct"] > 0).mean() * 100,

            "next_day_oc_median": group["next_day_open_to_close_pct"].median(),
            "next_day_green_rate": (group["next_day_open_to_close_pct"] > 0).mean() * 100,

            "fwd_5d_median": group["fwd_5d_close_pct"].median(),
            "runup_5d_median": group["runup_5d_from_close_pct"].median(),
            "drawdown_5d_median": group["drawdown_5d_from_close_pct"].median(),
        }
    )


def add_local_percentile_bucket(df, metric_col, out_col):
    df = df.copy()

    df["local_pct_rank"] = (
        df.groupby("price_regime")[metric_col]
        .rank(pct=True, method="first")
        * 100
    )

    bins = [0, 10, 25, 50, 75, 90, 95, 99, 99.5, 99.9, 100.000001]
    labels = [
        "p0_p10",
        "p10_p25",
        "p25_p50",
        "p50_p75",
        "p75_p90",
        "p90_p95",
        "p95_p99",
        "p99_p99_5",
        "p99_5_p99_9",
        "p99_9_p100",
    ]

    df[out_col] = pd.cut(
        df["local_pct_rank"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    return df.drop(columns=["local_pct_rank"])


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    usecols = [
        "ticker",
        "trade_date",
        "prev_close",
        "dollar_volume",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "next_day_open_to_close_pct",
        "fwd_5d_close_pct",
        "runup_5d_from_close_pct",
        "drawdown_5d_from_close_pct",
    ]

    df = pd.read_csv(INPUT_PATH, usecols=usecols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    numeric_cols = [c for c in usecols if c not in ["ticker", "trade_date"]]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    base = df[
        (df["prev_close"] > 0)
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
        & np.isfinite(df["volume_rvol_20d"])
        & np.isfinite(df["dollar_volume_rvol_20d"])
    ].copy()

    base["price_regime"] = base["prev_close"].apply(price_regime)

    base = add_local_percentile_bucket(
        base,
        metric_col="volume_rvol_20d",
        out_col="volume_rvol_local_percentile_bucket",
    )

    base = add_local_percentile_bucket(
        base,
        metric_col="dollar_volume_rvol_20d",
        out_col="dollar_volume_rvol_local_percentile_bucket",
    )

    volume_summary = (
        base.groupby(["price_regime", "volume_rvol_local_percentile_bucket"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    dollar_volume_summary = (
        base.groupby(["price_regime", "dollar_volume_rvol_local_percentile_bucket"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    volume_path = OUTPUT_DIR / "price_regime_local_volume_rvol_percentile_summary.csv"
    dollar_path = OUTPUT_DIR / "price_regime_local_dollar_volume_rvol_percentile_summary.csv"

    volume_summary.to_csv(volume_path, index=False)
    dollar_volume_summary.to_csv(dollar_path, index=False)

    print("saved:", volume_path)
    print("saved:", dollar_path)

    print()
    print("=== Local Volume RVOL Percentiles by Price Regime ===")
    print(volume_summary.to_string(index=False))

    print()
    print("=== Local Dollar-Volume RVOL Percentiles by Price Regime ===")
    print(dollar_volume_summary.to_string(index=False))


if __name__ == "__main__":
    main()
