from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research/from_scratch_base_rows_with_forward_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/price_x_rvol_robust"
)


def winsorized_mean(s, lower=0.01, upper=0.99):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) == 0:
        return np.nan
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lo, hi).mean()


def summarize(group):
    return pd.Series(
        {
            "rows": len(group),
            "tickers": group["ticker"].nunique(),

            "median_prev_close": group["prev_close"].median(),
            "median_volume_rvol": group["volume_rvol_20d"].median(),
            "median_dollar_volume_rvol": group["dollar_volume_rvol_20d"].median(),
            "median_dollar_volume": group["dollar_volume"].median(),

            "same_day_median": group["open_to_close_pct"].median(),
            "same_day_win_rate": (group["open_to_close_pct"] > 0).mean() * 100,

            "fwd_1d_median": group["fwd_1d_close_pct"].median(),
            "fwd_1d_q25": group["fwd_1d_close_pct"].quantile(0.25),
            "fwd_1d_q75": group["fwd_1d_close_pct"].quantile(0.75),
            "fwd_1d_winsor_mean": winsorized_mean(group["fwd_1d_close_pct"]),
            "fwd_1d_win_rate": (group["fwd_1d_close_pct"] > 0).mean() * 100,

            "next_day_oc_median": group["next_day_open_to_close_pct"].median(),
            "next_day_green_rate": (group["next_day_open_to_close_pct"] > 0).mean() * 100,

            "fwd_5d_median": group["fwd_5d_close_pct"].median(),
            "fwd_5d_winsor_mean": winsorized_mean(group["fwd_5d_close_pct"]),

            "runup_5d_median": group["runup_5d_from_close_pct"].median(),
            "drawdown_5d_median": group["drawdown_5d_from_close_pct"].median(),
        }
    )


def make_pivot(summary, row_col, col_col, value_col):
    return summary.pivot(
        index=row_col,
        columns=col_col,
        values=value_col,
    )


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

    numeric_cols = [
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

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Still from scratch:
    # no dormant filter
    # no gap filter
    # no liquidity filter
    base = df[
        (df["prev_close"] > 0)
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
        & np.isfinite(df["volume_rvol_20d"])
        & np.isfinite(df["dollar_volume_rvol_20d"])
    ].copy()

    print("=== Universe ===")
    print("rows:", len(base))
    print("tickers:", base["ticker"].nunique())
    print("date range:", base["trade_date"].min().date(), "to", base["trade_date"].max().date())

    # Price buckets.
    price_bins = [0, 1, 2, 5, 10, 20, 50, 100, np.inf]
    price_labels = [
        "0-1",
        "1-2",
        "2-5",
        "5-10",
        "10-20",
        "20-50",
        "50-100",
        "100+",
    ]

    base["price_bucket"] = pd.cut(
        base["prev_close"],
        bins=price_bins,
        labels=price_labels,
        right=False,
    )

    base["price_decile"] = pd.qcut(
        base["prev_close"].rank(method="first"),
        q=10,
        labels=[f"Q{i}" for i in range(1, 11)],
    )

    # RVOL buckets.
    rvol_bins = [0, 0.5, 1, 2, 5, 10, 20, 50, 100, 500, 1000, np.inf]
    rvol_labels = [
        "0-0.5",
        "0.5-1",
        "1-2",
        "2-5",
        "5-10",
        "10-20",
        "20-50",
        "50-100",
        "100-500",
        "500-1000",
        "1000+",
    ]

    base["volume_rvol_bucket"] = pd.cut(
        base["volume_rvol_20d"],
        bins=rvol_bins,
        labels=rvol_labels,
        right=False,
    )

    base["dollar_volume_rvol_bucket"] = pd.cut(
        base["dollar_volume_rvol_20d"],
        bins=rvol_bins,
        labels=rvol_labels,
        right=False,
    )

    # Price-only robust summaries.
    price_fixed_summary = (
        base.groupby("price_bucket", observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    price_decile_summary = (
        base.groupby("price_decile", observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    # Cross-tabs.
    price_x_volume_rvol = (
        base.groupby(["price_bucket", "volume_rvol_bucket"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    price_x_dollar_volume_rvol = (
        base.groupby(["price_bucket", "dollar_volume_rvol_bucket"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    price_fixed_summary.to_csv(OUTPUT_DIR / "price_fixed_robust_summary.csv", index=False)
    price_decile_summary.to_csv(OUTPUT_DIR / "price_decile_robust_summary.csv", index=False)
    price_x_volume_rvol.to_csv(OUTPUT_DIR / "price_x_volume_rvol_robust_summary.csv", index=False)
    price_x_dollar_volume_rvol.to_csv(OUTPUT_DIR / "price_x_dollar_volume_rvol_robust_summary.csv", index=False)

    # Save pivot matrices for easier reading.
    pivots = {
        "price_x_volume_rvol_fwd_1d_median.csv": make_pivot(
            price_x_volume_rvol,
            "price_bucket",
            "volume_rvol_bucket",
            "fwd_1d_median",
        ),
        "price_x_volume_rvol_fwd_1d_win_rate.csv": make_pivot(
            price_x_volume_rvol,
            "price_bucket",
            "volume_rvol_bucket",
            "fwd_1d_win_rate",
        ),
        "price_x_volume_rvol_fwd_5d_median.csv": make_pivot(
            price_x_volume_rvol,
            "price_bucket",
            "volume_rvol_bucket",
            "fwd_5d_median",
        ),
        "price_x_dollar_volume_rvol_fwd_1d_median.csv": make_pivot(
            price_x_dollar_volume_rvol,
            "price_bucket",
            "dollar_volume_rvol_bucket",
            "fwd_1d_median",
        ),
        "price_x_dollar_volume_rvol_fwd_1d_win_rate.csv": make_pivot(
            price_x_dollar_volume_rvol,
            "price_bucket",
            "dollar_volume_rvol_bucket",
            "fwd_1d_win_rate",
        ),
        "price_x_dollar_volume_rvol_fwd_5d_median.csv": make_pivot(
            price_x_dollar_volume_rvol,
            "price_bucket",
            "dollar_volume_rvol_bucket",
            "fwd_5d_median",
        ),
    }

    for filename, pivot in pivots.items():
        pivot.to_csv(OUTPUT_DIR / filename)

    display_cols = [
        "price_bucket",
        "rows",
        "tickers",
        "median_prev_close",
        "median_dollar_volume",
        "same_day_median",
        "same_day_win_rate",
        "fwd_1d_median",
        "fwd_1d_q25",
        "fwd_1d_q75",
        "fwd_1d_winsor_mean",
        "fwd_1d_win_rate",
        "next_day_oc_median",
        "next_day_green_rate",
        "fwd_5d_median",
        "fwd_5d_winsor_mean",
        "runup_5d_median",
        "drawdown_5d_median",
    ]

    print()
    print("=== Price Fixed Robust Summary ===")
    print(price_fixed_summary[display_cols].to_string(index=False))

    print()
    print("=== Price Decile Robust Summary ===")
    decile_cols = display_cols.copy()
    decile_cols[0] = "price_decile"
    print(price_decile_summary[decile_cols].to_string(index=False))

    print()
    print("=== Price x Volume RVOL: 1D Median Return ===")
    print(pivots["price_x_volume_rvol_fwd_1d_median.csv"].round(3).to_string())

    print()
    print("=== Price x Dollar-Volume RVOL: 1D Median Return ===")
    print(pivots["price_x_dollar_volume_rvol_fwd_1d_median.csv"].round(3).to_string())

    print()
    print("saved output dir:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
