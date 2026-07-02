from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research/from_scratch_base_rows_with_forward_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/price_x_volume_percentile_tasks"
)


def price_group(price):
    if price < 1:
        return "sub_1"
    if price < 2:
        return "1_to_2"
    if price < 5:
        return "2_to_5"
    if price < 10:
        return "5_to_10"
    if price < 20:
        return "10_to_20"
    if price < 50:
        return "20_to_50"
    if price < 100:
        return "50_to_100"
    return "100_plus"


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


def add_decile(df, metric_col, out_col):
    df[out_col] = pd.qcut(
        df[metric_col].rank(method="first"),
        q=10,
        labels=[f"Q{i}" for i in range(1, 11)],
    )
    return df


def add_tail_bucket(df, metric_col, out_col):
    df["pct_rank_temp"] = df[metric_col].rank(pct=True, method="first") * 100

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
        df["pct_rank_temp"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    df = df.drop(columns=["pct_rank_temp"])
    return df


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

    base["price_group"] = base["prev_close"].apply(price_group)

    base = add_decile(base, "volume_rvol_20d", "volume_rvol_decile")
    base = add_decile(base, "dollar_volume_rvol_20d", "dollar_volume_rvol_decile")

    base = add_tail_bucket(base, "volume_rvol_20d", "volume_rvol_percentile_bucket")
    base = add_tail_bucket(base, "dollar_volume_rvol_20d", "dollar_volume_rvol_percentile_bucket")

    summaries = {}

    groupings = {
        "price_x_volume_rvol_decile": ["price_group", "volume_rvol_decile"],
        "price_x_dollar_volume_rvol_decile": ["price_group", "dollar_volume_rvol_decile"],
        "price_x_volume_rvol_percentile": ["price_group", "volume_rvol_percentile_bucket"],
        "price_x_dollar_volume_rvol_percentile": ["price_group", "dollar_volume_rvol_percentile_bucket"],
    }

    for name, cols in groupings.items():
        summary = (
            base.groupby(cols, observed=True)
            .apply(summarize, include_groups=False)
            .reset_index()
        )

        summary.to_csv(OUTPUT_DIR / f"{name}_summary.csv", index=False)
        summaries[name] = summary

    base.to_csv(OUTPUT_DIR / "price_x_volume_percentile_rows.csv", index=False)

    print("saved output dir:", OUTPUT_DIR)

    print()
    print("=== Price x Volume RVOL Decile ===")
    print(summaries["price_x_volume_rvol_decile"].to_string(index=False))

    print()
    print("=== Price x Dollar-Volume RVOL Decile ===")
    print(summaries["price_x_dollar_volume_rvol_decile"].to_string(index=False))

    print()
    print("=== Price x Volume RVOL Percentile Tail ===")
    print(summaries["price_x_volume_rvol_percentile"].to_string(index=False))


if __name__ == "__main__":
    main()
