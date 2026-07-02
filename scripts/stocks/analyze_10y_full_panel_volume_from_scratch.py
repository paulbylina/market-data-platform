from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research"
)


def pct(a, b):
    return (a / b - 1.0) * 100.0


def add_forward_metrics(df):
    df = df.sort_values(["ticker", "trade_date"]).copy()
    g = df.groupby("ticker", group_keys=False)

    df["same_day_runup_from_open_pct"] = pct(df["high"], df["open"])
    df["same_day_drawdown_from_open_pct"] = pct(df["low"], df["open"])

    df["next_open"] = g["open"].shift(-1)
    df["next_high"] = g["high"].shift(-1)
    df["next_low"] = g["low"].shift(-1)
    df["next_close"] = g["close"].shift(-1)

    df["fwd_1d_close_pct"] = pct(g["close"].shift(-1), df["close"])
    df["fwd_2d_close_pct"] = pct(g["close"].shift(-2), df["close"])
    df["fwd_5d_close_pct"] = pct(g["close"].shift(-5), df["close"])

    df["next_day_open_to_close_pct"] = pct(df["next_close"], df["next_open"])

    future_highs = pd.concat([g["high"].shift(-i) for i in range(1, 6)], axis=1)
    future_lows = pd.concat([g["low"].shift(-i) for i in range(1, 6)], axis=1)

    df["future_5d_high"] = future_highs.max(axis=1)
    df["future_5d_low"] = future_lows.min(axis=1)

    df["runup_5d_from_close_pct"] = pct(df["future_5d_high"], df["close"])
    df["drawdown_5d_from_close_pct"] = pct(df["future_5d_low"], df["close"])

    return df


def summarize(label, df, metric_col):
    return {
        "label": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "metric_min": df[metric_col].min(),
        "metric_median": df[metric_col].median(),
        "metric_max": df[metric_col].max(),

        "median_prev_close": df["prev_close"].median(),
        "median_dollar_volume": df["dollar_volume"].median(),
        "median_volume_rvol": df["volume_rvol_20d"].median(),
        "median_dollar_volume_rvol": df["dollar_volume_rvol_20d"].median(),
        "median_gap_pct": df["gap_pct"].median(),

        "same_day_open_to_close_avg": df["open_to_close_pct"].mean(),
        "same_day_open_to_close_median": df["open_to_close_pct"].median(),
        "same_day_green_rate": (df["open_to_close_pct"] > 0).mean() * 100,

        "same_day_runup_from_open_avg": df["same_day_runup_from_open_pct"].mean(),
        "same_day_drawdown_from_open_avg": df["same_day_drawdown_from_open_pct"].mean(),

        "fwd_1d_close_avg": df["fwd_1d_close_pct"].mean(),
        "fwd_1d_close_median": df["fwd_1d_close_pct"].median(),
        "fwd_1d_close_win_rate": (df["fwd_1d_close_pct"] > 0).mean() * 100,

        "next_day_open_to_close_avg": df["next_day_open_to_close_pct"].mean(),
        "next_day_open_to_close_median": df["next_day_open_to_close_pct"].median(),
        "next_day_green_rate": (df["next_day_open_to_close_pct"] > 0).mean() * 100,

        "fwd_2d_close_avg": df["fwd_2d_close_pct"].mean(),
        "fwd_5d_close_avg": df["fwd_5d_close_pct"].mean(),

        "runup_5d_from_close_avg": df["runup_5d_from_close_pct"].mean(),
        "drawdown_5d_from_close_avg": df["drawdown_5d_from_close_pct"].mean(),
    }


def quantile_summary(df, metric_col, prefix, q=10):
    temp = df[df[metric_col].notna() & np.isfinite(df[metric_col])].copy()

    temp[f"{prefix}_quantile"] = pd.qcut(
        temp[metric_col].rank(method="first"),
        q=q,
        labels=[f"Q{i}" for i in range(1, q + 1)],
    )

    rows = []
    for bucket, sub in temp.groupby(f"{prefix}_quantile", observed=True):
        rows.append(summarize(f"{prefix}_{bucket}", sub, metric_col))

    return pd.DataFrame(rows), temp


def tail_summary(df, metric_col, prefix):
    temp = df[df[metric_col].notna() & np.isfinite(df[metric_col])].copy()
    temp["pct_rank"] = temp[metric_col].rank(pct=True, method="first") * 100

    bins = [0, 50, 75, 90, 95, 99, 99.5, 99.9, 100.000001]
    labels = [
        "p0-p50",
        "p50-p75",
        "p75-p90",
        "p90-p95",
        "p95-p99",
        "p99-p99.5",
        "p99.5-p99.9",
        "p99.9-p100",
    ]

    temp[f"{prefix}_tail_bucket"] = pd.cut(
        temp["pct_rank"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    rows = []
    for bucket, sub in temp.groupby(f"{prefix}_tail_bucket", observed=True):
        rows.append(summarize(f"{prefix}_{bucket}", sub, metric_col))

    return pd.DataFrame(rows)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dollar_volume",
        "prev_close",
        "avg_volume_20d_prior",
        "avg_dollar_volume_20d_prior",
        "gap_pct",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "open_to_close_pct",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = add_forward_metrics(df)

    # From-scratch universe:
    # no dormant filter
    # no gap filter
    # no price threshold except price must be valid/positive
    # no liquidity threshold
    base = df[
        (df["prev_close"] > 0)
        & (df["open"] > 0)
        & (df["close"] > 0)
        & (df["avg_volume_20d_prior"] > 0)
        & (df["avg_dollar_volume_20d_prior"] > 0)
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
    ].copy()

    print("=== From-Scratch Universe ===")
    print("rows:", len(base))
    print("tickers:", base["ticker"].nunique())
    print("date range:", base["trade_date"].min().date(), "to", base["trade_date"].max().date())

    studies = [
        ("volume_rvol", "volume_rvol_20d"),
        ("dollar_volume_rvol", "dollar_volume_rvol_20d"),
        ("absolute_dollar_volume", "dollar_volume"),
        ("price_prev_close", "prev_close"),
    ]

    all_summaries = []

    for prefix, metric_col in studies:
        q_summary, q_rows = quantile_summary(base, metric_col, prefix, q=10)
        t_summary = tail_summary(base, metric_col, prefix)

        q_summary.to_csv(OUTPUT_DIR / f"{prefix}_decile_summary.csv", index=False)
        t_summary.to_csv(OUTPUT_DIR / f"{prefix}_tail_summary.csv", index=False)

        all_summaries.append(q_summary.assign(study=f"{prefix}_decile"))
        all_summaries.append(t_summary.assign(study=f"{prefix}_tail"))

        display_cols = [
            "label",
            "rows",
            "tickers",
            "metric_min",
            "metric_median",
            "metric_max",
            "median_prev_close",
            "median_dollar_volume",
            "median_volume_rvol",
            "median_dollar_volume_rvol",
            "median_gap_pct",
            "same_day_open_to_close_avg",
            "same_day_open_to_close_median",
            "same_day_green_rate",
            "fwd_1d_close_avg",
            "fwd_1d_close_median",
            "fwd_1d_close_win_rate",
            "next_day_open_to_close_avg",
            "next_day_green_rate",
            "fwd_5d_close_avg",
            "runup_5d_from_close_avg",
            "drawdown_5d_from_close_avg",
        ]

        print()
        print(f"=== {prefix} Deciles ===")
        print(q_summary[display_cols].to_string(index=False))

        print()
        print(f"=== {prefix} Percentile Tail ===")
        print(t_summary[display_cols].to_string(index=False))

    combined = pd.concat(all_summaries, ignore_index=True)
    combined.to_csv(OUTPUT_DIR / "combined_from_scratch_summary.csv", index=False)

    base.to_csv(OUTPUT_DIR / "from_scratch_base_rows_with_forward_metrics.csv", index=False)

    print()
    print("saved:", OUTPUT_DIR / "combined_from_scratch_summary.csv")
    print("saved:", OUTPUT_DIR / "from_scratch_base_rows_with_forward_metrics.csv")


if __name__ == "__main__":
    main()
