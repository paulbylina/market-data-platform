from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_full_universe/historical_full_market_daily_panel.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_full_universe/daily_rvol_extreme_tail"
)


def pct(a, b):
    return (a / b - 1.0) * 100.0


def summarize(label, df, metric_col):
    if df.empty:
        return {"label": label, "rows": 0}

    return {
        "label": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "metric_min": df[metric_col].min(),
        "metric_median": df[metric_col].median(),
        "metric_max": df[metric_col].max(),

        "median_prev_close": df["prev_close"].median(),
        "median_dollar_volume": df["dollar_volume"].median(),
        "median_gap_pct": df["gap_pct"].median(),

        # Same-day behavior. Research only; full-day volume is known only after close.
        "same_day_open_to_close_avg": df["open_to_close_pct"].mean(),
        "same_day_open_to_close_median": df["open_to_close_pct"].median(),
        "same_day_green_rate": (df["open_to_close_pct"] > 0).mean() * 100,
        "same_day_runup_from_open_avg": df["same_day_runup_from_open_pct"].mean(),
        "same_day_drawdown_from_open_avg": df["same_day_drawdown_from_open_pct"].mean(),

        # Forward behavior.
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


def fixed_bucket_summary(df, metric_col, prefix):
    bins = [
        0,
        0.25,
        0.5,
        0.75,
        1,
        1.5,
        2,
        3,
        5,
        10,
        20,
        50,
        100,
        500,
        1000,
        5000,
        np.inf,
    ]

    labels = [
        "0-0.25",
        "0.25-0.5",
        "0.5-0.75",
        "0.75-1",
        "1-1.5",
        "1.5-2",
        "2-3",
        "3-5",
        "5-10",
        "10-20",
        "20-50",
        "50-100",
        "100-500",
        "500-1000",
        "1000-5000",
        "5000+",
    ]

    temp = df.copy()
    temp["bucket"] = pd.cut(
        temp[metric_col],
        bins=bins,
        labels=labels,
        right=False,
    )

    rows = []
    for bucket, sub in temp.groupby("bucket", observed=True):
        rows.append(summarize(f"{prefix}_fixed_{bucket}", sub, metric_col))

    return pd.DataFrame(rows)


def percentile_tail_summary(df, metric_col, prefix):
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

    temp["tail_bucket"] = pd.cut(
        temp["pct_rank"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    rows = []
    for bucket, sub in temp.groupby("tail_bucket", observed=True):
        rows.append(summarize(f"{prefix}_percentile_{bucket}", sub, metric_col))

    return pd.DataFrame(rows)


def run_study(df, universe_name, universe_df):
    studies = [
        ("volume_rvol", "volume_rvol_20d"),
        ("dollar_volume_rvol", "dollar_volume_rvol_20d"),
    ]

    all_rows = []

    for prefix, metric_col in studies:
        fixed = fixed_bucket_summary(
            universe_df,
            metric_col=metric_col,
            prefix=f"{universe_name}_{prefix}",
        )

        tail = percentile_tail_summary(
            universe_df,
            metric_col=metric_col,
            prefix=f"{universe_name}_{prefix}",
        )

        all_rows.append(fixed.assign(study=f"{universe_name}_{prefix}_fixed"))
        all_rows.append(tail.assign(study=f"{universe_name}_{prefix}_percentile"))

    return pd.concat(all_rows, ignore_index=True)


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
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = add_forward_metrics(df)

    # Broad base: avoid sub-$2 noise and invalid prior averages.
    base = df[
        (df["prev_close"] >= 2)
        & (df["avg_volume_20d_prior"] > 0)
        & (df["avg_dollar_volume_20d_prior"] > 0)
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
    ].copy()

    # Dormant activation universe from previous research.
    dormant = base[
        (base["avg_dollar_volume_20d_prior"] <= 1_000_000)
        & (base["dollar_volume"] >= 250_000)
        & (base["volume_rvol_20d"] >= 2)
        & (base["gap_pct"] >= 1)
    ].copy()

    print("=== Universes ===")
    print("base rows:", len(base), "tickers:", base["ticker"].nunique())
    print("dormant rows:", len(dormant), "tickers:", dormant["ticker"].nunique())
    print("date range:", base["trade_date"].min().date(), "to", base["trade_date"].max().date())

    base_summary = run_study(df, "base", base)
    dormant_summary = run_study(df, "dormant", dormant)

    summary = pd.concat([base_summary, dormant_summary], ignore_index=True)

    summary_path = OUTPUT_DIR / "daily_rvol_extreme_tail_summary.csv"
    base_path = OUTPUT_DIR / "base_rows_with_forward_metrics.csv"
    dormant_path = OUTPUT_DIR / "dormant_rows_with_forward_metrics.csv"

    summary.to_csv(summary_path, index=False)
    base.to_csv(base_path, index=False)
    dormant.to_csv(dormant_path, index=False)

    print()
    print("saved summary:", summary_path)
    print("saved base rows:", base_path)
    print("saved dormant rows:", dormant_path)

    display_cols = [
        "label",
        "rows",
        "tickers",
        "metric_min",
        "metric_median",
        "metric_max",
        "median_prev_close",
        "median_dollar_volume",
        "median_gap_pct",
        "same_day_open_to_close_avg",
        "same_day_open_to_close_median",
        "same_day_green_rate",
        "same_day_runup_from_open_avg",
        "same_day_drawdown_from_open_avg",
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
    print("=== Base Volume RVOL Fixed Buckets ===")
    x = summary[summary["label"].str.startswith("base_volume_rvol_fixed_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Base Volume RVOL Percentile Tail ===")
    x = summary[summary["label"].str.startswith("base_volume_rvol_percentile_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Dormant Volume RVOL Fixed Buckets ===")
    x = summary[summary["label"].str.startswith("dormant_volume_rvol_fixed_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Dormant Volume RVOL Percentile Tail ===")
    x = summary[summary["label"].str.startswith("dormant_volume_rvol_percentile_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Dormant Dollar-Volume RVOL Fixed Buckets ===")
    x = summary[summary["label"].str.startswith("dormant_dollar_volume_rvol_fixed_")]
    print(x[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
