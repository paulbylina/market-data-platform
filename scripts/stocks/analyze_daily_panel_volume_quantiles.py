from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_full_universe/historical_full_market_daily_panel.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_full_universe/daily_panel_volume_quantiles"
)


def pct(a, b):
    return (a / b - 1.0) * 100.0


def summarize(label, df, bucket_col):
    if df.empty:
        return {"label": label, "rows": 0}

    return {
        "label": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "bucket_min": df[bucket_col].min(),
        "bucket_median": df[bucket_col].median(),
        "bucket_max": df[bucket_col].max(),

        "median_price": df["prev_close"].median(),
        "median_dollar_volume": df["dollar_volume"].median(),

        # Same-day behavior. This uses full-day data, so research only.
        "same_day_open_to_close_avg": df["open_to_close_pct"].mean(),
        "same_day_open_to_close_median": df["open_to_close_pct"].median(),
        "same_day_green_rate": (df["open_to_close_pct"] > 0).mean() * 100,
        "same_day_runup_from_open_avg": df["same_day_runup_from_open_pct"].mean(),
        "same_day_drawdown_from_open_avg": df["same_day_drawdown_from_open_pct"].mean(),

        # Forward behavior after the volume day.
        "fwd_1d_close_avg": df["fwd_1d_close_return_pct"].mean(),
        "fwd_1d_close_median": df["fwd_1d_close_return_pct"].median(),
        "fwd_1d_close_win_rate": (df["fwd_1d_close_return_pct"] > 0).mean() * 100,

        "next_day_open_to_close_avg": df["next_day_open_to_close_pct"].mean(),
        "next_day_open_to_close_median": df["next_day_open_to_close_pct"].median(),
        "next_day_green_rate": (df["next_day_open_to_close_pct"] > 0).mean() * 100,

        "fwd_2d_close_avg": df["fwd_2d_close_return_pct"].mean(),
        "fwd_5d_close_avg": df["fwd_5d_close_return_pct"].mean(),

        "runup_5d_from_close_avg": df["runup_5d_from_close_pct"].mean(),
        "drawdown_5d_from_close_avg": df["drawdown_5d_from_close_pct"].mean(),
    }


def add_deciles(df, metric_col, prefix):
    out = df[df[metric_col].notna() & np.isfinite(df[metric_col])].copy()

    # Rank first avoids qcut errors from many identical values.
    ranked = out[metric_col].rank(method="first")

    out[f"{prefix}_decile"] = pd.qcut(
        ranked,
        q=10,
        labels=[f"Q{i}" for i in range(1, 11)],
    )

    return out


def build_quantile_summary(df, metric_col, prefix):
    decile_df = add_deciles(df, metric_col, prefix)
    bucket_col = metric_col

    rows = []

    for decile, sub in decile_df.groupby(f"{prefix}_decile", observed=True):
        rows.append(
            summarize(
                label=f"{prefix}_{decile}",
                df=sub,
                bucket_col=bucket_col,
            )
        )

    return pd.DataFrame(rows), decile_df


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["ticker", "trade_date"]).copy()

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

    g = df.groupby("ticker", group_keys=False)

    # Same-day daily path from regular open.
    df["same_day_runup_from_open_pct"] = pct(df["high"], df["open"])
    df["same_day_drawdown_from_open_pct"] = pct(df["low"], df["open"])

    # Forward bars.
    df["next_open"] = g["open"].shift(-1)
    df["next_high"] = g["high"].shift(-1)
    df["next_low"] = g["low"].shift(-1)
    df["next_close"] = g["close"].shift(-1)

    df["fwd_1d_close_return_pct"] = pct(g["close"].shift(-1), df["close"])
    df["fwd_2d_close_return_pct"] = pct(g["close"].shift(-2), df["close"])
    df["fwd_5d_close_return_pct"] = pct(g["close"].shift(-5), df["close"])

    df["next_day_open_to_close_pct"] = pct(df["next_close"], df["next_open"])

    future_highs = pd.concat([g["high"].shift(-i) for i in range(1, 6)], axis=1)
    future_lows = pd.concat([g["low"].shift(-i) for i in range(1, 6)], axis=1)

    df["future_5d_high"] = future_highs.max(axis=1)
    df["future_5d_low"] = future_lows.min(axis=1)

    df["runup_5d_from_close_pct"] = pct(df["future_5d_high"], df["close"])
    df["drawdown_5d_from_close_pct"] = pct(df["future_5d_low"], df["close"])

    # Broad research universe.
    # Keep this simple: avoid sub-$2 noise and require valid prior averages.
    base = df[
        (df["prev_close"] >= 2)
        & (df["avg_volume_20d_prior"] > 0)
        & (df["avg_dollar_volume_20d_prior"] > 0)
    ].copy()

    print("base rows:", len(base))
    print("unique tickers:", base["ticker"].nunique())
    print("date range:", base["trade_date"].min().date(), "to", base["trade_date"].max().date())

    all_summaries = []

    studies = [
        ("volume_rvol", "volume_rvol_20d"),
        ("dollar_volume_rvol", "dollar_volume_rvol_20d"),
        ("absolute_dollar_volume", "dollar_volume"),
    ]

    for prefix, metric_col in studies:
        summary, decile_df = build_quantile_summary(base, metric_col, prefix)

        summary_path = OUTPUT_DIR / f"{prefix}_decile_summary.csv"
        rows_path = OUTPUT_DIR / f"{prefix}_decile_rows.csv"

        summary.to_csv(summary_path, index=False)
        decile_df.to_csv(rows_path, index=False)

        all_summaries.append(summary.assign(study=prefix))

        print()
        print(f"=== {prefix} deciles ===")
        display_cols = [
            "label",
            "rows",
            "tickers",
            "bucket_min",
            "bucket_median",
            "bucket_max",
            "median_price",
            "median_dollar_volume",
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
        print(summary[display_cols].to_string(index=False))

    combined = pd.concat(all_summaries, ignore_index=True)
    combined_path = OUTPUT_DIR / "combined_volume_decile_summary.csv"
    combined.to_csv(combined_path, index=False)

    print()
    print("saved combined summary:", combined_path)


if __name__ == "__main__":
    main()
