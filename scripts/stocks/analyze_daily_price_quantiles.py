from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/daily_price_quantiles"
)


def pct(a, b):
    return (a / b - 1.0) * 100.0


def summarize(label, df):
    if df.empty:
        return {"label": label, "rows": 0}

    return {
        "label": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "price_min": df["prev_close"].min(),
        "price_median": df["prev_close"].median(),
        "price_max": df["prev_close"].max(),

        "median_volume_rvol": df["volume_rvol_20d"].median(),
        "median_dollar_volume_rvol": df["dollar_volume_rvol_20d"].median(),
        "median_dollar_volume": df["dollar_volume"].median(),
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

    base = df[
        (df["prev_close"] >= 2)
        & df["prev_close"].notna()
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
        & (df["avg_volume_20d_prior"] > 0)
        & (df["avg_dollar_volume_20d_prior"] > 0)
    ].copy()

    dormant = base[
        (base["avg_dollar_volume_20d_prior"] <= 1_000_000)
        & (base["dollar_volume"] >= 250_000)
        & (base["volume_rvol_20d"] >= 2)
        & (base["gap_pct"] >= 1)
    ].copy()

    rows = []

    for universe_name, universe in [
        ("base", base),
        ("dormant", dormant),
    ]:
        fixed_bins = [2, 3, 5, 10, 20, 50, np.inf]
        fixed_labels = ["2-3", "3-5", "5-10", "10-20", "20-50", "50+"]

        universe = universe.copy()
        universe["price_fixed_bucket"] = pd.cut(
            universe["prev_close"],
            bins=fixed_bins,
            labels=fixed_labels,
            right=False,
        )

        for bucket, sub in universe.groupby("price_fixed_bucket", observed=True):
            rows.append(summarize(f"{universe_name}_price_fixed_{bucket}", sub))

        universe["price_decile"] = pd.qcut(
            universe["prev_close"].rank(method="first"),
            q=10,
            labels=[f"Q{i}" for i in range(1, 11)],
        )

        for bucket, sub in universe.groupby("price_decile", observed=True):
            rows.append(summarize(f"{universe_name}_price_decile_{bucket}", sub))

    summary = pd.DataFrame(rows)

    summary_path = OUTPUT_DIR / "daily_price_quantile_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("saved summary:", summary_path)

    display_cols = [
        "label",
        "rows",
        "tickers",
        "price_min",
        "price_median",
        "price_max",
        "median_volume_rvol",
        "median_dollar_volume_rvol",
        "median_dollar_volume",
        "median_gap_pct",
        "same_day_open_to_close_avg",
        "same_day_open_to_close_median",
        "same_day_green_rate",
        "fwd_1d_close_avg",
        "fwd_1d_close_win_rate",
        "next_day_open_to_close_avg",
        "next_day_green_rate",
        "fwd_5d_close_avg",
        "runup_5d_from_close_avg",
        "drawdown_5d_from_close_avg",
    ]

    print()
    print("=== Base Price Fixed Buckets ===")
    x = summary[summary["label"].str.startswith("base_price_fixed_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Dormant Price Fixed Buckets ===")
    x = summary[summary["label"].str.startswith("dormant_price_fixed_")]
    print(x[display_cols].to_string(index=False))

    print()
    print("=== Dormant Price Deciles ===")
    x = summary[summary["label"].str.startswith("dormant_price_decile_")]
    print(x[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
