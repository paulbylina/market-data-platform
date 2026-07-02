from pathlib import Path

import numpy as np
import pandas as pd


TRADES_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/cheap_trade_realism_trades.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


def price_bucket(x):
    if pd.isna(x):
        return "unknown"
    if x < 0.25:
        return "0_0.25"
    if x < 0.50:
        return "0.25_0.50"
    if x < 1.00:
        return "0.50_1"
    if x < 2.00:
        return "1_2"
    if x < 5.00:
        return "2_5"
    return "5_plus"


def summarize(df):
    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_prev_close": df["prev_close"].median(),
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].astype(str).str.startswith("time")).mean() * 100,
        "worst_net_return_pct": df["net_return_pct"].min(),
        "best_net_return_pct": df["net_return_pct"].max(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(TRADES_PATH)

    numeric_cols = ["prev_close", "net_return_pct", "gross_return_pct", "cost_bps"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["price_bucket"] = df["prev_close"].apply(price_bucket)

    # Focus on realistic 100 bps results first.
    focus = df[df["cost_bps"] == 100].copy()

    rows = []
    for keys, sub in focus.groupby(
        ["setup", "side", "config", "price_bucket"], observed=True
    ):
        setup, side, config, bucket = keys
        row = {
            "setup": setup,
            "side": side,
            "config": config,
            "price_bucket": bucket,
        }
        row.update(summarize(sub))
        rows.append(row)

    out = pd.DataFrame(rows)

    bucket_order = {
        "0_0.25": 0,
        "0.25_0.50": 1,
        "0.50_1": 2,
        "1_2": 3,
        "2_5": 4,
        "5_plus": 5,
        "unknown": 99,
    }

    out["bucket_order"] = out["price_bucket"].map(bucket_order)
    out = out.sort_values(["side", "setup", "config", "bucket_order"])
    out = out.drop(columns=["bucket_order"])

    out_path = OUTPUT_DIR / "cheap_trade_realism_by_price_bucket_100bps.csv"
    out.to_csv(out_path, index=False)

    print("saved:", out_path)

    print()
    print("=== Long Best Config By Price Bucket | 100 bps ===")
    long_best = out[
        (out["side"] == "long")
        & (out["config"] == "LONG_target_15_stop_8_hold_eod")
    ].copy()
    print(long_best.to_string(index=False))

    print()
    print("=== Long Conservative Config By Price Bucket | 100 bps ===")
    long_conservative = out[
        (out["side"] == "long")
        & (out["config"] == "LONG_target_10_stop_5_hold_eod")
    ].copy()
    print(long_conservative.to_string(index=False))

    print()
    print("=== Short Best Config By Price Bucket | 100 bps ===")
    short_best = out[
        (out["side"] == "short")
        & (out["config"] == "SHORT_target_10_stop_15_hold_eod")
    ].copy()
    print(short_best.to_string(index=False))


if __name__ == "__main__":
    main()
