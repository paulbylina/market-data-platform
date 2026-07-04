from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")
INPUT = BASE / "gap_down_50_plus_3candle_color_pattern_trades.csv"

OUT_SUMMARY = BASE / "gap_down_50_plus_3candle_color_pattern_liquid_summary.csv"
OUT_PERIOD = BASE / "gap_down_50_plus_3candle_color_pattern_liquid_period_summary.csv"
OUT_TOP = BASE / "gap_down_50_plus_3candle_color_pattern_liquid_top.csv"


def summarize(g):
    vals = pd.to_numeric(g["net_pct"], errors="coerce")
    return pd.Series({
        "trades": len(g),
        "dates": g["trade_date"].dt.date.nunique(),
        "tickers": g["ticker"].nunique(),
        "avg_net": vals.mean(),
        "median_net": vals.median(),
        "win_rate": (vals > 0).mean() * 100,
        "target_rate": g["exit_type"].astype(str).str.contains("target", na=False).mean() * 100,
        "stop_rate": g["exit_type"].astype(str).str.contains("stop", na=False).mean() * 100,
        "eod_rate": (g["exit_type"].astype(str) == "eod").mean() * 100,
        "median_gap": g["gap_pct"].median(),
        "median_first15_ret": g["first15_ret"].median(),
        "median_first15_close_pos": g["first15_close_pos"].median(),
        "median_first15_range": g["first15_range"].median(),
        "median_avg_dollar_vol_20d": g["avg_dollar_volume_20d_prior"].median(),
        "median_first15_dollar_vol": g["first_15m_dollar_volume"].median(),
        "best": vals.max(),
        "worst": vals.min(),
    })


df = pd.read_csv(INPUT)
df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
df = df.dropna(subset=["trade_date"]).copy()

for c in ["avg_dollar_volume_20d_prior", "first_15m_dollar_volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df[
    (df["avg_dollar_volume_20d_prior"] >= 20_000_000)
    & (df["first_15m_dollar_volume"] >= 5_000_000)
].copy()

df["period"] = np.select(
    [
        df["trade_date"] < pd.Timestamp("2023-01-01"),
        (df["trade_date"] >= pd.Timestamp("2023-01-01")) & (df["trade_date"] < pd.Timestamp("2025-01-01")),
        df["trade_date"] >= pd.Timestamp("2025-01-01"),
    ],
    ["train_2016_2022", "validation_2023_2024", "test_2025_2026"],
    default="other",
)

summary = (
    df.groupby(["gap_bucket", "side", "pattern", "target_pct", "stop_pct"], observed=True)
    .apply(summarize)
    .reset_index()
)

summary = summary[summary["trades"] >= 50].copy()

top = (
    summary.sort_values(
        ["side", "median_net", "avg_net", "stop_rate"],
        ascending=[True, False, False, True],
    )
    .groupby(["side", "gap_bucket", "pattern"], observed=True)
    .head(1)
    .sort_values(["side", "median_net", "avg_net"], ascending=[True, False, False])
)

keys = top[["gap_bucket", "side", "pattern", "target_pct", "stop_pct"]].copy()

best_trades = df.merge(
    keys,
    on=["gap_bucket", "side", "pattern", "target_pct", "stop_pct"],
    how="inner",
)

period = (
    best_trades.groupby(["gap_bucket", "side", "pattern", "target_pct", "stop_pct", "period"], observed=True)
    .apply(summarize)
    .reset_index()
    .sort_values(["side", "gap_bucket", "pattern", "period"])
)

summary.to_csv(OUT_SUMMARY, index=False)
top.to_csv(OUT_TOP, index=False)
period.to_csv(OUT_PERIOD, index=False)

print()
print("=== Liquid 3-candle pattern TOP ===")
print(top.to_string(index=False))

print()
print("=== Liquid 3-candle pattern PERIOD validation ===")
print(period.to_string(index=False))

print()
print("saved summary:", OUT_SUMMARY)
print("saved top:", OUT_TOP)
print("saved period:", OUT_PERIOD)
