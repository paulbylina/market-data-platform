from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")
INPUT = BASE / "gap_down_50_plus_setups_fullsample_trades.csv"

OUT_SUMMARY = BASE / "gap_down_50_plus_liquidity_filter_summary.csv"
OUT_PERIOD = BASE / "gap_down_50_plus_liquidity_filter_period_summary.csv"
OUT_WEEK = BASE / "gap_down_50_plus_liquidity_filter_this_week.csv"


def summarize(g: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(g["net_pct"], errors="coerce")

    return pd.Series(
        {
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
            "median_avg_dollar_vol_20d": g["avg_dollar_volume_20d_prior"].median(),
            "median_first15_dollar_vol": g["first_15m_dollar_volume"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


df = pd.read_csv(INPUT)
df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
df = df.dropna(subset=["trade_date"]).copy()

for c in ["avg_dollar_volume_20d_prior", "first_15m_dollar_volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df["period"] = np.select(
    [
        df["trade_date"] < pd.Timestamp("2023-01-01"),
        (df["trade_date"] >= pd.Timestamp("2023-01-01")) & (df["trade_date"] < pd.Timestamp("2025-01-01")),
        df["trade_date"] >= pd.Timestamp("2025-01-01"),
    ],
    ["train_2016_2022", "validation_2023_2024", "test_2025_2026"],
    default="other",
)

filters = {
    "no_liquidity_filter": pd.Series(True, index=df.index),

    # Should remove STRT but keep PATK and VSAT.
    "soft_liquid_20m_avg_5m_first15": (
        (df["avg_dollar_volume_20d_prior"] >= 20_000_000)
        & (df["first_15m_dollar_volume"] >= 5_000_000)
    ),

    # Still should keep PATK and VSAT based on your threshold table.
    "week_clean_25m_avg_7m_first15": (
        (df["avg_dollar_volume_20d_prior"] >= 25_000_000)
        & (df["first_15m_dollar_volume"] >= 7_000_000)
    ),

    # Stricter version. May remove too much.
    "strict_50m_avg_10m_first15": (
        (df["avg_dollar_volume_20d_prior"] >= 50_000_000)
        & (df["first_15m_dollar_volume"] >= 10_000_000)
    ),
}

rows = []
period_rows = []

for filter_name, mask in filters.items():
    x = df[mask].copy()

    # Main setups from this gap-down test.
    keep = x[
        (
            (x["setup_name"].isin(["GD_LONG_mild_reclaim", "GD_LONG_flush_reclaim"]))
            & (x["target_pct"] == 3.0)
            & (x["stop_pct"] == 4.0)
        )
        |
        (
            (x["setup_name"].isin(["GD_SHORT_continuation"]))
            & (x["target_pct"] == 4.0)
            & (x["stop_pct"] == 5.0)
        )
        |
        (
            (x["setup_name"].isin(["GD_SHORT_three_lower_5m"]))
            & (x["target_pct"] == 3.0)
            & (x["stop_pct"] == 4.0)
        )
    ].copy()

    if keep.empty:
        continue

    s = (
        keep.groupby(["side", "setup_name", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
    )
    s["filter_name"] = filter_name
    rows.append(s)

    p = (
        keep.groupby(["side", "setup_name", "target_pct", "stop_pct", "period"], observed=True)
        .apply(summarize)
        .reset_index()
    )
    p["filter_name"] = filter_name
    period_rows.append(p)


summary = pd.concat(rows, ignore_index=True).sort_values(
    ["filter_name", "side", "setup_name", "median_net"],
    ascending=[True, True, True, False],
)

period = pd.concat(period_rows, ignore_index=True).sort_values(
    ["filter_name", "side", "setup_name", "period"]
)

week = df[
    (df["trade_date"] >= pd.Timestamp("2026-06-29"))
    & (df["trade_date"] <= pd.Timestamp("2026-07-03"))
    & (df["target_pct"] == 3.0)
    & (df["stop_pct"] == 4.0)
    & (
        (df["avg_dollar_volume_20d_prior"] >= 20_000_000)
        & (df["first_15m_dollar_volume"] >= 5_000_000)
    )
].copy()

week_cols = [
    "trade_date",
    "ticker",
    "side",
    "setup_name",
    "gap_pct",
    "avg_dollar_volume_20d_prior",
    "first_15m_dollar_volume",
    "entry_px",
    "net_pct",
    "exit_type",
    "raw_eod_pct",
    "raw_runup_pct",
    "raw_drawdown_pct",
]
week_cols = [c for c in week_cols if c in week.columns]

summary.to_csv(OUT_SUMMARY, index=False)
period.to_csv(OUT_PERIOD, index=False)
week[week_cols].to_csv(OUT_WEEK, index=False)

print()
print("=== Liquidity filter summary ===")
print(summary.to_string(index=False))

print()
print("=== Liquidity filter period validation ===")
print(period.to_string(index=False))

print()
print("=== This week after soft liquidity filter ===")
if week.empty:
    print("No this-week candidates after liquidity filter.")
else:
    print(week[week_cols].drop_duplicates().sort_values(["trade_date", "ticker", "side", "setup_name"]).to_string(index=False))

print()
print("saved summary:", OUT_SUMMARY)
print("saved period:", OUT_PERIOD)
print("saved week:", OUT_WEEK)
