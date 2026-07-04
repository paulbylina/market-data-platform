from pathlib import Path
import pandas as pd

TRADES_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_long_quiet_pre_market_first15_strong_expanded_target_stop_grid_trades.csv"
)

FEATURES_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_expanded_custom_setups_path_metrics.csv"
)

OUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_filtered_long_target_stop_summary.csv"
)

SETUP = "LONG_quiet_pm_first15_strong"

trades = pd.read_csv(TRADES_PATH)

if "net_pct" not in trades.columns:
    trades["net_pct"] = pd.to_numeric(trades["net_return_pct"], errors="coerce")

trades = trades[trades["setup"] == SETUP].copy()

features = pd.read_csv(FEATURES_PATH)

trades["trade_date"] = pd.to_datetime(trades["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
features["trade_date"] = pd.to_datetime(features["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

feature_cols = [
    "ticker",
    "trade_date",
    "setup",
    "prev_close",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_dollar_vs_prior_daily_avg",
    "first_15m_return_pct",
    "first15_range_pct",
]

df = trades.merge(
    features[feature_cols],
    on=["ticker", "trade_date", "setup"],
    how="left",
    validate="many_to_one",
)

for col in [
    "target_pct",
    "stop_pct",
    "net_pct",
    "prev_close",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_dollar_vs_prior_daily_avg",
    "first_15m_return_pct",
    "first15_range_pct",
]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

no_bad_gap = (
    (df["gap_pct"] >= 0)
    & (df["gap_pct"] < 10)
)

looser = (
    no_bad_gap
    & (df["first15_dollar_vs_prior_daily_avg"] >= 0.05)
    & (df["first15_dollar_vs_prior_daily_avg"] < 1.0)
    & (df["first_15m_return_pct"] >= 1)
    & (df["first_15m_return_pct"] < 8)
    & (df["first15_range_pct"] >= 2)
    & (df["first15_range_pct"] < 8)
)

moderate = (
    no_bad_gap
    & (df["first15_dollar_vs_prior_daily_avg"] >= 0.05)
    & (df["first15_dollar_vs_prior_daily_avg"] < 0.5)
    & (df["first_15m_return_pct"] >= 2)
    & (df["first_15m_return_pct"] < 8)
    & (df["first15_range_pct"] >= 2)
    & (df["first15_range_pct"] < 8)
)

filters = {
    "base_all": pd.Series(True, index=df.index),
    "B_no_bad_gap": no_bad_gap,
    "A_looser_quality": looser,
    "Aplus_moderate_quality": moderate,
}

rows = []

for label, mask in filters.items():
    sub = df[mask].copy()

    for (target, stop), g in sub.groupby(["target_pct", "stop_pct"]):
        exit_text = g["exit_reason"].astype(str).str.lower()

        rows.append({
            "quality_filter": label,
            "target_pct": target,
            "stop_pct": stop,
            "trades": len(g),
            "tickers": g["ticker"].nunique(),
            "avg_net": g["net_pct"].mean(),
            "median_net": g["net_pct"].median(),
            "win_rate": (g["net_pct"] > 0).mean() * 100,
            "target_rate": exit_text.str.contains("target").mean() * 100,
            "stop_rate": exit_text.str.contains("stop").mean() * 100,
            "eod_rate": exit_text.str.contains("eod").mean() * 100,
            "median_minutes_held": g["minutes_held"].median(),
            "best": g["net_pct"].max(),
            "worst": g["net_pct"].min(),
        })

out = pd.DataFrame(rows)
out = out.sort_values(
    ["quality_filter", "avg_net", "median_net"],
    ascending=[True, False, False],
)

out.to_csv(OUT_PATH, index=False)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

print("saved:", OUT_PATH)

for label in filters:
    print()
    print("=" * 100)
    print(label)
    print("=" * 100)

    show = out[out["quality_filter"] == label].copy()

    print()
    print("Top by avg net:")
    print(show.sort_values(["avg_net", "median_net"], ascending=False).head(15).to_string(index=False))

    print()
    print("Top with win rate >= 65%:")
    filt = show[show["win_rate"] >= 65].copy()
    if filt.empty:
        print("No combos.")
    else:
        print(filt.sort_values(["avg_net", "median_net"], ascending=False).head(15).to_string(index=False))
