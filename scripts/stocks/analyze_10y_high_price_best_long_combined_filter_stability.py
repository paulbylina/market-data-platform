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

OUT_SUMMARY = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_best_long_combined_filter_train_test_summary.csv"
)

OUT_YEARLY = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_best_long_combined_filter_yearly_summary.csv"
)

SETUP = "LONG_quiet_pm_first15_strong"
TARGET = 2.0
STOP = 3.0


def summarize(label: str, period: str, df: pd.DataFrame) -> dict:
    exit_text = df["exit_reason"].astype(str).str.lower()

    return {
        "label": label,
        "period": period,
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "avg_net": df["net_pct"].mean(),
        "median_net": df["net_pct"].median(),
        "win_rate": (df["net_pct"] > 0).mean() * 100,
        "target_rate": exit_text.str.contains("target").mean() * 100,
        "stop_rate": exit_text.str.contains("stop").mean() * 100,
        "eod_rate": exit_text.str.contains("eod").mean() * 100,
        "best": df["net_pct"].max(),
        "worst": df["net_pct"].min(),
    }


trades = pd.read_csv(TRADES_PATH)

if "net_pct" not in trades.columns:
    trades["net_pct"] = pd.to_numeric(trades["net_return_pct"], errors="coerce")

for col in ["target_pct", "stop_pct"]:
    trades[col] = pd.to_numeric(trades[col], errors="coerce")

trades = trades[
    (trades["setup"] == SETUP)
    & (trades["target_pct"] == TARGET)
    & (trades["stop_pct"] == STOP)
].copy()

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

feature_cols = [c for c in feature_cols if c in features.columns]

df = trades.merge(
    features[feature_cols],
    on=["ticker", "trade_date", "setup"],
    how="left",
    validate="many_to_one",
)

df["trade_date_dt"] = pd.to_datetime(df["trade_date"], errors="coerce")
df["year"] = df["trade_date_dt"].dt.year

for col in [
    "prev_close",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_dollar_vs_prior_daily_avg",
    "first_15m_return_pct",
    "first15_range_pct",
]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

filters = {
    "baseline_all": pd.Series(True, index=df.index),

    "no_bad_gap": (
        (df["gap_pct"] >= 0)
        & (df["gap_pct"] < 10)
    ),

    "combined_looser_quality": (
        (df["gap_pct"] >= 0)
        & (df["gap_pct"] < 10)
        & (df["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (df["first15_dollar_vs_prior_daily_avg"] < 1.0)
        & (df["first_15m_return_pct"] >= 1)
        & (df["first_15m_return_pct"] < 8)
        & (df["first15_range_pct"] >= 2)
        & (df["first15_range_pct"] < 8)
    ),

    "combined_moderate_quality": (
        (df["gap_pct"] >= 0)
        & (df["gap_pct"] < 10)
        & (df["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (df["first15_dollar_vs_prior_daily_avg"] < 0.5)
        & (df["first_15m_return_pct"] >= 2)
        & (df["first_15m_return_pct"] < 8)
        & (df["first15_range_pct"] >= 2)
        & (df["first15_range_pct"] < 8)
    ),
}

summary_rows = []
yearly_rows = []

for label, mask in filters.items():
    sub = df[mask].copy()

    train = sub[sub["year"] <= 2023].copy()
    test = sub[sub["year"] >= 2024].copy()

    summary_rows.append(summarize(label, "ALL", sub))
    summary_rows.append(summarize(label, "TRAIN_2016_2023", train))
    summary_rows.append(summarize(label, "TEST_2024_2026", test))

    for year, year_sub in sub.groupby("year"):
        yearly_rows.append(summarize(label, str(int(year)), year_sub))

summary = pd.DataFrame(summary_rows)
yearly = pd.DataFrame(yearly_rows)

summary.to_csv(OUT_SUMMARY, index=False)
yearly.to_csv(OUT_YEARLY, index=False)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

print("saved summary:", OUT_SUMMARY)
print("saved yearly:", OUT_YEARLY)
print()
print("=== Train/Test Summary ===")
print(summary.to_string(index=False))
print()
print("=== Yearly Summary ===")
print(yearly.to_string(index=False))
