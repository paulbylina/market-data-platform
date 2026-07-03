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

OUTPUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_best_long_combined_filter_summary.csv"
)

SETUP = "LONG_quiet_pm_first15_strong"
TARGET = 2.0
STOP = 3.0

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

trades = trades.merge(
    features[feature_cols],
    on=["ticker", "trade_date", "setup"],
    how="left",
    validate="many_to_one",
)

for col in [
    "prev_close",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_dollar_vs_prior_daily_avg",
    "first_15m_return_pct",
    "first15_range_pct",
]:
    trades[col] = pd.to_numeric(trades[col], errors="coerce")

def summarize(label, df):
    exit_text = df["exit_reason"].astype(str).str.lower()
    return {
        "label": label,
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

filters = {
    "baseline_all": pd.Series(True, index=trades.index),

    "no_bad_gap": (
        (trades["gap_pct"] >= 0)
        & (trades["gap_pct"] < 10)
    ),

    "first15_activity_0_05_to_0_5": (
        (trades["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (trades["first15_dollar_vs_prior_daily_avg"] < 0.5)
    ),

    "first15_return_2_to_8": (
        (trades["first_15m_return_pct"] >= 2)
        & (trades["first_15m_return_pct"] < 8)
    ),

    "first15_range_2_to_8": (
        (trades["first15_range_pct"] >= 2)
        & (trades["first15_range_pct"] < 8)
    ),

    "combined_moderate_quality": (
        (trades["gap_pct"] >= 0)
        & (trades["gap_pct"] < 10)
        & (trades["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (trades["first15_dollar_vs_prior_daily_avg"] < 0.5)
        & (trades["first_15m_return_pct"] >= 2)
        & (trades["first_15m_return_pct"] < 8)
        & (trades["first15_range_pct"] >= 2)
        & (trades["first15_range_pct"] < 8)
    ),

    "combined_looser_quality": (
        (trades["gap_pct"] >= 0)
        & (trades["gap_pct"] < 10)
        & (trades["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (trades["first15_dollar_vs_prior_daily_avg"] < 1.0)
        & (trades["first_15m_return_pct"] >= 1)
        & (trades["first_15m_return_pct"] < 8)
        & (trades["first15_range_pct"] >= 2)
        & (trades["first15_range_pct"] < 8)
    ),
}

rows = []
for label, mask in filters.items():
    sub = trades[mask].copy()
    rows.append(summarize(label, sub))

out = pd.DataFrame(rows)
out.to_csv(OUTPUT_PATH, index=False)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)

print("saved:", OUTPUT_PATH)
print()
print(out.to_string(index=False))
