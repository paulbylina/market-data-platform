from pathlib import Path
import pandas as pd
import numpy as np

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
    "high_price_best_long_filter_summary.csv"
)

TARGET = 2.0
STOP = 3.0
SETUP = "LONG_quiet_pm_first15_strong"


def qbucket(s: pd.Series, labels: list[str]) -> pd.Series:
    try:
        return pd.qcut(s, q=len(labels), labels=labels, duplicates="drop")
    except Exception:
        return pd.Series("unknown", index=s.index)


def summarize(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows = []

    for val, sub in df.groupby(group_col, dropna=False):
        rows.append({
            "filter": group_col,
            "bucket": val,
            "trades": len(sub),
            "tickers": sub["ticker"].nunique() if "ticker" in sub.columns else np.nan,
            "avg_net": sub["net_pct"].mean(),
            "median_net": sub["net_pct"].median(),
            "win_rate": (sub["net_pct"] > 0).mean() * 100,
            "target_rate": sub["exit_reason"].astype(str).str.contains("target").mean() * 100,
            "stop_rate": sub["exit_reason"].astype(str).str.contains("stop").mean() * 100,
            "eod_rate": sub["exit_reason"].astype(str).str.contains("eod").mean() * 100,
            "best": sub["net_pct"].max(),
            "worst": sub["net_pct"].min(),
        })

    return pd.DataFrame(rows)


df = pd.read_csv(TRADES_PATH)

# Normalize return column names across different target/stop scripts.
if "net_pct" not in df.columns:
    if "net_return_pct" in df.columns:
        df["net_pct"] = pd.to_numeric(df["net_return_pct"], errors="coerce")
    else:
        raise KeyError(f"No net return column found. Columns: {list(df.columns)}")

if "gross_pct" not in df.columns and "gross_return_pct" in df.columns:
    df["gross_pct"] = pd.to_numeric(df["gross_return_pct"], errors="coerce")

# Keep only the validated combo.
for col in ["target_pct", "stop_pct"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df = df[
    (df["setup"] == SETUP)
    & (df["target_pct"] == TARGET)
    & (df["stop_pct"] == STOP)
].copy()

if df.empty:
    raise SystemExit("No rows found for selected setup/target/stop. Check TRADES_PATH or setup name.")

# Join feature columns back onto the selected trade rows.
features = pd.read_csv(FEATURES_PATH)

features["trade_date"] = pd.to_datetime(features["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

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
    "first15_close_position_in_range",
    "regular_open_vs_premarket_high_pct",
    "regular_open_vs_premarket_close_pct",
]

feature_cols = [c for c in feature_cols if c in features.columns]

df = df.merge(
    features[feature_cols],
    on=["ticker", "trade_date", "setup"],
    how="left",
    validate="many_to_one",
)

missing_features = df["prev_close"].isna().sum() if "prev_close" in df.columns else len(df)
print("missing joined feature rows:", missing_features)

df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
df["year"] = df["trade_date"].dt.year

numeric_cols = [
    "prev_close",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_dollar_vs_prior_daily_avg",
    "first_15m_return_pct",
    "first15_range_pct",
    "first15_close_position_in_range",
    "regular_open_vs_premarket_high_pct",
    "regular_open_vs_premarket_close_pct",
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Human-readable buckets.
if "prev_close" in df.columns:
    df["prev_close_bucket"] = pd.cut(
        df["prev_close"],
        bins=[50, 75, 100, 150, 250, 500, float("inf")],
        labels=["50-75", "75-100", "100-150", "150-250", "250-500", "500+"],
        include_lowest=True,
    )

if "gap_pct" in df.columns:
    df["gap_bucket"] = pd.cut(
        df["gap_pct"],
        bins=[-float("inf"), -2, 0, 1, 2, 5, 10, float("inf")],
        labels=["gap_down_gt2", "gap_down_0_2", "flat_0_1", "gap_1_2", "gap_2_5", "gap_5_10", "gap_10_plus"],
    )

if "premarket_dollar_vs_prior_daily_avg" in df.columns:
    df["pre_market_activity_bucket"] = pd.cut(
        df["premarket_dollar_vs_prior_daily_avg"],
        bins=[-float("inf"), 0, 0.001, 0.01, 0.05, 0.1, float("inf")],
        labels=["none", "tiny_0_0.001", "low_0.001_0.01", "modest_0.01_0.05", "near_limit_0.05_0.1", "above_rule"],
    )

if "first15_dollar_vs_prior_daily_avg" in df.columns:
    df["first15_activity_bucket"] = pd.cut(
        df["first15_dollar_vs_prior_daily_avg"],
        bins=[-float("inf"), 0.01, 0.05, 0.1, 0.25, 0.5, 1, float("inf")],
        labels=["rule_min_0.01", "0.01_0.05", "0.05_0.1", "0.1_0.25", "0.25_0.5", "0.5_1", "1_plus"],
    )

if "first_15m_return_pct" in df.columns:
    df["first15_return_bucket"] = pd.cut(
        df["first_15m_return_pct"],
        bins=[1, 2, 3, 5, 8, 12, float("inf")],
        labels=["1_2", "2_3", "3_5", "5_8", "8_12", "12_plus"],
        include_lowest=True,
    )

if "first15_range_pct" in df.columns:
    df["first15_range_bucket"] = pd.cut(
        df["first15_range_pct"],
        bins=[-float("inf"), 1, 2, 3, 5, 8, 12, float("inf")],
        labels=["lt1", "1_2", "2_3", "3_5", "5_8", "8_12", "12_plus"],
    )

bucket_cols = [
    "year",
    "prev_close_bucket",
    "gap_bucket",
    "pre_market_activity_bucket",
    "first15_activity_bucket",
    "first15_return_bucket",
    "first15_range_bucket",
]

bucket_cols = [c for c in bucket_cols if c in df.columns]

summaries = []
for col in bucket_cols:
    summaries.append(summarize(df, col))

out = pd.concat(summaries, ignore_index=True)
out = out.sort_values(["filter", "median_net", "avg_net"], ascending=[True, False, False])

out.to_csv(OUTPUT_PATH, index=False)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

print("input trades:", len(df))
print("saved:", OUTPUT_PATH)
print()
print(out.to_string(index=False))
