from pathlib import Path
import argparse
import pandas as pd
import numpy as np

DEFAULT_INPUT_PATH = Path("data/reference/stocks/today_watchlist_fundamentals_latest.csv")
DEFAULT_OUTPUT_PATH = Path("data/reference/stocks/today_watchlist_fundamentals_risk_latest.csv")


def num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors="coerce")


parser = argparse.ArgumentParser()
parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
args = parser.parse_args()

INPUT_PATH = Path(args.input)
OUTPUT_PATH = Path(args.output)

df = pd.read_csv(INPUT_PATH)

float_shares = num(df, "float")
free_float_market_cap = num(df, "free_float_market_cap")
pre_market_volume_to_float = num(df, "pre_market_volume_to_float")
first_15m_volume_to_float = num(df, "first_15m_volume_to_float")
short_interest_pct_float = num(df, "short_interest_pct_float")
short_volume_ratio = num(df, "short_volume_ratio")
days_to_cover = num(df, "days_to_cover")

df["risk_low_float_under_10m"] = float_shares < 10_000_000
df["risk_tiny_float_under_3m"] = float_shares < 3_000_000
df["risk_micro_free_float_mcap_under_25m"] = free_float_market_cap < 25_000_000

df["risk_pre_market_volume_gt_50pct_float"] = pre_market_volume_to_float >= 0.50
df["risk_pre_market_volume_gt_float"] = pre_market_volume_to_float >= 1.00
df["risk_pre_market_volume_gt_2x_float"] = pre_market_volume_to_float >= 2.00

df["risk_first15_volume_gt_10pct_float"] = first_15m_volume_to_float >= 0.10
df["risk_first15_volume_gt_25pct_float"] = first_15m_volume_to_float >= 0.25

df["risk_short_interest_gt_10pct_float"] = short_interest_pct_float >= 10.0
df["risk_short_interest_gt_20pct_float"] = short_interest_pct_float >= 20.0

df["risk_short_volume_gt_50pct"] = short_volume_ratio >= 50.0
df["risk_days_to_cover_gt_3"] = days_to_cover >= 3.0

risk_cols = [c for c in df.columns if c.startswith("risk_")]

df["risk_flag_count"] = df[risk_cols].sum(axis=1)

def risk_label(row):
    labels = []

    if row.get("risk_tiny_float_under_3m", False):
        labels.append("TINY_FLOAT")
    elif row.get("risk_low_float_under_10m", False):
        labels.append("LOW_FLOAT")

    if row.get("risk_micro_free_float_mcap_under_25m", False):
        labels.append("MICRO_FREE_FLOAT_MCAP")

    if row.get("risk_pre_market_volume_gt_2x_float", False):
        labels.append("PRE_MARKET_VOLUME_2X_FLOAT")
    elif row.get("risk_pre_market_volume_gt_float", False):
        labels.append("PRE_MARKET_VOLUME_GT_FLOAT")
    elif row.get("risk_pre_market_volume_gt_50pct_float", False):
        labels.append("PRE_MARKET_VOLUME_GT_50PCT_FLOAT")

    if row.get("risk_first15_volume_gt_25pct_float", False):
        labels.append("FIRST15_VOLUME_GT_25PCT_FLOAT")
    elif row.get("risk_first15_volume_gt_10pct_float", False):
        labels.append("FIRST15_VOLUME_GT_10PCT_FLOAT")

    if row.get("risk_short_interest_gt_20pct_float", False):
        labels.append("SHORT_INTEREST_GT_20PCT_FLOAT")
    elif row.get("risk_short_interest_gt_10pct_float", False):
        labels.append("SHORT_INTEREST_GT_10PCT_FLOAT")

    if row.get("risk_short_volume_gt_50pct", False):
        labels.append("SHORT_VOLUME_GT_50PCT")

    if row.get("risk_days_to_cover_gt_3", False):
        labels.append("DAYS_TO_COVER_GT_3")

    if not labels:
        return "NORMAL"

    return "|".join(labels)

df["risk_labels"] = df.apply(risk_label, axis=1)

def risk_bucket(row):
    if row["risk_flag_count"] >= 5:
        return "EXTREME_CHAOS_RISK"
    if row["risk_flag_count"] >= 3:
        return "HIGH_CHAOS_RISK"
    if row["risk_flag_count"] >= 1:
        return "ELEVATED_RISK"
    return "NORMAL"

df["risk_bucket"] = df.apply(risk_bucket, axis=1)

df.to_csv(OUTPUT_PATH, index=False)

show_cols = [
    "ticker",
    "prev_close",
    "float",
    "free_float_market_cap",
    "pre_market_volume_to_float",
    "first_15m_volume_to_float",
    "short_interest_pct_float",
    "days_to_cover",
    "short_volume_ratio",
    "risk_flag_count",
    "risk_bucket",
    "risk_labels",
]

show_cols = [c for c in show_cols if c in df.columns]

print("saved:", OUTPUT_PATH)
print()
print(df.sort_values(["risk_flag_count", "pre_market_volume_to_float"], ascending=False)[show_cols].to_string(index=False))
