from pathlib import Path
import pandas as pd
import numpy as np

INPUT_PATH = Path("data/reference/stocks/today_all_confirmed_scanner_rows_fundamentals_risk_latest.csv")
OUTPUT_PATH = Path("data/reference/stocks/today_all_confirmed_scanner_rows_setup_risk_latest.csv")


def first_existing(df: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def num(df: pd.DataFrame, col: str | None) -> pd.Series:
    if col is None:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors="coerce")


df = pd.read_csv(INPUT_PATH)

pm_col = first_existing(df, [
    "premarket_dollar_rvol",
    "premarket_dollar_vs_prior_daily_avg",
])

f15_col = first_existing(df, [
    "first_15m_dollar_rvol",
    "first15_dollar_vs_prior_daily_avg",
])

f15_ret_col = first_existing(df, [
    "first_15m_return_pct",
])

open_vs_pm_high_col = first_existing(df, [
    "regular_open_vs_premarket_high_pct",
    "today_open_vs_premarket_high_pct",
])

prev_close = num(df, "prev_close")
pm = num(df, pm_col)
f15 = num(df, f15_col)
f15_ret = num(df, f15_ret_col)
open_vs_pm_high = num(df, open_vs_pm_high_col)

df["historical_setup_label"] = "CONTROL_other_high_daily"

high_price = prev_close >= 50

long_quiet_strong = (
    high_price
    & (pm <= 0.1)
    & (f15 >= 0.01)
    & (f15_ret >= 1)
)

long_quiet_green = (
    high_price
    & (pm <= 0.1)
    & (f15 >= 0.01)
    & (f15_ret > 0)
    & ~long_quiet_strong
)

df.loc[long_quiet_strong, "historical_setup_label"] = "LONG_quiet_pm_first15_strong"
df.loc[long_quiet_green, "historical_setup_label"] = "LONG_quiet_pm_first15_green"

if open_vs_pm_high_col is not None:
    short_super_mania = (
        high_price
        & (pm > 10)
        & (open_vs_pm_high <= -15)
    )

    short_mania_weak = (
        high_price
        & (pm > 1)
        & (open_vs_pm_high <= -5)
        & (f15_ret <= -1)
    )

    short_hot_red = (
        high_price
        & (pm > 0.1)
        & (open_vs_pm_high <= -5)
        & (f15_ret < 0)
    )

    df.loc[short_super_mania, "historical_setup_label"] = "SHORT_super_mania_pm_collapse"
    df.loc[short_mania_weak, "historical_setup_label"] = "SHORT_mania_pm_big_fade_weak_first15"
    df.loc[short_hot_red, "historical_setup_label"] = "SHORT_hot_pm_big_fade_red_first15"

df["validated_trade_label"] = "NO_VALIDATED_HIGH_PRICE_TRADE"
df["validated_side"] = ""
df["validated_target_pct"] = np.nan
df["validated_stop_pct"] = np.nan
df["validated_rank"] = np.nan
df["validated_notes"] = ""

df.loc[long_quiet_strong, "validated_trade_label"] = "LONG_quiet_pre_market_first15_strong__2t_3s"
df.loc[long_quiet_strong, "validated_side"] = "long"
df.loc[long_quiet_strong, "validated_target_pct"] = 2.0
df.loc[long_quiet_strong, "validated_stop_pct"] = 3.0
df.loc[long_quiet_strong, "validated_rank"] = 1
df.loc[long_quiet_strong, "validated_notes"] = "Best validated high-price setup; uses 2pct target and 3pct stop."

df["high_price_universe"] = high_price

df.to_csv(OUTPUT_PATH, index=False)

print("saved:", OUTPUT_PATH)
print("rows:", len(df))
print()
print("columns used:")
print("pm_col:", pm_col)
print("f15_col:", f15_col)
print("f15_ret_col:", f15_ret_col)
print("open_vs_pm_high_col:", open_vs_pm_high_col)
print()
print("historical setup counts:")
print(df["historical_setup_label"].value_counts(dropna=False).to_string())
print()
print("validated trade counts:")
print(df["validated_trade_label"].value_counts(dropna=False).to_string())
print()

show_cols = [
    "ticker",
    "prev_close",
    "gap_pct",
    "premarket_dollar_rvol",
    "first_15m_dollar_rvol",
    "first_15m_return_pct",
    "high_price_universe",
    "historical_setup_label",
    "validated_trade_label",
    "validated_side",
    "validated_target_pct",
    "validated_stop_pct",
    "risk_bucket",
    "risk_labels",
]

show_cols = [c for c in show_cols if c in df.columns]

print(df.sort_values(
    ["high_price_universe", "validated_rank", "prev_close"],
    ascending=[False, True, False],
    na_position="last",
)[show_cols].to_string(index=False))
