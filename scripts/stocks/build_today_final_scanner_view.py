from pathlib import Path
import pandas as pd

INPUT_PATH = Path("data/reference/stocks/today_all_confirmed_scanner_rows_setup_risk_latest.csv")
OUTPUT_PATH = Path("data/reference/stocks/today_final_scanner_view_latest.csv")

df = pd.read_csv(INPUT_PATH)

# Cleaner risk score: count visible labels instead of nested boolean flags.
def clean_risk_count(x):
    if pd.isna(x) or str(x).strip() == "" or str(x).strip().upper() == "NORMAL":
        return 0
    return len(str(x).split("|"))

df["clean_risk_label_count"] = df["risk_labels"].apply(clean_risk_count)

def action_bucket(row):
    trade = str(row.get("validated_trade_label", ""))
    risk = str(row.get("risk_bucket", ""))

    if trade != "NO_VALIDATED_HIGH_PRICE_TRADE":
        if risk in {"EXTREME_CHAOS_RISK", "HIGH_CHAOS_RISK"}:
            return "VALIDATED_SETUP_BUT_HIGH_RISK"
        return "VALIDATED_SETUP"

    if risk in {"EXTREME_CHAOS_RISK", "HIGH_CHAOS_RISK"}:
        return "WATCH_ONLY_CHAOS_RISK"

    return "NO_VALIDATED_TRADE"

df["scanner_action_bucket"] = df.apply(action_bucket, axis=1)

keep_cols = [
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
    "clean_risk_label_count",
    "risk_labels",
    "scanner_action_bucket",
    "float",
    "free_float_market_cap",
    "pre_market_volume_to_float",
    "first_15m_volume_to_float",
    "short_interest_pct_float",
    "days_to_cover",
    "short_volume_ratio",
]

keep_cols = [c for c in keep_cols if c in df.columns]

sort_cols = [
    "scanner_action_bucket",
    "high_price_universe",
    "clean_risk_label_count",
    "gap_pct",
]

sort_cols = [c for c in sort_cols if c in df.columns]

out = df[keep_cols].copy()
out = out.sort_values(
    sort_cols,
    ascending=[True, False, False, False][:len(sort_cols)],
    na_position="last",
)

out.to_csv(OUTPUT_PATH, index=False)

print("saved:", OUTPUT_PATH)
print("rows:", len(out))
print()
print("action bucket counts:")
print(out["scanner_action_bucket"].value_counts(dropna=False).to_string())
print()
print(out.to_string(index=False))
