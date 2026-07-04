from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")

OUT_SUMMARY = BASE / "validated_50_plus_surviving_setups_train_test_summary.csv"
OUT_YEARLY = BASE / "validated_50_plus_surviving_setups_yearly_summary.csv"
OUT_TRADES = BASE / "validated_50_plus_surviving_setups_trades.csv"


def load(path: Path) -> pd.DataFrame:
    if not path.exists():
        print("missing:", path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    return df.dropna(subset=["trade_date"]).copy()


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
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


pieces = []


# SHORT-A
df = load(BASE / "hot_premarket_short_first5_structure_trades.csv")
if not df.empty:
    x = df[
        (df["base_setup"] == "first5_balanced_gap_0_10")
        & (df["entry_mode"] == "confirm_0945_three_lower_5m")
        & (df["target_pct"] == 4.0)
        & (df["stop_pct"] == 5.0)
    ].copy()

    x["setup_id"] = "SHORT-A"
    x["side"] = "SHORT"
    x["setup_desc"] = "first5 failed pre-market high three-lower"
    pieces.append(x)


# SHORT-B
df = load(BASE / "hot_premarket_short_5m_confirmation_trades.csv")
if not df.empty:
    x = df[
        (df["base_setup"] == "clean_gap_0_5")
        & (df["entry_mode"] == "immediate_0945")
        & (df["target_pct"] == 4.0)
        & (df["stop_pct"] == 5.0)
    ].copy()

    x["setup_id"] = "SHORT-B"
    x["side"] = "SHORT"
    x["setup_desc"] = "failed pre-market high immediate"
    pieces.append(x)


# LONG-A
df = load(BASE / "long_first5_three_up_structure_trades_2024_2026.csv")
if not df.empty:
    x = df[
        (df["setup_name"] == "first5_three_higher_5m")
        & (df["target_pct"] == 3.0)
        & (df["stop_pct"] == 4.0)
    ].copy()

    x["setup_id"] = "LONG-A"
    x["side"] = "LONG"
    x["setup_desc"] = "ABC three-higher 5m"
    pieces.append(x)


# LONG-B
df = load(BASE / "long_first5_pullback_reclaim_trades_2024_2026.csv")
if not df.empty:
    x = df[
        (df["pullback_setup"] == "shallow_pullback_reclaim")
        & (df["target_pct"] == 3.0)
        & (df["stop_pct"] == 4.0)
    ].copy()

    x["setup_id"] = "LONG-B"
    x["side"] = "LONG"
    x["setup_desc"] = "ABC first5 shallow pullback reclaim"
    pieces.append(x)


if not pieces:
    raise SystemExit("No setup trades found.")

trades = pd.concat(pieces, ignore_index=True, sort=False)

trades["year"] = trades["trade_date"].dt.year

trades["period"] = np.select(
    [
        trades["trade_date"] < pd.Timestamp("2023-01-01"),
        (trades["trade_date"] >= pd.Timestamp("2023-01-01"))
        & (trades["trade_date"] < pd.Timestamp("2025-01-01")),
        trades["trade_date"] >= pd.Timestamp("2025-01-01"),
    ],
    [
        "train_2016_2022",
        "validation_2023_2024",
        "test_2025_2026",
    ],
    default="other",
)

summary = (
    trades.groupby(["setup_id", "side", "setup_desc", "period"], observed=True)
    .apply(summarize)
    .reset_index()
    .sort_values(["setup_id", "period"])
)

yearly = (
    trades.groupby(["setup_id", "side", "setup_desc", "year"], observed=True)
    .apply(summarize)
    .reset_index()
    .sort_values(["setup_id", "year"])
)

trades.to_csv(OUT_TRADES, index=False)
summary.to_csv(OUT_SUMMARY, index=False)
yearly.to_csv(OUT_YEARLY, index=False)

print()
print("=== Train/Test Validation ===")
print(summary.to_string(index=False))

print()
print("=== Yearly Validation ===")
print(yearly.to_string(index=False))

print()
print("saved trades:", OUT_TRADES)
print("saved train/test summary:", OUT_SUMMARY)
print("saved yearly summary:", OUT_YEARLY)
