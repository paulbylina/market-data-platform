from __future__ import annotations

from pathlib import Path
import pandas as pd


CANDIDATES = [
    {
        "label": "LONG_quiet_pre_market_first15_strong__2t_3s",
        "path": Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_long_quiet_pre_market_first15_strong_expanded_target_stop_grid_trades.csv"),
        "setup": "LONG_quiet_pm_first15_strong",
        "side": "long",
        "target_pct": 2.0,
        "stop_pct": 3.0,
    },
    {
        "label": "LONG_pre_market_flush_reclaim_first15_hold__3t_3s",
        "path": Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_long_pre_market_flush_reclaim_first15_hold_target_stop_grid_trades.csv"),
        "setup": "LONG_pre_market_flush_reclaim_first15_hold",
        "side": "long",
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "label": "SHORT_pre_market_flush_reclaim_failed_first15__2_5t_2_5s",
        "path": Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_short_pre_market_flush_reclaim_failed_first15_target_stop_grid_trades.csv"),
        "setup": "SHORT_pre_market_flush_reclaim_failed_first15",
        "side": "short",
        "target_pct": 2.5,
        "stop_pct": 2.5,
    },
    {
        "label": "SHORT_hot_pre_market_big_fade_red_first15__2_5t_3s",
        "path": Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_short_hot_pre_market_big_fade_red_first15_expanded_target_stop_grid_trades.csv"),
        "setup": "SHORT_hot_pm_big_fade_red_first15",
        "side": "short",
        "target_pct": 2.5,
        "stop_pct": 3.0,
    },
]


DATE_CANDIDATES = [
    "trade_date",
    "date",
    "session_date",
    "regular_date",
    "entry_date",
]

NET_CANDIDATES = [
    "net_pct",
    "net_return_pct",
    "net_result_pct",
    "result_net_pct",
    "pnl_net_pct",
    "pnl_pct",
    "return_pct",
    "result_pct",
]


def find_col(df: pd.DataFrame, candidates: list[str], contains: str | None = None) -> str:
    for c in candidates:
        if c in df.columns:
            return c

    if contains:
        matches = [c for c in df.columns if contains.lower() in c.lower()]
        if matches:
            return matches[0]

    raise KeyError(
        "Could not find required column. Available columns:\n"
        + "\n".join(df.columns.astype(str))
    )


def summarize(sub: pd.DataFrame, label: str, group: str, date_col: str, net_col: str) -> dict:
    if len(sub) == 0:
        return {
            "label": label,
            "group": group,
            "start": None,
            "end": None,
            "trades": 0,
            "tickers": 0,
            "avg_net": None,
            "median_net": None,
            "win_rate": None,
            "target_rate": None,
            "stop_rate": None,
            "eod_rate": None,
        }

    exit_col = "exit_reason" if "exit_reason" in sub.columns else None

    target_rate = None
    stop_rate = None
    eod_rate = None

    if exit_col:
        exit_text = sub[exit_col].astype(str).str.lower()
        target_rate = exit_text.str.contains("target").mean() * 100
        stop_rate = exit_text.str.contains("stop").mean() * 100
        eod_rate = exit_text.str.contains("eod|close|end").mean() * 100

    return {
        "label": label,
        "group": group,
        "start": sub[date_col].min().date(),
        "end": sub[date_col].max().date(),
        "trades": len(sub),
        "tickers": sub["ticker"].nunique() if "ticker" in sub.columns else None,
        "avg_net": sub[net_col].mean(),
        "median_net": sub[net_col].median(),
        "win_rate": (sub[net_col] > 0).mean() * 100,
        "target_rate": target_rate,
        "stop_rate": stop_rate,
        "eod_rate": eod_rate,
    }


all_rows = []
year_rows = []

for c in CANDIDATES:
    print("\n" + "=" * 120)
    print(c["label"])
    print(c["path"])

    if not c["path"].exists():
        print("MISSING FILE")
        continue

    df = pd.read_csv(c["path"])

    date_col = find_col(df, DATE_CANDIDATES, contains="date")
    net_col = find_col(df, NET_CANDIDATES, contains="net")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[net_col] = pd.to_numeric(df[net_col], errors="coerce")

    for col in ["target_pct", "stop_pct"]:
        if col not in df.columns:
            raise KeyError(f"Missing {col} in {c['path']}")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    sub = df[
        (df["target_pct"].round(4) == c["target_pct"])
        & (df["stop_pct"].round(4) == c["stop_pct"])
    ].copy()

    if "setup" in sub.columns:
        sub = sub[sub["setup"] == c["setup"]].copy()

    if "side" in sub.columns:
        sub = sub[sub["side"] == c["side"]].copy()

    sub = sub.dropna(subset=[date_col, net_col]).copy()
    sub["year"] = sub[date_col].dt.year

    train = sub[sub["year"] <= 2023]
    test = sub[sub["year"] >= 2024]

    all_rows.append(summarize(sub, c["label"], "ALL", date_col, net_col))
    all_rows.append(summarize(train, c["label"], "TRAIN_2016_2023", date_col, net_col))
    all_rows.append(summarize(test, c["label"], "TEST_2024_2026", date_col, net_col))

    for year, ydf in sub.groupby("year"):
        row = summarize(ydf, c["label"], str(year), date_col, net_col)
        year_rows.append(row)

summary = pd.DataFrame(all_rows)
yearly = pd.DataFrame(year_rows)

out_dir = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")
summary_path = out_dir / "high_price_candidate_train_test_stability_summary.csv"
yearly_path = out_dir / "high_price_candidate_yearly_stability_summary.csv"

summary.to_csv(summary_path, index=False)
yearly.to_csv(yearly_path, index=False)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)

print("\n" + "=" * 120)
print("=== Train/Test Stability Summary ===")
print(summary.to_string(index=False))

print("\n" + "=" * 120)
print("=== Yearly Stability Summary ===")
print(yearly.to_string(index=False))

print()
print("saved:", summary_path)
print("saved:", yearly_path)
