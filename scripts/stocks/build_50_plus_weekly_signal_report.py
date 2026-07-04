from pathlib import Path
import pandas as pd


OUT_DIR = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")

START = "2026-06-29"
END = "2026-07-03"

outputs = []


def load(path):
    if not path.exists():
        print("missing:", path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    return df


# SHORT-A: early failed pre-market high, three lower 5m candles.
short_a = load(OUT_DIR / "hot_premarket_short_first5_structure_trades.csv")
if not short_a.empty:
    x = short_a[
        (short_a["trade_date"] >= START)
        & (short_a["trade_date"] <= END)
        & (short_a["base_setup"] == "first5_balanced_gap_0_10")
        & (short_a["entry_mode"] == "confirm_0945_three_lower_5m")
        & (short_a["target_pct"] == 4.0)
        & (short_a["stop_pct"] == 5.0)
    ].copy()

    if not x.empty:
        x["grade"] = "A"
        x["side"] = "SHORT"
        x["setup"] = "SHORT-A failed pre-market high three-lower"
        x["target_stop"] = "4 / 5"
        x["realized_net_pct"] = x["net_pct"]
        outputs.append(x)


# SHORT-B: immediate failed pre-market high setup. Watchlist only.
short_b = load(OUT_DIR / "hot_premarket_short_5m_confirmation_trades.csv")
if not short_b.empty:
    x = short_b[
        (short_b["trade_date"] >= START)
        & (short_b["trade_date"] <= END)
        & (short_b["base_setup"] == "clean_gap_0_5")
        & (short_b["entry_mode"] == "immediate_0945")
        & (short_b["target_pct"] == 4.0)
        & (short_b["stop_pct"] == 5.0)
    ].copy()

    if not x.empty:
        x["grade"] = "WATCH"
        x["side"] = "SHORT"
        x["setup"] = "SHORT-B failed pre-market high immediate"
        x["target_stop"] = "4 / 5"
        x["realized_net_pct"] = x["net_pct"]
        outputs.append(x)


# LONG-A and LONG-B from full-sample 2016-2026 validation file.
long_full = load(OUT_DIR / "long_first5_structures_fullsample_trades.csv")
if not long_full.empty:
    # LONG-A: ABC long + three higher 5m candles.
    x = long_full[
        (long_full["trade_date"] >= START)
        & (long_full["trade_date"] <= END)
        & (long_full["base_variant"] == "abc_gap_base")
        & (long_full["setup_name"] == "LONG-A_three_higher_5m")
        & (long_full["target_pct"] == 3.0)
        & (long_full["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        x["grade"] = "A"
        x["side"] = "LONG"
        x["setup"] = "LONG-A ABC three-higher 5m"
        x["target_stop"] = "3 / 4"
        x["realized_net_pct"] = x["net_pct"]
        outputs.append(x)

    # LONG-B: ABC long + shallow pullback reclaim.
    x = long_full[
        (long_full["trade_date"] >= START)
        & (long_full["trade_date"] <= END)
        & (long_full["base_variant"] == "abc_gap_base")
        & (long_full["setup_name"] == "LONG-B_shallow_pullback_reclaim")
        & (long_full["target_pct"] == 3.0)
        & (long_full["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        x["grade"] = "A-"
        x["side"] = "LONG"
        x["setup"] = "LONG-B first5 shallow pullback reclaim"
        x["target_stop"] = "3 / 4"
        x["realized_net_pct"] = x["net_pct"]
        outputs.append(x)


if outputs:
    report = pd.concat(outputs, ignore_index=True, sort=False)
else:
    report = pd.DataFrame()

cols = [
    "trade_date",
    "ticker",
    "grade",
    "side",
    "setup",
    "entry_time",
    "entry_px",
    "target_stop",
    "realized_net_pct",
    "exit_type",
    "gap_pct",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_ret",
    "first15_close_pos",
    "first15_range",
    "first5_ret",
    "first5_close_pos",
    "first5_range",
    "second5_pullback_from_first5_close_pct",
    "third5_reclaim_vs_first5_close_pct",
    "long_eod_pct",
    "long_max_runup_pct",
    "long_max_drawdown_pct",
    "short_eod_pct",
    "short_max_runup_pct",
    "short_max_drawdown_pct",
]

cols = [c for c in cols if c in report.columns]

out = OUT_DIR / f"weekly_50_plus_signal_report_{START}_to_{END}.csv"

if report.empty:
    print(f"No A/A-/WATCH signals found from {START} to {END}.")
else:
    grade_order = {"A": 0, "A-": 1, "WATCH": 2}
    report["_grade_order"] = report["grade"].map(grade_order).fillna(99)
    report = report.sort_values(["trade_date", "_grade_order", "side", "ticker"]).drop(columns=["_grade_order"])

    report[cols].to_csv(out, index=False)

    print()
    print("=== Weekly $50+ Signal Report ===")
    print(report[cols].to_string(index=False))
    print()
    print("saved:", out)
