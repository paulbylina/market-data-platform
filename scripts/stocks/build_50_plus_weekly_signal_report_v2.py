from pathlib import Path
import pandas as pd


OUT_DIR = Path("data/research/full_market_scanner_10y/high_price_short_fade_expanded_features")

START = "2026-06-29"
END = "2026-07-03"

outputs = []


def load(path: Path) -> pd.DataFrame:
    if not path.exists():
        print("missing:", path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.astype(str)
    return df


def add_common(x: pd.DataFrame, grade: str, side: str, setup: str, target_stop: str) -> pd.DataFrame:
    x = x.copy()
    x["grade"] = grade
    x["side"] = side
    x["setup"] = setup
    x["target_stop"] = target_stop
    x["realized_net_pct"] = x["net_pct"]
    return x


# LONG-A / LONG-B from full-sample validated 2016-2026 file.
long_full = load(OUT_DIR / "long_first5_structures_fullsample_trades.csv")
if not long_full.empty:
    x = long_full[
        (long_full["trade_date"] >= START)
        & (long_full["trade_date"] <= END)
        & (long_full["base_variant"] == "abc_gap_base")
        & (long_full["setup_name"] == "LONG-A_three_higher_5m")
        & (long_full["target_pct"] == 3.0)
        & (long_full["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "A", "LONG", "LONG-A ABC three-higher 5m", "3 / 4"))

    x = long_full[
        (long_full["trade_date"] >= START)
        & (long_full["trade_date"] <= END)
        & (long_full["base_variant"] == "abc_gap_base")
        & (long_full["setup_name"] == "LONG-B_shallow_pullback_reclaim")
        & (long_full["target_pct"] == 3.0)
        & (long_full["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "A-", "LONG", "LONG-B first5 shallow pullback reclaim", "3 / 4"))


# Existing failed pre-market high short.
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
        outputs.append(add_common(x, "A", "SHORT", "SHORT-A failed pre-market high three-lower", "4 / 5"))


# Existing immediate failed pre-market high short, watch only.
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
        outputs.append(add_common(x, "WATCH", "SHORT", "SHORT-B failed pre-market high immediate", "4 / 5"))


# Gap-down 3-candle patterns with soft liquidity filter.
gd = load(OUT_DIR / "gap_down_50_plus_3candle_color_pattern_trades.csv")
if not gd.empty:
    for c in ["avg_dollar_volume_20d_prior", "first_15m_dollar_volume"]:
        gd[c] = pd.to_numeric(gd[c], errors="coerce")

    gd = gd[
        (gd["avg_dollar_volume_20d_prior"] >= 20_000_000)
        & (gd["first_15m_dollar_volume"] >= 5_000_000)
    ].copy()

    # GD-SHORT-A: deeper gap-down bounce-fail short.
    x = gd[
        (gd["trade_date"] >= START)
        & (gd["trade_date"] <= END)
        & (gd["gap_bucket"] == "gap_down_10_to_5")
        & (gd["side"] == "SHORT")
        & (gd["pattern"] == "RGR")
        & (gd["target_pct"] == 3.0)
        & (gd["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "B+", "SHORT", "GD-SHORT-A gap-down RGR bounce-fail", "3 / 4"))

    # GD-SHORT-B: moderate gap-down continuation short.
    x = gd[
        (gd["trade_date"] >= START)
        & (gd["trade_date"] <= END)
        & (gd["gap_bucket"] == "gap_down_5_to_2")
        & (gd["side"] == "SHORT")
        & (gd["pattern"] == "RRR")
        & (gd["target_pct"] == 3.0)
        & (gd["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "B", "SHORT", "GD-SHORT-B gap-down RRR continuation", "3 / 4"))

    # GD-LONG-WATCH: mild gap-down all-green reclaim, watch only.
    x = gd[
        (gd["trade_date"] >= START)
        & (gd["trade_date"] <= END)
        & (gd["gap_bucket"] == "gap_down_2_to_0")
        & (gd["side"] == "LONG")
        & (gd["pattern"] == "GGG")
        & (gd["target_pct"] == 3.0)
        & (gd["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "WATCH", "LONG", "GD-LONG-WATCH mild gap-down GGG reclaim", "3 / 4"))


# Gap-down flush reclaim long, watch only.
gd_named = load(OUT_DIR / "gap_down_50_plus_setups_fullsample_trades.csv")
if not gd_named.empty:
    for c in ["avg_dollar_volume_20d_prior", "first_15m_dollar_volume"]:
        gd_named[c] = pd.to_numeric(gd_named[c], errors="coerce")

    gd_named = gd_named[
        (gd_named["avg_dollar_volume_20d_prior"] >= 20_000_000)
        & (gd_named["first_15m_dollar_volume"] >= 5_000_000)
    ].copy()

    x = gd_named[
        (gd_named["trade_date"] >= START)
        & (gd_named["trade_date"] <= END)
        & (gd_named["side"] == "LONG")
        & (gd_named["setup_name"] == "GD_LONG_flush_reclaim")
        & (gd_named["target_pct"] == 3.0)
        & (gd_named["stop_pct"] == 4.0)
    ].copy()

    if not x.empty:
        outputs.append(add_common(x, "WATCH", "LONG", "GD-LONG-WATCH gap-down flush reclaim", "3 / 4"))

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
    "gap_bucket",
    "pattern",
    "entry_time",
    "entry_px",
    "target_stop",
    "realized_net_pct",
    "exit_type",
    "gap_pct",
    "avg_dollar_volume_20d_prior",
    "first_15m_dollar_volume",
    "premarket_dollar_vs_prior_daily_avg",
    "first15_ret",
    "first15_close_pos",
    "first15_range",
    "first5_ret",
    "second5_ret",
    "third5_ret",
    "raw_eod_pct",
    "raw_runup_pct",
    "raw_drawdown_pct",
    "long_eod_pct",
    "long_max_runup_pct",
    "long_max_drawdown_pct",
    "short_eod_pct",
    "short_max_runup_pct",
    "short_max_drawdown_pct",
]

cols = [c for c in cols if c in report.columns]

out = OUT_DIR / f"weekly_50_plus_signal_report_v2_{START}_to_{END}.csv"

if report.empty:
    print(f"No signals found from {START} to {END}.")
else:
    grade_order = {"A": 0, "A-": 1, "B+": 2, "B": 3, "WATCH": 4}
    report["_grade_order"] = report["grade"].map(grade_order).fillna(99)
    report = report.sort_values(["trade_date", "_grade_order", "side", "ticker"]).drop(columns=["_grade_order"])

    report[cols].to_csv(out, index=False)

    print()
    print("=== Weekly $50+ Signal Report V2 ===")
    print(report[cols].to_string(index=False))
    print()
    print("saved:", out)
