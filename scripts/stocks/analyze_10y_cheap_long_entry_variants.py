from pathlib import Path

import numpy as np
import pandas as pd


FEATURES_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/extended_hours_features_pilot.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


HIGH_DAILY_REGIMES = [
    "extreme_p95_p99",
    "mania_p99_p99_9",
    "super_mania_p99_9_p100",
]


ENTRY_VARIANTS = [
    {"entry_rule": "entry_0945_first15_close", "type": "fixed_time", "time": "09:45", "vwap_filter": False},
    {"entry_rule": "entry_1000", "type": "fixed_time", "time": "10:00", "vwap_filter": False},
    {"entry_rule": "entry_1015", "type": "fixed_time", "time": "10:15", "vwap_filter": False},
    {"entry_rule": "entry_1030", "type": "fixed_time", "time": "10:30", "vwap_filter": False},

    {"entry_rule": "entry_0945_above_vwap", "type": "fixed_time", "time": "09:45", "vwap_filter": True},
    {"entry_rule": "entry_1015_above_vwap", "type": "fixed_time", "time": "10:15", "vwap_filter": True},

    {"entry_rule": "entry_vwap_pullback_hold_before_1100", "type": "vwap_pullback_hold", "latest_time": "11:00"},
    {"entry_rule": "entry_vwap_reclaim_before_1100", "type": "vwap_reclaim", "latest_time": "11:00"},
]


COST_BPS_LIST = [0, 50, 100]


TARGET_PCT = 15.0
STOP_PCT = 8.0


def find_cache_file(ticker, trade_date, prev_trade_date=None):
    ticker = str(ticker)

    candidates = []

    if prev_trade_date is not None and not pd.isna(prev_trade_date):
        candidates.append(CACHE_DIR / f"{ticker}_{prev_trade_date}_to_{trade_date}_1m.csv")

    candidates.extend(CACHE_DIR.glob(f"{ticker}_*_to_{trade_date}_1m.csv"))
    candidates.extend(CACHE_DIR.glob(f"{ticker}*{trade_date}*.csv"))

    for p in candidates:
        if p.exists():
            return p

    return None


def detect_time_col(df):
    for col in [
        "timestamp_ms",
        "timestamp",
        "bar_start_utc",
        "bar_start_et",
        "datetime",
        "window_start",
        "t",
    ]:
        if col in df.columns:
            return col
    return None


def normalize_bars(raw):
    df = raw.copy()

    time_col = detect_time_col(df)
    if time_col is None:
        return None

    close_col = "close" if "close" in df.columns else "c" if "c" in df.columns else None
    high_col = "high" if "high" in df.columns else "h" if "h" in df.columns else None
    low_col = "low" if "low" in df.columns else "l" if "l" in df.columns else None
    volume_col = "volume" if "volume" in df.columns else "v" if "v" in df.columns else None

    if close_col is None or high_col is None or low_col is None or volume_col is None:
        return None

    if time_col in ["timestamp_ms", "t"] or pd.api.types.is_numeric_dtype(df[time_col]):
        ts = pd.to_datetime(df[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df["ts_et"] = ts.dt.tz_convert("America/New_York")
    df["close_px"] = pd.to_numeric(df[close_col], errors="coerce")
    df["high_px"] = pd.to_numeric(df[high_col], errors="coerce")
    df["low_px"] = pd.to_numeric(df[low_col], errors="coerce")
    df["volume"] = pd.to_numeric(df[volume_col], errors="coerce")

    df = df.dropna(subset=["ts_et", "close_px", "high_px", "low_px", "volume"])
    df = df[
        np.isfinite(df["close_px"])
        & np.isfinite(df["high_px"])
        & np.isfinite(df["low_px"])
        & np.isfinite(df["volume"])
        & (df["close_px"] > 0)
        & (df["high_px"] > 0)
        & (df["low_px"] > 0)
        & (df["volume"] >= 0)
    ].copy()

    df = df.sort_values("ts_et").copy()

    return df[["ts_et", "close_px", "high_px", "low_px", "volume"]]


def add_running_vwap(day_bars):
    df = day_bars.copy()

    regular_start = pd.to_datetime("09:30").time()
    regular_end = pd.to_datetime("16:00").time()

    df = df[
        (df["ts_et"].dt.time >= regular_start)
        & (df["ts_et"].dt.time <= regular_end)
    ].copy()

    if df.empty:
        return df

    typical = (df["high_px"] + df["low_px"] + df["close_px"]) / 3.0
    df["pv"] = typical * df["volume"]
    df["cum_pv"] = df["pv"].cumsum()
    df["cum_volume"] = df["volume"].cumsum()
    df["running_vwap"] = np.where(
        df["cum_volume"] > 0,
        df["cum_pv"] / df["cum_volume"],
        np.nan,
    )

    return df


def get_day_bars(row):
    p = find_cache_file(
        row["ticker"],
        str(row["trade_date"]),
        row.get("prev_trade_date", None),
    )

    if p is None:
        return None, "missing_cache"

    try:
        raw = pd.read_csv(p)
    except Exception:
        return None, "read_error"

    bars = normalize_bars(raw)

    if bars is None or bars.empty:
        return None, "bad_bars"

    trade_day = pd.to_datetime(str(row["trade_date"])).date()
    day_bars = bars[bars["ts_et"].dt.date == trade_day].copy()

    if day_bars.empty:
        return None, "no_trade_day_bars"

    day_bars = add_running_vwap(day_bars)

    if day_bars.empty:
        return None, "no_regular_bars"

    return day_bars, "ok"


def first_bar_at_or_after(day_bars, hhmm):
    t = pd.to_datetime(hhmm).time()
    sub = day_bars[day_bars["ts_et"].dt.time >= t].copy()
    if sub.empty:
        return None
    return sub.iloc[0]


def find_entry(day_bars, variant):
    rule_type = variant["type"]

    if rule_type == "fixed_time":
        bar = first_bar_at_or_after(day_bars, variant["time"])
        if bar is None:
            return None, "no_entry_bar"

        if variant.get("vwap_filter", False):
            if pd.isna(bar["running_vwap"]) or bar["close_px"] < bar["running_vwap"]:
                return None, "failed_vwap_filter"

        return {
            "entry_ts": bar["ts_et"],
            "entry_px": bar["close_px"],
            "entry_vwap": bar["running_vwap"],
        }, "ok"

    if rule_type == "vwap_pullback_hold":
        latest = pd.to_datetime(variant["latest_time"]).time()

        sub = day_bars[
            (day_bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
            & (day_bars["ts_et"].dt.time <= latest)
        ].copy()

        if sub.empty:
            return None, "no_entry_window"

        # Pullback/hold: price trades near VWAP but closes above it.
        # 1% band avoids requiring an exact VWAP touch in sparse 1m data.
        hit = sub[
            (sub["running_vwap"].notna())
            & (sub["low_px"] <= sub["running_vwap"] * 1.01)
            & (sub["close_px"] >= sub["running_vwap"])
        ].copy()

        if hit.empty:
            return None, "no_vwap_pullback_hold"

        bar = hit.iloc[0]

        return {
            "entry_ts": bar["ts_et"],
            "entry_px": bar["close_px"],
            "entry_vwap": bar["running_vwap"],
        }, "ok"

    if rule_type == "vwap_reclaim":
        latest = pd.to_datetime(variant["latest_time"]).time()

        sub = day_bars[
            (day_bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
            & (day_bars["ts_et"].dt.time <= latest)
        ].copy()

        if len(sub) < 2:
            return None, "no_entry_window"

        sub["prev_close"] = sub["close_px"].shift(1)
        sub["prev_vwap"] = sub["running_vwap"].shift(1)

        hit = sub[
            (sub["running_vwap"].notna())
            & (sub["prev_vwap"].notna())
            & (sub["prev_close"] < sub["prev_vwap"])
            & (sub["close_px"] >= sub["running_vwap"])
        ].copy()

        if hit.empty:
            return None, "no_vwap_reclaim"

        bar = hit.iloc[0]

        return {
            "entry_ts": bar["ts_et"],
            "entry_px": bar["close_px"],
            "entry_vwap": bar["running_vwap"],
        }, "ok"

    return None, "unknown_entry_rule"


def simulate_long(day_bars, entry):
    entry_ts = entry["entry_ts"]
    entry_px = entry["entry_px"]

    if pd.isna(entry_px) or entry_px <= 0 or not np.isfinite(entry_px):
        return {"sim_status": "bad_entry"}

    trade_bars = day_bars[
        (day_bars["ts_et"] >= entry_ts)
        & (day_bars["ts_et"].dt.time <= pd.to_datetime("16:00").time())
    ].copy()

    if trade_bars.empty:
        return {"sim_status": "no_trade_bars"}

    target_px = entry_px * (1.0 + TARGET_PCT / 100.0)
    stop_px = entry_px * (1.0 - STOP_PCT / 100.0)

    exit_px = trade_bars.iloc[-1]["close_px"]
    exit_ts = trade_bars.iloc[-1]["ts_et"]
    exit_reason = "time_eod"

    for _, bar in trade_bars.iterrows():
        stop_hit = bar["low_px"] <= stop_px
        target_hit = bar["high_px"] >= target_px

        # Conservative if both hit in same minute: stop first.
        if stop_hit:
            exit_px = stop_px
            exit_ts = bar["ts_et"]
            exit_reason = "stop"
            break

        if target_hit:
            exit_px = target_px
            exit_ts = bar["ts_et"]
            exit_reason = "target"
            break

    gross_return_pct = (exit_px / entry_px - 1.0) * 100.0
    minutes_held = (exit_ts - entry_ts).total_seconds() / 60.0

    return {
        "sim_status": "ok",
        "entry_ts": entry_ts,
        "entry_px": entry_px,
        "entry_vwap": entry.get("entry_vwap", np.nan),
        "exit_ts": exit_ts,
        "exit_px": exit_px,
        "target_px": target_px,
        "stop_px": stop_px,
        "exit_reason": exit_reason,
        "minutes_held": minutes_held,
        "gross_return_pct": gross_return_pct,
    }


def summarize(df):
    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_prev_close": df["prev_close"].median(),
        "median_entry_time": df["entry_hhmm"].median(),
        "median_gross_return_pct": df["gross_return_pct"].median(),
        "avg_gross_return_pct": df["gross_return_pct"].mean(),
        "gross_win_rate": (df["gross_return_pct"] > 0).mean() * 100,
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "median_minutes_held": df["minutes_held"].median(),
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].astype(str).str.startswith("time")).mean() * 100,
        "worst_net_return_pct": df["net_return_pct"].min(),
        "best_net_return_pct": df["net_return_pct"].max(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FEATURES_PATH)

    numeric_cols = [
        "prev_close",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_vs_prior_daily_avg",
        "regular_open_vs_premarket_high_pct",
        "first_15m_dollar_volume",
        "first_15m_close",
        "first_15m_return_pct",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["download_status"] == "ok"].copy()

    df["first15_dollar_vs_prior_daily_avg"] = np.where(
        df["avg_dollar_volume_20d_prior"] > 0,
        df["first_15m_dollar_volume"] / df["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    base = df[
        (df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES))
        & (df["prev_close"] >= 1.0)
        & (df["prev_close"] < 5.0)
        & (df["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
        & (df["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        & (df["first_15m_return_pct"] >= 1.0)
    ].copy()

    print("base events:", len(base))
    print("tickers:", base["ticker"].nunique())

    rows = []
    entry_status_rows = []

    for i, row in base.reset_index(drop=True).iterrows():
        day_bars, bar_status = get_day_bars(row)

        if bar_status != "ok":
            for variant in ENTRY_VARIANTS:
                entry_status_rows.append({
                    "entry_rule": variant["entry_rule"],
                    "status": bar_status,
                })
            continue

        for variant in ENTRY_VARIANTS:
            entry, entry_status = find_entry(day_bars, variant)

            entry_status_rows.append({
                "entry_rule": variant["entry_rule"],
                "status": entry_status,
            })

            if entry_status != "ok":
                continue

            sim = simulate_long(day_bars, entry)

            if sim.get("sim_status") != "ok":
                continue

            for cost_bps in COST_BPS_LIST:
                out = {
                    "ticker": row["ticker"],
                    "trade_date": row["trade_date"],
                    "prev_close": row["prev_close"],
                    "dollar_volume_regime": row["dollar_volume_regime"],
                    "premarket_dollar_vs_prior_daily_avg": row["premarket_dollar_vs_prior_daily_avg"],
                    "first15_dollar_vs_prior_daily_avg": row["first15_dollar_vs_prior_daily_avg"],
                    "first_15m_return_pct": row["first_15m_return_pct"],
                    "entry_rule": variant["entry_rule"],
                    "cost_bps": cost_bps,
                    **sim,
                }

                out["entry_hhmm"] = int(out["entry_ts"].strftime("%H%M"))
                out["net_return_pct"] = out["gross_return_pct"] - (cost_bps / 100.0)

                rows.append(out)

        if (i + 1) % 250 == 0:
            print("processed:", i + 1)

    trades = pd.DataFrame(rows)
    status = pd.DataFrame(entry_status_rows)

    trades_path = OUTPUT_DIR / "cheap_long_entry_variants_trades.csv"
    summary_path = OUTPUT_DIR / "cheap_long_entry_variants_summary.csv"
    status_path = OUTPUT_DIR / "cheap_long_entry_variants_entry_status.csv"

    trades.to_csv(trades_path, index=False)
    status.to_csv(status_path, index=False)

    summary_rows = []

    for keys, sub in trades.groupby(["entry_rule", "cost_bps"], observed=True):
        entry_rule, cost_bps = keys
        item = {
            "entry_rule": entry_rule,
            "cost_bps": cost_bps,
        }
        item.update(summarize(sub))
        summary_rows.append(item)

    summary = pd.DataFrame(summary_rows).sort_values(
        ["cost_bps", "median_net_return_pct"],
        ascending=[True, False],
    )

    summary.to_csv(summary_path, index=False)

    print()
    print("saved trades:", trades_path)
    print("saved summary:", summary_path)
    print("saved entry status:", status_path)

    print()
    print("=== Entry Status ===")
    print(status.groupby(["entry_rule", "status"], observed=True).size().reset_index(name="rows").to_string(index=False))

    print()
    print("=== Cheap Long Entry Variants Summary ===")
    display_cols = [
        "entry_rule",
        "cost_bps",
        "trades",
        "tickers",
        "median_prev_close",
        "median_entry_time",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "median_minutes_held",
        "target_rate",
        "stop_rate",
        "time_exit_rate",
        "worst_net_return_pct",
        "best_net_return_pct",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
