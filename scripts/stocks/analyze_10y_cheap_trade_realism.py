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


CONFIGS = [
    # Long configs.
    {
        "config": "LONG_no_target_hold_eod_stop_5",
        "side": "long",
        "setup": "LONG_quiet_pm_first15_strong",
        "stop_mode": "pct",
        "stop_pct": 5,
        "target_pct": None,
        "max_hold_min": None,
    },
    {
        "config": "LONG_target_10_stop_5_hold_eod",
        "side": "long",
        "setup": "LONG_quiet_pm_first15_strong",
        "stop_mode": "pct",
        "stop_pct": 5,
        "target_pct": 10,
        "max_hold_min": None,
    },
    {
        "config": "LONG_target_15_stop_8_hold_eod",
        "side": "long",
        "setup": "LONG_quiet_pm_first15_strong",
        "stop_mode": "pct",
        "stop_pct": 8,
        "target_pct": 15,
        "max_hold_min": None,
    },
    {
        "config": "LONG_no_target_hold_140m_stop_5",
        "side": "long",
        "setup": "LONG_quiet_pm_first15_strong",
        "stop_mode": "pct",
        "stop_pct": 5,
        "target_pct": None,
        "max_hold_min": 140,
    },
    {
        "config": "LONG_target_10_stop_first15_low_hold_eod",
        "side": "long",
        "setup": "LONG_quiet_pm_first15_strong",
        "stop_mode": "first15_low",
        "stop_pct": None,
        "target_pct": 10,
        "max_hold_min": None,
    },

    # Short configs.
    {
        "config": "SHORT_no_target_hold_eod_stop_15",
        "side": "short",
        "setup": "SHORT_super_mania_pm_collapse",
        "stop_mode": "pct",
        "stop_pct": 15,
        "target_pct": None,
        "max_hold_min": None,
    },
    {
        "config": "SHORT_target_10_stop_15_hold_eod",
        "side": "short",
        "setup": "SHORT_super_mania_pm_collapse",
        "stop_mode": "pct",
        "stop_pct": 15,
        "target_pct": 10,
        "max_hold_min": None,
    },
    {
        "config": "SHORT_target_15_stop_20_hold_eod",
        "side": "short",
        "setup": "SHORT_super_mania_pm_collapse",
        "stop_mode": "pct",
        "stop_pct": 20,
        "target_pct": 15,
        "max_hold_min": None,
    },
    {
        "config": "SHORT_no_target_hold_140m_stop_15",
        "side": "short",
        "setup": "SHORT_super_mania_pm_collapse",
        "stop_mode": "pct",
        "stop_pct": 15,
        "target_pct": None,
        "max_hold_min": 140,
    },
    {
        "config": "SHORT_target_10_stop_first15_high_hold_eod",
        "side": "short",
        "setup": "SHORT_super_mania_pm_collapse",
        "stop_mode": "first15_high",
        "stop_pct": None,
        "target_pct": 10,
        "max_hold_min": None,
    },
]


COST_BPS_LIST = [0, 50, 100]


def safe_short_return(entry, exit_px):
    if pd.isna(entry) or pd.isna(exit_px) or entry <= 0 or exit_px <= 0:
        return np.nan
    return (entry / exit_px - 1.0) * 100.0


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

    if close_col is None or high_col is None or low_col is None:
        return None

    if time_col in ["timestamp_ms", "t"] or pd.api.types.is_numeric_dtype(df[time_col]):
        ts = pd.to_datetime(df[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df["ts_et"] = ts.dt.tz_convert("America/New_York")
    df["close_px"] = pd.to_numeric(df[close_col], errors="coerce")
    df["high_px"] = pd.to_numeric(df[high_col], errors="coerce")
    df["low_px"] = pd.to_numeric(df[low_col], errors="coerce")

    df = df.dropna(subset=["ts_et", "close_px", "high_px", "low_px"])
    df = df[
        np.isfinite(df["close_px"])
        & np.isfinite(df["high_px"])
        & np.isfinite(df["low_px"])
    ].copy()

    df = df.sort_values("ts_et").copy()

    return df[["ts_et", "close_px", "high_px", "low_px"]]


def setup_label(row):
    pm = row["premarket_dollar_vs_prior_daily_avg"]
    f15 = row["first15_dollar_vs_prior_daily_avg"]
    f15_ret = row["first_15m_return_pct"]
    open_vs_pm_high = row["regular_open_vs_premarket_high_pct"]

    if pm <= 0.1 and f15 >= 0.01 and f15_ret >= 1:
        return "LONG_quiet_pm_first15_strong"

    if pm > 10 and open_vs_pm_high <= -15:
        return "SHORT_super_mania_pm_collapse"

    return "OTHER"


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

    return day_bars, "ok"


def compute_first15_levels(day_bars):
    first15 = day_bars[
        (day_bars["ts_et"].dt.time >= pd.to_datetime("09:30").time())
        & (day_bars["ts_et"].dt.time <= pd.to_datetime("09:44").time())
    ].copy()

    if first15.empty:
        return np.nan, np.nan

    return first15["high_px"].max(), first15["low_px"].min()


def simulate_trade(row, day_bars, config):
    side = config["side"]

    entry = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")

    regular = day_bars[
        (day_bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day_bars["ts_et"].dt.time <= pd.to_datetime("16:00").time())
    ].copy()

    if regular.empty:
        return {"sim_status": "no_post_first15_bars"}

    if pd.isna(entry) or entry <= 0 or not np.isfinite(entry):
        entry = regular.iloc[0]["close_px"]

    if pd.isna(entry) or entry <= 0 or not np.isfinite(entry):
        return {"sim_status": "bad_entry"}

    first15_high, first15_low = compute_first15_levels(day_bars)

    if config["stop_mode"] == "pct":
        if side == "long":
            stop_px = entry * (1.0 - config["stop_pct"] / 100.0)
        else:
            stop_px = entry * (1.0 + config["stop_pct"] / 100.0)
    elif config["stop_mode"] == "first15_low":
        stop_px = first15_low
    elif config["stop_mode"] == "first15_high":
        stop_px = first15_high
    else:
        stop_px = np.nan

    if pd.isna(stop_px) or stop_px <= 0 or not np.isfinite(stop_px):
        return {"sim_status": "bad_stop"}

    target_pct = config["target_pct"]
    if target_pct is None:
        target_px = np.nan
    else:
        if side == "long":
            target_px = entry * (1.0 + target_pct / 100.0)
        else:
            target_px = entry * (1.0 - target_pct / 100.0)

    max_hold_min = config["max_hold_min"]
    if max_hold_min is not None:
        start_ts = regular.iloc[0]["ts_et"]
        max_exit_ts = start_ts + pd.Timedelta(minutes=max_hold_min)
        regular = regular[regular["ts_et"] <= max_exit_ts].copy()

    if regular.empty:
        return {"sim_status": "no_bars_after_hold_filter"}

    exit_px = regular.iloc[-1]["close_px"]
    exit_reason = "time_eod" if max_hold_min is None else "time_max_hold"
    exit_ts = regular.iloc[-1]["ts_et"]

    for _, bar in regular.iterrows():
        high = bar["high_px"]
        low = bar["low_px"]

        if side == "long":
            stop_hit = low <= stop_px
            target_hit = pd.notna(target_px) and high >= target_px

            # Conservative if both hit in same minute: assume stop first.
            if stop_hit:
                exit_px = stop_px
                exit_reason = "stop"
                exit_ts = bar["ts_et"]
                break

            if target_hit:
                exit_px = target_px
                exit_reason = "target"
                exit_ts = bar["ts_et"]
                break

        else:
            stop_hit = high >= stop_px
            target_hit = pd.notna(target_px) and low <= target_px

            # Conservative if both hit in same minute: assume stop first.
            if stop_hit:
                exit_px = stop_px
                exit_reason = "stop"
                exit_ts = bar["ts_et"]
                break

            if target_hit:
                exit_px = target_px
                exit_reason = "target"
                exit_ts = bar["ts_et"]
                break

    if side == "long":
        gross_return_pct = (exit_px / entry - 1.0) * 100.0
    else:
        gross_return_pct = safe_short_return(entry, exit_px)

    minutes_held = (exit_ts - regular.iloc[0]["ts_et"]).total_seconds() / 60.0

    return {
        "sim_status": "ok",
        "entry_px": entry,
        "exit_px": exit_px,
        "stop_px": stop_px,
        "target_px": target_px,
        "exit_reason": exit_reason,
        "minutes_held": minutes_held,
        "gross_return_pct": gross_return_pct,
    }


def summarize(df):
    if df.empty:
        return {}

    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_gross_return_pct": df["gross_return_pct"].median(),
        "avg_gross_return_pct": df["gross_return_pct"].mean(),
        "gross_win_rate": (df["gross_return_pct"] > 0).mean() * 100,
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "median_minutes_held": df["minutes_held"].median(),
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].str.startswith("time")).mean() * 100,
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

    high_daily = df[df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES)].copy()
    high_daily["setup"] = high_daily.apply(setup_label, axis=1)
    high_daily = high_daily[high_daily["setup"] != "OTHER"].copy()

    print("events to simulate:", len(high_daily))
    print(high_daily["setup"].value_counts().to_string())

    rows = []

    grouped_configs = {}
    for cfg in CONFIGS:
        grouped_configs.setdefault(cfg["setup"], []).append(cfg)

    for i, row in high_daily.reset_index(drop=True).iterrows():
        day_bars, bar_status = get_day_bars(row)

        if bar_status != "ok":
            continue

        setup = row["setup"]

        for cfg in grouped_configs.get(setup, []):
            result = simulate_trade(row, day_bars, cfg)

            if result.get("sim_status") != "ok":
                continue

            for cost_bps in COST_BPS_LIST:
                out = {
                    "ticker": row["ticker"],
                    "trade_date": row["trade_date"],
                    "setup": setup,
                    "config": cfg["config"],
                    "side": cfg["side"],
                    "cost_bps": cost_bps,
                    "prev_close": row.get("prev_close", np.nan),
                    "premarket_dollar_vs_prior_daily_avg": row.get("premarket_dollar_vs_prior_daily_avg", np.nan),
                    "regular_open_vs_premarket_high_pct": row.get("regular_open_vs_premarket_high_pct", np.nan),
                    "first15_dollar_vs_prior_daily_avg": row.get("first15_dollar_vs_prior_daily_avg", np.nan),
                    "first_15m_return_pct": row.get("first_15m_return_pct", np.nan),
                    **result,
                }

                out["net_return_pct"] = out["gross_return_pct"] - (cost_bps / 100.0)
                rows.append(out)

        if (i + 1) % 500 == 0:
            print("processed:", i + 1)

    trades = pd.DataFrame(rows)

    trades_path = OUTPUT_DIR / "cheap_trade_realism_trades.csv"
    summary_path = OUTPUT_DIR / "cheap_trade_realism_summary.csv"

    trades.to_csv(trades_path, index=False)

    summary_rows = []
    for keys, sub in trades.groupby(["setup", "side", "config", "cost_bps"], observed=True):
        setup, side, config, cost_bps = keys
        item = {
            "setup": setup,
            "side": side,
            "config": config,
            "cost_bps": cost_bps,
        }
        item.update(summarize(sub))
        summary_rows.append(item)

    summary = pd.DataFrame(summary_rows).sort_values(
        ["side", "setup", "cost_bps", "median_net_return_pct"],
        ascending=[True, True, True, False],
    )

    summary.to_csv(summary_path, index=False)

    print()
    print("saved trades:", trades_path)
    print("saved summary:", summary_path)

    print()
    print("=== Cheap Trade Realism Summary ===")
    display_cols = [
        "setup",
        "side",
        "config",
        "cost_bps",
        "trades",
        "tickers",
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
