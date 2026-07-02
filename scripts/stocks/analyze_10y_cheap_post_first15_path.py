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


def safe_short_return(long_return_pct):
    if pd.isna(long_return_pct):
        return np.nan
    r = long_return_pct / 100.0
    if r <= -0.999:
        return np.nan
    return (1.0 / (1.0 + r) - 1.0) * 100.0


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


def detect_close_col(df):
    for col in ["close", "c"]:
        if col in df.columns:
            return col
    return None


def normalize_bars(raw):
    df = raw.copy()

    time_col = detect_time_col(df)
    close_col = detect_close_col(df)

    if time_col is None or close_col is None:
        return None

    # Massive/Polygon often uses millisecond timestamps.
    if time_col in ["t", "timestamp_ms"] or pd.api.types.is_numeric_dtype(df[time_col]):
        ts = pd.to_datetime(df[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df["ts_et"] = ts.dt.tz_convert("America/New_York")
    df["close_px"] = pd.to_numeric(df[close_col], errors="coerce")

    df = df.dropna(subset=["ts_et", "close_px"])
    df = df[np.isfinite(df["close_px"])]
    df = df.sort_values("ts_et").copy()

    return df[["ts_et", "close_px"]]


def get_close_at_or_after(day_bars, hhmm):
    target_time = pd.to_datetime(hhmm).time()
    sub = day_bars[day_bars["ts_et"].dt.time >= target_time]
    if sub.empty:
        return np.nan
    return sub.iloc[0]["close_px"]


def compute_path_metrics(row):
    ticker = row["ticker"]
    trade_date = str(row["trade_date"])
    prev_trade_date = row.get("prev_trade_date", None)

    p = find_cache_file(ticker, trade_date, prev_trade_date)

    if p is None:
        return {
            "path_status": "missing_cache",
            "cache_file": None,
        }

    try:
        raw = pd.read_csv(p)
    except Exception:
        return {
            "path_status": "read_error",
            "cache_file": str(p),
        }

    bars = normalize_bars(raw)

    if bars is None or bars.empty:
        return {
            "path_status": "bad_bars",
            "cache_file": str(p),
        }

    trade_day = pd.to_datetime(trade_date).date()

    day_bars = bars[bars["ts_et"].dt.date == trade_day].copy()

    # After the first 15m bar is complete.
    # If bars are minute-close bars, 09:45 is the first bar after the 09:30-09:44 window.
    regular = day_bars[
        (day_bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day_bars["ts_et"].dt.time <= pd.to_datetime("16:00").time())
    ].copy()

    if regular.empty:
        return {
            "path_status": "no_post_first15_bars",
            "cache_file": str(p),
        }

    entry = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")

    if pd.isna(entry) or entry <= 0:
        entry = regular.iloc[0]["close_px"]

    eod = regular.iloc[-1]["close_px"]

    px_10_00 = get_close_at_or_after(regular, "10:00")
    px_10_15 = get_close_at_or_after(regular, "10:15")
    px_10_30 = get_close_at_or_after(regular, "10:30")
    px_11_00 = get_close_at_or_after(regular, "11:00")

    if pd.isna(entry) or not np.isfinite(entry) or entry <= 0:
        return {
            "path_status": "bad_entry",
            "cache_file": str(p),
        }

    regular["long_ret_pct"] = (regular["close_px"] / entry - 1.0) * 100.0
    regular = regular[np.isfinite(regular["long_ret_pct"])].copy()

    if regular.empty:
        return {
            "path_status": "no_valid_returns",
            "cache_file": str(p),
        }

    max_runup = regular["long_ret_pct"].max()
    max_drawdown = regular["long_ret_pct"].min()

    idx_max = regular["long_ret_pct"].idxmax()
    idx_min = regular["long_ret_pct"].idxmin()

    time_to_max_runup_min = (
        regular.loc[idx_max, "ts_et"] - regular.iloc[0]["ts_et"]
    ).total_seconds() / 60.0

    time_to_max_drawdown_min = (
        regular.loc[idx_min, "ts_et"] - regular.iloc[0]["ts_et"]
    ).total_seconds() / 60.0

    long_15m = (px_10_00 / entry - 1.0) * 100.0 if pd.notna(px_10_00) else np.nan
    long_30m = (px_10_15 / entry - 1.0) * 100.0 if pd.notna(px_10_15) else np.nan
    long_45m = (px_10_30 / entry - 1.0) * 100.0 if pd.notna(px_10_30) else np.nan
    long_75m = (px_11_00 / entry - 1.0) * 100.0 if pd.notna(px_11_00) else np.nan
    long_eod = (eod / entry - 1.0) * 100.0

    return {
        "path_status": "ok",
        "cache_file": str(p),
        "entry_px": entry,

        "long_15m_pct": long_15m,
        "long_30m_pct": long_30m,
        "long_45m_pct": long_45m,
        "long_75m_pct": long_75m,
        "long_eod_pct": long_eod,

        "short_15m_pct": safe_short_return(long_15m),
        "short_30m_pct": safe_short_return(long_30m),
        "short_45m_pct": safe_short_return(long_45m),
        "short_75m_pct": safe_short_return(long_75m),
        "short_eod_pct": safe_short_return(long_eod),

        "long_max_runup_pct": max_runup,
        "long_max_drawdown_pct": max_drawdown,
        "short_max_runup_pct": safe_short_return(max_drawdown),
        "short_max_drawdown_pct": safe_short_return(max_runup),

        "time_to_max_runup_min": time_to_max_runup_min,
        "time_to_max_drawdown_min": time_to_max_drawdown_min,
    }


def setup_label(row):
    pm = row["premarket_dollar_vs_prior_daily_avg"]
    f15 = row["first15_dollar_vs_prior_daily_avg"]
    f15_ret = row["first_15m_return_pct"]
    open_vs_pm_high = row["regular_open_vs_premarket_high_pct"]

    if pm <= 0.1 and f15 >= 0.01 and f15_ret >= 1:
        return "LONG_quiet_pm_first15_strong"

    if pm <= 0.1 and f15 >= 0.01 and f15_ret > 0:
        return "LONG_quiet_pm_first15_green"

    if pm > 10 and open_vs_pm_high <= -15:
        return "SHORT_super_mania_pm_collapse"

    if pm > 1 and open_vs_pm_high <= -5 and f15_ret <= -1:
        return "SHORT_mania_pm_big_fade_weak_first15"

    if pm > 0.1 and open_vs_pm_high <= -5 and f15_ret < 0:
        return "SHORT_hot_pm_big_fade_red_first15"

    return "CONTROL_other_high_daily"


def summarize_group(label, df):
    if df.empty:
        return {"setup": label, "rows": 0}

    return {
        "setup": label,
        "rows": len(df),
        "tickers": df["ticker"].nunique(),

        "median_prev_close": df["prev_close"].median(),
        "median_pm_dollar_vs_daily_avg": df["premarket_dollar_vs_prior_daily_avg"].median(),
        "median_first15_dollar_vs_daily_avg": df["first15_dollar_vs_prior_daily_avg"].median(),
        "median_first15_return": df["first_15m_return_pct"].median(),
        "median_open_vs_pm_high": df["regular_open_vs_premarket_high_pct"].median(),

        "long_15m_median": df["long_15m_pct"].median(),
        "long_30m_median": df["long_30m_pct"].median(),
        "long_45m_median": df["long_45m_pct"].median(),
        "long_75m_median": df["long_75m_pct"].median(),
        "long_eod_median": df["long_eod_pct"].median(),
        "long_eod_win_rate": (df["long_eod_pct"] > 0).mean() * 100,

        "short_15m_median": df["short_15m_pct"].median(),
        "short_30m_median": df["short_30m_pct"].median(),
        "short_45m_median": df["short_45m_pct"].median(),
        "short_75m_median": df["short_75m_pct"].median(),
        "short_eod_median": df["short_eod_pct"].median(),
        "short_eod_win_rate": (df["short_eod_pct"] > 0).mean() * 100,

        "long_max_runup_median": df["long_max_runup_pct"].median(),
        "long_max_drawdown_median": df["long_max_drawdown_pct"].median(),
        "short_max_runup_median": df["short_max_runup_pct"].median(),
        "short_max_drawdown_median": df["short_max_drawdown_pct"].median(),

        "median_time_to_max_runup_min": df["time_to_max_runup_min"].median(),
        "median_time_to_max_drawdown_min": df["time_to_max_drawdown_min"].median(),
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

    print("events to process:", len(high_daily))

    metrics = []
    for i, row in high_daily.iterrows():
        m = compute_path_metrics(row)
        metrics.append(m)

        if len(metrics) % 500 == 0:
            print("processed:", len(metrics))

    metric_df = pd.DataFrame(metrics)
    out = pd.concat([high_daily.reset_index(drop=True), metric_df], axis=1)

    path_out = OUTPUT_DIR / "cheap_post_first15_path_metrics.csv"
    summary_out = OUTPUT_DIR / "cheap_post_first15_path_summary.csv"

    out.to_csv(path_out, index=False)

    ok = out[out["path_status"] == "ok"].copy()

    summary_rows = []
    for label, sub in ok.groupby("setup", observed=True):
        summary_rows.append(summarize_group(label, sub))

    summary = pd.DataFrame(summary_rows).sort_values("setup")
    summary.to_csv(summary_out, index=False)

    print()
    print("saved path metrics:", path_out)
    print("saved summary:", summary_out)

    print()
    print("=== Path Status ===")
    print(out["path_status"].value_counts(dropna=False).to_string())

    print()
    print("=== Cheap Post-First15 Path Summary ===")
    display_cols = [
        "setup",
        "rows",
        "tickers",
        "median_prev_close",
        "median_pm_dollar_vs_daily_avg",
        "median_first15_dollar_vs_daily_avg",
        "median_first15_return",
        "median_open_vs_pm_high",

        "long_15m_median",
        "long_30m_median",
        "long_45m_median",
        "long_75m_median",
        "long_eod_median",
        "long_eod_win_rate",

        "short_15m_median",
        "short_30m_median",
        "short_45m_median",
        "short_75m_median",
        "short_eod_median",
        "short_eod_win_rate",

        "long_max_runup_median",
        "long_max_drawdown_median",
        "short_max_runup_median",
        "short_max_drawdown_median",
        "median_time_to_max_runup_min",
        "median_time_to_max_drawdown_min",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
