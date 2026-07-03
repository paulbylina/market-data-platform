from pathlib import Path

import numpy as np
import pandas as pd


FEATURES_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_short_fade_expanded_features.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
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


def pct(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def position_in_range(x, low, high):
    if pd.isna(x) or pd.isna(low) or pd.isna(high) or high == low:
        return np.nan
    return (x - low) / (high - low)


def price_bucket_detail(prev_close):
    if pd.isna(prev_close):
        return "unknown"

    if prev_close < 5:
        return "under_5"
    if prev_close < 10:
        return "price_5_to_10"
    if prev_close < 20:
        return "price_10_to_20"
    if prev_close < 50:
        return "price_20_to_50"

    return "price_50_plus"


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


def summarize_group(group):
    return pd.Series(
        {
            "rows": len(group),
            "tickers": group["ticker"].nunique(),

            "median_prev_close": group["prev_close"].median(),
            "median_pm_dollar_vs_daily_avg": group["premarket_dollar_vs_prior_daily_avg"].median(),
            "median_first15_dollar_vs_daily_avg": group["first15_dollar_vs_prior_daily_avg"].median(),
            "median_first15_return": group["first_15m_return_pct"].median(),
            "median_first15_close_pos_range": group["first15_close_position_in_range"].median(),
            "median_first15_range_pct": group["first15_range_pct"].median(),
            "median_first15_close_vs_open": group["first15_close_vs_regular_open_pct"].median(),
            "median_open_vs_pm_high": group["regular_open_vs_premarket_high_pct"].median(),

            "long_15m_median": group["long_15m_pct"].median(),
            "long_30m_median": group["long_30m_pct"].median(),
            "long_45m_median": group["long_45m_pct"].median(),
            "long_75m_median": group["long_75m_pct"].median(),
            "long_eod_median": group["long_eod_pct"].median(),
            "long_eod_win_rate": (group["long_eod_pct"] > 0).mean() * 100,

            "short_15m_median": group["short_15m_pct"].median(),
            "short_30m_median": group["short_30m_pct"].median(),
            "short_45m_median": group["short_45m_pct"].median(),
            "short_75m_median": group["short_75m_pct"].median(),
            "short_eod_median": group["short_eod_pct"].median(),
            "short_eod_win_rate": (group["short_eod_pct"] > 0).mean() * 100,

            "long_max_runup_median": group["long_max_runup_pct"].median(),
            "long_max_drawdown_median": group["long_max_drawdown_pct"].median(),
            "short_max_runup_median": group["short_max_runup_pct"].median(),
            "short_max_drawdown_median": group["short_max_drawdown_pct"].median(),

            "median_time_to_max_runup_min": group["time_to_max_runup_min"].median(),
            "median_time_to_max_drawdown_min": group["time_to_max_drawdown_min"].median(),
        }
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FEATURES_PATH)

    numeric_cols = [
        "prev_close",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_vs_prior_daily_avg",
        "regular_open_vs_premarket_high_pct",
        "regular_open",
        "first_15m_dollar_volume",
        "first_15m_open",
        "first_15m_high",
        "first_15m_low",
        "first_15m_close",
        "first_15m_return_pct",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["download_status"] == "ok"].copy()

    df["price_bucket_detail"] = df["prev_close"].apply(price_bucket_detail)

    df["first15_dollar_vs_prior_daily_avg"] = np.where(
        df["avg_dollar_volume_20d_prior"] > 0,
        df["first_15m_dollar_volume"] / df["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    df["first15_close_position_in_range"] = df.apply(
        lambda row: position_in_range(
            row["first_15m_close"],
            row["first_15m_low"],
            row["first_15m_high"],
        ),
        axis=1,
    )

    df["first15_range_pct"] = df.apply(
        lambda row: pct(row["first_15m_high"], row["first_15m_low"]),
        axis=1,
    )

    df["first15_close_vs_regular_open_pct"] = df.apply(
        lambda row: pct(row["first_15m_close"], row["regular_open"]),
        axis=1,
    )

    high_daily = df[df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES)].copy()
    high_daily["setup"] = high_daily.apply(setup_label, axis=1)

    print("events to process:", len(high_daily))
    print("tickers:", high_daily["ticker"].nunique())

    metrics = []

    for i, row in high_daily.iterrows():
        metrics.append(compute_path_metrics(row))

        if len(metrics) % 500 == 0:
            print("processed:", len(metrics))

    metric_df = pd.DataFrame(metrics)
    out = pd.concat([high_daily.reset_index(drop=True), metric_df], axis=1)

    metrics_out = OUTPUT_DIR / "high_price_short_fade_expanded_post_first15_path_metrics.csv"
    setup_summary_out = OUTPUT_DIR / "high_price_short_fade_expanded_setup_summary.csv"
    bucket_setup_summary_out = OUTPUT_DIR / "high_price_short_fade_expanded_bucket_setup_summary.csv"

    out.to_csv(metrics_out, index=False)

    ok = out[out["path_status"] == "ok"].copy()

    setup_summary = (
        ok.groupby("setup", observed=True)
        .apply(summarize_group, include_groups=False)
        .reset_index()
        .sort_values("setup")
    )

    bucket_setup_summary = (
        ok.groupby(["price_regime", "price_bucket_detail", "setup"], observed=True)
        .apply(summarize_group, include_groups=False)
        .reset_index()
        .sort_values(["price_regime", "price_bucket_detail", "setup"])
    )

    setup_summary.to_csv(setup_summary_out, index=False)
    bucket_setup_summary.to_csv(bucket_setup_summary_out, index=False)

    print()
    print("saved metrics:", metrics_out)
    print("saved setup summary:", setup_summary_out)
    print("saved bucket/setup summary:", bucket_setup_summary_out)

    print()
    print("=== Path Status ===")
    print(out["path_status"].value_counts(dropna=False).to_string())

    print()
    print("=== Setup Summary ===")
    print(setup_summary.to_string(index=False))

    print()
    print("=== Bucket + Setup Summary | rows >= 20 ===")
    display = bucket_setup_summary[bucket_setup_summary["rows"] >= 20].copy()
    print(display.to_string(index=False))

    print()
    print("=== Best Long EOD by Bucket/Setup | rows >= 20 ===")
    print(
        display.sort_values("long_eod_median", ascending=False)
        .head(20)
        .to_string(index=False)
    )

    print()
    print("=== Best Short EOD by Bucket/Setup | rows >= 20 ===")
    print(
        display.sort_values("short_eod_median", ascending=False)
        .head(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()