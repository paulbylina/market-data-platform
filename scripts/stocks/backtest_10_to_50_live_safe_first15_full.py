from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


FEATURES_PATH = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/extended_hours_features_pilot.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/price_10_to_50_live_safe_first15"
)

OUT_TRADES = OUT_DIR / "price_10_to_50_live_safe_first15_exit_trades.csv"
OUT_SUMMARY = OUT_DIR / "price_10_to_50_live_safe_first15_exit_summary.csv"
OUT_FEATURES = OUT_DIR / "price_10_to_50_live_safe_first15_path_features.csv"

COST_BPS = 10.0


def pct(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def position_in_range(x, low, high):
    if pd.isna(x) or pd.isna(low) or pd.isna(high) or high == low:
        return np.nan
    return (x - low) / (high - low)


def price_bucket(prev_close):
    if pd.isna(prev_close):
        return "unknown"
    if prev_close < 10:
        return "under_10"
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


def normalize_bars(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path)

    time_col = None
    for c in ["timestamp_ms", "timestamp", "bar_start_utc", "bar_start_et", "datetime", "window_start", "t"]:
        if c in raw.columns:
            time_col = c
            break

    if time_col is None:
        return pd.DataFrame()

    if time_col in ["timestamp_ms", "t"] or pd.api.types.is_numeric_dtype(raw[time_col]):
        ts = pd.to_datetime(raw[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(raw[time_col], utc=True, errors="coerce")

    out = raw.copy()
    out["ts_et"] = ts.dt.tz_convert("America/New_York")

    rename = {}
    for short, long in [("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")]:
        if short in out.columns and long not in out.columns:
            rename[short] = long
    out = out.rename(columns=rename)

    for c in ["open", "high", "low", "close"]:
        if c not in out.columns:
            return pd.DataFrame()
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["ts_et", "open", "high", "low", "close"]).copy()
    out = out.sort_values("ts_et").reset_index(drop=True)

    return out


def compute_path_metrics(row: pd.Series) -> dict:
    ticker = row["ticker"]
    trade_date = str(row["trade_date"])
    prev_trade_date = row.get("prev_trade_date", None)

    p = find_cache_file(ticker, trade_date, prev_trade_date)

    if p is None:
        return {"path_status": "missing_cache", "cache_file": None}

    try:
        bars = normalize_bars(p)
    except Exception:
        return {"path_status": "read_error", "cache_file": str(p)}

    if bars.empty:
        return {"path_status": "bad_bars", "cache_file": str(p)}

    trade_day = pd.to_datetime(trade_date).date()
    day = bars[bars["ts_et"].dt.date == trade_day].copy()

    after = day[
        (day["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty:
        return {"path_status": "no_after_entry_bars", "cache_file": str(p)}

    entry = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")
    if pd.isna(entry) or entry <= 0:
        entry = float(after.iloc[0]["open"])

    if pd.isna(entry) or entry <= 0:
        return {"path_status": "bad_entry", "cache_file": str(p)}

    eod = float(after.iloc[-1]["close"])

    after["long_high_ret_pct"] = (after["high"] / entry - 1.0) * 100.0
    after["long_low_ret_pct"] = (after["low"] / entry - 1.0) * 100.0
    after["long_close_ret_pct"] = (after["close"] / entry - 1.0) * 100.0

    max_runup = after["long_high_ret_pct"].max()
    max_drawdown = after["long_low_ret_pct"].min()

    idx_max = after["long_high_ret_pct"].idxmax()
    idx_min = after["long_low_ret_pct"].idxmin()

    time_to_max_runup_min = (after.loc[idx_max, "ts_et"] - after.iloc[0]["ts_et"]).total_seconds() / 60.0
    time_to_max_drawdown_min = (after.loc[idx_min, "ts_et"] - after.iloc[0]["ts_et"]).total_seconds() / 60.0

    def close_at_or_after(hhmm: str):
        t = pd.to_datetime(hhmm).time()
        sub = after[after["ts_et"].dt.time >= t]
        if sub.empty:
            return np.nan
        return float(sub.iloc[0]["close"])

    px_10_00 = close_at_or_after("10:00")
    px_10_15 = close_at_or_after("10:15")
    px_10_30 = close_at_or_after("10:30")
    px_11_00 = close_at_or_after("11:00")

    return {
        "path_status": "ok",
        "cache_file": str(p),
        "entry_px": entry,
        "long_15m_pct": pct(px_10_00, entry),
        "long_30m_pct": pct(px_10_15, entry),
        "long_45m_pct": pct(px_10_30, entry),
        "long_75m_pct": pct(px_11_00, entry),
        "long_eod_pct": pct(eod, entry),
        "long_max_runup_pct": max_runup,
        "long_max_drawdown_pct": max_drawdown,
        "time_to_max_runup_min": time_to_max_runup_min,
        "time_to_max_drawdown_min": time_to_max_drawdown_min,
    }


def setup_bucket(row: pd.Series) -> str:
    gap = row["gap_pct"]
    pm = row["premarket_dollar_vs_prior_daily_avg"]
    ret = row["first_15m_return_pct"]
    rng = row["first15_range_pct"]
    close_pos = row["first15_close_position_in_range"]
    f15_dollar = row["first15_dollar_vs_prior_daily_avg"]

    if pd.isna(gap) or pd.isna(pm) or pd.isna(ret) or pd.isna(rng) or pd.isna(close_pos):
        return "reject_missing"

    if gap < 0:
        return "reject_gap_down"
    if gap > 5:
        return "reject_gap_too_hot"
    if pm > 0.03:
        return "reject_active_premarket"
    if ret < 1.5:
        return "reject_weak_first15"
    if ret >= 7:
        return "reject_first15_too_hot"
    if rng < 2:
        return "reject_range_too_small"
    if rng >= 8:
        return "reject_range_too_wide"
    if close_pos < 0.75:
        return "reject_weak_close_position"

    # Best C-style / fresh setup.
    if (
        1 <= gap <= 5
        and pm <= 0.01
        and 1.5 <= ret < 5
        and 2 <= rng < 6
        and close_pos >= 0.90
    ):
        return "fresh_gap_strict"

    # Clean but not necessarily fresh-gap strict.
    if (
        0 <= gap <= 5
        and pm <= 0.01
        and 1.5 <= ret < 5
        and 2 <= rng < 6
        and close_pos >= 0.90
    ):
        return "strict"

    # Wider valid setup.
    if (
        0 <= gap <= 5
        and pm <= 0.03
        and 1.5 <= ret < 6
        and 2 <= rng < 7
        and close_pos >= 0.75
    ):
        return "valid"

    return "reject_other"


def simulate_exit(row: pd.Series, target_pct: float, stop_pct: float) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    # Conservative if both are touched intraday.
    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous"
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target"
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop"
    return eod - COST_BPS / 100.0, "eod"


def summarize(g: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(g["net_pct"], errors="coerce")

    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].nunique(),
            "tickers": g["ticker"].nunique(),
            "avg_net": vals.mean(),
            "median_net": vals.median(),
            "win_rate": (vals > 0).mean() * 100,
            "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
            "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,
            "median_eod_raw": g["long_eod_pct"].median(),
            "median_runup_raw": g["long_max_runup_pct"].median(),
            "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
            "median_time_to_max_runup": g["time_to_max_runup_min"].median(),
            "median_time_to_max_drawdown": g["time_to_max_drawdown_min"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FEATURES_PATH)

    numeric_cols = [
        "prev_close",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_vs_prior_daily_avg",
        "gap_pct",
        "regular_open",
        "first_15m_dollar_volume",
        "first_15m_open",
        "first_15m_high",
        "first_15m_low",
        "first_15m_close",
        "first_15m_return_pct",
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[df["download_status"].eq("ok")].copy()
    df["price_bucket"] = df["prev_close"].apply(price_bucket)

    df = df[df["price_bucket"].isin(["price_10_to_20", "price_20_to_50"])].copy()

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

    print("base $10-50 rows:", len(df))
    print("price buckets:")
    print(df["price_bucket"].value_counts().to_string())
    print()

    print("computing path metrics...")
    metrics = []
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        metrics.append(compute_path_metrics(row))
        if i % 250 == 0:
            print("processed:", i)

    metric_df = pd.DataFrame(metrics)
    feat = pd.concat([df.reset_index(drop=True), metric_df], axis=1)

    feat["setup_bucket"] = feat.apply(setup_bucket, axis=1)
    feat.to_csv(OUT_FEATURES, index=False)

    ok = feat[feat["path_status"].eq("ok")].copy()
    test = ok[~ok["setup_bucket"].str.startswith("reject")].copy()

    print()
    print("path status:")
    print(feat["path_status"].value_counts(dropna=False).to_string())
    print()
    print("setup buckets:")
    print(feat["setup_bucket"].value_counts(dropna=False).to_string())
    print()
    print("test trades:", len(test))
    print(test.groupby(["price_bucket", "setup_bucket"]).size().to_string())
    print()

    combos = [
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
        (5.0, 6.0),
    ]

    rows = []
    for target, stop in combos:
        for _, row in test.iterrows():
            net, exit_type = simulate_exit(row, target, stop)
            r = row.to_dict()
            r["target_pct"] = target
            r["stop_pct"] = stop
            r["net_pct"] = net
            r["exit_type"] = exit_type
            r["cost_bps"] = COST_BPS
            rows.append(r)

    trades = pd.DataFrame(rows)

    summary = (
        trades.groupby(["price_bucket", "setup_bucket", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=False)
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print("=== Top results by median net ===")
    print(summary.head(40).to_string(index=False))

    print()
    print("saved features:", OUT_FEATURES)
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
