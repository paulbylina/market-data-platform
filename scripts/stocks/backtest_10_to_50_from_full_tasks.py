from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


TASKS_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/price_10_to_50_full_tasks_first15"
)

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


def build_cache_index() -> dict[tuple[str, str], Path]:
    idx = {}

    for p in CACHE_DIR.glob("*_1m.csv"):
        name = p.name

        if "_to_" not in name:
            continue

        left, right = name.rsplit("_to_", 1)
        trade_date = right.replace("_1m.csv", "")

        if "_" not in left:
            continue

        ticker = left.rsplit("_", 1)[0]
        idx[(ticker, trade_date)] = p

    return idx


def find_cache_file(cache_index, ticker, trade_date, prev_trade_date=None):
    ticker = str(ticker)
    trade_date = str(trade_date)

    p = cache_index.get((ticker, trade_date))
    if p is not None and p.exists():
        return p

    # Slow fallback.
    if prev_trade_date is not None and not pd.isna(prev_trade_date):
        p = CACHE_DIR / f"{ticker}_{prev_trade_date}_to_{trade_date}_1m.csv"
        if p.exists():
            return p

    matches = list(CACHE_DIR.glob(f"{ticker}_*_to_{trade_date}_1m.csv"))
    if matches:
        return matches[0]

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

    if "volume" in out.columns:
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0)
    else:
        out["volume"] = 0

    out = out.dropna(subset=["ts_et", "open", "high", "low", "close"]).copy()
    out = out.sort_values("ts_et").reset_index(drop=True)

    return out


def window_ohlcv(day: pd.DataFrame, start: str, end: str) -> dict:
    st = pd.to_datetime(start).time()
    en = pd.to_datetime(end).time()

    sub = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < en)
    ].copy()

    if sub.empty:
        return {
            "bars": 0,
            "open": np.nan,
            "high": np.nan,
            "low": np.nan,
            "close": np.nan,
            "volume": np.nan,
            "dollar_volume": np.nan,
        }

    dollar_volume = (sub["close"] * sub["volume"]).sum()

    return {
        "bars": len(sub),
        "open": float(sub.iloc[0]["open"]),
        "high": float(sub["high"].max()),
        "low": float(sub["low"].min()),
        "close": float(sub.iloc[-1]["close"]),
        "volume": float(sub["volume"].sum()),
        "dollar_volume": float(dollar_volume),
    }


def compute_row_features(row: pd.Series, cache_index) -> dict:
    ticker = row["ticker"]
    trade_date = str(row["trade_date"])
    prev_trade_date = row.get("prev_trade_date", None)

    p = find_cache_file(cache_index, ticker, trade_date, prev_trade_date)

    if p is None:
        return {"status": "missing_cache", "cache_file": None}

    try:
        bars = normalize_bars(p)
    except Exception:
        return {"status": "read_error", "cache_file": str(p)}

    if bars.empty:
        return {"status": "bad_bars", "cache_file": str(p)}

    trade_day = pd.to_datetime(trade_date).date()
    day = bars[bars["ts_et"].dt.date == trade_day].copy()

    if day.empty:
        return {"status": "missing_trade_day", "cache_file": str(p)}

    pre = window_ohlcv(day, "04:00", "09:30")
    f15 = window_ohlcv(day, "09:30", "09:45")
    after = day[
        (day["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if f15["bars"] == 0:
        return {"status": "missing_first15", "cache_file": str(p)}

    if after.empty:
        return {"status": "missing_after_entry", "cache_file": str(p)}

    entry = f15["close"]
    if pd.isna(entry) or entry <= 0:
        return {"status": "bad_entry", "cache_file": str(p)}

    daily_dollar = pd.to_numeric(row.get("dollar_volume", np.nan), errors="coerce")
    daily_rvol = pd.to_numeric(row.get("dollar_volume_rvol_20d", np.nan), errors="coerce")

    prior_avg_dollar = np.nan
    if pd.notna(daily_dollar) and pd.notna(daily_rvol) and daily_rvol > 0:
        prior_avg_dollar = daily_dollar / daily_rvol

    regular_open = pd.to_numeric(row.get("open", np.nan), errors="coerce")
    if pd.isna(regular_open) or regular_open <= 0:
        regular_open = f15["open"]

    after["long_high_ret_pct"] = (after["high"] / entry - 1.0) * 100.0
    after["long_low_ret_pct"] = (after["low"] / entry - 1.0) * 100.0
    after["long_close_ret_pct"] = (after["close"] / entry - 1.0) * 100.0

    max_runup = after["long_high_ret_pct"].max()
    max_drawdown = after["long_low_ret_pct"].min()
    eod = float(after.iloc[-1]["close"])

    idx_max = after["long_high_ret_pct"].idxmax()
    idx_min = after["long_low_ret_pct"].idxmin()

    time_to_max_runup = (after.loc[idx_max, "ts_et"] - after.iloc[0]["ts_et"]).total_seconds() / 60.0
    time_to_max_drawdown = (after.loc[idx_min, "ts_et"] - after.iloc[0]["ts_et"]).total_seconds() / 60.0

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
        "status": "ok",
        "cache_file": str(p),
        "entry_px": entry,

        "premarket_bars": pre["bars"],
        "premarket_dollar_volume": pre["dollar_volume"],
        "premarket_dollar_vs_prior_daily_avg": (
            pre["dollar_volume"] / prior_avg_dollar if pd.notna(prior_avg_dollar) and prior_avg_dollar > 0 else np.nan
        ),

        "first15_bars": f15["bars"],
        "first15_open": f15["open"],
        "first15_high": f15["high"],
        "first15_low": f15["low"],
        "first15_close": f15["close"],
        "first15_volume": f15["volume"],
        "first15_dollar_volume": f15["dollar_volume"],
        "first15_dollar_vs_prior_daily_avg": (
            f15["dollar_volume"] / prior_avg_dollar if pd.notna(prior_avg_dollar) and prior_avg_dollar > 0 else np.nan
        ),
        "first15_return_pct": pct(f15["close"], regular_open),
        "first15_body_pct": pct(f15["close"], f15["open"]),
        "first15_range_pct": pct(f15["high"], f15["low"]),
        "first15_close_position_in_range": position_in_range(f15["close"], f15["low"], f15["high"]),

        "long_15m_pct": pct(px_10_00, entry),
        "long_30m_pct": pct(px_10_15, entry),
        "long_45m_pct": pct(px_10_30, entry),
        "long_75m_pct": pct(px_11_00, entry),
        "long_eod_pct": pct(eod, entry),
        "long_max_runup_pct": max_runup,
        "long_max_drawdown_pct": max_drawdown,
        "time_to_max_runup_min": time_to_max_runup,
        "time_to_max_drawdown_min": time_to_max_drawdown,
    }


def setup_bucket(row: pd.Series) -> str:
    gap = row["gap_pct"]
    pm = row["premarket_dollar_vs_prior_daily_avg"]
    ret = row["first15_return_pct"]
    rng = row["first15_range_pct"]
    close_pos = row["first15_close_position_in_range"]

    if pd.isna(pm) or pd.isna(ret) or pd.isna(rng) or pd.isna(close_pos):
        return "reject_missing"

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

    if 1 <= gap <= 5 and pm <= 0.01 and 1.5 <= ret < 5 and 2 <= rng < 6 and close_pos >= 0.90:
        return "fresh_gap_strict"

    if 0 <= gap <= 5 and pm <= 0.01 and 1.5 <= ret < 5 and 2 <= rng < 6 and close_pos >= 0.90:
        return "strict"

    if 0 <= gap <= 5 and pm <= 0.03 and 1.5 <= ret < 6 and 2 <= rng < 7 and close_pos >= 0.75:
        return "valid"

    return "reject_other"


def volume_bucket(x):
    if pd.isna(x):
        return "missing"
    if x < 0.02:
        return "<0.02"
    if x < 0.05:
        return "0.02-0.05"
    if x < 0.10:
        return "0.05-0.10"
    if x < 0.20:
        return "0.10-0.20"
    return "0.20+"


def simulate_exit(row: pd.Series, target_pct: float, stop_pct: float) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--price-min", type=float, default=10)
    parser.add_argument("--price-max", type=float, default=50)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("building cache index...")
    cache_index = build_cache_index()
    print("cache files indexed:", len(cache_index))

    df = pd.read_csv(TASKS_PATH)

    for c in ["prev_close", "gap_pct", "open", "dollar_volume", "dollar_volume_rvol_20d"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    df["price_bucket"] = df["prev_close"].apply(price_bucket)

    if args.start_date:
        df = df[df["trade_date"] >= args.start_date].copy()
    if args.end_date:
        df = df[df["trade_date"] <= args.end_date].copy()

    # Hard live-safe prefilter.
    df = df[
        (df["prev_close"] >= args.price_min)
        & (df["prev_close"] < args.price_max)
        & (df["gap_pct"] >= 0)
        & (df["gap_pct"] <= 5)
    ].copy()

    if args.max_rows:
        df = df.head(args.max_rows).copy()

    print("rows after price/date/gap filter:", len(df))
    print("price buckets:")
    print(df["price_bucket"].value_counts(dropna=False).to_string())
    print()

    metrics = []
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        metrics.append(compute_row_features(row, cache_index))
        if i % 1000 == 0:
            print("processed:", i)

    feat = pd.concat([df.reset_index(drop=True), pd.DataFrame(metrics)], axis=1)
    feat["setup_bucket"] = feat.apply(setup_bucket, axis=1)
    feat["first15_volume_bucket"] = feat["first15_dollar_vs_prior_daily_avg"].map(volume_bucket)

    features_out = OUT_DIR / f"price_{int(args.price_min)}_to_{int(args.price_max)}_full_tasks_first15_features.csv"
    trades_out = OUT_DIR / f"price_{int(args.price_min)}_to_{int(args.price_max)}_full_tasks_first15_exit_trades.csv"
    summary_out = OUT_DIR / f"price_{int(args.price_min)}_to_{int(args.price_max)}_full_tasks_first15_exit_summary.csv"

    feat.to_csv(features_out, index=False)

    ok = feat[feat["status"].eq("ok")].copy()
    test = ok[~ok["setup_bucket"].str.startswith("reject")].copy()

    print()
    print("status:")
    print(feat["status"].value_counts(dropna=False).to_string())
    print()
    print("setup buckets:")
    print(feat["setup_bucket"].value_counts(dropna=False).to_string())
    print()
    print("test trades:", len(test))
    if not test.empty:
        print(test.groupby(["price_bucket", "setup_bucket", "first15_volume_bucket"]).size().to_string())
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
    trades.to_csv(trades_out, index=False)

    if trades.empty:
        print("No trades passed filters.")
        return

    summary = (
        trades.groupby(
            ["price_bucket", "setup_bucket", "first15_volume_bucket", "target_pct", "stop_pct"],
            observed=True,
        )
        .apply(summarize)
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=False)
    )

    summary.to_csv(summary_out, index=False)

    print("=== Top results by median net | rows >= 20 ===")
    display = summary[summary["trades"] >= 20].copy()
    print(display.head(50).to_string(index=False))

    print()
    print("=== Top results by median net | all rows ===")
    print(summary.head(50).to_string(index=False))

    print()
    print("saved features:", features_out)
    print("saved trades:", trades_out)
    print("saved summary:", summary_out)


if __name__ == "__main__":
    main()
