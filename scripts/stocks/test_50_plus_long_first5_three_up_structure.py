from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "abc_gap_early_bar_behavior_trades_2024_2026.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

OUT_TRADES = OUT_DIR / "long_first5_three_up_structure_trades_2024_2026.csv"
OUT_SUMMARY = OUT_DIR / "long_first5_three_up_structure_summary_2024_2026.csv"

COST_BPS = 10.0


def pct(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def close_position(close, low, high):
    if pd.isna(close) or pd.isna(low) or pd.isna(high) or high == low:
        return np.nan
    return (close - low) / (high - low)


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


def find_cache_file(row: pd.Series) -> Path | None:
    if "cache_file" in row.index and isinstance(row["cache_file"], str):
        p = Path(row["cache_file"])
        if p.exists():
            return p

    if "cache_file_used" in row.index and isinstance(row["cache_file_used"], str):
        p = Path(row["cache_file_used"])
        if p.exists():
            return p

    ticker = str(row["ticker"])
    trade_date = str(row["trade_date"])
    prev_trade_date = str(row.get("prev_trade_date", ""))

    if prev_trade_date and prev_trade_date != "nan":
        p = CACHE_DIR / f"{ticker}_{prev_trade_date}_to_{trade_date}_1m.csv"
        if p.exists():
            return p

    matches = list(CACHE_DIR.glob(f"{ticker}_*_to_{trade_date}_1m.csv"))
    if matches:
        return matches[0]

    return None


def window_ohlc(day: pd.DataFrame, start: str, end: str) -> dict:
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
        }

    return {
        "bars": len(sub),
        "open": float(sub.iloc[0]["open"]),
        "high": float(sub["high"].max()),
        "low": float(sub["low"].min()),
        "close": float(sub.iloc[-1]["close"]),
    }


def long_return(entry, px):
    if pd.isna(entry) or pd.isna(px) or entry <= 0:
        return np.nan
    return (px / entry - 1.0) * 100.0


def simulate_long_from(day: pd.DataFrame, start_time: str, entry_px: float, target_pct: float, stop_pct: float):
    st = pd.to_datetime(start_time).time()

    after = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty or pd.isna(entry_px) or entry_px <= 0:
        return np.nan, "no_after_entry", np.nan, np.nan, np.nan

    after["long_runup_pct"] = (after["high"] / entry_px - 1.0) * 100.0
    after["long_drawdown_pct"] = (after["low"] / entry_px - 1.0) * 100.0

    max_runup = after["long_runup_pct"].max()
    max_drawdown = after["long_drawdown_pct"].min()
    eod_ret = long_return(entry_px, float(after.iloc[-1]["close"]))

    target_hit = pd.notna(max_runup) and max_runup >= target_pct
    stop_hit = pd.notna(max_drawdown) and max_drawdown <= -stop_pct

    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous", eod_ret, max_runup, max_drawdown
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target", eod_ret, max_runup, max_drawdown
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop", eod_ret, max_runup, max_drawdown

    return eod_ret - COST_BPS / 100.0, "eod", eod_ret, max_runup, max_drawdown


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
            "eod_rate": (g["exit_type"] == "eod").mean() * 100,
            "median_long_eod_raw": g["long_eod_pct"].median(),
            "median_long_runup_raw": g["long_max_runup_pct"].median(),
            "median_long_drawdown_raw": g["long_max_drawdown_pct"].median(),
            "median_entry_delay_min": g["entry_delay_min"].median(),
            "median_gap": g["gap_pct"].median() if "gap_pct" in g.columns else np.nan,
            "median_first5_ret": g["first5_ret"].median(),
            "median_first5_close_pos": g["first5_close_pos"].median(),
            "median_first5_range": g["first5_range"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)

    # Deduplicate in case the input has one row per exit.
    keep_cols = ["ticker", "trade_date"]
    df = df.drop_duplicates(subset=keep_cols).copy()

    print("base long rows:", len(df))

    combos = [
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    rows = []
    missing_cache = 0
    bad_bars = 0

    for _, row in df.iterrows():
        p = find_cache_file(row)

        if p is None:
            missing_cache += 1
            continue

        bars = normalize_bars(p)
        if bars.empty:
            bad_bars += 1
            continue

        trade_day = pd.to_datetime(row["trade_date"]).date()
        day = bars[bars["ts_et"].dt.date == trade_day].copy()

        if day.empty:
            bad_bars += 1
            continue

        first5 = window_ohlc(day, "09:30", "09:35")
        second5 = window_ohlc(day, "09:35", "09:40")
        third5 = window_ohlc(day, "09:40", "09:45")

        if first5["bars"] == 0:
            continue

        first5_ret = pct(first5["close"], first5["open"])
        first5_range = pct(first5["high"], first5["low"])
        first5_close_pos = close_position(first5["close"], first5["low"], first5["high"])

        base = row.to_dict()
        base.update(
            {
                "cache_file_used": str(p),
                "first5_open": first5["open"],
                "first5_high": first5["high"],
                "first5_low": first5["low"],
                "first5_close": first5["close"],
                "first5_ret": first5_ret,
                "first5_range": first5_range,
                "first5_close_pos": first5_close_pos,
                "second5_open": second5["open"],
                "second5_close": second5["close"],
                "third5_open": third5["open"],
                "third5_close": third5["close"],
            }
        )

        # Long first5 setups.
        setup_passes = []

        if first5_ret >= 0.5 and first5_close_pos >= 0.65:
            setup_passes.append("first5_green_structure")

        if (
            first5_ret >= 0.5
            and first5_close_pos >= 0.65
            and second5["bars"] > 0
            and second5["close"] > first5["close"]
        ):
            setup_passes.append("first5_plus_second5_higher")

        if (
            first5_ret >= 0.5
            and first5_close_pos >= 0.65
            and second5["bars"] > 0
            and third5["bars"] > 0
            and second5["close"] > first5["close"]
            and third5["close"] > second5["close"]
        ):
            setup_passes.append("first5_three_higher_5m")

        for setup_name in setup_passes:
            if setup_name == "first5_green_structure":
                entry_time = "09:35"
                entry_px = first5["close"]
                delay = 0
            elif setup_name == "first5_plus_second5_higher":
                entry_time = "09:40"
                entry_px = second5["close"]
                delay = 5
            else:
                entry_time = "09:45"
                entry_px = third5["close"]
                delay = 10

            for target, stop in combos:
                net, exit_type, eod_ret, runup, drawdown = simulate_long_from(
                    day=day,
                    start_time=entry_time,
                    entry_px=entry_px,
                    target_pct=target,
                    stop_pct=stop,
                )

                r = dict(base)
                r["setup_name"] = setup_name
                r["entry_time"] = entry_time
                r["entry_px"] = entry_px
                r["entry_delay_min"] = delay
                r["target_pct"] = target
                r["stop_pct"] = stop
                r["net_pct"] = net
                r["exit_type"] = exit_type
                r["long_eod_pct"] = eod_ret
                r["long_max_runup_pct"] = runup
                r["long_max_drawdown_pct"] = drawdown
                r["cost_bps"] = COST_BPS
                rows.append(r)

    print("missing_cache:", missing_cache)
    print("bad_bars:", bad_bars)

    trades = pd.DataFrame(rows)

    if trades.empty:
        print("No trades created.")
        return

    summary = (
        trades.groupby(["setup_name", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=False)
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print()
    print("=== Long first5 structure summary ===")
    print(summary.to_string(index=False))

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
