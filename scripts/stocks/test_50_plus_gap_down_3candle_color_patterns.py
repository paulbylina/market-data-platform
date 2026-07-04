from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_expanded_custom_setups_path_metrics.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

OUT_TRADES = OUT_DIR / "gap_down_50_plus_3candle_color_pattern_trades.csv"
OUT_SUMMARY = OUT_DIR / "gap_down_50_plus_3candle_color_pattern_summary.csv"
OUT_BEST = OUT_DIR / "gap_down_50_plus_3candle_color_pattern_best_summary.csv"
OUT_PERIOD = OUT_DIR / "gap_down_50_plus_3candle_color_pattern_best_period_summary.csv"
OUT_YEARLY = OUT_DIR / "gap_down_50_plus_3candle_color_pattern_best_yearly_summary.csv"

COST_BPS = 10.0


def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise SystemExit(f"Missing required column. Tried: {names}")


def pct(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def close_position(close, low, high):
    if pd.isna(close) or pd.isna(low) or pd.isna(high) or high == low:
        return np.nan
    return (close - low) / (high - low)


def candle_color(o, c):
    if pd.isna(o) or pd.isna(c):
        return None
    if c > o:
        return "G"
    if c < o:
        return "R"
    return "F"


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

    return out.dropna(subset=["ts_et", "open", "high", "low", "close"]).sort_values("ts_et").reset_index(drop=True)


def find_cache_file(row: pd.Series) -> Path | None:
    ticker = str(row["ticker"])
    trade_date = pd.to_datetime(row["trade_date"]).date().isoformat()

    for c in ["cache_file", "cache_file_used"]:
        if c in row.index and isinstance(row[c], str):
            p = Path(row[c])
            if p.exists():
                return p

    prev_trade_date = row.get("prev_trade_date", None)
    if pd.notna(prev_trade_date):
        prev_trade_date = pd.to_datetime(prev_trade_date, errors="coerce")
        if pd.notna(prev_trade_date):
            p = CACHE_DIR / f"{ticker}_{prev_trade_date.date().isoformat()}_to_{trade_date}_1m.csv"
            if p.exists():
                return p

    matches = list(CACHE_DIR.glob(f"{ticker}_*_to_{trade_date}_1m.csv"))
    if matches:
        return matches[0]

    return None


def window_ohlc(day: pd.DataFrame, start: str, end: str) -> dict:
    st = pd.to_datetime(start).time()
    en = pd.to_datetime(end).time()

    sub = day[(day["ts_et"].dt.time >= st) & (day["ts_et"].dt.time < en)].copy()

    if sub.empty:
        return {"bars": 0, "open": np.nan, "high": np.nan, "low": np.nan, "close": np.nan}

    return {
        "bars": len(sub),
        "open": float(sub.iloc[0]["open"]),
        "high": float(sub["high"].max()),
        "low": float(sub["low"].min()),
        "close": float(sub.iloc[-1]["close"]),
    }


def simulate_trade(day: pd.DataFrame, side: str, entry_time: str, entry_px: float, target_pct: float, stop_pct: float):
    st = pd.to_datetime(entry_time).time()

    after = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty or pd.isna(entry_px) or entry_px <= 0:
        return np.nan, "no_after_entry", np.nan, np.nan, np.nan

    if side == "LONG":
        after["runup_pct"] = (after["high"] / entry_px - 1.0) * 100.0
        after["drawdown_pct"] = (after["low"] / entry_px - 1.0) * 100.0

        raw_eod = (after.iloc[-1]["close"] / entry_px - 1.0) * 100.0
        raw_runup = after["runup_pct"].max()
        raw_drawdown = after["drawdown_pct"].min()

        target_hit = raw_runup >= target_pct
        stop_hit = raw_drawdown <= -stop_pct

    elif side == "SHORT":
        after["runup_pct"] = (entry_px / after["low"] - 1.0) * 100.0
        after["drawdown_pct"] = (after["high"] / entry_px - 1.0) * 100.0

        raw_eod = (entry_px / after.iloc[-1]["close"] - 1.0) * 100.0
        raw_runup = after["runup_pct"].max()
        raw_drawdown = after["drawdown_pct"].max()

        target_hit = raw_runup >= target_pct
        stop_hit = raw_drawdown >= stop_pct

    else:
        raise ValueError(side)

    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous", raw_eod, raw_runup, raw_drawdown
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target", raw_eod, raw_runup, raw_drawdown
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop", raw_eod, raw_runup, raw_drawdown

    return raw_eod - COST_BPS / 100.0, "eod", raw_eod, raw_runup, raw_drawdown


def summarize(g: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(g["net_pct"], errors="coerce")

    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].dt.date.nunique(),
            "tickers": g["ticker"].nunique(),
            "avg_net": vals.mean(),
            "median_net": vals.median(),
            "win_rate": (vals > 0).mean() * 100,
            "target_rate": g["exit_type"].astype(str).str.contains("target", na=False).mean() * 100,
            "stop_rate": g["exit_type"].astype(str).str.contains("stop", na=False).mean() * 100,
            "eod_rate": (g["exit_type"].astype(str) == "eod").mean() * 100,
            "median_gap": g["gap_pct"].median(),
            "median_first15_ret": g["first15_ret"].median(),
            "median_first15_close_pos": g["first15_close_pos"].median(),
            "median_first15_range": g["first15_range"].median(),
            "median_first5_ret": g["first5_ret"].median(),
            "median_second5_ret": g["second5_ret"].median(),
            "median_third5_ret": g["third5_ret"].median(),
            "median_raw_eod": g["raw_eod_pct"].median(),
            "median_raw_runup": g["raw_runup_pct"].median(),
            "median_raw_drawdown": g["raw_drawdown_pct"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def add_periods(trades: pd.DataFrame) -> pd.DataFrame:
    trades = trades.copy()
    trades["year"] = trades["trade_date"].dt.year

    trades["period"] = np.select(
        [
            trades["trade_date"] < pd.Timestamp("2023-01-01"),
            (trades["trade_date"] >= pd.Timestamp("2023-01-01"))
            & (trades["trade_date"] < pd.Timestamp("2025-01-01")),
            trades["trade_date"] >= pd.Timestamp("2025-01-01"),
        ],
        ["train_2016_2022", "validation_2023_2024", "test_2025_2026"],
        default="other",
    )

    return trades


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()

    prev_close_col = pick_col(df, ["prev_close"])
    gap_col = pick_col(df, ["gap_pct"])

    df[prev_close_col] = pd.to_numeric(df[prev_close_col], errors="coerce")
    df[gap_col] = pd.to_numeric(df[gap_col], errors="coerce")

    df["prev_close"] = df[prev_close_col]
    df["gap_pct"] = df[gap_col]

    base = df[
        (df["prev_close"] >= 50)
        & (df["gap_pct"] >= -10)
        & (df["gap_pct"] < 0)
    ].copy()

    base["gap_bucket"] = pd.cut(
        base["gap_pct"],
        bins=[-10, -5, -2, 0],
        labels=["gap_down_10_to_5", "gap_down_5_to_2", "gap_down_2_to_0"],
        include_lowest=True,
        right=False,
    ).astype(str)

    print("input:", INPUT)
    print("input rows:", len(df))
    print("$50+ gap-down base rows:", len(base))
    print("date range:", base["trade_date"].min().date(), "to", base["trade_date"].max().date())
    print()
    print("gap-down rows by year:")
    print(base["trade_date"].dt.year.value_counts().sort_index().to_string())
    print()
    print("gap-down rows by bucket:")
    print(base["gap_bucket"].value_counts().sort_index().to_string())

    combos = [
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    rows = []
    missing_cache = 0
    bad_bars = 0
    flat_skipped = 0

    for _, row in base.iterrows():
        p = find_cache_file(row)

        if p is None:
            missing_cache += 1
            continue

        bars = normalize_bars(p)

        if bars.empty:
            bad_bars += 1
            continue

        trade_day = row["trade_date"].date()
        day = bars[bars["ts_et"].dt.date == trade_day].copy()

        if day.empty:
            bad_bars += 1
            continue

        first5 = window_ohlc(day, "09:30", "09:35")
        second5 = window_ohlc(day, "09:35", "09:40")
        third5 = window_ohlc(day, "09:40", "09:45")
        first15 = window_ohlc(day, "09:30", "09:45")

        if first5["bars"] == 0 or second5["bars"] == 0 or third5["bars"] == 0 or first15["bars"] == 0:
            bad_bars += 1
            continue

        c1 = candle_color(first5["open"], first5["close"])
        c2 = candle_color(second5["open"], second5["close"])
        c3 = candle_color(third5["open"], third5["close"])

        if "F" in [c1, c2, c3] or None in [c1, c2, c3]:
            flat_skipped += 1
            continue

        pattern = f"{c1}{c2}{c3}"

        first5_ret = pct(first5["close"], first5["open"])
        second5_ret = pct(second5["close"], second5["open"])
        third5_ret = pct(third5["close"], third5["open"])

        first15_ret = pct(first15["close"], first15["open"])
        first15_range = pct(first15["high"], first15["low"])
        first15_close_pos = close_position(first15["close"], first15["low"], first15["high"])

        first5_close_pos = close_position(first5["close"], first5["low"], first5["high"])
        second5_close_pos = close_position(second5["close"], second5["low"], second5["high"])
        third5_close_pos = close_position(third5["close"], third5["low"], third5["high"])

        base_row = row.to_dict()
        base_row.update(
            {
                "cache_file_used": str(p),
                "pattern": pattern,
                "first5_open": first5["open"],
                "first5_high": first5["high"],
                "first5_low": first5["low"],
                "first5_close": first5["close"],
                "first5_ret": first5_ret,
                "first5_close_pos": first5_close_pos,
                "second5_open": second5["open"],
                "second5_high": second5["high"],
                "second5_low": second5["low"],
                "second5_close": second5["close"],
                "second5_ret": second5_ret,
                "second5_close_pos": second5_close_pos,
                "third5_open": third5["open"],
                "third5_high": third5["high"],
                "third5_low": third5["low"],
                "third5_close": third5["close"],
                "third5_ret": third5_ret,
                "third5_close_pos": third5_close_pos,
                "first15_open": first15["open"],
                "first15_high": first15["high"],
                "first15_low": first15["low"],
                "first15_close": first15["close"],
                "first15_ret": first15_ret,
                "first15_range": first15_range,
                "first15_close_pos": first15_close_pos,
            }
        )

        entry_time = "09:45"
        entry_px = first15["close"]

        for side in ["SHORT", "LONG"]:
            for target, stop in combos:
                net, exit_type, eod, runup, drawdown = simulate_trade(
                    day=day,
                    side=side,
                    entry_time=entry_time,
                    entry_px=entry_px,
                    target_pct=target,
                    stop_pct=stop,
                )

                r = dict(base_row)
                r["side"] = side
                r["entry_time"] = entry_time
                r["entry_px"] = entry_px
                r["target_pct"] = target
                r["stop_pct"] = stop
                r["net_pct"] = net
                r["exit_type"] = exit_type
                r["raw_eod_pct"] = eod
                r["raw_runup_pct"] = runup
                r["raw_drawdown_pct"] = drawdown
                r["cost_bps"] = COST_BPS
                rows.append(r)

    print()
    print("missing_cache:", missing_cache)
    print("bad_bars:", bad_bars)
    print("flat candles skipped:", flat_skipped)

    trades = pd.DataFrame(rows)

    if trades.empty:
        print("No trades created.")
        return

    trades = add_periods(trades)

    summary = (
        trades.groupby(["gap_bucket", "side", "pattern", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["side", "median_net", "avg_net"], ascending=[True, False, False])
    )

    # Best target/stop per side + gap bucket + 3-candle pattern, requiring at least 30 trades.
    eligible = summary[summary["trades"] >= 30].copy()

    best = (
        eligible.sort_values(
            ["side", "gap_bucket", "pattern", "median_net", "avg_net"],
            ascending=[True, True, True, False, False],
        )
        .groupby(["side", "gap_bucket", "pattern"], observed=True)
        .head(1)
        .sort_values(["side", "median_net", "avg_net"], ascending=[True, False, False])
    )

    best_keys = best[["gap_bucket", "side", "pattern", "target_pct", "stop_pct"]].copy()

    best_trades = trades.merge(
        best_keys,
        on=["gap_bucket", "side", "pattern", "target_pct", "stop_pct"],
        how="inner",
    )

    period = (
        best_trades.groupby(["gap_bucket", "side", "pattern", "target_pct", "stop_pct", "period"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["side", "gap_bucket", "pattern", "period"])
    )

    yearly = (
        best_trades.groupby(["gap_bucket", "side", "pattern", "target_pct", "stop_pct", "year"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["side", "gap_bucket", "pattern", "year"])
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)
    best.to_csv(OUT_BEST, index=False)
    period.to_csv(OUT_PERIOD, index=False)
    yearly.to_csv(OUT_YEARLY, index=False)

    print()
    print("=== All gap-down $50+ 3-candle pattern summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== Best target/stop per side + gap bucket + pattern | trades >= 30 ===")
    if best.empty:
        print("No pattern had >= 30 trades.")
    else:
        print(best.to_string(index=False))

    print()
    print("=== Best pattern period validation ===")
    if period.empty:
        print("No best-pattern period validation.")
    else:
        print(period.to_string(index=False))

    print()
    print("=== This week gap-down 3-candle candidates | 3/4 exit ===")
    week = trades[
        (trades["trade_date"] >= pd.Timestamp("2026-06-29"))
        & (trades["trade_date"] <= pd.Timestamp("2026-07-03"))
        & (trades["target_pct"] == 3.0)
        & (trades["stop_pct"] == 4.0)
    ].copy()

    cols = [
        "trade_date",
        "ticker",
        "gap_bucket",
        "side",
        "pattern",
        "gap_pct",
        "first15_ret",
        "first15_close_pos",
        "first15_range",
        "entry_px",
        "net_pct",
        "exit_type",
        "raw_eod_pct",
        "raw_runup_pct",
        "raw_drawdown_pct",
    ]
    cols = [c for c in cols if c in week.columns]

    if week.empty:
        print("No this-week gap-down 3-candle candidates.")
    else:
        print(week[cols].sort_values(["trade_date", "ticker", "side", "pattern"]).to_string(index=False))

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)
    print("saved best:", OUT_BEST)
    print("saved period:", OUT_PERIOD)
    print("saved yearly:", OUT_YEARLY)


if __name__ == "__main__":
    main()
