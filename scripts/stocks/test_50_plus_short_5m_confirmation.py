from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_short_fade_expanded_post_first15_path_metrics.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

OUT_TRADES = OUT_DIR / "hot_premarket_short_5m_confirmation_trades.csv"
OUT_SUMMARY = OUT_DIR / "hot_premarket_short_5m_confirmation_summary.csv"

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

COST_BPS = 10.0


def pick_col(df: pd.DataFrame, names: list[str], required: bool = True) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    if required:
        raise SystemExit(f"Missing required column. Tried: {names}")
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


def find_cache_file(row: pd.Series) -> Path | None:
    if "cache_file" in row.index and isinstance(row["cache_file"], str):
        p = Path(row["cache_file"])
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


def short_return(entry: float, px: float) -> float:
    if pd.isna(entry) or pd.isna(px) or entry <= 0:
        return np.nan
    return (entry - px) / entry * 100.0


def simulate_short_from(day: pd.DataFrame, start_time: str, entry_px: float, target_pct: float, stop_pct: float) -> tuple[float, str, float, float, float]:
    st = pd.to_datetime(start_time).time()

    after = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty or pd.isna(entry_px) or entry_px <= 0:
        return np.nan, "no_after_entry", np.nan, np.nan, np.nan

    after["short_runup_pct"] = (entry_px - after["low"]) / entry_px * 100.0
    after["short_drawdown_pct"] = (entry_px - after["high"]) / entry_px * 100.0

    max_runup = after["short_runup_pct"].max()
    max_drawdown = after["short_drawdown_pct"].min()
    eod_ret = short_return(entry_px, float(after.iloc[-1]["close"]))

    target_hit = pd.notna(max_runup) and max_runup >= target_pct
    stop_hit = pd.notna(max_drawdown) and max_drawdown <= -stop_pct

    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous", eod_ret, max_runup, max_drawdown
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target", eod_ret, max_runup, max_drawdown
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop", eod_ret, max_runup, max_drawdown

    return eod_ret - COST_BPS / 100.0, "eod", eod_ret, max_runup, max_drawdown


def first_break_after(day: pd.DataFrame, start_time: str, level: float) -> tuple[str | None, float | None]:
    st = pd.to_datetime(start_time).time()
    after = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty or pd.isna(level):
        return None, None

    hit = after[after["low"] <= level]
    if hit.empty:
        return None, None

    t = hit.iloc[0]["ts_et"].strftime("%H:%M")
    return t, float(level)


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
            "median_short_eod_raw": g["short_eod_pct"].median(),
            "median_short_runup_raw": g["short_max_runup_pct"].median(),
            "median_short_drawdown_raw": g["short_max_drawdown_pct"].median(),
            "median_entry_delay_min": g["entry_delay_min"].median(),
            "median_gap": g["gap_pct"].median(),
            "median_pm_vs_daily": g["premarket_dollar_vs_prior_daily_avg"].median(),
            "median_open_vs_pm_high": g["regular_open_vs_premarket_high_pct"].median(),
            "median_first15_ret": g["first15_ret"].median(),
            "median_first15_close_pos": g["first15_close_pos"].median(),
            "median_first15_range": g["first15_range"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    df = pd.read_csv(INPUT)

    pm_col = pick_col(df, ["premarket_dollar_vs_prior_daily_avg"])
    open_vs_pm_high_col = pick_col(df, ["regular_open_vs_premarket_high_pct"])
    first15_ret_col = pick_col(df, ["first_15m_return_pct", "first15_close_vs_regular_open_pct"])
    first15_close_pos_col = pick_col(df, ["first15_close_position_in_range"])
    first15_range_col = pick_col(df, ["first15_range_pct"])
    first15_close_col = pick_col(df, ["first_15m_close", "first15_close"])
    first15_low_col = pick_col(df, ["first_15m_low", "first15_low"])

    needed = [
        pm_col,
        open_vs_pm_high_col,
        first15_ret_col,
        first15_close_pos_col,
        first15_range_col,
        first15_close_col,
        first15_low_col,
        "gap_pct",
        "prev_close",
    ]

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["premarket_dollar_vs_prior_daily_avg"] = df[pm_col]
    df["regular_open_vs_premarket_high_pct"] = df[open_vs_pm_high_col]
    df["first15_ret"] = df[first15_ret_col]
    df["first15_close_pos"] = df[first15_close_pos_col]
    df["first15_range"] = df[first15_range_col]
    df["first15_close"] = df[first15_close_col]
    df["first15_low"] = df[first15_low_col]

    df = df[df["prev_close"] >= 50].copy()

    base_filters = [
        {
            "base_setup": "balanced_gap_0_10",
            "gap_min": 0,
            "gap_max": 10,
            "pm_min": 0.10,
            "open_vs_pm_high_max": -2,
            "first15_ret_max": 0.0,
            "close_pos_max": 0.35,
            "range_min": 1.0,
        },
        {
            "base_setup": "clean_gap_0_5",
            "gap_min": 0,
            "gap_max": 5,
            "pm_min": 0.10,
            "open_vs_pm_high_max": -2,
            "first15_ret_max": -0.5,
            "close_pos_max": 0.35,
            "range_min": 1.0,
        },
        {
            "base_setup": "strict_gap_0_5",
            "gap_min": 0,
            "gap_max": 5,
            "pm_min": 0.10,
            "open_vs_pm_high_max": -2,
            "first15_ret_max": -0.5,
            "close_pos_max": 0.25,
            "range_min": 2.0,
        },
    ]

    combos = [
        (2.5, 4.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    rows = []

    for bf in base_filters:
        sub = df[
            (df["gap_pct"] >= bf["gap_min"])
            & (df["gap_pct"] <= bf["gap_max"])
            & (df["premarket_dollar_vs_prior_daily_avg"] >= bf["pm_min"])
            & (df["regular_open_vs_premarket_high_pct"] <= bf["open_vs_pm_high_max"])
            & (df["first15_ret"] <= bf["first15_ret_max"])
            & (df["first15_close_pos"] <= bf["close_pos_max"])
            & (df["first15_range"] >= bf["range_min"])
        ].copy()

        print()
        print("base setup:", bf["base_setup"])
        print("base rows:", len(sub))

        for i, (_, row) in enumerate(sub.iterrows(), start=1):
            p = find_cache_file(row)
            if p is None:
                continue

            bars = normalize_bars(p)
            if bars.empty:
                continue

            trade_day = pd.to_datetime(row["trade_date"]).date()
            day = bars[bars["ts_et"].dt.date == trade_day].copy()
            if day.empty:
                continue

            first15_close = row["first15_close"]
            first15_low = row["first15_low"]

            w1 = window_ohlc(day, "09:45", "09:50")
            w2 = window_ohlc(day, "09:50", "09:55")
            w3 = window_ohlc(day, "09:55", "10:00")
            w15 = window_ohlc(day, "09:45", "10:00")

            entry_modes = []

            # Baseline.
            entry_modes.append(
                {
                    "entry_mode": "immediate_0945",
                    "entry_time": "09:45",
                    "entry_px": first15_close,
                    "entry_delay_min": 0,
                    "passed_confirmation": True,
                }
            )

            if w1["bars"] > 0 and w1["close"] < first15_close:
                entry_modes.append(
                    {
                        "entry_mode": "confirm_0950_close_below_entry",
                        "entry_time": "09:50",
                        "entry_px": w1["close"],
                        "entry_delay_min": 5,
                        "passed_confirmation": True,
                    }
                )

            if w1["bars"] > 0 and w1["close"] < first15_close and w1["close"] < w1["open"]:
                entry_modes.append(
                    {
                        "entry_mode": "confirm_0950_red_and_below_entry",
                        "entry_time": "09:50",
                        "entry_px": w1["close"],
                        "entry_delay_min": 5,
                        "passed_confirmation": True,
                    }
                )

            if (
                w1["bars"] > 0
                and w2["bars"] > 0
                and w1["close"] < first15_close
                and w2["close"] < w1["close"]
            ):
                entry_modes.append(
                    {
                        "entry_mode": "confirm_0955_two_lower_5m",
                        "entry_time": "09:55",
                        "entry_px": w2["close"],
                        "entry_delay_min": 10,
                        "passed_confirmation": True,
                    }
                )

            if w15["bars"] > 0 and w15["close"] < first15_close:
                entry_modes.append(
                    {
                        "entry_mode": "confirm_1000_second15_lower",
                        "entry_time": "10:00",
                        "entry_px": w15["close"],
                        "entry_delay_min": 15,
                        "passed_confirmation": True,
                    }
                )

            break_time, break_px = first_break_after(day, "09:45", first15_low)
            if break_time is not None:
                hh, mm = break_time.split(":")
                delay = (int(hh) * 60 + int(mm)) - (9 * 60 + 45)
                entry_modes.append(
                    {
                        "entry_mode": "break_first15_low",
                        "entry_time": break_time,
                        "entry_px": break_px,
                        "entry_delay_min": delay,
                        "passed_confirmation": True,
                    }
                )

            for mode in entry_modes:
                for target, stop in combos:
                    net, exit_type, eod_ret, runup, drawdown = simulate_short_from(
                        day=day,
                        start_time=mode["entry_time"],
                        entry_px=mode["entry_px"],
                        target_pct=target,
                        stop_pct=stop,
                    )

                    r = row.to_dict()
                    r.update(bf)
                    r.update(mode)
                    r["target_pct"] = target
                    r["stop_pct"] = stop
                    r["net_pct"] = net
                    r["exit_type"] = exit_type
                    r["short_eod_pct"] = eod_ret
                    r["short_max_runup_pct"] = runup
                    r["short_max_drawdown_pct"] = drawdown
                    r["cost_bps"] = COST_BPS
                    rows.append(r)

    trades = pd.DataFrame(rows)

    if trades.empty:
        print("No trades created.")
        return

    summary = (
        trades.groupby(
            ["base_setup", "entry_mode", "target_pct", "stop_pct"],
            observed=True,
        )
        .apply(summarize)
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=False)
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print()
    print("=== Top summary | trades >= 30 ===")
    display = summary[summary["trades"] >= 30].copy()
    print(display.head(60).to_string(index=False))

    print()
    print("=== Entry mode comparison | 4 target / 5 stop ===")
    comp = summary[
        (summary["target_pct"] == 4.0)
        & (summary["stop_pct"] == 5.0)
    ].copy()
    print(
        comp.sort_values(["base_setup", "avg_net"], ascending=[True, False])
        .to_string(index=False)
    )

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
