from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_context_scored_daily_best_2024_2026.csv"
)

OUT_TRADES = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "abc_gap_early_bar_behavior_trades_2024_2026.csv"
)

OUT_SUMMARY = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "abc_gap_early_bar_behavior_summary_2024_2026.csv"
)


TARGET_PCT = 3.0
STOP_PCT = 4.0
COST_BPS = 10.0


def normalize_bars(path: str) -> pd.DataFrame:
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


def window_metrics(bars: pd.DataFrame, entry: float, start: str, end: str) -> dict:
    start_t = pd.to_datetime(start).time()
    end_t = pd.to_datetime(end).time()

    sub = bars[
        (bars["ts_et"].dt.time >= start_t)
        & (bars["ts_et"].dt.time < end_t)
    ].copy()

    if sub.empty or entry <= 0:
        return {
            f"{start}_{end}_runup_pct": np.nan,
            f"{start}_{end}_drawdown_pct": np.nan,
            f"{start}_{end}_close_pct": np.nan,
            f"{start}_{end}_body_pct": np.nan,
            f"{start}_{end}_close_position": np.nan,
        }

    o = float(sub.iloc[0]["open"])
    h = float(sub["high"].max())
    l = float(sub["low"].min())
    c = float(sub.iloc[-1]["close"])

    return {
        f"{start}_{end}_runup_pct": (h / entry - 1.0) * 100,
        f"{start}_{end}_drawdown_pct": (l / entry - 1.0) * 100,
        f"{start}_{end}_close_pct": (c / entry - 1.0) * 100,
        f"{start}_{end}_body_pct": (c / o - 1.0) * 100 if o > 0 else np.nan,
        f"{start}_{end}_close_position": (c - l) / (h - l) if h > l else np.nan,
    }


def intraday_path_metrics(row: pd.Series) -> dict:
    cache_file = row.get("cache_file", None)
    trade_date = str(row["trade_date"])

    if pd.isna(cache_file) or not Path(str(cache_file)).exists():
        return {"early_status": "missing_cache"}

    bars = normalize_bars(str(cache_file))
    if bars.empty:
        return {"early_status": "bad_bars"}

    trade_day = pd.to_datetime(trade_date).date()
    day = bars[bars["ts_et"].dt.date == trade_day].copy()

    after = day[
        (day["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty:
        return {"early_status": "no_after_entry"}

    entry = pd.to_numeric(row.get("entry_px", np.nan), errors="coerce")
    if pd.isna(entry) or entry <= 0:
        entry = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")
    if pd.isna(entry) or entry <= 0:
        entry = float(after.iloc[0]["open"])

    if pd.isna(entry) or entry <= 0:
        return {"early_status": "bad_entry"}

    target_px = entry * (1 + TARGET_PCT / 100.0)
    stop_px = entry * (1 - STOP_PCT / 100.0)

    out = {
        "early_status": "ok",
        "entry_px_check": entry,
    }

    # 15-minute bars after entry.
    windows = [
        ("09:45", "10:00"),
        ("10:00", "10:15"),
        ("10:15", "10:30"),
        ("09:45", "10:15"),
        ("09:45", "10:30"),
    ]

    for start, end in windows:
        out.update(window_metrics(after, entry, start, end))

    # First 1m and first 2m after entry.
    first_1m = after.head(1)
    first_2m = after.head(2)
    first_5m = after.head(5)

    for label, sub in [("first1m", first_1m), ("first2m", first_2m), ("first5m", first_5m)]:
        if sub.empty:
            out[f"{label}_runup_pct"] = np.nan
            out[f"{label}_drawdown_pct"] = np.nan
            out[f"{label}_close_pct"] = np.nan
        else:
            out[f"{label}_runup_pct"] = (sub["high"].max() / entry - 1.0) * 100
            out[f"{label}_drawdown_pct"] = (sub["low"].min() / entry - 1.0) * 100
            out[f"{label}_close_pct"] = (sub.iloc[-1]["close"] / entry - 1.0) * 100

    # Target / stop timing using 1m bars.
    target_time = None
    stop_time = None
    first_exit_type = None
    first_exit_time = None
    first_exit_net = None

    for i, (_, bar) in enumerate(after.iterrows(), start=1):
        high = float(bar["high"])
        low = float(bar["low"])
        ts = bar["ts_et"]

        target_hit = high >= target_px
        stop_hit = low <= stop_px

        if target_hit and target_time is None:
            target_time = ts
            out["bars_to_target_1m"] = i
            out["minutes_to_target"] = (ts - after.iloc[0]["ts_et"]).total_seconds() / 60.0

        if stop_hit and stop_time is None:
            stop_time = ts
            out["bars_to_stop_1m"] = i
            out["minutes_to_stop"] = (ts - after.iloc[0]["ts_et"]).total_seconds() / 60.0

        if first_exit_type is None:
            if target_hit and stop_hit:
                first_exit_type = "stop_ambiguous"
                first_exit_time = ts
                first_exit_net = -STOP_PCT - COST_BPS / 100.0
            elif target_hit:
                first_exit_type = "target"
                first_exit_time = ts
                first_exit_net = TARGET_PCT - COST_BPS / 100.0
            elif stop_hit:
                first_exit_type = "stop"
                first_exit_time = ts
                first_exit_net = -STOP_PCT - COST_BPS / 100.0

    out["target_hit"] = target_time is not None
    out["stop_hit"] = stop_time is not None

    out["target_in_first_15m_after_entry"] = (
        out.get("minutes_to_target", np.nan) <= 15 if target_time is not None else False
    )
    out["target_in_first_30m_after_entry"] = (
        out.get("minutes_to_target", np.nan) <= 30 if target_time is not None else False
    )

    out["stop_in_first_15m_after_entry"] = (
        out.get("minutes_to_stop", np.nan) <= 15 if stop_time is not None else False
    )
    out["stop_in_first_30m_after_entry"] = (
        out.get("minutes_to_stop", np.nan) <= 30 if stop_time is not None else False
    )

    if first_exit_type is None:
        eod = float(after.iloc[-1]["close"])
        out["first_exit_type"] = "eod"
        out["first_exit_net"] = (eod / entry - 1.0) * 100 - COST_BPS / 100.0
        out["minutes_to_first_exit"] = (after.iloc[-1]["ts_et"] - after.iloc[0]["ts_et"]).total_seconds() / 60.0
    else:
        out["first_exit_type"] = first_exit_type
        out["first_exit_net"] = first_exit_net
        out["minutes_to_first_exit"] = (first_exit_time - after.iloc[0]["ts_et"]).total_seconds() / 60.0

    return out


def summarize(g: pd.DataFrame) -> pd.Series:
    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].nunique(),
            "tickers": g["ticker"].nunique(),

            "avg_net": g["first_exit_net"].mean(),
            "median_net": g["first_exit_net"].median(),
            "win_rate": (g["first_exit_net"] > 0).mean() * 100,

            "target_rate": g["target_hit"].mean() * 100,
            "stop_rate": g["stop_hit"].mean() * 100,
            "target_first15_rate": g["target_in_first_15m_after_entry"].mean() * 100,
            "target_first30_rate": g["target_in_first_30m_after_entry"].mean() * 100,
            "stop_first15_rate": g["stop_in_first_15m_after_entry"].mean() * 100,
            "stop_first30_rate": g["stop_in_first_30m_after_entry"].mean() * 100,

            "median_minutes_to_target": g.loc[g["target_hit"], "minutes_to_target"].median(),
            "median_minutes_to_stop": g.loc[g["stop_hit"], "minutes_to_stop"].median(),

            "median_first1m_drawdown": g["first1m_drawdown_pct"].median(),
            "median_first2m_drawdown": g["first2m_drawdown_pct"].median(),
            "median_first5m_drawdown": g["first5m_drawdown_pct"].median(),

            "median_bar1_drawdown": g["09:45_10:00_drawdown_pct"].median(),
            "median_bar1_runup": g["09:45_10:00_runup_pct"].median(),
            "median_bar1_close": g["09:45_10:00_close_pct"].median(),

            "median_bar2_drawdown": g["10:00_10:15_drawdown_pct"].median(),
            "median_bar2_runup": g["10:00_10:15_runup_pct"].median(),
            "median_bar2_close": g["10:00_10:15_close_pct"].median(),

            "bar1_dd_le_minus1_rate": (g["09:45_10:00_drawdown_pct"] <= -1).mean() * 100,
            "bar1_dd_le_minus2_rate": (g["09:45_10:00_drawdown_pct"] <= -2).mean() * 100,
            "bar1_green_close_rate": (g["09:45_10:00_close_pct"] > 0).mean() * 100,
        }
    )


def main() -> None:
    df = pd.read_csv(INPUT)

    numeric_cols = [
        "gap_pct",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    pm = df["premarket_dollar_vs_prior_daily_avg"]
    prior = df["prior_day_last15_dollar_rvol_20d"]
    gap = df["gap_pct"]
    ret = df["first_15m_return_pct"]
    rng = df["first15_range_pct"]
    close_pos = df["first15_close_position_in_range"]

    strict_shape = (
        (pm <= 0.01)
        & (gap >= 0) & (gap <= 5)
        & (ret >= 1) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.90)
    )

    strong_shape = (
        (pm <= 0.03)
        & (gap >= 0) & (gap <= 5)
        & (ret >= 2) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.75)
    )

    extended_shape = (
        (pm <= 0.01)
        & (gap >= 0) & (gap <= 5)
        & (ret >= 1) & (ret < 4)
        & (rng >= 4) & (rng < 6)
        & (close_pos >= 0.75)
    )

    base = df[strict_shape | strong_shape | extended_shape].copy()

    base["abc"] = np.select(
        [
            base["prior_day_last15_dollar_rvol_20d"] >= 3,
            (base["prior_day_last15_dollar_rvol_20d"] >= 1.5)
            & (base["prior_day_last15_dollar_rvol_20d"] < 3),
        ],
        ["A", "B"],
        default="C",
    )

    print("ABC + gap base trades:", len(base))

    metrics = base.apply(intraday_path_metrics, axis=1, result_type="expand")
    out = pd.concat([base.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)
    out = out[out["early_status"].eq("ok")].copy()

    summary = (
        out.groupby("abc", observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values("abc")
    )

    out.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print("with early metrics:", len(out))
    print()
    print("=== ABC early bar / target timing summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== Hit target inside first 1-2 post-entry 15m bars ===")
    quick = out.groupby("abc", observed=True).agg(
        trades=("ticker", "size"),
        target_in_first_15m=("target_in_first_15m_after_entry", "mean"),
        target_in_first_30m=("target_in_first_30m_after_entry", "mean"),
        bar1_dd_median=("09:45_10:00_drawdown_pct", "median"),
        bar1_runup_median=("09:45_10:00_runup_pct", "median"),
        bar1_close_median=("09:45_10:00_close_pct", "median"),
        first2m_dd_median=("first2m_drawdown_pct", "median"),
        first5m_dd_median=("first5m_drawdown_pct", "median"),
        minutes_to_target_median=("minutes_to_target", "median"),
    ).reset_index()

    quick["target_in_first_15m"] *= 100
    quick["target_in_first_30m"] *= 100

    print(quick.to_string(index=False))

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
