from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_context_scored_daily_best_2024_2026.csv"
)

OUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "abc_gap_second15_behavior_2024_2026.csv"
)


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
    for a, b in [("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")]:
        if a in out.columns and b not in out.columns:
            rename[a] = b
    out = out.rename(columns=rename)

    for c in ["open", "high", "low", "close"]:
        if c not in out.columns:
            return pd.DataFrame()
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["ts_et", "open", "high", "low", "close"]).copy()
    return out.sort_values("ts_et")


def second15_metrics(row: pd.Series) -> dict:
    cache_file = row.get("cache_file", None)
    trade_date = str(row["trade_date"])

    if pd.isna(cache_file) or not Path(str(cache_file)).exists():
        return {"second15_status": "missing_cache"}

    bars = normalize_bars(str(cache_file))
    if bars.empty:
        return {"second15_status": "bad_bars"}

    trade_day = pd.to_datetime(trade_date).date()
    day = bars[bars["ts_et"].dt.date == trade_day].copy()

    second = day[
        (day["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day["ts_et"].dt.time < pd.to_datetime("10:00").time())
    ].copy()

    if second.empty:
        return {"second15_status": "missing_second15"}

    entry = pd.to_numeric(row.get("entry_px", np.nan), errors="coerce")
    if pd.isna(entry) or entry <= 0:
        entry = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")

    if pd.isna(entry) or entry <= 0:
        return {"second15_status": "bad_entry"}

    o = second.iloc[0]["open"]
    h = second["high"].max()
    l = second["low"].min()
    c = second.iloc[-1]["close"]

    runup = (h / entry - 1.0) * 100
    drawdown = (l / entry - 1.0) * 100
    close_pct = (c / entry - 1.0) * 100
    body_pct = (c / o - 1.0) * 100 if o > 0 else np.nan
    range_pct = (h / l - 1.0) * 100 if l > 0 else np.nan
    close_pos = (c - l) / (h - l) if h > l else np.nan

    return {
        "second15_status": "ok",
        "second15_open": o,
        "second15_high": h,
        "second15_low": l,
        "second15_close": c,
        "second15_runup_pct": runup,
        "second15_drawdown_pct": drawdown,
        "second15_close_pct": close_pct,
        "second15_body_pct": body_pct,
        "second15_range_pct": range_pct,
        "second15_close_position": close_pos,
    }


def second15_bucket(row: pd.Series) -> str:
    dd = row["second15_drawdown_pct"]
    close = row["second15_close_pct"]

    if pd.isna(dd) or pd.isna(close):
        return "missing"

    if dd <= -3:
        return "very_bad_dd_le_-3"
    if dd <= -2:
        return "bad_dd_-2_to_-3"
    if close < 0 and dd <= -1:
        return "weak_close_red_dd"
    if close >= 0 and dd > -1:
        return "clean_close_green_dd_lt_1"
    if close >= -0.25 and dd > -1.5:
        return "ok_mild_pullback"
    return "mixed"


def simulate_exit(row: pd.Series, target_pct: float = 3.0, stop_pct: float = 4.0) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    if target_hit and stop_hit:
        return -stop_pct - 0.10, "stop_ambiguous"
    if target_hit:
        return target_pct - 0.10, "target"
    if stop_hit:
        return -stop_pct - 0.10, "stop"
    return eod - 0.10, "eod"


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
            "median_second15_runup": g["second15_runup_pct"].median(),
            "median_second15_drawdown": g["second15_drawdown_pct"].median(),
            "median_second15_close": g["second15_close_pct"].median(),
            "median_eod_raw": g["long_eod_pct"].median(),
            "median_runup_raw": g["long_max_runup_pct"].median(),
            "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
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

    print("base ABC + gap trades:", len(base))

    metrics = base.apply(second15_metrics, axis=1, result_type="expand")
    base = pd.concat([base.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)

    base = base[base["second15_status"].eq("ok")].copy()

    exits = base.apply(simulate_exit, axis=1, result_type="expand")
    base["net_pct"] = exits[0]
    base["exit_type"] = exits[1]

    base["second15_bucket"] = base.apply(second15_bucket, axis=1)

    print("with second15 metrics:", len(base))
    print()

    print("=== Second15 bucket performance | ABC + gap | 3/4 stop-first ===")
    out = (
        base.groupby(["abc", "second15_bucket"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["abc", "median_net"], ascending=[True, False])
    )
    print(out.to_string(index=False))

    print()
    print("=== Simple second15 filters ===")
    tests = {
        "all": base.index == base.index,
        "second close >= 0": base["second15_close_pct"] >= 0,
        "second drawdown > -1": base["second15_drawdown_pct"] > -1,
        "second drawdown > -1.5": base["second15_drawdown_pct"] > -1.5,
        "second close >= 0 and drawdown > -1": (base["second15_close_pct"] >= 0) & (base["second15_drawdown_pct"] > -1),
        "second close >= -0.25 and drawdown > -1.5": (base["second15_close_pct"] >= -0.25) & (base["second15_drawdown_pct"] > -1.5),
        "second drawdown <= -2": base["second15_drawdown_pct"] <= -2,
    }

    rows = []
    for name, mask in tests.items():
        g = base[mask].copy()
        if g.empty:
            continue
        row = summarize(g).to_dict()
        row["filter"] = name
        rows.append(row)

    filt = pd.DataFrame(rows).sort_values(["median_net", "avg_net"], ascending=False)
    print(filt.to_string(index=False))

    base.to_csv(OUT, index=False)
    print()
    print("saved:", OUT)


if __name__ == "__main__":
    main()
