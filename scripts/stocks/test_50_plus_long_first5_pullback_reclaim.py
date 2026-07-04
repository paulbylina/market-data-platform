from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "long_first5_three_up_structure_trades_2024_2026.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

OUT_TRADES = OUT_DIR / "long_first5_pullback_reclaim_trades_2024_2026.csv"
OUT_SUMMARY = OUT_DIR / "long_first5_pullback_reclaim_summary_2024_2026.csv"

COST_BPS = 10.0


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
    return out.sort_values("ts_et").reset_index(drop=True)


def simulate_long_from(day: pd.DataFrame, start_time: str, entry_px: float, target_pct: float, stop_pct: float):
    st = pd.to_datetime(start_time).time()

    after = day[
        (day["ts_et"].dt.time >= st)
        & (day["ts_et"].dt.time < pd.to_datetime("16:00").time())
    ].copy()

    if after.empty or pd.isna(entry_px) or entry_px <= 0:
        return np.nan, "no_after_entry", np.nan, np.nan, np.nan

    after["runup_pct"] = (after["high"] / entry_px - 1.0) * 100.0
    after["drawdown_pct"] = (after["low"] / entry_px - 1.0) * 100.0

    runup = after["runup_pct"].max()
    drawdown = after["drawdown_pct"].min()
    eod = (after.iloc[-1]["close"] / entry_px - 1.0) * 100.0

    target_hit = runup >= target_pct
    stop_hit = drawdown <= -stop_pct

    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous", eod, runup, drawdown
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target", eod, runup, drawdown
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop", eod, runup, drawdown

    return eod - COST_BPS / 100.0, "eod", eod, runup, drawdown


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
            "median_eod_raw": g["long_eod_pct"].median(),
            "median_runup_raw": g["long_max_runup_pct"].median(),
            "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
            "median_gap": g["gap_pct"].median() if "gap_pct" in g.columns else np.nan,
            "median_first5_ret": g["first5_ret"].median(),
            "median_first5_close_pos": g["first5_close_pos"].median(),
            "median_pullback_pct": g["second5_pullback_from_first5_close_pct"].median(),
            "median_reclaim_pct": g["third5_reclaim_vs_first5_close_pct"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    df = pd.read_csv(INPUT)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)

    # One row per ticker/date from the first5 green structure population.
    df = df[
        (df["setup_name"] == "first5_green_structure")
        & (df["target_pct"] == 3.0)
        & (df["stop_pct"] == 4.0)
    ].drop_duplicates(subset=["ticker", "trade_date"]).copy()

    for c in [
        "first5_close",
        "second5_close",
        "third5_close",
        "first5_ret",
        "first5_close_pos",
        "first5_range",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["second5_pullback_from_first5_close_pct"] = (df["second5_close"] / df["first5_close"] - 1.0) * 100.0
    df["third5_reclaim_vs_first5_close_pct"] = (df["third5_close"] / df["first5_close"] - 1.0) * 100.0

    setups = {
        "pullback_reclaim": (
            (df["second5_close"] < df["first5_close"])
            & (df["third5_close"] > df["first5_close"])
        ),
        "shallow_pullback_reclaim": (
            (df["second5_close"] < df["first5_close"])
            & (df["second5_pullback_from_first5_close_pct"] >= -0.75)
            & (df["third5_close"] > df["first5_close"])
        ),
        "strong_reclaim": (
            (df["second5_close"] < df["first5_close"])
            & (df["third5_reclaim_vs_first5_close_pct"] >= 0.25)
        ),
    }

    combos = [
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    rows = []

    for setup_name, mask in setups.items():
        sub = df[mask].copy()
        print(setup_name, "rows:", len(sub))

        for _, row in sub.iterrows():
            p = Path(row["cache_file_used"])
            if not p.exists():
                continue

            bars = normalize_bars(p)
            trade_day = pd.to_datetime(row["trade_date"]).date()
            day = bars[bars["ts_et"].dt.date == trade_day].copy()

            if day.empty:
                continue

            entry_time = "09:45"
            entry_px = row["third5_close"]

            for target, stop in combos:
                net, exit_type, eod, runup, drawdown = simulate_long_from(
                    day=day,
                    start_time=entry_time,
                    entry_px=entry_px,
                    target_pct=target,
                    stop_pct=stop,
                )

                r = row.to_dict()
                r["pullback_setup"] = setup_name
                r["entry_time"] = entry_time
                r["entry_px"] = entry_px
                r["target_pct"] = target
                r["stop_pct"] = stop
                r["net_pct"] = net
                r["exit_type"] = exit_type
                r["long_eod_pct"] = eod
                r["long_max_runup_pct"] = runup
                r["long_max_drawdown_pct"] = drawdown
                r["cost_bps"] = COST_BPS
                rows.append(r)

    trades = pd.DataFrame(rows)

    if trades.empty:
        print("No trades created.")
        return

    summary = (
        trades.groupby(["pullback_setup", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=False)
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print()
    print("=== Pullback reclaim summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== This week pullback reclaim candidates ===")
    week = trades[
        (trades["trade_date"] >= "2026-06-29")
        & (trades["trade_date"] <= "2026-07-03")
        & (trades["target_pct"] == 2.0)
        & (trades["stop_pct"] == 2.5)
    ].copy()

    cols = [
        "trade_date",
        "ticker",
        "pullback_setup",
        "gap_pct",
        "first5_ret",
        "first5_close_pos",
        "second5_pullback_from_first5_close_pct",
        "third5_reclaim_vs_first5_close_pct",
        "entry_px",
        "net_pct",
        "exit_type",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]
    cols = [c for c in cols if c in week.columns]

    if week.empty:
        print("No this-week pullback reclaim candidates.")
    else:
        print(week[cols].sort_values(["trade_date", "ticker", "pullback_setup"]).to_string(index=False))

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
