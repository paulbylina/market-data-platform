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

OUT_TRADES = OUT_DIR / "long_first5_structures_fullsample_trades.csv"
OUT_SUMMARY = OUT_DIR / "long_first5_structures_fullsample_summary.csv"
OUT_PERIOD = OUT_DIR / "long_first5_structures_fullsample_period_summary.csv"
OUT_YEARLY = OUT_DIR / "long_first5_structures_fullsample_yearly_summary.csv"

COST_BPS = 10.0


def pick_col(df: pd.DataFrame, names: list[str], required: bool = True) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    if required:
        raise SystemExit(f"Missing required column. Tried: {names}")
    return None


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
    return out.sort_values("ts_et").reset_index(drop=True)


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

    runup = after["long_runup_pct"].max()
    drawdown = after["long_drawdown_pct"].min()
    eod = (after.iloc[-1]["close"] / entry_px - 1.0) * 100.0

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

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
            "dates": g["trade_date"].dt.date.nunique(),
            "tickers": g["ticker"].nunique(),
            "avg_net": vals.mean(),
            "median_net": vals.median(),
            "win_rate": (vals > 0).mean() * 100,
            "target_rate": g["exit_type"].astype(str).str.contains("target", na=False).mean() * 100,
            "stop_rate": g["exit_type"].astype(str).str.contains("stop", na=False).mean() * 100,
            "eod_rate": (g["exit_type"].astype(str) == "eod").mean() * 100,
            "median_long_eod_raw": g["long_eod_pct"].median(),
            "median_long_runup_raw": g["long_max_runup_pct"].median(),
            "median_long_drawdown_raw": g["long_max_drawdown_pct"].median(),
            "median_gap": g["gap_pct"].median(),
            "median_pm_vs_daily": g["premarket_dollar_vs_prior_daily_avg"].median(),
            "median_first15_ret": g["first15_ret"].median(),
            "median_first15_close_pos": g["first15_close_pos"].median(),
            "median_first15_range": g["first15_range"].median(),
            "median_first5_ret": g["first5_ret"].median(),
            "median_first5_close_pos": g["first5_close_pos"].median(),
            "median_first5_range": g["first5_range"].median(),
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
        [
            "train_2016_2022",
            "validation_2023_2024",
            "test_2025_2026",
        ],
        default="other",
    )

    return trades


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()

    print("input:", INPUT)
    print("rows:", len(df))
    print("date range:", df["trade_date"].min().date(), "to", df["trade_date"].max().date())

    prev_close_col = pick_col(df, ["prev_close"])
    gap_col = pick_col(df, ["gap_pct"])
    pm_col = pick_col(df, ["premarket_dollar_vs_prior_daily_avg", "pre_market_dollar_vs_prior_daily_avg"])
    first15_ret_col = pick_col(df, ["first_15m_return_pct", "first15_close_vs_regular_open_pct", "first15_ret"])
    first15_close_pos_col = pick_col(df, ["first15_close_position_in_range", "first15_close_pos"])
    first15_range_col = pick_col(df, ["first15_range_pct", "first15_range"])
    first15_rvol_col = pick_col(
        df,
        ["first15_dollar_rvol_20d", "first_15m_dollar_rvol_20d", "first15_dollar_volume_rvol_20d"],
        required=False,
    )

    for c in [prev_close_col, gap_col, pm_col, first15_ret_col, first15_close_pos_col, first15_range_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["prev_close"] = df[prev_close_col]
    df["gap_pct"] = df[gap_col]
    df["premarket_dollar_vs_prior_daily_avg"] = df[pm_col]
    df["first15_ret"] = df[first15_ret_col]
    df["first15_close_pos"] = df[first15_close_pos_col]
    df["first15_range"] = df[first15_range_col]

    if first15_rvol_col is not None:
        df[first15_rvol_col] = pd.to_numeric(df[first15_rvol_col], errors="coerce")
        df["first15_rvol"] = df[first15_rvol_col]
    else:
        df["first15_rvol"] = np.nan

    # Full ABC + gap long base.
    # This is intentionally transparent. If 2024-2026 count does not line up with old 346-row file,
    # we can tune the exact thresholds after seeing the printed counts.
    base_mask = (
        (df["prev_close"] >= 50)
        & (df["gap_pct"] >= 0)
        & (df["gap_pct"] <= 5)
        & (df["premarket_dollar_vs_prior_daily_avg"] <= 0.03)
        & (df["first15_ret"] >= 1.0)
        & (df["first15_ret"] <= 4.0)
        & (df["first15_close_pos"] >= 0.75)
        & (df["first15_range"] >= 2.0)
        & (df["first15_range"] <= 6.0)
    )

    # Optional stricter live-quality variant if the column exists.
    base_variants = {
        "abc_gap_base": base_mask,
    }

    if first15_rvol_col is not None:
        base_variants["abc_gap_rvol3"] = base_mask & (df["first15_rvol"] >= 3.0)

    combos = [
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    rows = []

    for base_name, mask in base_variants.items():
        sub = df[mask].copy()

        print()
        print("base variant:", base_name)
        print("base rows:", len(sub))
        print("base rows by year:")
        print(sub["trade_date"].dt.year.value_counts().sort_index().to_string())

        old_window = sub[
            (sub["trade_date"] >= pd.Timestamp("2024-01-01"))
            & (sub["trade_date"] <= pd.Timestamp("2026-07-02"))
        ]
        print("base rows 2024-2026 window:", len(old_window))

        missing_cache = 0
        bad_bars = 0

        for _, row in sub.iterrows():
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

            if first5["bars"] == 0 or second5["bars"] == 0 or third5["bars"] == 0:
                continue

            first5_ret = pct(first5["close"], first5["open"])
            first5_range = pct(first5["high"], first5["low"])
            first5_close_pos = close_position(first5["close"], first5["low"], first5["high"])

            second5_pullback_from_first5_close_pct = pct(second5["close"], first5["close"])
            third5_reclaim_vs_first5_close_pct = pct(third5["close"], first5["close"])

            base = row.to_dict()
            base.update(
                {
                    "base_variant": base_name,
                    "cache_file_used": str(p),
                    "first5_open": first5["open"],
                    "first5_high": first5["high"],
                    "first5_low": first5["low"],
                    "first5_close": first5["close"],
                    "first5_ret": first5_ret,
                    "first5_range": first5_range,
                    "first5_close_pos": first5_close_pos,
                    "second5_open": second5["open"],
                    "second5_high": second5["high"],
                    "second5_low": second5["low"],
                    "second5_close": second5["close"],
                    "third5_open": third5["open"],
                    "third5_high": third5["high"],
                    "third5_low": third5["low"],
                    "third5_close": third5["close"],
                    "second5_pullback_from_first5_close_pct": second5_pullback_from_first5_close_pct,
                    "third5_reclaim_vs_first5_close_pct": third5_reclaim_vs_first5_close_pct,
                }
            )

            setup_passes = []

            # LONG-A: strong first5 and three higher 5m closes by 09:45.
            if (
                first5_ret >= 0.5
                and first5_close_pos >= 0.65
                and second5["close"] > first5["close"]
                and third5["close"] > second5["close"]
            ):
                setup_passes.append("LONG-A_three_higher_5m")

            # LONG-B: strong first5, shallow pullback, reclaim by 09:45.
            if (
                first5_ret >= 0.5
                and first5_close_pos >= 0.65
                and second5["close"] < first5["close"]
                and second5_pullback_from_first5_close_pct >= -0.75
                and third5["close"] > first5["close"]
            ):
                setup_passes.append("LONG-B_shallow_pullback_reclaim")

            for setup_name in setup_passes:
                entry_time = "09:45"
                entry_px = third5["close"]

                for target, stop in combos:
                    net, exit_type, eod, runup, drawdown = simulate_long_from(
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
                    r["target_pct"] = target
                    r["stop_pct"] = stop
                    r["net_pct"] = net
                    r["exit_type"] = exit_type
                    r["long_eod_pct"] = eod
                    r["long_max_runup_pct"] = runup
                    r["long_max_drawdown_pct"] = drawdown
                    r["cost_bps"] = COST_BPS
                    rows.append(r)

        print("missing_cache:", missing_cache)
        print("bad_bars:", bad_bars)

    trades = pd.DataFrame(rows)

    if trades.empty:
        print("No full-sample long trades created.")
        return

    trades = add_periods(trades)

    summary = (
        trades.groupby(["base_variant", "setup_name", "target_pct", "stop_pct"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["base_variant", "setup_name", "median_net", "avg_net"], ascending=[True, True, False, False])
    )

    period = (
        trades[
            ((trades["setup_name"] == "LONG-A_three_higher_5m") & (trades["target_pct"] == 3.0) & (trades["stop_pct"] == 4.0))
            | ((trades["setup_name"] == "LONG-B_shallow_pullback_reclaim") & (trades["target_pct"] == 3.0) & (trades["stop_pct"] == 4.0))
        ]
        .groupby(["base_variant", "setup_name", "period"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["base_variant", "setup_name", "period"])
    )

    yearly = (
        trades[
            ((trades["setup_name"] == "LONG-A_three_higher_5m") & (trades["target_pct"] == 3.0) & (trades["stop_pct"] == 4.0))
            | ((trades["setup_name"] == "LONG-B_shallow_pullback_reclaim") & (trades["target_pct"] == 3.0) & (trades["stop_pct"] == 4.0))
        ]
        .groupby(["base_variant", "setup_name", "year"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["base_variant", "setup_name", "year"])
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)
    period.to_csv(OUT_PERIOD, index=False)
    yearly.to_csv(OUT_YEARLY, index=False)

    print()
    print("=== Full-sample long first5 structure summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== Full-sample long period validation | 3/4 exit ===")
    print(period.to_string(index=False))

    print()
    print("=== Full-sample long yearly validation | 3/4 exit ===")
    print(yearly.to_string(index=False))

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)
    print("saved period:", OUT_PERIOD)
    print("saved yearly:", OUT_YEARLY)


if __name__ == "__main__":
    main()
