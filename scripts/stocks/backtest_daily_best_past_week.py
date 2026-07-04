from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from scripts.stocks.build_10y_high_price_short_fade_expanded_features import get_api_key
from scripts.stocks.add_first15_opening_rvol_for_date import fetch_1m_range


CACHE_DIR = Path("data/cache/massive/daily_best_past_week_1m")

PICKS = [
    ("2026-06-29", "KNSA", "near_Aplus_extended"),
    ("2026-06-30", "GPOR", "true_Aplus"),
    ("2026-07-01", "IQV", "daily_best"),
    ("2026-07-02", "DCO", "conservative_daily_best"),
    # Alternative for 2026-07-02:
    # ("2026-07-02", "FRHC", "higher_prior_last15_alternative"),
]


def get_rth_bars(ticker: str, date: str, api_key: str) -> pd.DataFrame:
    bars = fetch_1m_range(
        ticker=ticker,
        start_date=date,
        end_date=date,
        api_key=api_key,
        cache_dir=CACHE_DIR,
        sleep_seconds=0.05,
    )

    if bars.empty:
        return bars

    out = bars.copy()

    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["ts_utc"] = pd.to_datetime(
        pd.to_numeric(out["timestamp_ms"], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )

    out = out[out["ts_utc"].notna()].copy()
    out["ts_et"] = out["ts_utc"].dt.tz_convert("America/New_York")
    out["date"] = out["ts_et"].dt.strftime("%Y-%m-%d")
    out["time"] = out["ts_et"].dt.time

    start = pd.to_datetime("09:30").time()
    end = pd.to_datetime("16:00").time()

    out = out[(out["date"].eq(date)) & (out["time"] >= start) & (out["time"] < end)].copy()
    return out.sort_values("ts_et")


def summarize_trade(ticker: str, date: str, label: str, api_key: str) -> dict:
    bars = get_rth_bars(ticker, date, api_key)

    if bars.empty:
        return {
            "trade_date": date,
            "ticker": ticker,
            "label": label,
            "error": "no bars",
        }

    first15 = bars[
        (bars["time"] >= pd.to_datetime("09:30").time())
        & (bars["time"] < pd.to_datetime("09:45").time())
    ].copy()

    after_entry = bars[bars["time"] >= pd.to_datetime("09:45").time()].copy()

    if first15.empty or after_entry.empty:
        return {
            "trade_date": date,
            "ticker": ticker,
            "label": label,
            "error": "missing first15 or after-entry bars",
        }

    first15_open = first15.iloc[0]["open"]
    first15_high = first15["high"].max()
    first15_low = first15["low"].min()
    first15_close = first15.iloc[-1]["close"]

    # Entry at the first 1m bar open after the 15m signal completes.
    entry = after_entry.iloc[0]["open"]

    eod_close = after_entry.iloc[-1]["close"]
    max_high = after_entry["high"].max()
    min_low = after_entry["low"].min()

    eod_pct = (eod_close / entry - 1) * 100
    max_runup_pct = (max_high / entry - 1) * 100
    max_drawdown_pct = (min_low / entry - 1) * 100

    first15_body_pct = (first15_close / first15_open - 1) * 100
    first15_range_pct = (first15_high / first15_low - 1) * 100
    close_pos = (
        (first15_close - first15_low) / (first15_high - first15_low)
        if first15_high > first15_low
        else np.nan
    )

    return {
        "trade_date": date,
        "ticker": ticker,
        "label": label,
        "entry_time": str(after_entry.iloc[0]["ts_et"]),
        "entry": entry,
        "eod_close": eod_close,
        "eod_pct": eod_pct,
        "max_runup_pct": max_runup_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "first15_body_pct": first15_body_pct,
        "first15_range_pct": first15_range_pct,
        "first15_close_position": close_pos,
        "error": "",
    }


def main() -> None:
    api_key = get_api_key()

    rows = []
    for date, ticker, label in PICKS:
        print(f"checking {date} {ticker}")
        rows.append(summarize_trade(ticker, date, label, api_key))

    out = pd.DataFrame(rows)

    out_path = Path(
        "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
        "daily_best_past_week_performance_2026-06-29_to_2026-07-02.csv"
    )
    out.to_csv(out_path, index=False)

    good = out[out["error"].eq("")].copy()

    print()
    print("=== Daily best performance ===")
    print(out.to_string(index=False))

    if not good.empty:
        eod_sum = good["eod_pct"].sum()
        eod_avg = good["eod_pct"].mean()
        eod_median = good["eod_pct"].median()
        win_rate = (good["eod_pct"] > 0).mean() * 100

        print()
        print("=== Summary, one trade per day, EOD exit ===")
        print("trades:", len(good))
        print("sum_return_pct:", round(eod_sum, 3))
        print("avg_return_pct:", round(eod_avg, 3))
        print("median_return_pct:", round(eod_median, 3))
        print("win_rate:", round(win_rate, 2))
        print("avg_max_runup_pct:", round(good["max_runup_pct"].mean(), 3))
        print("avg_max_drawdown_pct:", round(good["max_drawdown_pct"].mean(), 3))

    print()
    print("saved:", out_path)


if __name__ == "__main__":
    main()
