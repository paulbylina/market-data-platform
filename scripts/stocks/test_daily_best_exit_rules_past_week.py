from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from scripts.stocks.build_10y_high_price_short_fade_expanded_features import get_api_key
from scripts.stocks.add_first15_opening_rvol_for_date import fetch_1m_range


CACHE_DIR = Path("data/cache/massive/daily_best_past_week_1m")

PICK_SETS = {
    "daily_best_conservative": [
        ("2026-06-29", "KNSA"),
        ("2026-06-30", "GPOR"),
        ("2026-07-01", "IQV"),
        ("2026-07-02", "DCO"),
    ],
    "daily_best_alt_FRHC": [
        ("2026-06-29", "KNSA"),
        ("2026-06-30", "GPOR"),
        ("2026-07-01", "IQV"),
        ("2026-07-02", "FRHC"),
    ],
    "clean_only_available": [
        ("2026-06-29", "KNSA"),
        ("2026-06-30", "GPOR"),
        ("2026-06-30", "ONTO"),
    ],
}


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

    rth_start = pd.to_datetime("09:30").time()
    rth_end = pd.to_datetime("16:00").time()

    out = out[
        out["date"].eq(date)
        & (out["time"] >= rth_start)
        & (out["time"] < rth_end)
    ].copy()

    return out.sort_values("ts_et").reset_index(drop=True)


def simulate_trade(
    bars: pd.DataFrame,
    target_pct: float,
    stop_pct: float,
    max_hold_minutes: int | None,
    both_policy: str = "stop_first",
) -> dict:
    after_entry = bars[bars["time"] >= pd.to_datetime("09:45").time()].copy()

    if after_entry.empty:
        return {"error": "no after-entry bars"}

    entry_row = after_entry.iloc[0]
    entry = float(entry_row["open"])
    entry_ts = entry_row["ts_et"]

    if max_hold_minutes is not None:
        cutoff = entry_ts + pd.Timedelta(minutes=max_hold_minutes)
        trade_bars = after_entry[after_entry["ts_et"] <= cutoff].copy()
    else:
        trade_bars = after_entry.copy()

    if trade_bars.empty:
        return {"error": "no trade bars"}

    target_px = entry * (1 + target_pct / 100)
    stop_px = entry * (1 - stop_pct / 100)

    for _, row in trade_bars.iterrows():
        high = float(row["high"])
        low = float(row["low"])

        target_hit = high >= target_px
        stop_hit = low <= stop_px

        if target_hit and stop_hit:
            if both_policy == "target_first":
                return {
                    "net_pct": target_pct,
                    "exit_type": "target_ambiguous",
                    "exit_time": str(row["ts_et"]),
                    "entry": entry,
                    "exit_px": target_px,
                    "error": "",
                }

            return {
                "net_pct": -stop_pct,
                "exit_type": "stop_ambiguous",
                "exit_time": str(row["ts_et"]),
                "entry": entry,
                "exit_px": stop_px,
                "error": "",
            }

        if target_hit:
            return {
                "net_pct": target_pct,
                "exit_type": "target",
                "exit_time": str(row["ts_et"]),
                "entry": entry,
                "exit_px": target_px,
                "error": "",
            }

        if stop_hit:
            return {
                "net_pct": -stop_pct,
                "exit_type": "stop",
                "exit_time": str(row["ts_et"]),
                "entry": entry,
                "exit_px": stop_px,
                "error": "",
            }

    exit_row = trade_bars.iloc[-1]
    exit_px = float(exit_row["close"])
    net_pct = (exit_px / entry - 1) * 100

    return {
        "net_pct": net_pct,
        "exit_type": "time_exit" if max_hold_minutes is not None else "eod",
        "exit_time": str(exit_row["ts_et"]),
        "entry": entry,
        "exit_px": exit_px,
        "error": "",
    }


def summarize(g: pd.DataFrame) -> dict:
    vals = pd.to_numeric(g["net_pct"], errors="coerce").dropna()

    return {
        "trades": len(vals),
        "sum_return_pct": vals.sum(),
        "avg_return_pct": vals.mean(),
        "median_return_pct": vals.median(),
        "win_rate": (vals > 0).mean() * 100,
        "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
        "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,
        "time_or_eod_rate": g["exit_type"].isin(["time_exit", "eod"]).mean() * 100,
        "best": vals.max(),
        "worst": vals.min(),
    }


def main() -> None:
    api_key = get_api_key()

    bars_cache: dict[tuple[str, str], pd.DataFrame] = {}

    combos = [
        (0.75, 1.00),
        (1.00, 1.25),
        (1.00, 1.50),
        (1.25, 1.50),
        (1.50, 2.00),
        (2.00, 2.50),
    ]

    hold_minutes = [30, 60, 90, 120, None]

    rows = []

    for set_name, picks in PICK_SETS.items():
        for date, ticker in picks:
            key = (date, ticker)

            if key not in bars_cache:
                print(f"fetch/check {date} {ticker}")
                bars_cache[key] = get_rth_bars(ticker, date, api_key)

            bars = bars_cache[key]

            for target, stop in combos:
                for max_hold in hold_minutes:
                    result = simulate_trade(
                        bars=bars,
                        target_pct=target,
                        stop_pct=stop,
                        max_hold_minutes=max_hold,
                        both_policy="stop_first",
                    )

                    rows.append(
                        {
                            "set_name": set_name,
                            "trade_date": date,
                            "ticker": ticker,
                            "target_pct": target,
                            "stop_pct": stop,
                            "max_hold_minutes": max_hold if max_hold is not None else "EOD",
                            **result,
                        }
                    )

    trades = pd.DataFrame(rows)

    summary_rows = []

    for keys, g in trades[trades["error"].eq("")].groupby(
        ["set_name", "target_pct", "stop_pct", "max_hold_minutes"],
        observed=True,
    ):
        set_name, target, stop, max_hold = keys
        row = {
            "set_name": set_name,
            "target_pct": target,
            "stop_pct": stop,
            "max_hold_minutes": max_hold,
        }
        row.update(summarize(g))
        summary_rows.append(row)

    summary = pd.DataFrame(summary_rows)

    out_dir = Path("data/research/full_market_scanner_10y/high_price_full_universe_first15_checks")
    trades_path = out_dir / "daily_best_exit_rule_trades_2026-06-29_to_2026-07-02.csv"
    summary_path = out_dir / "daily_best_exit_rule_summary_2026-06-29_to_2026-07-02.csv"

    trades.to_csv(trades_path, index=False)
    summary.to_csv(summary_path, index=False)

    print()
    print("=== Top exit rules by set ===")
    show_cols = [
        "set_name",
        "target_pct",
        "stop_pct",
        "max_hold_minutes",
        "trades",
        "sum_return_pct",
        "avg_return_pct",
        "median_return_pct",
        "win_rate",
        "target_rate",
        "stop_rate",
        "best",
        "worst",
    ]

    ranked = summary.sort_values(
        ["set_name", "sum_return_pct", "avg_return_pct", "worst"],
        ascending=[True, False, False, False],
    )

    for set_name, g in ranked.groupby("set_name", observed=True):
        print()
        print(f"--- {set_name} ---")
        print(g[show_cols].head(10).to_string(index=False))

    print()
    print("saved trades:", trades_path)
    print("saved summary:", summary_path)


if __name__ == "__main__":
    main()
