from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from scripts.stocks.backtest_historical_activated_intraday import (
    get_api_key,
    get_intraday_features,
    load_or_fetch_15m,
)


def simulate_short_trade(
    ticker: str,
    trade_date: str,
    bars: pd.DataFrame,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> dict:
    rth = bars[
        (bars["trade_date"] == trade_date)
        & (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    if rth.empty:
        return {"ticker": ticker, "trade_date": trade_date, "trade_status": "no_rth_bars"}

    first_bar = rth[rth["time_et"] == "09:30"]

    if first_bar.empty:
        return {"ticker": ticker, "trade_date": trade_date, "trade_status": "missing_first_bar"}

    entry_bar = first_bar.iloc[0]
    entry_price = float(entry_bar["close"])

    target_price = entry_price * (1 - target_pct / 100)
    stop_price = entry_price * (1 + stop_pct / 100)

    after_entry = rth[rth["time_et"] > "09:30"].copy()

    if after_entry.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "trade_status": "no_bars_after_entry",
            "entry_price": entry_price,
        }

    exit_price = float(after_entry.iloc[-1]["close"])
    exit_time = after_entry.iloc[-1]["time_et"]
    exit_reason = "eod"

    for _, bar in after_entry.iterrows():
        low = float(bar["low"])
        high = float(bar["high"])

        hit_target = low <= target_price
        hit_stop = high >= stop_price

        # Conservative with 15m bars: stop first if both target and stop hit.
        if hit_stop:
            exit_price = stop_price
            exit_time = bar["time_et"]
            exit_reason = "stop"
            break

        if hit_target:
            exit_price = target_price
            exit_time = bar["time_et"]
            exit_reason = "target"
            break

    gross_return_pct = (entry_price - exit_price) / entry_price * 100
    net_return_pct = gross_return_pct - (cost_bps / 100)

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "trade_status": "ok",
        "entry_price": entry_price,
        "target_price": target_price,
        "stop_price": stop_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "gross_return_pct": gross_return_pct,
        "net_return_pct": net_return_pct,
    }


def summarize(label: str, df: pd.DataFrame) -> dict:
    ok = df[df["trade_status"] == "ok"].copy()

    if ok.empty:
        return {"label": label, "trades": 0}

    return {
        "label": label,
        "trades": len(ok),
        "avg": ok["net_return_pct"].mean(),
        "median": ok["net_return_pct"].median(),
        "win_rate": (ok["net_return_pct"] > 0).mean() * 100,
        "target_rate": (ok["exit_reason"] == "target").mean() * 100,
        "stop_rate": (ok["exit_reason"] == "stop").mean() * 100,
        "eod_rate": (ok["exit_reason"] == "eod").mean() * 100,
        "worst": ok["net_return_pct"].min(),
        "best": ok["net_return_pct"].max(),
        "total": ok["net_return_pct"].sum(),
    }


def add_summary_rows(results: pd.DataFrame) -> pd.DataFrame:
    rows = []

    rows.append(summarize("all_short", results))

    # Failed-gap / red first bar.
    for threshold in [0, -0.25, -0.5, -1.0, -2.0]:
        sub = results[results["first_15m_return_pct"] <= threshold].copy()
        rows.append(summarize(f"stock_first15_le_{threshold}", sub))

    # Blow-off / green first bar.
    for threshold in [0, 0.25, 0.5, 1.0, 2.0, 5.0]:
        sub = results[results["first_15m_return_pct"] >= threshold].copy()
        rows.append(summarize(f"stock_first15_ge_{threshold}", sub))

    # Bigger daily gap.
    for gap in [1, 2, 3, 5, 10, 20, 30, 50]:
        sub = results[results["gap_pct"] >= gap].copy()
        rows.append(summarize(f"gap_ge_{gap}", sub))

    # Bigger relative gap vs SPY.
    if "relative_gap_vs_spy_pct" in results.columns:
        for rel_gap in [0, 1, 2, 3, 5, 10, 20]:
            sub = results[results["relative_gap_vs_spy_pct"] >= rel_gap].copy()
            rows.append(summarize(f"rel_gap_vs_spy_ge_{rel_gap}", sub))

    # First 15m RVOL.
    for rvol in [1.5, 2, 3, 5, 10]:
        sub = results[
            (results["first_15m_rvol"] >= rvol)
            & (results["today_first_15m_dollar_volume"] >= 100_000)
        ].copy()
        rows.append(summarize(f"first15_rvol_ge_{rvol}_dollar_ge_100k", sub))

    # Premarket RVOL.
    for rvol in [1.5, 2, 3, 5, 10]:
        sub = results[
            (results["premarket_rvol"] >= rvol)
            & (results["today_premarket_dollar_volume"] >= 100_000)
        ].copy()
        rows.append(summarize(f"premarket_rvol_ge_{rvol}_dollar_ge_100k", sub))

    # SPY first 15m filters.
    if "spy_first_15m_return_pct" in results.columns:
        for threshold in [-1, -0.5, 0]:
            sub = results[results["spy_first_15m_return_pct"] <= threshold].copy()
            rows.append(summarize(f"spy_first15_le_{threshold}", sub))

        for threshold in [0, 0.5, 1]:
            sub = results[results["spy_first_15m_return_pct"] >= threshold].copy()
            rows.append(summarize(f"spy_first15_ge_{threshold}", sub))

    # Combined short setups.
    combo = results[
        (results["gap_pct"] >= 5)
        & (results["first_15m_return_pct"] <= -0.25)
    ].copy()
    rows.append(summarize("gap_ge_5_and_first15_le_-0.25", combo))

    combo = results[
        (results["gap_pct"] >= 5)
        & (results["first_15m_return_pct"] >= 0)
    ].copy()
    rows.append(summarize("gap_ge_5_and_first15_green", combo))

    combo = results[
        (results["relative_gap_vs_spy_pct"] >= 3)
        & (results["first_15m_return_pct"] <= -0.25)
    ].copy()
    rows.append(summarize("rel_gap_ge_3_and_first15_le_-0.25", combo))

    combo = results[
        (results["relative_gap_vs_spy_pct"] >= 3)
        & (results["first_15m_return_pct"] >= 0)
    ].copy()
    rows.append(summarize("rel_gap_ge_3_and_first15_green", combo))

    combo = results[
        (results["first_15m_rvol"] >= 3)
        & (results["today_first_15m_dollar_volume"] >= 100_000)
        & (results["first_15m_return_pct"] <= -0.25)
    ].copy()
    rows.append(summarize("first15_rvol_ge_3_and_first15_le_-0.25", combo))

    combo = results[
        (results["first_15m_rvol"] >= 3)
        & (results["today_first_15m_dollar_volume"] >= 100_000)
        & (results["first_15m_return_pct"] >= 0)
    ].copy()
    rows.append(summarize("first15_rvol_ge_3_and_first15_green", combo))

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--candidates",
        default="data/research/full_market_scanner/historical_activated_dormant_gap_candidates_with_spy.csv",
    )
    parser.add_argument("--output-dir", default="data/research/full_market_scanner")
    parser.add_argument("--cache-dir", default="data/cache/massive/intraday_15m")
    parser.add_argument("--lookback-sessions", type=int, default=20)
    parser.add_argument("--lookback-calendar-buffer-days", type=int, default=60)
    parser.add_argument("--target-pct", type=float, default=2.0)
    parser.add_argument("--stop-pct", type=float, default=3.0)
    parser.add_argument("--cost-bps", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    args = parser.parse_args()

    api_key = get_api_key()

    candidates = pd.read_csv(args.candidates)
    candidates["trade_date"] = pd.to_datetime(candidates["trade_date"]).dt.date.astype(str)

    min_date = datetime.fromisoformat(candidates["trade_date"].min()).date()
    max_date = datetime.fromisoformat(candidates["trade_date"].max()).date()

    fetch_start = (min_date - timedelta(days=args.lookback_calendar_buffer_days)).isoformat()
    fetch_end = max_date.isoformat()

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    tickers = sorted(candidates["ticker"].unique())

    print("candidate rows:", len(candidates))
    print("unique tickers:", len(tickers))
    print("fetch start:", fetch_start)
    print("fetch end:", fetch_end)
    print()

    ticker_bars: dict[str, pd.DataFrame] = {}

    for i, ticker in enumerate(tickers, start=1):
        print(f"[{i}/{len(tickers)}] fetching/loading {ticker}")

        ticker_bars[ticker] = load_or_fetch_15m(
            ticker=ticker,
            start_date=fetch_start,
            end_date=fetch_end,
            api_key=api_key,
            cache_dir=cache_dir,
            sleep_seconds=args.sleep_seconds,
        )

    print()
    print("Fetching/loading SPY")

    spy_bars = load_or_fetch_15m(
        ticker="SPY",
        start_date=fetch_start,
        end_date=fetch_end,
        api_key=api_key,
        cache_dir=cache_dir,
        sleep_seconds=args.sleep_seconds,
    )

    rows = []

    for i, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]
        print(f"[{i + 1}/{len(candidates)}] short analyzing {ticker} {trade_date}")

        bars = ticker_bars.get(ticker, pd.DataFrame())

        stock_features = get_intraday_features(
            ticker=ticker,
            trade_date=trade_date,
            bars=bars,
            lookback_sessions=args.lookback_sessions,
        )

        spy_features = get_intraday_features(
            ticker="SPY",
            trade_date=trade_date,
            bars=spy_bars,
            lookback_sessions=args.lookback_sessions,
        )

        trade_result = simulate_short_trade(
            ticker=ticker,
            trade_date=trade_date,
            bars=bars,
            target_pct=args.target_pct,
            stop_pct=args.stop_pct,
            cost_bps=args.cost_bps,
        )

        merged = row.to_dict()
        merged.update(stock_features)

        merged["spy_first_15m_return_pct"] = spy_features.get("first_15m_return_pct")
        merged["spy_first_15m_rvol"] = spy_features.get("first_15m_rvol")
        merged["spy_premarket_rvol"] = spy_features.get("premarket_rvol")

        merged.update(trade_result)

        rows.append(merged)

    results = pd.DataFrame(rows)

    results_path = output_dir / "historical_activated_intraday_short_results.csv"
    results.to_csv(results_path, index=False)

    summary = add_summary_rows(results)

    summary_path = output_dir / "historical_activated_intraday_short_summary.csv"
    summary.to_csv(summary_path, index=False)

    print()
    print("saved short results:", results_path)
    print("saved short summary:", summary_path)

    print()
    print("=== Short Summary ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
