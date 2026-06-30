from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen

import pandas as pd


BASE_URL = "https://api.massive.com/v2/aggs/ticker"


def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_api_key() -> str:
    load_env_file()

    api_key = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")

    if not api_key:
        raise SystemExit("Missing MASSIVE_API_KEY or POLYGON_API_KEY in .env")

    return api_key


def fetch_15m_bars_window(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str,
) -> list[dict]:
    encoded_ticker = quote(ticker, safe="")
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{encoded_ticker}/range/15/minute/{start_date}/{end_date}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload.get("results", [])


def fetch_15m_bars(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str,
    window_days: int = 45,
) -> list[dict]:
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    cursor = start
    all_results: list[dict] = []

    while cursor <= end:
        window_end = min(cursor + timedelta(days=window_days - 1), end)

        print(f"    window {ticker}: {cursor} to {window_end}")

        results = fetch_15m_bars_window(
            ticker=ticker,
            start_date=cursor.isoformat(),
            end_date=window_end.isoformat(),
            api_key=api_key,
        )

        all_results.extend(results)
        cursor = window_end + timedelta(days=1)

    # Deduplicate in case the provider overlaps windows.
    deduped = {}
    for bar in all_results:
        if "t" in bar:
            deduped[bar["t"]] = bar

    return [deduped[k] for k in sorted(deduped)]


def bars_to_df(ticker: str, results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    df = df.rename(
        columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
            "t": "timestamp_ms",
        }
    )

    df["ticker"] = ticker
    df["bar_start_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["bar_start_et"] = df["bar_start_utc"].dt.tz_convert("America/New_York")
    df["trade_date"] = df["bar_start_et"].dt.date.astype(str)
    df["time_et"] = df["bar_start_et"].dt.strftime("%H:%M")

    if "vwap" not in df.columns:
        df["vwap"] = df["close"]

    df["dollar_volume"] = df["volume"] * df["vwap"].fillna(df["close"])

    return df.sort_values("bar_start_et").reset_index(drop=True)


def load_or_fetch_15m(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str,
    cache_dir: Path,
    sleep_seconds: float,
) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{ticker}_{start_date}_{end_date}_15m.csv"

    if cache_path.exists():
        return pd.read_csv(cache_path)

    results = fetch_15m_bars(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        api_key=api_key,
    )

    df = bars_to_df(ticker, results)
    df.to_csv(cache_path, index=False)

    time.sleep(sleep_seconds)

    return df


def safe_ratio(current: float, avg: float | None) -> float | None:
    if avg is None or pd.isna(avg):
        return None

    if avg > 0:
        return current / avg

    if current > 0:
        return float("inf")

    return None


def get_intraday_features(
    ticker: str,
    trade_date: str,
    bars: pd.DataFrame,
    lookback_sessions: int,
) -> dict:
    if bars.empty:
        return {"ticker": ticker, "trade_date": trade_date, "status": "no_bars"}

    rth = bars[
        (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    if rth.empty:
        return {"ticker": ticker, "trade_date": trade_date, "status": "no_rth_bars"}

    prior_dates = sorted(
        d for d in rth["trade_date"].unique()
        if d < trade_date
    )[-lookback_sessions:]

    first_bars = rth[rth["time_et"] == "09:30"].copy()

    first_by_date = (
        first_bars
        .groupby("trade_date")
        .agg(
            first_15m_volume=("volume", "sum"),
            first_15m_dollar_volume=("dollar_volume", "sum"),
        )
    )

    prior_first = first_by_date.reindex(prior_dates).fillna(0)

    avg_prior_first_15m_volume = prior_first["first_15m_volume"].mean()
    avg_prior_first_15m_dollar_volume = prior_first["first_15m_dollar_volume"].mean()

    today_first = first_bars[first_bars["trade_date"] == trade_date]

    if today_first.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "status": "missing_today_first_bar",
        }

    first = today_first.iloc[0]

    today_first_15m_volume = float(first["volume"])
    today_first_15m_dollar_volume = float(first["dollar_volume"])
    first_15m_return_pct = (float(first["close"]) / float(first["open"]) - 1) * 100

    premarket = bars[
        (bars["time_et"] >= "04:00")
        & (bars["time_et"] < "09:30")
    ].copy()

    premarket_by_date = (
        premarket
        .groupby("trade_date")
        .agg(
            premarket_volume=("volume", "sum"),
            premarket_dollar_volume=("dollar_volume", "sum"),
        )
    )

    prior_premarket = premarket_by_date.reindex(prior_dates).fillna(0)

    avg_prior_premarket_volume = prior_premarket["premarket_volume"].mean()
    avg_prior_premarket_dollar_volume = prior_premarket["premarket_dollar_volume"].mean()

    today_premarket = premarket_by_date.reindex([trade_date]).fillna(0)

    today_premarket_volume = float(today_premarket["premarket_volume"].iloc[0])
    today_premarket_dollar_volume = float(today_premarket["premarket_dollar_volume"].iloc[0])

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "status": "ok",
        "prior_sessions_used": len(prior_dates),
        "today_first_15m_volume": today_first_15m_volume,
        "avg_prior_first_15m_volume": avg_prior_first_15m_volume,
        "first_15m_rvol": safe_ratio(today_first_15m_volume, avg_prior_first_15m_volume),
        "today_first_15m_dollar_volume": today_first_15m_dollar_volume,
        "avg_prior_first_15m_dollar_volume": avg_prior_first_15m_dollar_volume,
        "first_15m_dollar_rvol": safe_ratio(
            today_first_15m_dollar_volume,
            avg_prior_first_15m_dollar_volume,
        ),
        "first_15m_return_pct": first_15m_return_pct,
        "today_premarket_volume": today_premarket_volume,
        "avg_prior_premarket_volume": avg_prior_premarket_volume,
        "premarket_rvol": safe_ratio(today_premarket_volume, avg_prior_premarket_volume),
        "today_premarket_dollar_volume": today_premarket_dollar_volume,
        "avg_prior_premarket_dollar_volume": avg_prior_premarket_dollar_volume,
        "premarket_dollar_rvol": safe_ratio(
            today_premarket_dollar_volume,
            avg_prior_premarket_dollar_volume,
        ),
    }


def simulate_trade(
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

    target_price = entry_price * (1 + target_pct / 100)
    stop_price = entry_price * (1 - stop_pct / 100)

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

        hit_stop = low <= stop_price
        hit_target = high >= target_price

        # Conservative with 15m bars: stop first if both hit in same candle.
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

    gross_return_pct = (exit_price / entry_price - 1) * 100
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

    feature_rows = []

    for i, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]
        print(f"[{i + 1}/{len(candidates)}] analyzing {ticker} {trade_date}")

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

        trade_result = simulate_trade(
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

        feature_rows.append(merged)

    results = pd.DataFrame(feature_rows)

    results_path = output_dir / "historical_activated_intraday_results.csv"
    results.to_csv(results_path, index=False)

    summary_rows = []

    summary_rows.append(summarize("all", results))

    for threshold in [0, -0.25, -0.5, -1.0]:
        sub = results[results["first_15m_return_pct"] <= threshold].copy()
        summary_rows.append(summarize(f"stock_first15_le_{threshold}", sub))

    for rvol in [1.5, 2, 3, 5, 10]:
        sub = results[
            (results["first_15m_rvol"] >= rvol)
            & (results["today_first_15m_dollar_volume"] >= 100_000)
        ].copy()
        summary_rows.append(summarize(f"first15_rvol_ge_{rvol}_dollar_ge_100k", sub))

    for rvol in [1.5, 2, 3, 5, 10]:
        sub = results[
            (results["premarket_rvol"] >= rvol)
            & (results["today_premarket_dollar_volume"] >= 100_000)
        ].copy()
        summary_rows.append(summarize(f"premarket_rvol_ge_{rvol}_dollar_ge_100k", sub))

    for threshold in [-1, -0.5, 0, 0.5, 1]:
        sub = results[results["spy_first_15m_return_pct"] >= threshold].copy()
        summary_rows.append(summarize(f"spy_first15_ge_{threshold}", sub))

    for threshold in [-1, -0.5, 0]:
        sub = results[results["spy_first_15m_return_pct"] <= threshold].copy()
        summary_rows.append(summarize(f"spy_first15_le_{threshold}", sub))

    # Combined setup tests.
    combo = results[
        (results["first_15m_return_pct"] <= -0.25)
        & (results["first_15m_rvol"] >= 1.5)
        & (results["today_first_15m_dollar_volume"] >= 100_000)
    ].copy()
    summary_rows.append(summarize("pullback_le_-0.25_and_first15_rvol_ge_1.5", combo))

    combo = results[
        (results["first_15m_return_pct"] <= -0.25)
        & (results["premarket_rvol"] >= 1.5)
        & (results["today_premarket_dollar_volume"] >= 100_000)
    ].copy()
    summary_rows.append(summarize("pullback_le_-0.25_and_premarket_rvol_ge_1.5", combo))

    combo = results[
        (results["first_15m_return_pct"] <= -0.25)
        & (results["spy_first_15m_return_pct"] >= 0)
    ].copy()
    summary_rows.append(summarize("pullback_le_-0.25_and_spy_first15_green", combo))

    summary = pd.DataFrame(summary_rows)

    summary_path = output_dir / "historical_activated_intraday_summary.csv"
    summary.to_csv(summary_path, index=False)

    print()
    print("saved results:", results_path)
    print("saved summary:", summary_path)

    print()
    print("=== Summary ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
