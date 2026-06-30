from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.stocks.backtest_historical_activated_intraday import get_api_key


BASE_URL = "https://api.massive.com/v2/aggs/ticker"


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def fetch_1m_bars(ticker: str, trade_date: str, api_key: str) -> pd.DataFrame:
    encoded_ticker = quote(ticker, safe="")
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{encoded_ticker}/range/1/minute/{trade_date}/{trade_date}?{params}"
    req = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(req, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results = payload.get("results", [])

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
            "t": "timestamp_ms",
        }
    )

    df["bar_start_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["bar_start_et"] = df["bar_start_utc"].dt.tz_convert("America/New_York")
    df["trade_date"] = df["bar_start_et"].dt.date.astype(str)
    df["time_et"] = df["bar_start_et"].dt.strftime("%H:%M")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("bar_start_et").reset_index(drop=True)


def load_or_fetch_1m(
    ticker: str,
    trade_date: str,
    api_key: str,
    cache_dir: Path,
    sleep_seconds: float,
) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{safe_filename(ticker)}_{trade_date}_1m.csv"

    if cache_path.exists():
        return pd.read_csv(cache_path)

    df = fetch_1m_bars(ticker, trade_date, api_key)
    df.to_csv(cache_path, index=False)

    time.sleep(sleep_seconds)

    return df


def simulate_1m(
    side: str,
    ticker: str,
    trade_date: str,
    bars: pd.DataFrame,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> dict:
    if bars.empty:
        return {"trade_status_1m": "no_1m_bars"}

    bars = bars.copy()
    bars["trade_date"] = bars["trade_date"].astype(str)

    rth = bars[
        (bars["trade_date"] == trade_date)
        & (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    if rth.empty:
        return {"trade_status_1m": "no_rth_1m_bars"}

    first_15m = rth[
        (rth["time_et"] >= "09:30")
        & (rth["time_et"] <= "09:44")
    ].copy()

    if first_15m.empty:
        return {"trade_status_1m": "missing_first_15m"}

    first_15m = first_15m.sort_values("time_et")

    first_open = float(first_15m.iloc[0]["open"])
    entry_bar = first_15m.iloc[-1]
    entry_time = entry_bar["time_et"]
    entry_price = float(entry_bar["close"])

    first_15m_return_1m_pct = (entry_price / first_open - 1) * 100

    after_entry = rth[rth["time_et"] > entry_time].copy()

    if after_entry.empty:
        return {
            "trade_status_1m": "no_bars_after_entry",
            "entry_time_1m": entry_time,
            "entry_price_1m": entry_price,
            "first_15m_return_1m_pct": first_15m_return_1m_pct,
        }

    if side == "long":
        target_price = entry_price * (1 + target_pct / 100)
        stop_price = entry_price * (1 - stop_pct / 100)
    else:
        target_price = entry_price * (1 - target_pct / 100)
        stop_price = entry_price * (1 + stop_pct / 100)

    exit_price = float(after_entry.iloc[-1]["close"])
    exit_time = after_entry.iloc[-1]["time_et"]
    exit_reason = "eod"

    for _, bar in after_entry.iterrows():
        high = float(bar["high"])
        low = float(bar["low"])

        if side == "long":
            hit_stop = low <= stop_price
            hit_target = high >= target_price
        else:
            hit_stop = high >= stop_price
            hit_target = low <= target_price

        # Conservative: if both happen inside same 1m candle, count stop first.
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

    if side == "long":
        gross_return_pct = (exit_price / entry_price - 1) * 100
    else:
        gross_return_pct = (entry_price - exit_price) / entry_price * 100

    net_return_pct = gross_return_pct - (cost_bps / 100)

    return {
        "trade_status_1m": "ok",
        "entry_time_1m": entry_time,
        "entry_price_1m": entry_price,
        "first_15m_return_1m_pct": first_15m_return_1m_pct,
        "target_price_1m": target_price,
        "stop_price_1m": stop_price,
        "exit_time_1m": exit_time,
        "exit_price_1m": exit_price,
        "exit_reason_1m": exit_reason,
        "gross_return_1m_pct": gross_return_pct,
        "net_return_1m_pct": net_return_pct,
    }


def summarize(label: str, df: pd.DataFrame) -> dict:
    ok = df[df["trade_status_1m"] == "ok"].copy()

    if ok.empty:
        return {"label": label, "trades": 0}

    return {
        "label": label,
        "trades": len(ok),
        "avg": ok["net_return_1m_pct"].mean(),
        "median": ok["net_return_1m_pct"].median(),
        "win_rate": (ok["net_return_1m_pct"] > 0).mean() * 100,
        "target_rate": (ok["exit_reason_1m"] == "target").mean() * 100,
        "stop_rate": (ok["exit_reason_1m"] == "stop").mean() * 100,
        "eod_rate": (ok["exit_reason_1m"] == "eod").mean() * 100,
        "worst": ok["net_return_1m_pct"].min(),
        "best": ok["net_return_1m_pct"].max(),
        "total": ok["net_return_1m_pct"].sum(),
    }


def prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)

    for col in [
        "first_15m_return_pct",
        "relative_gap_vs_spy_pct",
        "gap_pct",
        "spy_first_15m_return_pct",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--long-results",
        default="data/research/full_market_scanner/tight_stop_20bps/historical_activated_intraday_results.csv",
    )
    parser.add_argument(
        "--short-results",
        default="data/research/full_market_scanner/tight_stop_20bps/historical_activated_intraday_short_results.csv",
    )
    parser.add_argument(
        "--output-dir",
        default="data/research/full_market_scanner/one_minute_validation",
    )
    parser.add_argument("--cache-dir", default="data/cache/massive/intraday_1m")
    parser.add_argument("--target-pct", type=float, default=2.0)
    parser.add_argument("--stop-pct", type=float, default=0.2)
    parser.add_argument("--cost-bps", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    args = parser.parse_args()

    api_key = get_api_key()

    long_df = prep(pd.read_csv(args.long_results))
    short_df = prep(pd.read_csv(args.short_results))

    long_ok = long_df[long_df["trade_status"] == "ok"].copy()
    short_ok = short_df[short_df["trade_status"] == "ok"].copy()

    setups = []

    s = long_ok[long_ok["first_15m_return_pct"] <= -1.0].copy()
    s["setup"] = "long_first15_le_-1"
    s["side"] = "long"
    setups.append(s)

    s = long_ok[
        (long_ok["first_15m_return_pct"] <= -0.25)
        & (long_ok["spy_first_15m_return_pct"] >= 0)
    ].copy()
    s["setup"] = "long_first15_le_-0.25_spy_green"
    s["side"] = "long"
    setups.append(s)

    s = short_ok[
        (short_ok["relative_gap_vs_spy_pct"] >= 3)
        & (short_ok["first_15m_return_pct"] >= 0)
    ].copy()
    s["setup"] = "short_rel_gap_ge_3_first15_green"
    s["side"] = "short"
    setups.append(s)

    s = short_ok[
        (short_ok["gap_pct"] >= 5)
        & (short_ok["first_15m_return_pct"] >= 0)
    ].copy()
    s["setup"] = "short_gap_ge_5_first15_green"
    s["side"] = "short"
    setups.append(s)

    selected = pd.concat(setups, ignore_index=True)

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    print("selected rows:", len(selected))
    print(selected.groupby(["side", "setup"]).size().to_string())
    print()

    rows = []

    for i, row in selected.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]
        side = row["side"]
        setup = row["setup"]

        print(f"[{i + 1}/{len(selected)}] {setup} {ticker} {trade_date}")

        bars = load_or_fetch_1m(
            ticker=ticker,
            trade_date=trade_date,
            api_key=api_key,
            cache_dir=cache_dir,
            sleep_seconds=args.sleep_seconds,
        )

        result = simulate_1m(
            side=side,
            ticker=ticker,
            trade_date=trade_date,
            bars=bars,
            target_pct=args.target_pct,
            stop_pct=args.stop_pct,
            cost_bps=args.cost_bps,
        )

        merged = row.to_dict()
        merged.update(result)
        rows.append(merged)

    results = pd.DataFrame(rows)

    results_path = output_dir / "historical_best_setups_1m_results.csv"
    results.to_csv(results_path, index=False)

    summary_rows = [summarize("all_selected_1m", results)]

    for setup, group in results.groupby("setup"):
        summary_rows.append(summarize(setup, group))

    summary = pd.DataFrame(summary_rows)

    summary_path = output_dir / "historical_best_setups_1m_summary.csv"
    summary.to_csv(summary_path, index=False)

    print()
    print("saved 1m results:", results_path)
    print("saved 1m summary:", summary_path)

    print()
    print("=== 1m Summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== 1m trade status ===")
    print(results["trade_status_1m"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
