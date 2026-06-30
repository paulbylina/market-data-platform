from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date
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

    api_key = (
        os.environ.get("MASSIVE_API_KEY")
        or os.environ.get("POLYGON_API_KEY")
    )

    if not api_key:
        raise SystemExit(
            "Missing API key. Add MASSIVE_API_KEY=... or POLYGON_API_KEY=... to .env"
        )

    return api_key


def fetch_15m_bars(ticker: str, trade_date: str, api_key: str) -> list[dict]:
    encoded_ticker = quote(ticker, safe="")
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{encoded_ticker}/range/15/minute/{trade_date}/{trade_date}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload.get("results", [])


def bars_to_df(results: list[dict]) -> pd.DataFrame:
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

    df["bar_start_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["bar_start_et"] = df["bar_start_utc"].dt.tz_convert("America/New_York")
    df["time_et"] = df["bar_start_et"].dt.strftime("%H:%M")
    df["dollar_volume"] = df["volume"] * df["vwap"].fillna(df["close"])

    return df.sort_values("bar_start_et").reset_index(drop=True)


def simulate_trade(
    ticker: str,
    trade_date: str,
    api_key: str,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> dict:
    try:
        results = fetch_15m_bars(ticker=ticker, trade_date=trade_date, api_key=api_key)
    except Exception as exc:
        return {
            "ticker": ticker,
            "trade_status": f"fetch_error: {exc}",
        }

    bars = bars_to_df(results)

    if bars.empty:
        return {
            "ticker": ticker,
            "trade_status": "no_15m_bars",
        }

    rth = bars[
        (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    if rth.empty:
        return {
            "ticker": ticker,
            "trade_status": "no_rth_bars",
        }

    first_bar = rth[rth["time_et"] == "09:30"]

    if first_bar.empty:
        return {
            "ticker": ticker,
            "trade_status": "missing_first_15m_bar",
        }

    entry_bar = first_bar.iloc[0]
    entry_price = float(entry_bar["close"])

    target_price = entry_price * (1 + target_pct / 100)
    stop_price = entry_price * (1 - stop_pct / 100)

    after_entry = rth[rth["time_et"] > "09:30"].copy()

    if after_entry.empty:
        return {
            "ticker": ticker,
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

        # Conservative assumption with 15m bars:
        # if stop and target are both touched in the same bar, count the stop first.
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
        "trade_status": "ok",
        "entry_time": "09:45",
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
        return {
            "label": label,
            "trades": 0,
        }

    return {
        "label": label,
        "trades": len(ok),
        "avg_net": ok["net_return_pct"].mean(),
        "median_net": ok["net_return_pct"].median(),
        "win_rate": (ok["net_return_pct"] > 0).mean() * 100,
        "target_rate": (ok["exit_reason"] == "target").mean() * 100,
        "stop_rate": (ok["exit_reason"] == "stop").mean() * 100,
        "eod_rate": (ok["exit_reason"] == "eod").mean() * 100,
        "best": ok["net_return_pct"].max(),
        "worst": ok["net_return_pct"].min(),
        "total_net": ok["net_return_pct"].sum(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scanner-rows",
        default="data/reference/stocks/today_all_confirmed_scanner_rows_latest.csv",
    )
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--target-pct", type=float, default=2.0)
    parser.add_argument("--stop-pct", type=float, default=3.0)
    parser.add_argument("--cost-bps", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    args = parser.parse_args()

    api_key = get_api_key()

    scanner_path = Path(args.scanner_rows)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    scanner = pd.read_csv(scanner_path)

    rows = []

    for i, row in scanner.iterrows():
        ticker = row["ticker"]
        print(f"[{i + 1}/{len(scanner)}] {ticker}")

        result = simulate_trade(
            ticker=ticker,
            trade_date=args.trade_date,
            api_key=api_key,
            target_pct=args.target_pct,
            stop_pct=args.stop_pct,
            cost_bps=args.cost_bps,
        )

        merged = row.to_dict()
        merged.update(result)
        rows.append(merged)

        time.sleep(args.sleep_seconds)

    out = pd.DataFrame(rows)

    dated_path = output_dir / f"today_scanner_exit_results_{args.trade_date}.csv"
    latest_path = output_dir / "today_scanner_exit_results_latest.csv"

    out.to_csv(dated_path, index=False)
    out.to_csv(latest_path, index=False)

    summary_rows = [
        summarize("all_rows", out),
        summarize("trade_candidates", out[out["trade_candidate"] == True]),
        summarize("watchlist", out[out["watchlist_candidate"] == True]),
        summarize("strict_first15", out[out["strict_first15"] == True]),
        summarize("strict_premarket", out[out["strict_premarket"] == True]),
    ]

    summary = pd.DataFrame(summary_rows)

    summary_dated_path = output_dir / f"today_scanner_exit_summary_{args.trade_date}.csv"
    summary_latest_path = output_dir / "today_scanner_exit_summary_latest.csv"

    summary.to_csv(summary_dated_path, index=False)
    summary.to_csv(summary_latest_path, index=False)

    print()
    print("saved results:", dated_path)
    print("saved results latest:", latest_path)
    print("saved summary:", summary_dated_path)
    print("saved summary latest:", summary_latest_path)

    print()
    print("=== Summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== Results ===")
    cols = [
        "ticker",
        "volume_signal",
        "trade_candidate",
        "watchlist_candidate",
        "gap_pct",
        "first_15m_rvol",
        "first_15m_return_pct",
        "entry_price",
        "exit_price",
        "exit_reason",
        "net_return_pct",
        "name",
    ]

    existing_cols = [col for col in cols if col in out.columns]
    print(out[existing_cols].to_string(index=False))


if __name__ == "__main__":
    main()
