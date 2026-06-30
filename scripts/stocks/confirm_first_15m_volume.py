from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime, timedelta
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


def fetch_15m_bars(
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
    df["date_et"] = df["bar_start_et"].dt.date.astype(str)
    df["time_et"] = df["bar_start_et"].dt.strftime("%H:%M")

    df["dollar_volume"] = df["volume"] * df["vwap"].fillna(df["close"])

    return df.sort_values("bar_start_et").reset_index(drop=True)


def safe_ratio(current: float, avg: float | None) -> float | None:
    if avg is None or math.isnan(avg):
        return None

    if avg > 0:
        return current / avg

    if current > 0:
        return float("inf")

    return None


def analyze_ticker(
    ticker: str,
    trade_date: str,
    api_key: str,
    lookback_calendar_days: int,
    lookback_sessions: int,
) -> dict:
    end_dt = datetime.fromisoformat(trade_date).date()
    start_dt = end_dt - timedelta(days=lookback_calendar_days)

    try:
        results = fetch_15m_bars(
            ticker=ticker,
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
            api_key=api_key,
        )
    except Exception as exc:
        return {
            "ticker": ticker,
            "status": f"fetch_error: {exc}",
        }

    bars = bars_to_df(results)

    if bars.empty:
        return {
            "ticker": ticker,
            "status": "no_15m_bars",
        }

    rth = bars[
        (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    if rth.empty:
        return {
            "ticker": ticker,
            "status": "no_rth_bars",
        }

    prior_dates = sorted(
        d for d in rth["date_et"].unique()
        if d < trade_date
    )[-lookback_sessions:]

    first_bars = rth[rth["time_et"] == "09:30"].copy()

    first_by_date = (
        first_bars
        .groupby("date_et")
        .agg(
            first_15m_volume=("volume", "sum"),
            first_15m_dollar_volume=("dollar_volume", "sum"),
        )
    )

    prior_first = first_by_date.reindex(prior_dates).fillna(0)

    avg_prior_first_15m_volume = prior_first["first_15m_volume"].mean()
    avg_prior_first_15m_dollar_volume = prior_first["first_15m_dollar_volume"].mean()

    today_first = first_bars[first_bars["date_et"] == trade_date]

    if today_first.empty:
        today_first_15m_volume = 0.0
        today_first_15m_dollar_volume = 0.0
        first_15m_return_pct = None
        status = "missing_today_first_15m_bar"
    else:
        bar = today_first.iloc[0]
        today_first_15m_volume = float(bar["volume"])
        today_first_15m_dollar_volume = float(bar["dollar_volume"])
        first_15m_return_pct = (float(bar["close"]) / float(bar["open"]) - 1) * 100
        status = "ok"

    premarket = bars[
        (bars["time_et"] >= "04:00")
        & (bars["time_et"] < "09:30")
    ].copy()

    premarket_by_date = (
        premarket
        .groupby("date_et")
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
        "status": status,
        "prior_sessions_used": len(prior_dates),
        "today_first_15m_volume": today_first_15m_volume,
        "avg_prior_first_15m_volume": avg_prior_first_15m_volume,
        "first_15m_rvol": safe_ratio(
            today_first_15m_volume,
            avg_prior_first_15m_volume,
        ),
        "today_first_15m_dollar_volume": today_first_15m_dollar_volume,
        "avg_prior_first_15m_dollar_volume": avg_prior_first_15m_dollar_volume,
        "first_15m_dollar_rvol": safe_ratio(
            today_first_15m_dollar_volume,
            avg_prior_first_15m_dollar_volume,
        ),
        "first_15m_return_pct": first_15m_return_pct,
        "today_premarket_volume": today_premarket_volume,
        "avg_prior_premarket_volume": avg_prior_premarket_volume,
        "premarket_rvol": safe_ratio(
            today_premarket_volume,
            avg_prior_premarket_volume,
        ),
        "today_premarket_dollar_volume": today_premarket_dollar_volume,
        "avg_prior_premarket_dollar_volume": avg_prior_premarket_dollar_volume,
        "premarket_dollar_rvol": safe_ratio(
            today_premarket_dollar_volume,
            avg_prior_premarket_dollar_volume,
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--candidates",
        default="data/reference/stocks/gap_up_activated_dormant_common_stocks_latest.csv",
    )
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--lookback-calendar-days", type=int, default=45)
    parser.add_argument("--lookback-sessions", type=int, default=20)
    parser.add_argument("--min-first-15m-rvol", type=float, default=3.0)
    parser.add_argument("--min-first-15m-dollar-volume", type=float, default=100_000)
    parser.add_argument("--min-premarket-rvol", type=float, default=3.0)
    parser.add_argument("--min-premarket-dollar-volume", type=float, default=100_000)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    args = parser.parse_args()

    api_key = get_api_key()

    candidate_path = Path(args.candidates)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = pd.read_csv(candidate_path)

    rows = []

    for i, row in candidates.iterrows():
        ticker = row["ticker"]
        print(f"[{i + 1}/{len(candidates)}] {ticker}")

        analysis = analyze_ticker(
            ticker=ticker,
            trade_date=args.trade_date,
            api_key=api_key,
            lookback_calendar_days=args.lookback_calendar_days,
            lookback_sessions=args.lookback_sessions,
        )

        merged = row.to_dict()
        merged.update(analysis)
        rows.append(merged)

        time.sleep(args.sleep_seconds)

    out = pd.DataFrame(rows)

    out["confirmed_first_15m"] = (
        (out["first_15m_rvol"] >= args.min_first_15m_rvol)
        & (out["today_first_15m_dollar_volume"] >= args.min_first_15m_dollar_volume)
    )

    out["watch_first_15m"] = (
        (out["first_15m_rvol"] >= 1.5)
        & (out["today_first_15m_dollar_volume"] >= 100_000)
    )

    out["confirmed_premarket"] = (
        (out["premarket_rvol"] >= args.min_premarket_rvol)
        & (out["today_premarket_dollar_volume"] >= args.min_premarket_dollar_volume)
    )

    out["confirmed_any_volume"] = (
        out["confirmed_first_15m"]
        | out["confirmed_premarket"]
    )

    out["watch_any_volume"] = (
        out["watch_first_15m"]
        | out["confirmed_premarket"]
    )

    out = out.sort_values(
        [
            "confirmed_any_volume",
            "confirmed_first_15m",
            "today_first_15m_dollar_volume",
            "first_15m_rvol",
        ],
        ascending=[False, False, False, False],
    )

    dated_path = output_dir / f"confirmed_first_15m_activated_dormant_{args.trade_date}.csv"
    latest_path = output_dir / "confirmed_first_15m_activated_dormant_latest.csv"

    out.to_csv(dated_path, index=False)
    out.to_csv(latest_path, index=False)

    print()
    print("saved:", dated_path)
    print("saved:", latest_path)

    print()
    print("=== Confirmation counts ===")
    print("rows:", len(out))
    print("confirmed first 15m:", int(out["confirmed_first_15m"].sum()))
    print("watch first 15m:", int(out["watch_first_15m"].sum()))
    print("confirmed premarket:", int(out["confirmed_premarket"].sum()))
    print("confirmed any volume:", int(out["confirmed_any_volume"].sum()))
    print("watch any volume:", int(out["watch_any_volume"].sum()))

    print()
    print("=== Top confirmed candidates ===")
    cols = [
        "ticker",
        "prev_close",
        "gap_pct",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "today_first_15m_volume",
        "avg_prior_first_15m_volume",
        "first_15m_rvol",
        "today_first_15m_dollar_volume",
        "first_15m_return_pct",
        "today_premarket_volume",
        "premarket_rvol",
        "confirmed_first_15m",
        "watch_first_15m",
        "confirmed_premarket",
        "confirmed_any_volume",
        "watch_any_volume",
        "name",
    ]

    existing_cols = [col for col in cols if col in out.columns]
    print(out[existing_cols].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
