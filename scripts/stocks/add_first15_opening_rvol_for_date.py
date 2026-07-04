from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import numpy as np
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
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_api_key() -> str:
    load_env_file()
    api_key = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit("Missing MASSIVE_API_KEY or POLYGON_API_KEY in .env")
    return api_key


def safe_name(ticker: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", ticker)


def fetch_1m_range(
    ticker: str,
    start_date: str,
    end_date: str,
    api_key: str,
    cache_dir: Path,
    sleep_seconds: float,
) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{safe_name(ticker)}_{start_date}_to_{end_date}_1m.csv"

    if cache_path.exists():
        return pd.read_csv(cache_path)

    encoded_ticker = quote(ticker, safe="")
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{encoded_ticker}/range/1/minute/{start_date}/{end_date}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results = payload.get("results", [])

    if not results:
        df = pd.DataFrame()
        df.to_csv(cache_path, index=False)
        time.sleep(sleep_seconds)
        return df

    df = pd.DataFrame(results).rename(
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

    if "vwap" not in df.columns:
        df["vwap"] = df["close"]

    df["ticker"] = ticker
    df.to_csv(cache_path, index=False)

    time.sleep(sleep_seconds)
    return df


def first15_by_date(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "prior_first15_volume",
                "prior_first15_dollar_volume",
                "prior_first15_bars",
            ]
        )

    df = bars.copy()

    if "timestamp_ms" not in df.columns:
        return pd.DataFrame()

    df["bar_start_utc"] = pd.to_datetime(
        pd.to_numeric(df["timestamp_ms"], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )

    df = df[df["bar_start_utc"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["bar_start_et"] = df["bar_start_utc"].dt.tz_convert("America/New_York")
    df["trade_date"] = df["bar_start_et"].dt.date.astype(str)
    df["bar_time"] = df["bar_start_et"].dt.time

    start = pd.to_datetime("09:30").time()
    end = pd.to_datetime("09:45").time()

    first15 = df[(df["bar_time"] >= start) & (df["bar_time"] < end)].copy()
    if first15.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "prior_first15_volume",
                "prior_first15_dollar_volume",
                "prior_first15_bars",
            ]
        )

    for col in ["volume", "vwap", "close"]:
        first15[col] = pd.to_numeric(first15[col], errors="coerce")

    first15["dollar_volume"] = first15["volume"] * first15["vwap"].fillna(first15["close"])

    out = (
        first15.groupby("trade_date", observed=True)
        .agg(
            prior_first15_volume=("volume", "sum"),
            prior_first15_dollar_volume=("dollar_volume", "sum"),
            prior_first15_bars=("volume", "size"),
        )
        .reset_index()
    )

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument(
        "--features",
        default=None,
        help="Feature CSV for the date. If omitted, uses high_price_full_universe_first15_checks path.",
    )
    parser.add_argument(
        "--daily-panel",
        default="data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv",
    )
    parser.add_argument(
        "--cache-dir",
        default="data/cache/massive/first15_prior_1m",
    )
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    date_str = args.date

    if args.features:
        features_path = Path(args.features)
    else:
        features_path = Path(
            "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks"
        ) / f"high_price_full_universe_first15_features_{date_str}.csv"

    out_features_path = features_path.with_name(
        features_path.stem + "_with_first15_rvol.csv"
    )
    out_signals_path = features_path.with_name(
        f"high_price_full_universe_first15_signals_{date_str}_with_first15_rvol.csv"
    )

    api_key = get_api_key()
    cache_dir = Path(args.cache_dir)

    features = pd.read_csv(features_path)
    if "download_status" in features.columns:
        features = features[features["download_status"].eq("ok")].copy()

    features["trade_date"] = pd.to_datetime(features["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if args.limit:
        features = features.head(args.limit).copy()

    panel = pd.read_csv(args.daily_panel, usecols=["ticker", "trade_date"])
    panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel = panel.dropna(subset=["ticker", "trade_date"]).sort_values(["ticker", "trade_date"])

    target_date = pd.to_datetime(date_str).strftime("%Y-%m-%d")

    rows = []
    tickers = features["ticker"].dropna().astype(str).unique().tolist()

    print("date:", target_date)
    print("feature rows:", len(features))
    print("tickers:", len(tickers))
    print("lookback days:", args.lookback_days)

    for i, ticker in enumerate(tickers):
        if i % 25 == 0:
            print(f"processing {i}/{len(tickers)}")

        ticker_dates = panel[
            (panel["ticker"].astype(str) == ticker)
            & (panel["trade_date"] < target_date)
        ]["trade_date"].drop_duplicates().sort_values()

        prior_dates = ticker_dates.tail(args.lookback_days).tolist()

        if len(prior_dates) == 0:
            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": np.nan,
                    "avg_prior_20d_first15_dollar_volume": np.nan,
                    "prior_first15_days_used": 0,
                }
            )
            continue

        start_date = prior_dates[0]
        end_date = prior_dates[-1]

        try:
            bars = fetch_1m_range(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                api_key=api_key,
                cache_dir=cache_dir,
                sleep_seconds=args.sleep_seconds,
            )
            first15 = first15_by_date(bars)
            first15 = first15[first15["trade_date"].isin(prior_dates)].copy()

            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": first15["prior_first15_volume"].mean(),
                    "avg_prior_20d_first15_dollar_volume": first15["prior_first15_dollar_volume"].mean(),
                    "median_prior_20d_first15_volume": first15["prior_first15_volume"].median(),
                    "median_prior_20d_first15_dollar_volume": first15["prior_first15_dollar_volume"].median(),
                    "prior_first15_days_used": int(first15["trade_date"].nunique()),
                }
            )

        except Exception as exc:
            print(f"ERROR {ticker}: {exc}")
            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": np.nan,
                    "avg_prior_20d_first15_dollar_volume": np.nan,
                    "median_prior_20d_first15_volume": np.nan,
                    "median_prior_20d_first15_dollar_volume": np.nan,
                    "prior_first15_days_used": 0,
                }
            )

    avg = pd.DataFrame(rows)

    out = features.merge(avg, on="ticker", how="left")

    for col in [
        "first_15m_volume",
        "first_15m_dollar_volume",
        "avg_prior_20d_first15_volume",
        "avg_prior_20d_first15_dollar_volume",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["first15_volume_rvol_20d"] = np.where(
        out["avg_prior_20d_first15_volume"] > 0,
        out["first_15m_volume"] / out["avg_prior_20d_first15_volume"],
        np.nan,
    )

    out["first15_dollar_rvol_20d"] = np.where(
        out["avg_prior_20d_first15_dollar_volume"] > 0,
        out["first_15m_dollar_volume"] / out["avg_prior_20d_first15_dollar_volume"],
        np.nan,
    )

    out.to_csv(out_features_path, index=False)

    print()
    print("saved enriched features:", out_features_path)
    print("rows:", len(out))
    print("prior first15 days used:")
    print(out["prior_first15_days_used"].value_counts(dropna=False).sort_index().to_string())

    # Rebuild the old signal labels, but include the new first15 RVOL columns for sorting/review.
    for col in [
        "prev_close",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "first15_dollar_vs_prior_daily_avg" not in out.columns:
        out["first15_dollar_vs_prior_daily_avg"] = (
            out["first_15m_dollar_volume"] / pd.to_numeric(out["avg_dollar_volume_20d_prior"], errors="coerce")
        )

    base = (
        (out["prev_close"] >= 50)
        & (out["premarket_dollar_vs_prior_daily_avg"] <= 0.10)
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        & (out["first_15m_return_pct"] >= 1)
    )

    b_no_bad_gap = (
        base
        & (out["gap_pct"] >= 0)
        & (out["gap_pct"] < 10)
    )

    a_looser = (
        b_no_bad_gap
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (out["first15_dollar_vs_prior_daily_avg"] < 1.00)
        & (out["first_15m_return_pct"] >= 1)
        & (out["first_15m_return_pct"] < 8)
        & (out["first15_range_pct"] >= 2)
        & (out["first15_range_pct"] < 8)
    )

    aplus = (
        b_no_bad_gap
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (out["first15_dollar_vs_prior_daily_avg"] < 0.50)
        & (out["first_15m_return_pct"] >= 2)
        & (out["first_15m_return_pct"] < 8)
        & (out["first15_range_pct"] >= 2)
        & (out["first15_range_pct"] < 8)
    )

    out["signal_quality"] = ""
    out.loc[base, "signal_quality"] = "BASE"
    out.loc[b_no_bad_gap, "signal_quality"] = "B"
    out.loc[a_looser, "signal_quality"] = "A"
    out.loc[aplus, "signal_quality"] = "A+"

    signals = out[out["signal_quality"] != ""].copy()

    cols = [
        "trade_date",
        "ticker",
        "signal_quality",
        "prev_close",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first15_volume_rvol_20d",
        "prior_first15_days_used",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
    ]

    cols = [c for c in cols if c in signals.columns]

    signals = signals.sort_values(
        ["signal_quality", "first15_dollar_rvol_20d", "first15_dollar_vs_prior_daily_avg"],
        ascending=[True, False, False],
    )

    signals[cols].to_csv(out_signals_path, index=False)

    print()
    print("saved signals:", out_signals_path)
    print("signals:", len(signals))
    print()
    if len(signals):
        print("signal counts:")
        print(signals["signal_quality"].value_counts().to_string())
        print()
        print("Top 60 by first15 dollar RVOL:")
        print(
            signals[cols]
            .sort_values("first15_dollar_rvol_20d", ascending=False)
            .head(60)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
