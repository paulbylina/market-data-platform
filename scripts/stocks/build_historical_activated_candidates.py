from __future__ import annotations

import argparse
import csv
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


def fetch_daily_bars(
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

    url = f"{BASE_URL}/{encoded_ticker}/range/1/day/{start_date}/{end_date}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload.get("results", [])


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
    df["trade_date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.date.astype(str)

    if "vwap" not in df.columns:
        df["vwap"] = df["close"]

    df["dollar_volume"] = df["volume"] * df["vwap"].fillna(df["close"])

    return df[
        [
            "ticker",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "dollar_volume",
            "transactions",
        ]
    ].copy()


def load_common_stocks(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Common stock reference not found: {path}")

    df = pd.read_csv(path)

    required = {"ticker", "name"}
    missing = required - set(df.columns)

    if missing:
        raise SystemExit(f"Common stock reference missing columns: {sorted(missing)}")

    return df


BAD_SECURITY_NAME_PATTERNS = [
    "preferred",
    "depositary share",
    "depositary shares",
    "senior notes",
    "subordinated notes",
    "notes due",
    "bond",
    "bonds",
    "debenture",
    "cumulative redeemable",
    "redeemable preferred",
    "warrant",
    "warrants",
    "right",
    "rights",
    "unit",
    "units",
    "acquisition",
    "blank check",
    "spac",
    "gigcapital",
    "newhold",
    "equity partners",
]


def is_operating_common_stock(name: str | None) -> bool:
    if not name:
        return True

    lowered = name.lower()

    return not any(pattern in lowered for pattern in BAD_SECURITY_NAME_PATTERNS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--common-stock-reference",
        default="data/reference/stocks/massive_common_stocks_reference_latest.csv",
    )
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--output-dir", default="data/research/full_market_scanner")
    parser.add_argument("--cache-dir", default="data/cache/massive/daily")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--random-sample", action="store_true")
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)

    parser.add_argument("--min-prev-close", type=float, default=2.0)
    parser.add_argument("--max-prev-20d-dollar-volume", type=float, default=1_000_000)
    parser.add_argument("--min-today-dollar-volume", type=float, default=250_000)
    parser.add_argument("--min-volume-rvol-20d", type=float, default=2.0)
    parser.add_argument("--min-gap-pct", type=float, default=1.0)
    parser.add_argument("--lookback-days", type=int, default=20)

    args = parser.parse_args()

    api_key = get_api_key()

    common = load_common_stocks(Path(args.common_stock_reference))
    common = common[common["name"].apply(is_operating_common_stock)].copy()

    if args.limit:
        if args.random_sample:
            common = common.sample(
                n=min(args.limit, len(common)),
                random_state=args.random_seed,
            ).sort_values("ticker").reset_index(drop=True).copy()
        else:
            common = common.head(args.limit).reset_index(drop=True).copy()
    else:
        common = common.reset_index(drop=True).copy()

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    errors = []

    print("tickers:", len(common))
    print("start:", args.start_date)
    print("end:", args.end_date)
    print()

    for i, row in common.iterrows():
        ticker = row["ticker"]
        name = row.get("name")

        cache_path = cache_dir / f"{ticker}_{args.start_date}_{args.end_date}_daily.csv"

        print(f"[{i + 1}/{len(common)}] {ticker}")

        try:
            if cache_path.exists():
                df = pd.read_csv(cache_path)
            else:
                results = fetch_daily_bars(
                    ticker=ticker,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    api_key=api_key,
                )
                df = bars_to_df(ticker, results)
                df.to_csv(cache_path, index=False)
                time.sleep(args.sleep_seconds)

            if df.empty:
                continue

            df["name"] = name
            all_rows.append(df)

        except Exception as exc:
            errors.append({"ticker": ticker, "error": str(exc)})
            print(f"ERROR {ticker}: {exc}")

    if not all_rows:
        raise SystemExit("No daily bars loaded")

    panel = pd.concat(all_rows, ignore_index=True)

    for col in ["open", "high", "low", "close", "volume", "vwap", "dollar_volume"]:
        panel[col] = pd.to_numeric(panel[col], errors="coerce")

    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    panel = panel.sort_values(["ticker", "trade_date"]).reset_index(drop=True)

    grouped = panel.groupby("ticker", group_keys=False)

    panel["prev_close"] = grouped["close"].shift(1)
    panel["prev_volume"] = grouped["volume"].shift(1)
    panel["prev_dollar_volume"] = grouped["dollar_volume"].shift(1)

    panel["avg_volume_20d_prior"] = grouped["volume"].transform(
        lambda s: s.shift(1).rolling(args.lookback_days).mean()
    )
    panel["avg_dollar_volume_20d_prior"] = grouped["dollar_volume"].transform(
        lambda s: s.shift(1).rolling(args.lookback_days).mean()
    )

    panel["gap_pct"] = (panel["open"] / panel["prev_close"] - 1) * 100
    panel["volume_rvol_20d"] = panel["volume"] / panel["avg_volume_20d_prior"]
    panel["dollar_volume_rvol_20d"] = panel["dollar_volume"] / panel["avg_dollar_volume_20d_prior"]
    panel["open_to_close_pct"] = (panel["close"] / panel["open"] - 1) * 100

    panel["is_dormant_prior"] = (
        panel["avg_dollar_volume_20d_prior"] <= args.max_prev_20d_dollar_volume
    )

    panel["is_activated_today"] = (
        (panel["dollar_volume"] >= args.min_today_dollar_volume)
        & (panel["volume_rvol_20d"] >= args.min_volume_rvol_20d)
    )

    panel["is_gap_up"] = panel["gap_pct"] >= args.min_gap_pct

    candidates = panel[
        (panel["prev_close"] >= args.min_prev_close)
        & panel["is_dormant_prior"]
        & panel["is_activated_today"]
        & panel["is_gap_up"]
    ].copy()

    candidates = candidates.sort_values(
        ["trade_date", "dollar_volume", "volume_rvol_20d"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    panel_path = output_dir / "historical_full_market_daily_panel.csv"
    candidates_path = output_dir / "historical_activated_dormant_gap_candidates.csv"
    tasks_path = output_dir / "historical_activated_dormant_gap_intraday_tasks.csv"
    errors_path = output_dir / "historical_activated_dormant_gap_errors.csv"

    panel.to_csv(panel_path, index=False)
    candidates.to_csv(candidates_path, index=False)

    tasks = candidates[["ticker", "trade_date"]].drop_duplicates().copy()
    tasks["trade_date"] = tasks["trade_date"].dt.strftime("%Y-%m-%d")
    tasks["start_date"] = tasks["trade_date"]
    tasks["end_date"] = tasks["trade_date"]
    tasks = tasks[["ticker", "trade_date", "start_date", "end_date"]]
    tasks.to_csv(tasks_path, index=False)

    if errors:
        pd.DataFrame(errors).to_csv(errors_path, index=False)

    print()
    print("saved panel:", panel_path)
    print("saved candidates:", candidates_path)
    print("saved intraday tasks:", tasks_path)
    if errors:
        print("saved errors:", errors_path)

    print()
    print("=== Summary ===")
    print("daily panel rows:", len(panel))
    print("candidate rows:", len(candidates))
    print("unique candidate tickers:", candidates["ticker"].nunique())
    print("unique candidate dates:", candidates["trade_date"].nunique())

    if len(candidates):
        print()
        print("=== Candidate daily open-to-close ===")
        print("avg:", round(candidates["open_to_close_pct"].mean(), 4))
        print("median:", round(candidates["open_to_close_pct"].median(), 4))
        print("win rate:", round((candidates["open_to_close_pct"] > 0).mean() * 100, 2))
        print("worst:", round(candidates["open_to_close_pct"].min(), 4))
        print("best:", round(candidates["open_to_close_pct"].max(), 4))

        print()
        print("=== Top candidates ===")
        cols = [
            "ticker",
            "name",
            "trade_date",
            "prev_close",
            "gap_pct",
            "avg_dollar_volume_20d_prior",
            "dollar_volume",
            "volume_rvol_20d",
            "open_to_close_pct",
        ]
        print(candidates[cols].tail(30).to_string(index=False))


if __name__ == "__main__":
    main()
