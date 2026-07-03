from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd


BASE_URL = "https://api.massive.com"

DEFAULT_INPUT = Path("data/reference/stocks/today_watchlist_latest.csv")
DEFAULT_OUTPUT = Path("data/reference/stocks/today_watchlist_fundamentals_latest.csv")
DEFAULT_CACHE_DIR = Path("data/cache/massive/fundamentals_latest")


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def first_present(row: pd.Series, names: list[str]) -> float | None:
    for name in names:
        if name in row.index:
            value = safe_float(row[name])
            if value is not None:
                return value
    return None


def api_get(endpoint: str, params: dict[str, Any], api_key: str, retries: int = 4) -> dict[str, Any]:
    params = {k: v for k, v in params.items() if v is not None}
    params["apiKey"] = api_key

    url = f"{BASE_URL}{endpoint}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "market-data-platform/1.0",
        },
    )

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code in {429, 500, 502, 503, 504} and attempt < retries:
                wait = 1.5 * (attempt + 1)
                print(f"retry {attempt + 1}/{retries}: HTTP {e.code} {endpoint} waiting {wait:.1f}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} for {endpoint}: {body[:500]}") from e
        except urllib.error.URLError as e:
            if attempt < retries:
                wait = 1.5 * (attempt + 1)
                print(f"retry {attempt + 1}/{retries}: URL error {endpoint} waiting {wait:.1f}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"URL error for {endpoint}: {e}") from e

    raise RuntimeError(f"failed after retries: {endpoint}")


def cached_api_get(
    cache_dir: Path,
    ticker: str,
    name: str,
    endpoint: str,
    params: dict[str, Any],
    api_key: str,
    refresh: bool,
) -> dict[str, Any]:
    ticker_dir = cache_dir / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    cache_path = ticker_dir / f"{name}.json"

    if cache_path.exists() and not refresh:
        return json.loads(cache_path.read_text())

    payload = api_get(endpoint, params, api_key)
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def first_result(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results") or []
    if isinstance(results, list) and results:
        return results[0] or {}
    return {}


def enrich_ticker(ticker: str, api_key: str, cache_dir: Path, refresh: bool) -> dict[str, Any]:
    ticker = ticker.upper().strip()

    float_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="float",
        endpoint="/stocks/vX/float",
        params={
            "ticker": ticker,
            "limit": 1,
        },
        api_key=api_key,
        refresh=refresh,
    )

    ratios_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="ratios",
        endpoint="/stocks/financials/v1/ratios",
        params={
            "ticker": ticker,
            "limit": 1,
        },
        api_key=api_key,
        refresh=refresh,
    )

    balance_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="balance_sheet",
        endpoint="/stocks/financials/v1/balance-sheets",
        params={
            "tickers": ticker,
            "timeframe": "quarterly",
            "limit": 1,
            "sort": "filing_date.desc",
        },
        api_key=api_key,
        refresh=refresh,
    )

    income_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="income_statement",
        endpoint="/stocks/financials/v1/income-statements",
        params={
            "tickers": ticker,
            "timeframe": "trailing_twelve_months",
            "limit": 1,
            "sort": "filing_date.desc",
        },
        api_key=api_key,
        refresh=refresh,
    )

    short_interest_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="short_interest",
        endpoint="/stocks/v1/short-interest",
        params={
            "ticker": ticker,
            "limit": 1,
            "sort": "settlement_date.desc",
        },
        api_key=api_key,
        refresh=refresh,
    )

    short_volume_payload = cached_api_get(
        cache_dir=cache_dir,
        ticker=ticker,
        name="short_volume",
        endpoint="/stocks/v1/short-volume",
        params={
            "ticker": ticker,
            "limit": 1,
            "sort": "date.desc",
        },
        api_key=api_key,
        refresh=refresh,
    )

    flt = first_result(float_payload)
    ratios = first_result(ratios_payload)
    bs = first_result(balance_payload)
    inc = first_result(income_payload)
    si = first_result(short_interest_payload)
    sv = first_result(short_volume_payload)

    free_float = safe_float(flt.get("free_float"))
    market_cap = safe_float(ratios.get("market_cap"))
    price = safe_float(ratios.get("price"))

    shares_from_market_cap = None
    if market_cap is not None and price not in (None, 0):
        shares_from_market_cap = market_cap / price

    basic_shares = safe_float(inc.get("basic_shares_outstanding"))
    diluted_shares = safe_float(inc.get("diluted_shares_outstanding"))

    shares_outstanding = basic_shares or diluted_shares or shares_from_market_cap

    cash = safe_float(bs.get("cash_and_equivalents"))
    debt_current = safe_float(bs.get("debt_current"))
    long_term_debt = safe_float(bs.get("long_term_debt_and_capital_lease_obligations"))

    total_debt = None
    if debt_current is not None or long_term_debt is not None:
        total_debt = (debt_current or 0.0) + (long_term_debt or 0.0)

    net_cash = None
    if cash is not None or total_debt is not None:
        net_cash = (cash or 0.0) - (total_debt or 0.0)

    short_interest = safe_float(si.get("short_interest"))

    short_interest_pct_float = None
    if short_interest is not None and free_float not in (None, 0):
        short_interest_pct_float = short_interest / free_float * 100

    cash_to_market_cap = None
    if cash is not None and market_cap not in (None, 0):
        cash_to_market_cap = cash / market_cap

    debt_to_market_cap = None
    if total_debt is not None and market_cap not in (None, 0):
        debt_to_market_cap = total_debt / market_cap

    return {
        "ticker": ticker,

        "fund_float_effective_date": flt.get("effective_date"),
        "float": free_float,
        "free_float_percent": safe_float(flt.get("free_float_percent")),

        "fund_ratios_price": price,
        "market_cap": market_cap,
        "ratios_average_volume": safe_float(ratios.get("average_volume")),
        "current_ratio": safe_float(ratios.get("current")),
        "debt_to_equity": safe_float(ratios.get("debt_to_equity")),
        "price_to_sales": safe_float(ratios.get("price_to_sales")),
        "price_to_book": safe_float(ratios.get("price_to_book")),

        "fund_balance_filing_date": bs.get("filing_date"),
        "fund_balance_period_end": bs.get("period_end"),
        "cash": cash,
        "debt_current": debt_current,
        "long_term_debt": long_term_debt,
        "total_debt": total_debt,
        "net_cash": net_cash,
        "cash_to_market_cap": cash_to_market_cap,
        "debt_to_market_cap": debt_to_market_cap,

        "fund_income_filing_date": inc.get("filing_date"),
        "fund_income_period_end": inc.get("period_end"),
        "basic_shares_outstanding": basic_shares,
        "diluted_shares_outstanding": diluted_shares,
        "shares_outstanding": shares_outstanding,

        "short_interest_settlement_date": si.get("settlement_date"),
        "short_interest": short_interest,
        "short_interest_avg_daily_volume": safe_float(si.get("avg_daily_volume")),
        "days_to_cover": safe_float(si.get("days_to_cover")),
        "short_interest_pct_float": short_interest_pct_float,

        "short_volume_date": sv.get("date"),
        "short_volume": safe_float(sv.get("short_volume")),
        "short_volume_total_volume": safe_float(sv.get("total_volume")),
        "short_volume_ratio": safe_float(sv.get("short_volume_ratio")),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--refresh-cache", action="store_true")
    args = parser.parse_args()

    load_dotenv()

    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise SystemExit("Missing MASSIVE_API_KEY in environment or .env")

    input_path = Path(args.input)
    output_path = Path(args.output)
    cache_dir = Path(args.cache_dir)

    if not input_path.exists():
        raise SystemExit(f"missing input: {input_path}")

    df = pd.read_csv(input_path)

    if "ticker" not in df.columns:
        raise SystemExit(f"input has no ticker column: {input_path}")

    tickers = (
        df["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    if args.max_tickers is not None:
        tickers = tickers[: args.max_tickers]

    print(f"input rows: {len(df)}")
    print(f"unique tickers to enrich: {len(tickers)}")
    print(f"cache dir: {cache_dir}")

    enriched_rows = []

    for i, ticker in enumerate(tickers, start=1):
        print(f"[{i}/{len(tickers)}] {ticker}")
        try:
            enriched_rows.append(
                enrich_ticker(
                    ticker=ticker,
                    api_key=api_key,
                    cache_dir=cache_dir,
                    refresh=args.refresh_cache,
                )
            )
        except Exception as e:
            print(f"ERROR {ticker}: {e}")
            enriched_rows.append({"ticker": ticker, "fundamentals_error": str(e)})

        if args.sleep > 0:
            time.sleep(args.sleep)

    fund = pd.DataFrame(enriched_rows)

    out = df.merge(fund, on="ticker", how="left")

    for col in ["prev_close", "today_premarket_volume", "today_first_15m_volume", "today_volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "float" in out.columns:
        out["free_float_market_cap"] = out["float"] * pd.to_numeric(out.get("prev_close"), errors="coerce")

        if "today_premarket_volume" in out.columns:
            out["pre_market_volume_to_float"] = out["today_premarket_volume"] / out["float"]

        if "today_first_15m_volume" in out.columns:
            out["first_15m_volume_to_float"] = out["today_first_15m_volume"] / out["float"]

        if "today_volume" in out.columns:
            out["today_volume_to_float"] = out["today_volume"] / out["float"]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    print()
    print(f"saved: {output_path}")
    print(f"rows: {len(out)}")
    print(f"columns: {len(out.columns)}")

    show_cols = [
        "ticker",
        "prev_close",
        "market_cap",
        "shares_outstanding",
        "float",
        "free_float_percent",
        "free_float_market_cap",
        "pre_market_volume_to_float",
        "first_15m_volume_to_float",
        "short_interest_pct_float",
        "days_to_cover",
        "short_volume_ratio",
        "cash",
        "total_debt",
        "net_cash",
        "current_ratio",
        "debt_to_equity",
    ]

    show_cols = [c for c in show_cols if c in out.columns]
    print()
    print(out[show_cols].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
