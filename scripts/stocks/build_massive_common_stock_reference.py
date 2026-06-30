from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from urllib.request import Request, urlopen


BASE_URL = "https://api.massive.com/v3/reference/tickers"


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


def add_api_key_to_url(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query["apiKey"] = api_key

    return urlunparse(
        parsed._replace(query=urlencode(query))
    )


def fetch_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--market", default="stocks")
    parser.add_argument("--type", default="CS", help="CS = common stock")
    parser.add_argument("--active", default="true")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    load_env_file()

    api_key = (
        os.environ.get("MASSIVE_API_KEY")
        or os.environ.get("POLYGON_API_KEY")
    )

    if not api_key:
        raise SystemExit(
            "Missing API key. Add MASSIVE_API_KEY=... or POLYGON_API_KEY=... to .env"
        )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    params = urlencode(
        {
            "market": args.market,
            "type": args.type,
            "active": args.active,
            "limit": args.limit,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}?{params}"

    rows: list[dict] = []
    page = 0

    while url:
        page += 1
        payload = fetch_json(url)
        results = payload.get("results", [])

        print(f"page {page}: {len(results)} rows")

        for item in results:
            rows.append(
                {
                    "ticker": item.get("ticker"),
                    "name": item.get("name"),
                    "market": item.get("market"),
                    "locale": item.get("locale"),
                    "primary_exchange": item.get("primary_exchange"),
                    "type": item.get("type"),
                    "active": item.get("active"),
                    "currency_name": item.get("currency_name"),
                    "cik": item.get("cik"),
                    "composite_figi": item.get("composite_figi"),
                    "share_class_figi": item.get("share_class_figi"),
                    "last_updated_utc": item.get("last_updated_utc"),
                }
            )

        next_url = payload.get("next_url")
        url = add_api_key_to_url(next_url, api_key) if next_url else ""

    rows = [
        row for row in rows
        if row["ticker"]
        and row["type"] == args.type
        and row["market"] == args.market
        and row["active"] is True
    ]

    rows = sorted(rows, key=lambda row: row["ticker"])

    today = date.today().isoformat()
    dated_path = output_dir / f"massive_common_stocks_reference_{today}.csv"
    latest_path = output_dir / "massive_common_stocks_reference_latest.csv"

    fieldnames = [
        "ticker",
        "name",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "last_updated_utc",
    ]

    for path in [dated_path, latest_path]:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print()
    print("saved:", dated_path)
    print("saved:", latest_path)
    print("common stock rows:", len(rows))

    print()
    print("=== Sample ===")
    for row in rows[:25]:
        print(
            f"{row['ticker']:<8} "
            f"{row['primary_exchange'] or '':<8} "
            f"{row['type']:<3} "
            f"{row['name']}"
        )


if __name__ == "__main__":
    main()
