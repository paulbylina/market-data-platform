from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers"


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


def as_float(value):
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_snapshot(api_key: str, include_otc: bool) -> dict:
    params = urlencode(
        {
            "include_otc": str(include_otc).lower(),
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def classify_activity(prev_dollar_volume: float | None) -> str:
    if prev_dollar_volume is None:
        return "unknown"

    if prev_dollar_volume >= 20_000_000:
        return "very_liquid"

    if prev_dollar_volume >= 5_000_000:
        return "liquid"

    if prev_dollar_volume >= 1_000_000:
        return "thin_but_tradeable"

    return "dormant"


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


def is_operating_common_stock(name: str | None, include_spacs: bool = False) -> bool:
    if not name:
        return True

    lowered = name.lower()

    for pattern in BAD_SECURITY_NAME_PATTERNS:
        if include_spacs and pattern in {
            "acquisition",
            "blank check",
            "spac",
            "gigcapital",
            "newhold",
            "equity partners",
        }:
            continue

        if pattern in lowered:
            return False

    return True


def load_common_stock_reference(path: Path) -> dict[str, dict]:
    if not path.exists():
        raise SystemExit(f"Common stock reference not found: {path}")

    out: dict[str, dict] = {}

    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker")
            if ticker:
                out[ticker] = row

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--include-otc", action="store_true")
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument(
        "--common-stock-reference",
        default="data/reference/stocks/massive_common_stocks_reference_latest.csv",
    )
    parser.add_argument(
        "--no-common-stock-filter",
        action="store_true",
        help="Keep raw snapshot tickers instead of filtering to type=CS reference.",
    )
    parser.add_argument(
        "--no-operating-common-filter",
        action="store_true",
        help="Do not exclude SPACs, notes, preferreds, units, warrants, etc. by name.",
    )
    parser.add_argument(
        "--include-spacs",
        action="store_true",
        help="Allow acquisition/SPAC common shares through the operating common filter.",
    )
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

    common_reference: dict[str, dict] = {}
    if not args.no_common_stock_filter:
        common_reference = load_common_stock_reference(Path(args.common_stock_reference))

    payload = fetch_snapshot(api_key=api_key, include_otc=args.include_otc)

    today = date.today().isoformat()

    raw_path = output_dir / f"massive_snapshot_raw_{today}.json"
    raw_path.write_text(json.dumps(payload, indent=2))

    rows = []
    raw_count = 0
    skipped_not_common = 0
    skipped_low_price = 0

    for item in payload.get("tickers", []):
        raw_count += 1

        ticker = item.get("ticker")

        if not ticker:
            continue

        reference_row = common_reference.get(ticker)

        if common_reference and reference_row is None:
            skipped_not_common += 1
            continue

        if (
            not args.no_operating_common_filter
            and not is_operating_common_stock(
                (reference_row or {}).get("name"),
                include_spacs=args.include_spacs,
            )
        ):
            skipped_not_common += 1
            continue

        prev_day = item.get("prevDay") or {}
        day = item.get("day") or {}
        minute = item.get("min") or {}

        prev_close = as_float(prev_day.get("c"))
        prev_volume = as_float(prev_day.get("v"))
        prev_vwap = as_float(prev_day.get("vw"))

        today_open = as_float(day.get("o"))
        today_volume = as_float(day.get("v"))
        today_vwap = as_float(day.get("vw"))

        minute_volume = as_float(minute.get("v"))
        minute_vwap = as_float(minute.get("vw"))

        if prev_close is None:
            continue

        if prev_close < args.min_price:
            skipped_low_price += 1
            continue

        prev_dollar_volume = None
        if prev_volume is not None and prev_vwap is not None:
            prev_dollar_volume = prev_volume * prev_vwap

        today_dollar_volume = None
        if today_volume is not None and today_vwap is not None:
            today_dollar_volume = today_volume * today_vwap

        minute_dollar_volume = None
        if minute_volume is not None and minute_vwap is not None:
            minute_dollar_volume = minute_volume * minute_vwap

        gap_pct = None
        if today_open is not None and prev_close:
            gap_pct = (today_open / prev_close - 1) * 100

        today_vs_prev_volume_pct = None
        if today_volume is not None and prev_volume and prev_volume > 0:
            today_vs_prev_volume_pct = (today_volume / prev_volume) * 100

        rows.append(
            {
                "ticker": ticker,
                "name": (reference_row or {}).get("name"),
                "primary_exchange": (reference_row or {}).get("primary_exchange"),
                "type": (reference_row or {}).get("type"),
                "prev_close": prev_close,
                "prev_volume": prev_volume,
                "prev_vwap": prev_vwap,
                "prev_dollar_volume": prev_dollar_volume,
                "activity_tier": classify_activity(prev_dollar_volume),
                "today_open": today_open,
                "today_volume": today_volume,
                "today_vwap": today_vwap,
                "today_dollar_volume": today_dollar_volume,
                "today_vs_prev_volume_pct": today_vs_prev_volume_pct,
                "minute_volume": minute_volume,
                "minute_vwap": minute_vwap,
                "minute_dollar_volume": minute_dollar_volume,
                "gap_pct": gap_pct,
                "todays_change_pct": as_float(item.get("todaysChangePerc")),
                "updated": item.get("updated"),
            }
        )

    rows = sorted(
        rows,
        key=lambda row: (
            -(row["today_dollar_volume"] or 0),
            -(row["prev_dollar_volume"] or 0),
            row["ticker"],
        ),
    )

    filter_label = "common_stocks" if common_reference else "raw"
    csv_path = output_dir / f"massive_snapshot_{filter_label}_{today}.csv"
    latest_path = output_dir / f"massive_snapshot_{filter_label}_latest.csv"

    fieldnames = [
        "ticker",
        "name",
        "primary_exchange",
        "type",
        "prev_close",
        "prev_volume",
        "prev_vwap",
        "prev_dollar_volume",
        "activity_tier",
        "today_open",
        "today_volume",
        "today_vwap",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "minute_volume",
        "minute_vwap",
        "minute_dollar_volume",
        "gap_pct",
        "todays_change_pct",
        "updated",
    ]

    for path in [csv_path, latest_path]:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print("saved raw:", raw_path)
    print("saved csv:", csv_path)
    print("saved latest:", latest_path)
    print()
    print("raw snapshot tickers:", raw_count)
    print("skipped not common stock:", skipped_not_common)
    print("skipped low price:", skipped_low_price)
    print("final rows:", len(rows))

    counts = {}
    for row in rows:
        counts[row["activity_tier"]] = counts.get(row["activity_tier"], 0) + 1

    print()
    print("=== Activity tiers ===")
    for tier, count in sorted(counts.items()):
        print(f"{tier}: {count}")

    print()
    print("=== Top today dollar volume ===")
    for row in rows[:25]:
        print(
            f"{row['ticker']:<8} "
            f"prev_close={row['prev_close']:<8.2f} "
            f"prev_dollar_volume={(row['prev_dollar_volume'] or 0):>14,.0f} "
            f"today_dollar_volume={(row['today_dollar_volume'] or 0):>14,.0f} "
            f"gap_pct={(row['gap_pct'] if row['gap_pct'] is not None else 0):>7.2f} "
            f"tier={row['activity_tier']:<20} "
            f"name={row['name']}"
        )

    print()
    print("=== Dormant but active today ===")
    dormant_active = [
        row
        for row in rows
        if row["activity_tier"] == "dormant"
        and (row["today_dollar_volume"] or 0) >= 250_000
    ]

    dormant_active = sorted(
        dormant_active,
        key=lambda row: row["today_dollar_volume"] or 0,
        reverse=True,
    )[:25]

    for row in dormant_active:
        print(
            f"{row['ticker']:<8} "
            f"prev_close={row['prev_close']:<8.2f} "
            f"prev_dollar_volume={(row['prev_dollar_volume'] or 0):>12,.0f} "
            f"today_dollar_volume={(row['today_dollar_volume'] or 0):>12,.0f} "
            f"today_vs_prev_volume_pct={(row['today_vs_prev_volume_pct'] or 0):>8.2f} "
            f"gap_pct={(row['gap_pct'] if row['gap_pct'] is not None else 0):>7.2f} "
            f"name={row['name']}"
        )


if __name__ == "__main__":
    main()
