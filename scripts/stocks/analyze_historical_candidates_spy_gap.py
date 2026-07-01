from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


BASE_URL = "https://api.massive.com/v2/aggs/ticker/SPY/range/1/day"


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


def fetch_spy_daily(start_date: str, end_date: str, api_key: str) -> pd.DataFrame:
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{start_date}/{end_date}?{params}"
    request = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results = payload.get("results", [])

    if not results:
        raise SystemExit("No SPY daily bars returned")

    df = pd.DataFrame(results)
    df = df.rename(
        columns={
            "o": "spy_open",
            "h": "spy_high",
            "l": "spy_low",
            "c": "spy_close",
            "v": "spy_volume",
            "vw": "spy_vwap",
            "t": "timestamp_ms",
        }
    )

    df["trade_date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.date.astype(str)
    df["spy_prev_close"] = df["spy_close"].shift(1)
    df["spy_gap_pct"] = (df["spy_open"] / df["spy_prev_close"] - 1) * 100
    df["spy_open_to_close_pct"] = (df["spy_close"] / df["spy_open"] - 1) * 100

    return df[
        [
            "trade_date",
            "spy_open",
            "spy_close",
            "spy_prev_close",
            "spy_gap_pct",
            "spy_open_to_close_pct",
        ]
    ].copy()


def summarize(label: str, df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "label": label,
            "trades": 0,
        }

    return {
        "label": label,
        "trades": len(df),
        "avg": df["open_to_close_pct"].mean(),
        "median": df["open_to_close_pct"].median(),
        "win_rate": (df["open_to_close_pct"] > 0).mean() * 100,
        "worst": df["open_to_close_pct"].min(),
        "best": df["open_to_close_pct"].max(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--candidates",
        default="data/research/full_market_scanner/historical_activated_dormant_gap_candidates.csv",
    )
    parser.add_argument("--output-dir", default="data/research/full_market_scanner")
    args = parser.parse_args()

    api_key = get_api_key()

    candidates_path = Path(args.candidates)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = pd.read_csv(candidates_path)
    candidates["trade_date"] = pd.to_datetime(candidates["trade_date"]).dt.date.astype(str)

    start_date = candidates["trade_date"].min()
    end_date = candidates["trade_date"].max()

    spy = fetch_spy_daily(start_date=start_date, end_date=end_date, api_key=api_key)

    merged = candidates.merge(spy, on="trade_date", how="left")
    merged["relative_gap_vs_spy_pct"] = merged["gap_pct"] - merged["spy_gap_pct"]

    out_path = output_dir / "historical_activated_dormant_gap_candidates_with_spy.csv"
    merged.to_csv(out_path, index=False)

    rows = []

    for rel_gap in [0, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5]:
        sub = merged[merged["relative_gap_vs_spy_pct"] >= rel_gap].copy()
        rows.append(summarize(f"rel_gap_vs_spy_ge_{rel_gap}", sub))

    for spy_gap in [-2, -1, -0.5, 0, 0.5, 1]:
        sub = merged[merged["spy_gap_pct"] <= spy_gap].copy()
        rows.append(summarize(f"spy_gap_le_{spy_gap}", sub))

    for spy_oc in [-2, -1, -0.5, 0, 0.5, 1]:
        sub = merged[merged["spy_open_to_close_pct"] <= spy_oc].copy()
        rows.append(summarize(f"spy_open_to_close_le_{spy_oc}", sub))

    summary = pd.DataFrame(rows)
    summary_path = output_dir / "historical_activated_dormant_gap_spy_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("saved candidates with SPY:", out_path)
    print("saved summary:", summary_path)

    print()
    print("=== Relative gap vs SPY summary ===")
    print(summary[summary["label"].str.startswith("rel_gap")].to_string(index=False))

    print()
    print("=== SPY daily gap weakness summary ===")
    print(summary[summary["label"].str.startswith("spy_gap")].to_string(index=False))

    print()
    print("=== SPY open-to-close weakness summary ===")
    print(summary[summary["label"].str.startswith("spy_open")].to_string(index=False))


if __name__ == "__main__":
    main()
