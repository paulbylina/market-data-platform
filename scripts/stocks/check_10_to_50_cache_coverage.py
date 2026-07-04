from __future__ import annotations

from pathlib import Path
import pandas as pd


TASKS_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")


def build_cache_index() -> set[tuple[str, str]]:
    idx = set()

    for p in CACHE_DIR.glob("*_1m.csv"):
        name = p.name

        if "_to_" not in name:
            continue

        left, right = name.rsplit("_to_", 1)
        trade_date = right.replace("_1m.csv", "")

        if "_" not in left:
            continue

        ticker = left.rsplit("_", 1)[0]
        idx.add((ticker, trade_date))

    return idx


def price_bucket(x):
    if pd.isna(x):
        return "unknown"
    if x < 10:
        return "under_10"
    if x < 20:
        return "price_10_to_20"
    if x < 50:
        return "price_20_to_50"
    return "price_50_plus"


def main() -> None:
    cache = build_cache_index()
    print("cache files indexed:", len(cache))

    df = pd.read_csv(TASKS_PATH)

    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")
    df["gap_pct"] = pd.to_numeric(df["gap_pct"], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    df["price_bucket"] = df["prev_close"].apply(price_bucket)

    df["has_cache"] = [
        (str(t), str(d)) in cache
        for t, d in zip(df["ticker"], df["trade_date"])
    ]

    print()
    print("=== Overall cache coverage by price bucket ===")
    out = df.groupby("price_bucket").agg(
        rows=("ticker", "size"),
        cached=("has_cache", "sum"),
    )
    out["coverage_pct"] = out["cached"] / out["rows"] * 100
    print(out.sort_index().to_string())

    print()
    print("=== Gap 0-5 cache coverage by price bucket ===")
    g = df[(df["gap_pct"] >= 0) & (df["gap_pct"] <= 5)].copy()
    out = g.groupby("price_bucket").agg(
        rows=("ticker", "size"),
        cached=("has_cache", "sum"),
    )
    out["coverage_pct"] = out["cached"] / out["rows"] * 100
    print(out.sort_index().to_string())

    print()
    print("=== Cached $10-50 gap 0-5 rows by year ===")
    x = g[
        g["price_bucket"].isin(["price_10_to_20", "price_20_to_50"])
        & g["has_cache"]
    ].copy()
    x["year"] = x["trade_date"].str.slice(0, 4)
    print(x.groupby(["year", "price_bucket"]).size().to_string())

    out_path = Path(
        "data/research/full_market_scanner_10y/price_10_to_50_full_tasks_first15/"
        "cache_coverage_10_to_50.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print()
    print("saved:", out_path)


if __name__ == "__main__":
    main()
