from __future__ import annotations

from pathlib import Path
import pandas as pd


SEARCH_DIRS = [
    Path("data/reference/stocks"),
    Path("data/research/full_market_scanner_10y"),
]

PRICE_COL_CANDIDATES = [
    "prev_close",
    "close",
    "price",
    "last_price",
    "today_open",
    "open",
]


def find_price_col(df: pd.DataFrame) -> str | None:
    for c in PRICE_COL_CANDIDATES:
        if c in df.columns:
            return c
    return None


def analyze_file(path: Path) -> dict | None:
    try:
        df = pd.read_csv(path)
    except Exception:
        return None

    if "ticker" not in df.columns:
        return None

    price_col = find_price_col(df)
    if price_col is None:
        return None

    tmp = df.copy()
    tmp["ticker"] = tmp["ticker"].astype(str).str.upper()
    tmp[price_col] = pd.to_numeric(tmp[price_col], errors="coerce")

    tmp = tmp.dropna(subset=["ticker", price_col])
    tmp = tmp[tmp[price_col] > 0].copy()
    tmp = tmp.drop_duplicates(subset=["ticker"], keep="first")

    if tmp.empty:
        return None

    total = len(tmp)
    n_10_50 = ((tmp[price_col] >= 10) & (tmp[price_col] < 50)).sum()
    n_50_plus = (tmp[price_col] >= 50).sum()
    n_under_10 = (tmp[price_col] < 10).sum()

    return {
        "file": str(path),
        "price_col": price_col,
        "tickers": total,
        "under_10": int(n_under_10),
        "stocks_10_to_50": int(n_10_50),
        "stocks_50_plus": int(n_50_plus),
        "pct_10_to_50": n_10_50 / total * 100,
        "pct_50_plus": n_50_plus / total * 100,
        "ratio_10_50_to_50_plus": n_10_50 / n_50_plus if n_50_plus else None,
        "ratio_50_plus_to_10_50": n_50_plus / n_10_50 if n_10_50 else None,
    }


def main() -> None:
    rows = []

    for root in SEARCH_DIRS:
        if not root.exists():
            continue

        for path in root.rglob("*.csv"):
            result = analyze_file(path)
            if result:
                rows.append(result)

    if not rows:
        raise SystemExit("No usable files found with ticker + price column.")

    out = pd.DataFrame(rows)

    # Prefer large current reference files, not tiny watchlists.
    out = out.sort_values(["tickers", "file"], ascending=[False, True]).reset_index(drop=True)

    print("=== Candidate files ranked by ticker count ===")
    print(
        out[
            [
                "file",
                "price_col",
                "tickers",
                "under_10",
                "stocks_10_to_50",
                "stocks_50_plus",
                "pct_10_to_50",
                "pct_50_plus",
                "ratio_10_50_to_50_plus",
            ]
        ]
        .head(25)
        .to_string(index=False)
    )

    best = out.iloc[0]

    print()
    print("=== Best universe estimate ===")
    print("file:", best["file"])
    print("price_col:", best["price_col"])
    print("tickers:", int(best["tickers"]))
    print("$10-$50:", int(best["stocks_10_to_50"]), f"({best['pct_10_to_50']:.2f}%)")
    print("$50+:", int(best["stocks_50_plus"]), f"({best['pct_50_plus']:.2f}%)")
    print("under $10:", int(best["under_10"]))
    print()
    print("$10-$50 / $50+ ratio:", round(best["ratio_10_50_to_50_plus"], 2))
    print("$50+ / $10-$50 ratio:", round(best["ratio_50_plus_to_10_50"], 2))


if __name__ == "__main__":
    main()
