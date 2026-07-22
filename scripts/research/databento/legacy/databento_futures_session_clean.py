from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def symbol_to_slug(symbol: str) -> str:
    s = symbol.lower()
    s = s.replace(".v.", "_v")
    s = s.replace(".", "_")
    s = s.replace("-", "_")
    s = s.replace("/", "_")
    return s


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean normalized Databento MBP-1 BBO Parquet for one futures session."
    )
    parser.add_argument("--symbol", default="ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--schema", default="mbp-1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbol_slug = symbol_to_slug(args.symbol)

    src_path = Path(
        f"data/processed/databento/top_of_book/{symbol_slug}/"
        f"session_date={args.session_date}/"
        f"{symbol_slug}_{args.session_date}_{args.schema}_bbo.parquet"
    )

    out_dir = Path(
        f"data/processed/databento/top_of_book_clean/{symbol_slug}/"
        f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{args.session_date}_{args.schema}_bbo_clean.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    con = duckdb.connect()

    print(f"Reading: {src_path}")
    print(f"Writing: {out_path}")

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM parquet_scan('{src_path}')
            WHERE bid_px > 0
              AND ask_px > 0
              AND ask_px > bid_px
        )
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print()
    print("=== Clean File Stats ===")
    print(
        con.execute(
            f"""
            SELECT
                COUNT(*) AS rows,
                MIN(ts_recv) AS min_ts_recv,
                MAX(ts_recv) AS max_ts_recv,
                MIN(spread_ticks) AS min_spread_ticks,
                QUANTILE_CONT(spread_ticks, 0.50) AS median_spread_ticks,
                QUANTILE_CONT(spread_ticks, 0.99) AS p99_spread_ticks,
                QUANTILE_CONT(spread_ticks, 0.999) AS p999_spread_ticks,
                MAX(spread_ticks) AS max_spread_ticks
            FROM parquet_scan('{out_path}')
            """
        ).fetchdf()
    )

    print()
    print("=== Removed Rows Check ===")
    print(
        con.execute(
            f"""
            SELECT
                (SELECT COUNT(*) FROM parquet_scan('{src_path}')) AS original_rows,
                (SELECT COUNT(*) FROM parquet_scan('{out_path}')) AS clean_rows,
                (SELECT COUNT(*) FROM parquet_scan('{src_path}'))
                  - (SELECT COUNT(*) FROM parquet_scan('{out_path}')) AS removed_rows
            """
        ).fetchdf()
    )

    size_mb = out_path.stat().st_size / (1024 ** 2)
    print()
    print(f"Saved: {out_path}")
    print(f"Size MB: {size_mb:.2f}")


if __name__ == "__main__":
    main()
