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
        description="Build 1-minute quote bars from cleaned Databento MBP-1 BBO session Parquet."
    )
    parser.add_argument("--symbol", default="ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--schema", default="mbp-1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbol_slug = symbol_to_slug(args.symbol)

    src_path = Path(
        f"data/processed/databento/top_of_book_clean/{symbol_slug}/"
        f"session_date={args.session_date}/"
        f"{symbol_slug}_{args.session_date}_{args.schema}_bbo_clean.parquet"
    )

    out_dir = Path(
        f"data/processed/databento/quote_bars_1m/{symbol_slug}/"
        f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{args.session_date}_{args.schema}_quote_1m.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    con = duckdb.connect()

    print(f"Reading: {src_path}")
    print(f"Writing: {out_path}")

    con.execute(
        f"""
        COPY (
            SELECT
                DATE_TRUNC('minute', ts_recv) AS minute,

                symbol,
                instrument_id,

                COUNT(*) AS event_count,

                SUM(CASE WHEN action = 'A' THEN 1 ELSE 0 END) AS add_events,
                SUM(CASE WHEN action = 'C' THEN 1 ELSE 0 END) AS cancel_events,
                SUM(CASE WHEN action = 'M' THEN 1 ELSE 0 END) AS modify_events,
                SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END) AS trade_events,

                SUM(CASE WHEN action = 'T' THEN size ELSE 0 END) AS trade_size_sum,
                SUM(CASE WHEN action = 'T' AND side = 'B' THEN size ELSE 0 END) AS trade_size_side_b,
                SUM(CASE WHEN action = 'T' AND side = 'A' THEN size ELSE 0 END) AS trade_size_side_a,
                SUM(CASE WHEN action = 'T' AND side = 'N' THEN size ELSE 0 END) AS trade_size_side_n,

                FIRST(bid_px ORDER BY ts_recv, sequence) AS bid_open,
                MAX(bid_px) AS bid_high,
                MIN(bid_px) AS bid_low,
                LAST(bid_px ORDER BY ts_recv, sequence) AS bid_close,

                FIRST(ask_px ORDER BY ts_recv, sequence) AS ask_open,
                MAX(ask_px) AS ask_high,
                MIN(ask_px) AS ask_low,
                LAST(ask_px ORDER BY ts_recv, sequence) AS ask_close,

                FIRST(mid_px ORDER BY ts_recv, sequence) AS mid_open,
                MAX(mid_px) AS mid_high,
                MIN(mid_px) AS mid_low,
                LAST(mid_px ORDER BY ts_recv, sequence) AS mid_close,

                FIRST(microprice ORDER BY ts_recv, sequence) AS micro_open,
                MAX(microprice) AS micro_high,
                MIN(microprice) AS micro_low,
                LAST(microprice ORDER BY ts_recv, sequence) AS micro_close,

                AVG(spread_ticks) AS spread_ticks_avg,
                QUANTILE_CONT(spread_ticks, 0.50) AS spread_ticks_median,
                MAX(spread_ticks) AS spread_ticks_max,

                AVG(imbalance) AS imbalance_avg,
                QUANTILE_CONT(imbalance, 0.10) AS imbalance_p10,
                QUANTILE_CONT(imbalance, 0.50) AS imbalance_median,
                QUANTILE_CONT(imbalance, 0.90) AS imbalance_p90,

                AVG(bid_sz) AS bid_sz_avg,
                QUANTILE_CONT(bid_sz, 0.50) AS bid_sz_median,
                MAX(bid_sz) AS bid_sz_max,

                AVG(ask_sz) AS ask_sz_avg,
                QUANTILE_CONT(ask_sz, 0.50) AS ask_sz_median,
                MAX(ask_sz) AS ask_sz_max,

                MIN(ts_recv) AS first_ts_recv,
                MAX(ts_recv) AS last_ts_recv

            FROM parquet_scan('{src_path}')
            GROUP BY 1, 2, 3
            ORDER BY 1
        )
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print()
    print("=== 1-Minute Quote Bar Stats ===")
    print(
        con.execute(
            f"""
            SELECT
                COUNT(*) AS bars,
                MIN(minute) AS min_minute,
                MAX(minute) AS max_minute,
                MIN(event_count) AS min_events,
                QUANTILE_CONT(event_count, 0.50) AS median_events,
                QUANTILE_CONT(event_count, 0.90) AS p90_events,
                MAX(event_count) AS max_events,
                SUM(trade_events) AS total_trade_events,
                SUM(trade_size_sum) AS total_trade_size,
                MAX(spread_ticks_max) AS max_spread_ticks
            FROM parquet_scan('{out_path}')
            """
        ).fetchdf()
    )

    print()
    print("=== Sample Around 07:30 CT ===")
    print(
        con.execute(
            f"""
            SELECT
                minute,
                event_count,
                trade_events,
                trade_size_sum,
                mid_open,
                mid_high,
                mid_low,
                mid_close,
                spread_ticks_avg,
                spread_ticks_max,
                imbalance_avg
            FROM parquet_scan('{out_path}')
            WHERE CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) >= TIME '07:25:00'
              AND CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) <  TIME '07:35:00'
            ORDER BY minute
            """
        ).fetchdf()
    )

    size_mb = out_path.stat().st_size / (1024 ** 2)
    print()
    print(f"Saved: {out_path}")
    print(f"Size MB: {size_mb:.2f}")


if __name__ == "__main__":
    main()
