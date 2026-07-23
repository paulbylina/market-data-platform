from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb


DEFAULT_DATASET = "GLBX.MDP3"
INPUT_NAME = "mbo_bbo"
OUTPUT_NAME = "mbo_quote_1m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build 1-minute quote bars from reconstructed Databento futures MBO BBO parquet."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)

    parser.add_argument("--chunk-label", help="Chunk label, e.g. 2026-06-28_2026-07-05")
    parser.add_argument("--start-utc", help="UTC chunk start, e.g. 2026-06-28T00:00:00Z")
    parser.add_argument("--end-utc", help="UTC chunk end, e.g. 2026-07-05T00:00:00Z")

    parser.add_argument(
        "--input-root",
        default="data/processed/databento/mbo_bbo",
        help="Input reconstructed BBO root.",
    )
    parser.add_argument(
        "--output-root",
        default="data/processed/databento/mbo_quote_bars_1m",
        help="Output 1-minute quote bar root.",
    )
    parser.add_argument(
        "--events-root",
        default="data/processed/databento/mbo_events",
        help="Input normalized MBO events root used to add event counts and volume.",
    )
    parser.add_argument(
        "--bar-time",
        choices=["ts_event", "ts_recv"],
        default="ts_event",
        help="Timestamp used for minute buckets. Default: ts_event.",
    )
    parser.add_argument("--overwrite", action="store_true")

    return parser.parse_args()


def parse_utc(value: str) -> datetime:
    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def date_label(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def build_chunk_label(args: argparse.Namespace) -> str:
    has_explicit_dates = args.start_utc is not None or args.end_utc is not None

    if args.chunk_label and has_explicit_dates:
        raise ValueError("Use either --chunk-label or --start-utc/--end-utc, not both.")

    if args.chunk_label:
        return args.chunk_label

    if has_explicit_dates:
        if not args.start_utc or not args.end_utc:
            raise ValueError("Use both --start-utc and --end-utc together.")

        start_utc = parse_utc(args.start_utc)
        end_utc = parse_utc(args.end_utc)

        return f"{date_label(start_utc)}_{date_label(end_utc)}"

    raise ValueError("Use either --chunk-label or both --start-utc and --end-utc.")


def main() -> None:
    args = parse_args()

    chunk_label = build_chunk_label(args)
    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)

    src_path = (
        Path(args.input_root)
        / dataset_slug
        / symbol_slug
        / chunk_label
        / f"{symbol_slug}_{chunk_label}_{INPUT_NAME}.parquet"
    )

    events_path = (
        Path(args.events_root)
        / dataset_slug
        / symbol_slug
        / chunk_label
        / f"{symbol_slug}_{chunk_label}_mbo_events.parquet"
    )

    out_dir = Path(args.output_root) / dataset_slug / symbol_slug / chunk_label
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{chunk_label}_{OUTPUT_NAME}.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    if not events_path.exists():
        raise FileNotFoundError(events_path)

    if out_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Output already exists: {out_path}\n"
            "Use --overwrite if you want to replace it."
        )

    con = duckdb.connect()

    bar_time = args.bar_time

    print("Building 1-minute MBO quote bars:")
    print(f"  dataset:     {args.dataset}")
    print(f"  symbol:      {args.symbol}")
    print(f"  chunk_label: {chunk_label}")
    print(f"  bar_time:    {bar_time}")
    print(f"  input:       {src_path}")
    print(f"  events:      {events_path}")
    print(f"  output:      {out_path}")
    print()

    con.execute(
        f"""
        COPY (
            WITH quote_bars AS (
                SELECT
                    DATE_TRUNC('minute', {bar_time}) AS minute,

                    COUNT(*) AS bbo_update_count,

                    FIRST(bid_px ORDER BY {bar_time}, event_index) AS bid_open,
                    MAX(bid_px) AS bid_high,
                    MIN(bid_px) AS bid_low,
                    LAST(bid_px ORDER BY {bar_time}, event_index) AS bid_close,

                    FIRST(ask_px ORDER BY {bar_time}, event_index) AS ask_open,
                    MAX(ask_px) AS ask_high,
                    MIN(ask_px) AS ask_low,
                    LAST(ask_px ORDER BY {bar_time}, event_index) AS ask_close,

                    FIRST(mid_px ORDER BY {bar_time}, event_index) AS mid_open,
                    MAX(mid_px) AS mid_high,
                    MIN(mid_px) AS mid_low,
                    LAST(mid_px ORDER BY {bar_time}, event_index) AS mid_close,

                    FIRST(microprice ORDER BY {bar_time}, event_index) AS micro_open,
                    MAX(microprice) AS micro_high,
                    MIN(microprice) AS micro_low,
                    LAST(microprice ORDER BY {bar_time}, event_index) AS micro_close,

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

                    MIN(event_index) AS first_event_index,
                    MAX(event_index) AS last_event_index,

                    MIN(ts_event) AS first_ts_event,
                    MAX(ts_event) AS last_ts_event,
                    MIN(ts_recv) AS first_ts_recv,
                    MAX(ts_recv) AS last_ts_recv

                FROM parquet_scan('{src_path}')
                GROUP BY 1
            ),

            event_bars AS (
                SELECT
                    DATE_TRUNC('minute', {bar_time}) AS minute,

                    SUM(CASE WHEN action = 'A' THEN 1 ELSE 0 END) AS source_add_rows,
                    SUM(CASE WHEN action = 'C' THEN 1 ELSE 0 END) AS source_cancel_rows,
                    SUM(CASE WHEN action = 'M' THEN 1 ELSE 0 END) AS source_modify_rows,
                    SUM(CASE WHEN action = 'T' THEN 1 ELSE 0 END) AS source_trade_rows,
                    SUM(CASE WHEN action = 'F' THEN 1 ELSE 0 END) AS source_fill_rows,
                    SUM(CASE WHEN action = 'R' THEN 1 ELSE 0 END) AS source_reset_rows,

                    SUM(CASE WHEN action = 'A' THEN COALESCE(size, 0) ELSE 0 END) AS source_add_size,
                    SUM(CASE WHEN action = 'C' THEN COALESCE(size, 0) ELSE 0 END) AS source_cancel_size,
                    SUM(CASE WHEN action = 'M' THEN COALESCE(size, 0) ELSE 0 END) AS source_modify_size,
                    SUM(CASE WHEN action = 'T' THEN COALESCE(size, 0) ELSE 0 END) AS source_trade_volume,
                    SUM(CASE WHEN action = 'F' THEN COALESCE(size, 0) ELSE 0 END) AS source_fill_volume,

                    SUM(CASE WHEN action = 'A' AND side = 'B' THEN 1 ELSE 0 END) AS source_bid_add_rows,
                    SUM(CASE WHEN action = 'A' AND side = 'A' THEN 1 ELSE 0 END) AS source_ask_add_rows,
                    SUM(CASE WHEN action = 'C' AND side = 'B' THEN 1 ELSE 0 END) AS source_bid_cancel_rows,
                    SUM(CASE WHEN action = 'C' AND side = 'A' THEN 1 ELSE 0 END) AS source_ask_cancel_rows,

                    SUM(CASE WHEN action = 'A' AND side = 'B' THEN COALESCE(size, 0) ELSE 0 END) AS source_bid_add_size,
                    SUM(CASE WHEN action = 'A' AND side = 'A' THEN COALESCE(size, 0) ELSE 0 END) AS source_ask_add_size,
                    SUM(CASE WHEN action = 'C' AND side = 'B' THEN COALESCE(size, 0) ELSE 0 END) AS source_bid_cancel_size,
                    SUM(CASE WHEN action = 'C' AND side = 'A' THEN COALESCE(size, 0) ELSE 0 END) AS source_ask_cancel_size,

                    SUM(CASE WHEN side = 'B' THEN 1 ELSE 0 END) AS source_bid_side_rows,
                    SUM(CASE WHEN side = 'A' THEN 1 ELSE 0 END) AS source_ask_side_rows,
                    SUM(CASE WHEN side = 'N' THEN 1 ELSE 0 END) AS source_neutral_side_rows,

                    COUNT(*) AS source_total_rows

                FROM parquet_scan('{events_path}')
                GROUP BY 1
            )

            SELECT
                q.minute,
                q.bbo_update_count,

                COALESCE(e.source_add_rows, 0) AS source_add_rows,
                COALESCE(e.source_cancel_rows, 0) AS source_cancel_rows,
                COALESCE(e.source_modify_rows, 0) AS source_modify_rows,
                COALESCE(e.source_trade_rows, 0) AS source_trade_rows,
                COALESCE(e.source_fill_rows, 0) AS source_fill_rows,
                COALESCE(e.source_reset_rows, 0) AS source_reset_rows,

                COALESCE(e.source_add_size, 0) AS source_add_size,
                COALESCE(e.source_cancel_size, 0) AS source_cancel_size,
                COALESCE(e.source_modify_size, 0) AS source_modify_size,
                COALESCE(e.source_trade_volume, 0) AS source_trade_volume,
                COALESCE(e.source_fill_volume, 0) AS source_fill_volume,

                COALESCE(e.source_bid_add_rows, 0) AS source_bid_add_rows,
                COALESCE(e.source_ask_add_rows, 0) AS source_ask_add_rows,
                COALESCE(e.source_bid_cancel_rows, 0) AS source_bid_cancel_rows,
                COALESCE(e.source_ask_cancel_rows, 0) AS source_ask_cancel_rows,

                COALESCE(e.source_bid_add_size, 0) AS source_bid_add_size,
                COALESCE(e.source_ask_add_size, 0) AS source_ask_add_size,
                COALESCE(e.source_bid_cancel_size, 0) AS source_bid_cancel_size,
                COALESCE(e.source_ask_cancel_size, 0) AS source_ask_cancel_size,

                CASE
                    WHEN COALESCE(e.source_bid_cancel_size, 0) = 0 THEN NULL
                    ELSE COALESCE(e.source_bid_add_size, 0) / NULLIF(e.source_bid_cancel_size, 0)
                END AS source_bid_add_cancel_size_ratio,

                CASE
                    WHEN COALESCE(e.source_ask_cancel_size, 0) = 0 THEN NULL
                    ELSE COALESCE(e.source_ask_add_size, 0) / NULLIF(e.source_ask_cancel_size, 0)
                END AS source_ask_add_cancel_size_ratio,

                CASE
                    WHEN COALESCE(e.source_ask_add_size, 0) = 0 THEN NULL
                    ELSE COALESCE(e.source_bid_add_size, 0) / NULLIF(e.source_ask_add_size, 0)
                END AS source_bid_vs_ask_add_size_ratio,

                CASE
                    WHEN COALESCE(e.source_ask_cancel_size, 0) = 0 THEN NULL
                    ELSE COALESCE(e.source_bid_cancel_size, 0) / NULLIF(e.source_ask_cancel_size, 0)
                END AS source_bid_vs_ask_cancel_size_ratio,

                COALESCE(e.source_bid_add_size, 0) - COALESCE(e.source_bid_cancel_size, 0) AS source_bid_net_add_size,
                COALESCE(e.source_ask_add_size, 0) - COALESCE(e.source_ask_cancel_size, 0) AS source_ask_net_add_size,

                (
                    COALESCE(e.source_bid_add_size, 0) - COALESCE(e.source_bid_cancel_size, 0)
                ) - (
                    COALESCE(e.source_ask_add_size, 0) - COALESCE(e.source_ask_cancel_size, 0)
                ) AS source_book_pressure_size,

                COALESCE(e.source_bid_side_rows, 0) AS source_bid_side_rows,
                COALESCE(e.source_ask_side_rows, 0) AS source_ask_side_rows,
                COALESCE(e.source_neutral_side_rows, 0) AS source_neutral_side_rows,
                COALESCE(e.source_total_rows, 0) AS source_total_rows,

                q.bid_open,
                q.bid_high,
                q.bid_low,
                q.bid_close,

                q.ask_open,
                q.ask_high,
                q.ask_low,
                q.ask_close,

                q.mid_open,
                q.mid_high,
                q.mid_low,
                q.mid_close,

                q.micro_open,
                q.micro_high,
                q.micro_low,
                q.micro_close,

                q.spread_ticks_avg,
                q.spread_ticks_median,
                q.spread_ticks_max,

                q.imbalance_avg,
                q.imbalance_p10,
                q.imbalance_median,
                q.imbalance_p90,

                q.bid_sz_avg,
                q.bid_sz_median,
                q.bid_sz_max,

                q.ask_sz_avg,
                q.ask_sz_median,
                q.ask_sz_max,

                q.first_event_index,
                q.last_event_index,

                q.first_ts_event,
                q.last_ts_event,
                q.first_ts_recv,
                q.last_ts_recv

            FROM quote_bars q
            LEFT JOIN event_bars e
            USING (minute)
            ORDER BY q.minute
        )
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print("=== 1-Minute Quote Bar Stats ===")
    print(
        con.execute(
            f"""
            SELECT
                COUNT(*) AS bars,
                MIN(minute) AS min_minute,
                MAX(minute) AS max_minute,
                MIN(bbo_update_count) AS min_updates,
                QUANTILE_CONT(bbo_update_count, 0.50) AS median_updates,
                QUANTILE_CONT(bbo_update_count, 0.90) AS p90_updates,
                MAX(bbo_update_count) AS max_updates,
                MIN(spread_ticks_max) AS min_max_spread_ticks,
                MAX(spread_ticks_max) AS max_spread_ticks,
                AVG(spread_ticks_avg) AS avg_bar_spread_ticks
            FROM parquet_scan('{out_path}')
            """
        ).fetchdf()
    )

    print()
    print("=== Spread Sanity ===")
    print(
        con.execute(
            f"""
            SELECT
                SUM(CASE WHEN spread_ticks_max < 1 THEN 1 ELSE 0 END) AS bars_with_bad_low_spread,
                SUM(CASE WHEN spread_ticks_max > 20 THEN 1 ELSE 0 END) AS bars_with_spread_gt_20,
                MAX(spread_ticks_max) AS max_spread_ticks
            FROM parquet_scan('{out_path}')
            """
        ).fetchdf()
    )

    print()
    print("=== Sample First 20 Bars ===")
    print(
        con.execute(
            f"""
            SELECT
                minute,
                bbo_update_count,
                mid_open,
                mid_high,
                mid_low,
                mid_close,
                spread_ticks_avg,
                spread_ticks_max,
                imbalance_avg
            FROM parquet_scan('{out_path}')
            ORDER BY minute
            LIMIT 20
            """
        ).fetchdf().to_string()
    )

    size_mb = out_path.stat().st_size / (1024 ** 2)
    size_gb = out_path.stat().st_size / (1024 ** 3)

    print()
    print("Saved:")
    print(f"  {out_path}")
    print(f"  Size MB: {size_mb:.2f}")
    print(f"  Size GB: {size_gb:.3f}")


if __name__ == "__main__":
    main()