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

    out_dir = Path(args.output_root) / dataset_slug / symbol_slug / chunk_label
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{chunk_label}_{OUTPUT_NAME}.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

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
    print(f"  output:      {out_path}")
    print()

    con.execute(
        f"""
        COPY (
            SELECT
                DATE_TRUNC('minute', {bar_time}) AS minute,

                COUNT(*) AS bbo_update_count,

                SUM(CASE WHEN source_action = 'A' THEN 1 ELSE 0 END) AS source_add_rows,
                SUM(CASE WHEN source_action = 'C' THEN 1 ELSE 0 END) AS source_cancel_rows,
                SUM(CASE WHEN source_action = 'M' THEN 1 ELSE 0 END) AS source_modify_rows,
                SUM(CASE WHEN source_action = 'T' THEN 1 ELSE 0 END) AS source_trade_rows,
                SUM(CASE WHEN source_action = 'F' THEN 1 ELSE 0 END) AS source_fill_rows,
                SUM(CASE WHEN emit_reason = 'snapshot_ready' THEN 1 ELSE 0 END) AS snapshot_ready_rows,

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
            ORDER BY 1
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