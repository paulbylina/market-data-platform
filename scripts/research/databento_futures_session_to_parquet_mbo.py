from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import databento as db
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_DATASET = "GLBX.MDP3"
SCHEMA = "mbo"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Databento futures MBO raw DBN to normalized MBO event Parquet."
    )

    parser.add_argument(
        "--symbol",
        required=True,
        help="Databento symbol, e.g. ES.v.0, NQ.v.0",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help=f"Databento dataset. Default: {DEFAULT_DATASET}",
    )
    parser.add_argument(
        "--chunk-label",
        help="Raw chunk label, e.g. 2026-06-28_2026-07-05",
    )
    parser.add_argument(
        "--start-utc",
        help="UTC chunk start, e.g. 2026-06-28T00:00:00Z. Used to derive chunk label.",
    )
    parser.add_argument(
        "--end-utc",
        help="UTC chunk end, e.g. 2026-07-05T00:00:00Z. Used to derive chunk label.",
    )
    parser.add_argument(
        "--input-root",
        default="data/raw/databento",
        help="Raw Databento root directory. Default: data/raw/databento",
    )
    parser.add_argument(
        "--output-root",
        default="data/processed/databento/mbo_events",
        help="Processed output root directory. Default: data/processed/databento/mbo_events",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=1_000_000,
        help="Rows per conversion chunk. Default: 1,000,000",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output parquet if it already exists.",
    )

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
        / SCHEMA
        / chunk_label
        / f"{symbol_slug}_{chunk_label}_{SCHEMA}.dbn.zst"
    )

    out_dir = (
        Path(args.output_root)
        / dataset_slug
        / symbol_slug
        / chunk_label
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{chunk_label}_{SCHEMA}_events.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    if out_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Output file already exists: {out_path}\n"
            "Use --overwrite if you want to replace it."
        )

    print("Converting futures MBO DBN to event parquet:")
    print(f"  dataset:      {args.dataset}")
    print(f"  symbol:       {args.symbol}")
    print(f"  schema:       {SCHEMA}")
    print(f"  chunk_label:  {chunk_label}")
    print(f"  input:        {src_path}")
    print(f"  output:       {out_path}")
    print(f"  chunk_rows:   {args.chunk_rows:,}")
    print()

    store = db.DBNStore.from_file(src_path)

    writer = None
    total_rows = 0
    chunk_num = 0

    keep_cols = [
        "event_index",
        "ts_event",
        "ts_recv",
        "rtype",
        "publisher_id",
        "instrument_id",
        "symbol",
        "action",
        "side",
        "price",
        "size",
        "channel_id",
        "order_id",
        "flags",
        "ts_in_delta",
        "sequence",
    ]

    try:
        for df in store.to_df(count=args.chunk_rows):
            chunk_num += 1

            df = df.reset_index()

            # Preserve raw processing order. Do not sort.
            df.insert(
                0,
                "event_index",
                np.arange(total_rows, total_rows + len(df), dtype=np.int64),
            )

            missing_cols = [col for col in keep_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing expected MBO columns: {missing_cols}")

            df = df[keep_cols]

            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression="zstd")

            writer.write_table(table)

            total_rows += len(df)

            print(
                f"chunk={chunk_num:,} rows={len(df):,} "
                f"total={total_rows:,} "
                f"ts_event_from={df['ts_event'].min()} "
                f"ts_event_to={df['ts_event'].max()}"
            )

    finally:
        if writer is not None:
            writer.close()

    print()
    print("Done.")
    print(f"Output: {out_path}")
    print(f"Rows:   {total_rows:,}")

    if out_path.exists():
        size_mb = out_path.stat().st_size / (1024 ** 2)
        size_gb = out_path.stat().st_size / (1024 ** 3)
        print(f"Size MB: {size_mb:.2f}")
        print(f"Size GB: {size_gb:.3f}")


if __name__ == "__main__":
    main()