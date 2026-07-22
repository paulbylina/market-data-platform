from __future__ import annotations

import argparse
from pathlib import Path

import databento as db
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


TICK_SIZE_BY_ROOT = {
    "ES": 0.25,
    "NQ": 0.25,
    "YM": 1.0,
    "RTY": 0.1,
}


def symbol_to_slug(symbol: str) -> str:
    s = symbol.lower()
    s = s.replace(".v.", "_v")
    s = s.replace(".", "_")
    s = s.replace("-", "_")
    s = s.replace("/", "_")
    return s


def symbol_root(symbol: str) -> str:
    return symbol.split(".")[0].upper()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Databento MBP-1 session DBN to normalized BBO Parquet."
    )
    parser.add_argument("--symbol", default="ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--schema", default="mbp-1")
    parser.add_argument("--chunk-rows", type=int, default=500_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    symbol_slug = symbol_to_slug(args.symbol)
    root = symbol_root(args.symbol)
    tick_size = TICK_SIZE_BY_ROOT.get(root)

    if tick_size is None:
        raise ValueError(f"No tick size configured for root={root}")

    src_path = Path(
        f"data/raw/databento/glbx_mdp3/{symbol_slug}/"
        f"session_date={args.session_date}/"
        f"{symbol_slug}_{args.session_date}_{args.schema}_session.dbn.zst"
    )

    out_dir = Path(
        f"data/processed/databento/top_of_book/{symbol_slug}/"
        f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{args.session_date}_{args.schema}_bbo.parquet"

    print(f"Reading:    {src_path}")
    print(f"Writing:    {out_path}")
    print(f"Symbol:     {args.symbol}")
    print(f"Tick size:  {tick_size}")
    print(f"Chunk rows: {args.chunk_rows:,}")

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    store = db.DBNStore.from_file(src_path)

    writer = None
    total_rows = 0
    chunk_num = 0

    try:
        for df in store.to_df(count=args.chunk_rows):
            chunk_num += 1

            df = df.reset_index()

            df = df.rename(
                columns={
                    "bid_px_00": "bid_px",
                    "ask_px_00": "ask_px",
                    "bid_sz_00": "bid_sz",
                    "ask_sz_00": "ask_sz",
                    "bid_ct_00": "bid_ct",
                    "ask_ct_00": "ask_ct",
                }
            )

            df["mid_px"] = (df["bid_px"] + df["ask_px"]) / 2.0
            df["spread"] = df["ask_px"] - df["bid_px"]
            df["spread_ticks"] = df["spread"] / tick_size

            denom = df["bid_sz"] + df["ask_sz"]

            df["imbalance"] = np.where(
                denom > 0,
                df["bid_sz"] / denom,
                np.nan,
            )

            df["microprice"] = np.where(
                denom > 0,
                ((df["ask_px"] * df["bid_sz"]) + (df["bid_px"] * df["ask_sz"])) / denom,
                np.nan,
            )

            keep_cols = [
                "ts_recv",
                "ts_event",
                "symbol",
                "instrument_id",
                "action",
                "side",
                "depth",
                "price",
                "size",
                "flags",
                "sequence",
                "bid_px",
                "ask_px",
                "bid_sz",
                "ask_sz",
                "bid_ct",
                "ask_ct",
                "mid_px",
                "spread",
                "spread_ticks",
                "imbalance",
                "microprice",
            ]

            df = df[keep_cols]

            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression="zstd")

            writer.write_table(table)

            total_rows += len(df)

            print(
                f"chunk={chunk_num:,} rows={len(df):,} "
                f"total={total_rows:,} "
                f"from={df['ts_recv'].min()} to={df['ts_recv'].max()}"
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
        print(f"Size:   {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
