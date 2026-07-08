from __future__ import annotations

import argparse
import heapq
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_DATASET = "GLBX.MDP3"
INPUT_SCHEMA = "mbo"
OUTPUT_NAME = "mbo_bbo"

TICK_SIZE_BY_ROOT = {
    "ES": 0.25,
    "MES": 0.25,
    "NQ": 0.25,
    "MNQ": 0.25,
    "YM": 1.0,
    "MYM": 1.0,
    "RTY": 0.1,
    "M2K": 0.1,
    "CL": 0.01,
    "MCL": 0.01,
    "GC": 0.1,
    "MGC": 0.1,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconstruct BBO from normalized Databento futures MBO event parquet."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)

    parser.add_argument("--chunk-label", help="Chunk label, e.g. 2026-06-28_2026-07-05")
    parser.add_argument("--start-utc", help="UTC chunk start, e.g. 2026-06-28T00:00:00Z")
    parser.add_argument("--end-utc", help="UTC chunk end, e.g. 2026-07-05T00:00:00Z")

    parser.add_argument(
        "--input-root",
        default="data/processed/databento/mbo_events",
        help="Input MBO event parquet root.",
    )
    parser.add_argument(
        "--output-root",
        default="data/processed/databento/mbo_bbo",
        help="Output reconstructed BBO root.",
    )
    parser.add_argument(
        "--tick-size",
        type=float,
        help="Tick size override. If omitted, inferred from symbol root.",
    )
    parser.add_argument(
        "--input-batch-rows",
        type=int,
        default=500_000,
        help="Rows per input batch. Default: 500,000",
    )
    parser.add_argument(
        "--output-chunk-rows",
        type=int,
        default=500_000,
        help="Rows per output parquet write. Default: 500,000",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Optional max input rows for a small test run.",
    )
    parser.add_argument(
        "--max-spread-ticks",
        type=float,
        default=20.0,
        help="Only emit BBO rows with spread between 1 tick and this value. Default: 20.",
    )
    parser.add_argument(
        "--emit-crossed",
        action="store_true",
        help="Emit crossed/locked/wide BBO rows instead of suppressing them.",
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


def symbol_root(symbol: str) -> str:
    return symbol.split(".")[0].upper()


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


def price_to_ticks(price: float, tick_size: float) -> int | None:
    if price is None:
        return None

    value = float(price)

    if math.isnan(value):
        return None

    return int(round(value / tick_size))


def ticks_to_price(ticks: int | None, tick_size: float) -> float | None:
    if ticks is None:
        return None
    return ticks * tick_size


def main() -> None:
    args = parse_args()

    chunk_label = build_chunk_label(args)
    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)

    tick_size = args.tick_size
    if tick_size is None:
        root = symbol_root(args.symbol)
        tick_size = TICK_SIZE_BY_ROOT.get(root)
        if tick_size is None:
            raise ValueError(
                f"No tick size configured for symbol root={root}. "
                "Pass --tick-size explicitly."
            )

    src_path = (
        Path(args.input_root)
        / dataset_slug
        / symbol_slug
        / chunk_label
        / f"{symbol_slug}_{chunk_label}_{INPUT_SCHEMA}_events.parquet"
    )

    out_dir = Path(args.output_root) / dataset_slug / symbol_slug / chunk_label
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = "_test" if args.max_rows else ""
    out_path = out_dir / f"{symbol_slug}_{chunk_label}_{OUTPUT_NAME}{suffix}.parquet"

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    if out_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Output file already exists: {out_path}\n"
            "Use --overwrite if you want to replace it."
        )

    print("Reconstructing BBO from MBO:")
    print(f"  dataset:           {args.dataset}")
    print(f"  symbol:            {args.symbol}")
    print(f"  chunk_label:       {chunk_label}")
    print(f"  tick_size:         {tick_size}")
    print(f"  input:             {src_path}")
    print(f"  output:            {out_path}")
    print(f"  input_batch_rows:  {args.input_batch_rows:,}")
    print(f"  output_chunk_rows: {args.output_chunk_rows:,}")
    print(f"  max_rows:          {args.max_rows}")
    print()

    orders: dict[int, tuple[str, int, int]] = {}

    bid_size_by_px: dict[int, int] = defaultdict(int)
    ask_size_by_px: dict[int, int] = defaultdict(int)

    bid_ct_by_px: dict[int, int] = defaultdict(int)
    ask_ct_by_px: dict[int, int] = defaultdict(int)

    bid_heap: list[int] = []
    ask_heap: list[int] = []

    def maps_for_side(side: str):
        if side == "B":
            return bid_size_by_px, bid_ct_by_px, bid_heap
        if side == "A":
            return ask_size_by_px, ask_ct_by_px, ask_heap
        raise ValueError(f"Bad side for book mutation: {side}")

    def add_order(order_id: int, side: str, px_ticks: int, size: int) -> None:
        if order_id in orders:
            remove_order(order_id)

        orders[order_id] = (side, px_ticks, size)

        size_by_px, ct_by_px, heap = maps_for_side(side)
        size_by_px[px_ticks] += size
        ct_by_px[px_ticks] += 1

        if side == "B":
            heapq.heappush(heap, -px_ticks)
        else:
            heapq.heappush(heap, px_ticks)

    def remove_order(order_id: int) -> None:
        old = orders.pop(order_id, None)
        if old is None:
            return

        side, px_ticks, size = old
        size_by_px, ct_by_px, _ = maps_for_side(side)

        size_by_px[px_ticks] -= size
        ct_by_px[px_ticks] -= 1

        if size_by_px[px_ticks] <= 0:
            size_by_px.pop(px_ticks, None)

        if ct_by_px[px_ticks] <= 0:
            ct_by_px.pop(px_ticks, None)

    def reduce_order(order_id: int, reduce_size: int) -> bool:
        old = orders.get(order_id)
        if old is None:
            return False

        side, px_ticks, old_size = old

        if reduce_size >= old_size:
            remove_order(order_id)
            return True

        new_size = old_size - reduce_size
        orders[order_id] = (side, px_ticks, new_size)

        size_by_px, _, _ = maps_for_side(side)
        size_by_px[px_ticks] -= reduce_size

        if size_by_px[px_ticks] <= 0:
            size_by_px.pop(px_ticks, None)

        return True

    def modify_order(order_id: int, side: str, px_ticks: int, new_size: int) -> bool:
        old = orders.get(order_id)

        if old is None:
            if side in {"B", "A"} and px_ticks is not None and new_size > 0:
                add_order(order_id, side, px_ticks, new_size)
            return False

        old_side, old_px_ticks, _ = old
        new_side = side if side in {"B", "A"} else old_side
        new_px_ticks = px_ticks if px_ticks is not None else old_px_ticks

        remove_order(order_id)

        if new_size > 0:
            add_order(order_id, new_side, new_px_ticks, new_size)

        return True

    def clear_book() -> None:
        orders.clear()
        bid_size_by_px.clear()
        ask_size_by_px.clear()
        bid_ct_by_px.clear()
        ask_ct_by_px.clear()
        bid_heap.clear()
        ask_heap.clear()

    def best_bid_ticks() -> int | None:
        while bid_heap and bid_size_by_px.get(-bid_heap[0], 0) <= 0:
            heapq.heappop(bid_heap)

        if not bid_heap:
            return None

        return -bid_heap[0]

    def best_ask_ticks() -> int | None:
        while ask_heap and ask_size_by_px.get(ask_heap[0], 0) <= 0:
            heapq.heappop(ask_heap)

        if not ask_heap:
            return None

        return ask_heap[0]

    def current_bbo() -> tuple[int | None, int, int, int | None, int, int]:
        bid_ticks = best_bid_ticks()
        ask_ticks = best_ask_ticks()

        bid_sz = bid_size_by_px.get(bid_ticks, 0) if bid_ticks is not None else 0
        ask_sz = ask_size_by_px.get(ask_ticks, 0) if ask_ticks is not None else 0

        bid_ct = bid_ct_by_px.get(bid_ticks, 0) if bid_ticks is not None else 0
        ask_ct = ask_ct_by_px.get(ask_ticks, 0) if ask_ticks is not None else 0

        return bid_ticks, bid_sz, bid_ct, ask_ticks, ask_sz, ask_ct
    
    def is_bbo_valid_for_emit(
        bbo: tuple[int | None, int, int, int | None, int, int]
    ) -> bool:
        bid_ticks, _, _, ask_ticks, _, _ = bbo

        if bid_ticks is None or ask_ticks is None:
            return False

        if args.emit_crossed:
            return True

        spread_ticks = ask_ticks - bid_ticks

        return 1 <= spread_ticks <= args.max_spread_ticks

    output_buffer: list[dict] = []
    writer: pq.ParquetWriter | None = None

    def flush_output() -> None:
        nonlocal writer
        if not output_buffer:
            return

        out_df = pd.DataFrame(output_buffer)
        table = pa.Table.from_pandas(out_df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression="zstd")

        writer.write_table(table)
        output_buffer.clear()

    def emit_bbo(row, reason: str) -> None:
        bid_ticks, bid_sz, bid_ct, ask_ticks, ask_sz, ask_ct = current_bbo()

        if bid_ticks is None or ask_ticks is None:
            return

        bid_px = ticks_to_price(bid_ticks, tick_size)
        ask_px = ticks_to_price(ask_ticks, tick_size)

        spread = ask_px - bid_px
        denom = bid_sz + ask_sz

        mid_px = (bid_px + ask_px) / 2.0
        spread_ticks = spread / tick_size
        imbalance = bid_sz / denom if denom > 0 else None
        microprice = (
            ((ask_px * bid_sz) + (bid_px * ask_sz)) / denom
            if denom > 0
            else None
        )

        output_buffer.append(
            {
                "event_index": int(row.event_index),
                "ts_event": row.ts_event,
                "ts_recv": row.ts_recv,
                "source_action": row.action,
                "source_side": row.side,
                "source_price": row.price,
                "source_size": int(row.size),
                "source_order_id": int(row.order_id),
                "sequence": int(row.sequence),
                "emit_reason": reason,
                "bid_px": bid_px,
                "ask_px": ask_px,
                "bid_sz": int(bid_sz),
                "ask_sz": int(ask_sz),
                "bid_ct": int(bid_ct),
                "ask_ct": int(ask_ct),
                "mid_px": mid_px,
                "spread": spread,
                "spread_ticks": spread_ticks,
                "imbalance": imbalance,
                "microprice": microprice,
                "is_crossed": bool(bid_px >= ask_px),
            }
        )

        if len(output_buffer) >= args.output_chunk_rows:
            flush_output()

    pf = pq.ParquetFile(src_path)

    input_rows = 0
    emitted_rows = 0
    batch_num = 0

    missing_cancel = 0
    missing_modify = 0
    duplicate_add = 0
    resets = 0
    snapshot_rows_suppressed = 0
    invalid_bbo_suppressed = 0

    snapshot_ts_recv = None
    need_emit_after_snapshot = False
    last_bbo: tuple[int | None, int, int, int | None, int, int] | None = None

    input_columns = [
        "event_index",
        "ts_event",
        "ts_recv",
        "action",
        "side",
        "price",
        "size",
        "order_id",
        "flags",
        "sequence",
    ]

    try:
        for batch in pf.iter_batches(
            batch_size=args.input_batch_rows,
            columns=input_columns,
        ):
            batch_num += 1
            df = batch.to_pandas()

            if args.max_rows is not None:
                remaining = args.max_rows - input_rows
                if remaining <= 0:
                    break
                if len(df) > remaining:
                    df = df.iloc[:remaining]

            for row in df.itertuples(index=False):
                input_rows += 1

                action = row.action
                side = row.side
                order_id = int(row.order_id)
                size = int(row.size)
                px_ticks = price_to_ticks(row.price, tick_size)

                suppress_emit = False

                if action == "R":
                    clear_book()
                    resets += 1
                    snapshot_ts_recv = row.ts_recv
                    need_emit_after_snapshot = True
                    suppress_emit = True

                else:
                    if snapshot_ts_recv is not None and row.ts_recv == snapshot_ts_recv:
                        suppress_emit = True
                        snapshot_rows_suppressed += 1
                    elif snapshot_ts_recv is not None and row.ts_recv != snapshot_ts_recv:
                        snapshot_ts_recv = None

                    if action == "A":
                        if order_id in orders:
                            duplicate_add += 1

                        if side in {"B", "A"} and px_ticks is not None and size > 0:
                            add_order(order_id, side, px_ticks, size)

                    elif action == "M":
                        if not modify_order(order_id, side, px_ticks, size):
                            missing_modify += 1

                    elif action == "C":
                        if not reduce_order(order_id, size):
                            missing_cancel += 1

                    elif action in {"T", "F"}:
                        # Databento MBO T/F records do not directly mutate the book.
                        pass

                    else:
                        pass

                bbo = current_bbo()

                should_emit = False
                reason = "bbo_change"

                bbo_valid_for_emit = is_bbo_valid_for_emit(bbo)

                if not suppress_emit:
                    if not bbo_valid_for_emit and action in {"A", "M", "C"} and bbo != last_bbo:
                        invalid_bbo_suppressed += 1
                        last_bbo = bbo

                    elif bbo_valid_for_emit:
                        if need_emit_after_snapshot:
                            should_emit = True
                            reason = "snapshot_ready"
                            need_emit_after_snapshot = False

                        elif action in {"A", "M", "C"} and bbo != last_bbo:
                            should_emit = True
                            reason = "bbo_change"

                if should_emit:
                    emit_bbo(row, reason)
                    emitted_rows += 1
                    last_bbo = bbo
                    last_bbo = bbo

                if args.max_rows is not None and input_rows >= args.max_rows:
                    break

            print(
                f"batch={batch_num:,} input_rows={input_rows:,} "
                f"emitted={emitted_rows:,} orders={len(orders):,} "
                f"resets={resets:,} missing_cancel={missing_cancel:,} "
                f"missing_modify={missing_modify:,}"
            )

            if args.max_rows is not None and input_rows >= args.max_rows:
                break

        flush_output()

    finally:
        if writer is not None:
            writer.close()

    print()
    print("Done.")
    print(f"Input rows:               {input_rows:,}")
    print(f"Emitted BBO rows:         {emitted_rows:,}")
    print(f"Open orders at end:       {len(orders):,}")
    print(f"Resets:                   {resets:,}")
    print(f"Snapshot rows suppressed: {snapshot_rows_suppressed:,}")
    print(f"Invalid BBO suppressed:   {invalid_bbo_suppressed:,}")
    print(f"Duplicate adds handled:   {duplicate_add:,}")
    print(f"Missing cancels skipped:  {missing_cancel:,}")
    print(f"Missing modifies handled: {missing_modify:,}")

    if out_path.exists():
        size_mb = out_path.stat().st_size / (1024 ** 2)
        size_gb = out_path.stat().st_size / (1024 ** 3)
        print(f"Output:                   {out_path}")
        print(f"Size MB:                  {size_mb:.2f}")
        print(f"Size GB:                  {size_gb:.3f}")
    else:
        print("Output:                   no BBO rows written")


if __name__ == "__main__":
    main()