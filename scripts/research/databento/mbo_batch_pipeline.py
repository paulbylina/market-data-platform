from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


DEFAULT_DATASET = "GLBX.MDP3"


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def parse_utc_midnight(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError(f"Datetime must include UTC timezone/Z: {value}")

    parsed = parsed.astimezone(timezone.utc)

    if parsed.hour != 0 or parsed.minute != 0 or parsed.second != 0 or parsed.microsecond != 0:
        raise ValueError(f"Datetime must be UTC midnight: {value}")

    return parsed


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)

    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def fmt_size(path: Path) -> str:
    if not path.exists():
        return "missing"

    size = path.stat().st_size
    mb = size / 1024 / 1024
    gb = mb / 1024

    if gb >= 1:
        return f"{gb:.3f} GB"
    return f"{mb:.2f} MB"


def run_step(
    name: str,
    cmd: list[str],
    *,
    expected_path: Path | None,
    dry_run: bool,
    status_interval: int,
) -> None:
    print()
    print(f"[{name}]")
    print("$ " + " ".join(shlex.quote(part) for part in cmd), flush=True)

    if dry_run:
        return

    started = time.time()
    proc = subprocess.Popen(cmd)

    while True:
        try:
            return_code = proc.wait(timeout=status_interval)
            break
        except subprocess.TimeoutExpired:
            elapsed = fmt_duration(time.time() - started)

            if expected_path is None:
                print(f"  still running after {elapsed}", flush=True)
            else:
                print(
                    f"  still running after {elapsed}; "
                    f"expected output size: {fmt_size(expected_path)}",
                    flush=True,
                )

    elapsed = fmt_duration(time.time() - started)

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, cmd)

    print(f"  finished in {elapsed}", flush=True)

    if expected_path is not None and not expected_path.exists():
        raise FileNotFoundError(f"Expected output was not created: {expected_path}")


def date_chunks(
    start_dt: datetime,
    end_dt: datetime,
    chunk_days: int,
    direction: str,
) -> list[tuple[date, date]]:
    if chunk_days <= 0:
        raise ValueError("chunk-days must be positive")

    total_days = (end_dt.date() - start_dt.date()).days

    if total_days <= 0:
        raise ValueError("end-utc must be after start-utc")

    if total_days % chunk_days != 0:
        raise ValueError("Range must be an exact multiple of chunk-days")

    chunks: list[tuple[date, date]] = []
    current = start_dt.date()
    final = end_dt.date()

    while current < final:
        next_date = current + timedelta(days=chunk_days)
        chunks.append((current, next_date))
        current = next_date

    if direction == "backward":
        chunks.reverse()

    return chunks


def session_dates_for_chunk(
    chunk_start: date,
    chunk_end: date,
    session_date_mode: str,
) -> list[date]:
    dates: list[date] = []

    if session_date_mode == "futures":
        current = chunk_start + timedelta(days=1)
    elif session_date_mode == "calendar":
        current = chunk_start
    else:
        raise ValueError(f"Unsupported session-date-mode: {session_date_mode}")

    while current < chunk_end:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)

    return dates


def chunk_label(chunk_start: date, chunk_end: date) -> str:
    return f"{chunk_start.isoformat()}_{chunk_end.isoformat()}"


def raw_path(dataset_slug: str, symbol_slug: str, label: str) -> Path:
    return (
        Path("data/raw/databento")
        / dataset_slug
        / symbol_slug
        / "mbo"
        / label
        / f"{symbol_slug}_{label}_mbo.dbn.zst"
    )


def events_path(dataset_slug: str, symbol_slug: str, label: str) -> Path:
    return (
        Path("data/processed/databento/mbo_events")
        / dataset_slug
        / symbol_slug
        / label
        / f"{symbol_slug}_{label}_mbo_events.parquet"
    )


def bbo_path(dataset_slug: str, symbol_slug: str, label: str) -> Path:
    return (
        Path("data/processed/databento/mbo_bbo")
        / dataset_slug
        / symbol_slug
        / label
        / f"{symbol_slug}_{label}_mbo_bbo.parquet"
    )


def weekly_bars_path(dataset_slug: str, symbol_slug: str, label: str) -> Path:
    return (
        Path("data/processed/databento/mbo_quote_bars_1m")
        / dataset_slug
        / symbol_slug
        / label
        / f"{symbol_slug}_{label}_mbo_quote_1m.parquet"
    )


def session_file_path(dataset_slug: str, symbol_slug: str, session_date: date) -> Path:
    session_str = session_date.isoformat()

    return (
        Path("data/processed/databento/mbo_quote_bars_1m_sessions")
        / dataset_slug
        / symbol_slug
        / f"session_date={session_str}"
        / f"{symbol_slug}_{session_str}_mbo_quote_1m_session.parquet"
    )


def maybe_add_overwrite(cmd: list[str], overwrite: bool) -> list[str]:
    if overwrite:
        return [*cmd, "--overwrite"]
    return cmd


def process_chunk(
    *,
    symbol: str,
    dataset: str,
    stype_in: str | None,
    tick_size: float | None,
    max_spread_ticks: int | None,
    timezone_name: str | None,
    session_start_hour: int | None,
    session_end_hour: int | None,
    min_bars: int | None,
    session_date_mode: str,
    chunk_start: date,
    chunk_end: date,
    overwrite: bool,
    dry_run: bool,
    status_interval: int,
) -> None:
    dataset_slug = slug(dataset)
    symbol_slug = slug(symbol)
    label = chunk_label(chunk_start, chunk_end)

    start_utc = f"{chunk_start.isoformat()}T00:00:00Z"
    end_utc = f"{chunk_end.isoformat()}T00:00:00Z"

    raw = raw_path(dataset_slug, symbol_slug, label)
    events = events_path(dataset_slug, symbol_slug, label)
    bbo = bbo_path(dataset_slug, symbol_slug, label)
    weekly_bars = weekly_bars_path(dataset_slug, symbol_slug, label)

    sessions = session_dates_for_chunk(chunk_start, chunk_end, session_date_mode)
    session_files = [session_file_path(dataset_slug, symbol_slug, d) for d in sessions]

    print()
    print("=" * 90)
    print(f"Chunk: {label}")
    print(f"UTC:   {start_utc} -> {end_utc}")
    print(f"Sessions: {', '.join(d.isoformat() for d in sessions)}")
    print("=" * 90)

    if raw.exists():
        print(f"[skip] raw exists: {raw} ({fmt_size(raw)})")
    else:
        run_step(
            "1/5 download raw MBO",
            [
                sys.executable,
                "scripts/research/databento/mbo_download.py",
                "--symbol",
                symbol,
                "--dataset",
                dataset,
                "--start-utc",
                start_utc,
                "--end-utc",
                end_utc,
                *([] if stype_in is None else ["--stype-in", stype_in]),
            ],
            expected_path=raw,
            dry_run=dry_run,
            status_interval=status_interval,
        )

    if events.exists() and not overwrite:
        print(f"[skip] MBO events exist: {events} ({fmt_size(events)})")
    else:
        run_step(
            "2/5 DBN to MBO events parquet",
            maybe_add_overwrite(
                [
                    sys.executable,
                    "scripts/research/databento/mbo_to_events.py",
                    "--symbol",
                    symbol,
                    "--dataset",
                    dataset,
                    "--chunk-label",
                    label,
                ],
                overwrite,
            ),
            expected_path=events,
            dry_run=dry_run,
            status_interval=status_interval,
        )

    if bbo.exists() and not overwrite:
        print(f"[skip] BBO exists: {bbo} ({fmt_size(bbo)})")
    else:
        run_step(
            "3/5 MBO events to reconstructed BBO",
            maybe_add_overwrite(
                [
                    sys.executable,
                    "scripts/research/databento/mbo_to_bbo.py",
                    "--symbol",
                    symbol,
                    "--dataset",
                    dataset,
                    "--chunk-label",
                    label,
                    *([] if tick_size is None else ["--tick-size", str(tick_size)]),
                    *([] if max_spread_ticks is None else ["--max-spread-ticks", str(max_spread_ticks)]),
                ],
                overwrite,
            ),
            expected_path=bbo,
            dry_run=dry_run,
            status_interval=status_interval,
        )

    if weekly_bars.exists() and not overwrite:
        print(f"[skip] weekly 1m bars exist: {weekly_bars} ({fmt_size(weekly_bars)})")
    else:
        run_step(
            "4/5 BBO to weekly 1m quote bars",
            maybe_add_overwrite(
                [
                    sys.executable,
                    "scripts/research/databento/mbo_bbo_to_1m_bars.py",
                    "--symbol",
                    symbol,
                    "--dataset",
                    dataset,
                    "--chunk-label",
                    label,
                ],
                overwrite,
            ),
            expected_path=weekly_bars,
            dry_run=dry_run,
            status_interval=status_interval,
        )

    missing_session_files = [path for path in session_files if not path.exists()]

    if not missing_session_files and not overwrite:
        print("[skip] all sessionized files exist")
        for path in session_files:
            print(f"       {path} ({fmt_size(path)})")
    else:
        if not sessions:
            print("[skip] no weekday session dates for this chunk")
            return

        session_start = sessions[0].isoformat()
        session_end = sessions[-1].isoformat()

        # Always use --overwrite for sessionization if any session is missing.
        # This avoids issues when part of the same weekly chunk was already written.
        run_step(
            "5/5 sessionize weekly 1m bars",
            [
                sys.executable,
                "scripts/research/databento/mbo_sessionize_1m_bars.py",
                "--symbol",
                symbol,
                "--dataset",
                dataset,
                "--chunk-label",
                label,
                "--start-session-date",
                session_start,
                "--end-session-date",
                session_end,
                *([] if timezone_name is None else ["--timezone", timezone_name]),
                *([] if session_start_hour is None else ["--session-start-hour", str(session_start_hour)]),
                *([] if session_end_hour is None else ["--session-end-hour", str(session_end_hour)]),
                *([] if min_bars is None else ["--min-bars", str(min_bars)]),
                "--overwrite",
            ],
            expected_path=None,
            dry_run=dry_run,
            status_interval=status_interval,
        )

        if not dry_run:
            still_missing = [path for path in session_files if not path.exists()]
            if still_missing:
                print("Missing session files:")
                for path in still_missing:
                    print(f"  {path}")
                raise FileNotFoundError("One or more expected session files were not created")

    print()
    print(f"Done chunk: {label}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Databento MBO pipeline over UTC chunks."
    )

    parser.add_argument("--symbol", default="ES.v.0", help="Databento symbol. Default: ES.v.0")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help=f"Dataset. Default: {DEFAULT_DATASET}")
    parser.add_argument("--stype-in", default=None, help="Databento symbol type, e.g. raw_symbol for stocks.")
    parser.add_argument("--start-utc", required=True, help="UTC midnight start, e.g. 2026-04-12T00:00:00Z")
    parser.add_argument("--end-utc", required=True, help="UTC midnight end, e.g. 2026-06-28T00:00:00Z")
    parser.add_argument(
        "--direction",
        choices=["forward", "backward"],
        default="backward",
        help="Chunk processing order. Default: backward.",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=7,
        help="UTC days per chunk. Use 7 for ES weekly chunks or 1 for single stock days. Default: 7.",
    )
    parser.add_argument(
        "--session-date-mode",
        choices=["futures", "calendar"],
        default="futures",
        help="How to derive session dates from chunks. Use futures for ES Sunday-Sunday chunks, calendar for stock daily chunks. Default: futures.",
    )
    parser.add_argument(
        "--tick-size",
        type=float,
        default=None,
        help="Optional tick size override passed to mbo_to_bbo.py, e.g. 1.0 for SNDK raw units.",
    )
    parser.add_argument(
        "--max-spread-ticks",
        type=int,
        default=None,
        help="Optional max spread ticks passed to mbo_to_bbo.py.",
    )
    parser.add_argument(
        "--timezone",
        dest="timezone_name",
        default=None,
        help="Optional session timezone, e.g. America/New_York.",
    )
    parser.add_argument(
        "--session-start-hour",
        type=int,
        default=None,
        help="Optional local session start hour passed to sessionizer.",
    )
    parser.add_argument(
        "--session-end-hour",
        type=int,
        default=None,
        help="Optional local session end hour passed to sessionizer.",
    )
    parser.add_argument(
        "--min-bars",
        type=int,
        default=None,
        help="Optional minimum bars required per session.",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
        help="Optional limit for testing, e.g. --max-chunks 1.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Rebuild processed outputs even if they already exist. Raw downloads are still skipped if present.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned commands without running them.",
    )
    parser.add_argument(
        "--status-interval",
        type=int,
        default=60,
        help="Seconds between still-running status messages. Default: 60.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    start_dt = parse_utc_midnight(args.start_utc)
    end_dt = parse_utc_midnight(args.end_utc)

    chunks = date_chunks(start_dt, end_dt, args.chunk_days, args.direction)

    if args.max_chunks is not None:
        chunks = chunks[: args.max_chunks]

    print("Databento MBO batch pipeline")
    print(f"symbol:      {args.symbol}")
    print(f"dataset:     {args.dataset}")
    print(f"stype_in:    {args.stype_in}")
    print(f"start_utc:   {args.start_utc}")
    print(f"end_utc:     {args.end_utc}")
    print(f"direction:   {args.direction}")
    print(f"chunk_days:  {args.chunk_days}")
    print(f"date_mode:   {args.session_date_mode}")
    print(f"tick_size:   {args.tick_size}")
    print(f"max_spread:  {args.max_spread_ticks}")
    print(f"timezone:    {args.timezone_name}")
    print(f"chunks:      {len(chunks)}")
    print(f"dry_run:     {args.dry_run}")
    print(f"overwrite:   {args.overwrite}")

    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        print()
        print(f"Batch progress: chunk {index}/{len(chunks)}")

        process_chunk(
            symbol=args.symbol,
            dataset=args.dataset,
            stype_in=args.stype_in,
            tick_size=args.tick_size,
            max_spread_ticks=args.max_spread_ticks,
            timezone_name=args.timezone_name,
            session_start_hour=args.session_start_hour,
            session_end_hour=args.session_end_hour,
            min_bars=args.min_bars,
            session_date_mode=args.session_date_mode,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            status_interval=args.status_interval,
        )

    print()
    print("Batch pipeline complete.")


if __name__ == "__main__":
    main()
