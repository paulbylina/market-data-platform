from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb


DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_TZ = "America/Chicago"
INPUT_NAME = "mbo_quote_1m"
OUTPUT_NAME = "mbo_quote_1m_session"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split weekly futures MBO 1-minute quote bars into futures sessions."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)

    parser.add_argument("--chunk-label", required=True, help="Chunk label, e.g. 2026-06-28_2026-07-05")

    parser.add_argument(
        "--session-dates",
        nargs="+",
        help="Specific session end dates, e.g. 2026-06-30 2026-07-01",
    )
    parser.add_argument(
        "--start-session-date",
        help="First session end date, inclusive, e.g. 2026-06-29",
    )
    parser.add_argument(
        "--end-session-date",
        help="Last session end date, inclusive, e.g. 2026-07-03",
    )

    parser.add_argument(
        "--timezone",
        default=DEFAULT_TZ,
        help=f"Session timezone. Default: {DEFAULT_TZ}",
    )
    parser.add_argument(
        "--session-start-hour",
        type=int,
        default=17,
        help="Session start hour in local session timezone. Default: 17",
    )
    parser.add_argument(
        "--session-end-hour",
        type=int,
        default=16,
        help="Session end hour in local session timezone. Default: 16",
    )

    parser.add_argument(
        "--input-root",
        default="data/processed/databento/mbo_quote_bars_1m",
        help="Input weekly 1-minute quote bar root.",
    )
    parser.add_argument(
        "--output-root",
        default="data/processed/databento/mbo_quote_bars_1m_sessions",
        help="Output sessionized 1-minute quote bar root.",
    )

    parser.add_argument(
        "--min-bars",
        type=int,
        default=100,
        help="Warn when a session has fewer than this many bars. Default: 100.",
    )
    parser.add_argument("--overwrite", action="store_true")

    return parser.parse_args()


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def build_session_dates(args: argparse.Namespace) -> list[date]:
    has_list = args.session_dates is not None
    has_range = args.start_session_date is not None or args.end_session_date is not None

    if has_list and has_range:
        raise ValueError("Use either --session-dates or --start-session-date/--end-session-date, not both.")

    if has_list:
        return [date.fromisoformat(x) for x in args.session_dates]

    if has_range:
        if not args.start_session_date or not args.end_session_date:
            raise ValueError("Use both --start-session-date and --end-session-date together.")

        start = date.fromisoformat(args.start_session_date)
        end = date.fromisoformat(args.end_session_date)

        if end < start:
            raise ValueError(f"end-session-date must be >= start-session-date: {start} -> {end}")

        out = []
        cur = start
        while cur <= end:
            out.append(cur)
            cur += timedelta(days=1)

        return out

    raise ValueError("Use either --session-dates or --start-session-date/--end-session-date.")


def main() -> None:
    args = parse_args()

    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)
    session_tz = ZoneInfo(args.timezone)
    session_dates = build_session_dates(args)

    src_path = (
        Path(args.input_root)
        / dataset_slug
        / symbol_slug
        / args.chunk_label
        / f"{symbol_slug}_{args.chunk_label}_{INPUT_NAME}.parquet"
    )

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    con = duckdb.connect()

    print("Sessionizing MBO 1-minute quote bars:")
    print(f"  dataset:       {args.dataset}")
    print(f"  symbol:        {args.symbol}")
    print(f"  chunk_label:   {args.chunk_label}")
    print(f"  timezone:      {args.timezone}")
    print(f"  input:         {src_path}")
    print(f"  output_root:   {args.output_root}")
    print(f"  session_dates: {', '.join(str(x) for x in session_dates)}")
    print()

    total_sessions = 0
    total_bars = 0

    for session_date in session_dates:
        session_start_local = datetime.combine(
            session_date - timedelta(days=1),
            time(args.session_start_hour, 0),
            tzinfo=session_tz,
        )
        session_end_local = datetime.combine(
            session_date,
            time(args.session_end_hour, 0),
            tzinfo=session_tz,
        )

        out_dir = (
            Path(args.output_root)
            / dataset_slug
            / symbol_slug
            / f"session_date={session_date.isoformat()}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{symbol_slug}_{session_date.isoformat()}_{OUTPUT_NAME}.parquet"

        if out_path.exists() and not args.overwrite:
            raise FileExistsError(
                f"Output already exists: {out_path}\n"
                "Use --overwrite if you want to replace it."
            )

        session_start_str = session_start_local.isoformat()
        session_end_str = session_end_local.isoformat()

        con.execute(
            f"""
            COPY (
                SELECT
                    DATE '{session_date.isoformat()}' AS session_date,
                    TIMESTAMPTZ '{session_start_str}' AS session_start,
                    TIMESTAMPTZ '{session_end_str}' AS session_end,
                    *
                FROM parquet_scan('{src_path}')
                WHERE minute >= TIMESTAMPTZ '{session_start_str}'
                  AND minute <  TIMESTAMPTZ '{session_end_str}'
                ORDER BY minute
            )
            TO '{out_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        stats = con.execute(
            f"""
            SELECT
                COUNT(*) AS bars,
                MIN(minute) AS first_minute,
                MAX(minute) AS last_minute,
                SUM(bbo_update_count) AS total_bbo_updates,
                MIN(mid_low) AS min_mid,
                MAX(mid_high) AS max_mid,
                MAX(spread_ticks_max) AS max_spread_ticks
            FROM parquet_scan('{out_path}')
            """
        ).fetchone()

        bars, first_minute, last_minute, updates, min_mid, max_mid, max_spread = stats

        total_sessions += 1
        total_bars += bars

        warning = ""
        if bars < args.min_bars:
            warning = "  WARNING: low bar count"

        print(
            f"session_date={session_date} "
            f"bars={bars:,} "
            f"first={first_minute} "
            f"last={last_minute} "
            f"updates={updates:,} "
            f"min_mid={min_mid} "
            f"max_mid={max_mid} "
            f"max_spread={max_spread}"
            f"{warning}"
        )
        print(f"  saved: {out_path}")

    print()
    print("Done.")
    print(f"Sessions written: {total_sessions:,}")
    print(f"Total bars:       {total_bars:,}")


if __name__ == "__main__":
    main()