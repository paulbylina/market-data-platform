from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import databento as db


DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_STYPE_IN = "continuous"
DEFAULT_TZ = "America/Chicago"
SCHEMA = "mbo"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download futures MBO raw .dbn.zst data from Databento."
    )

    parser.add_argument(
        "--symbol",
        required=True,
        help="Databento symbol, e.g. ES.v.0, NQ.v.0, MES.v.0",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help=f"Databento dataset. Default: {DEFAULT_DATASET}",
    )
    parser.add_argument(
        "--stype-in",
        default=DEFAULT_STYPE_IN,
        help=f"Databento symbol type. Default: {DEFAULT_STYPE_IN}",
    )

    parser.add_argument(
        "--session-date",
        help="Futures session end date, e.g. 2026-07-06. "
        "Used for one session-style pull.",
    )
    parser.add_argument(
        "--start-utc",
        help="Explicit UTC start for chunk pull, e.g. 2026-06-28T00:00:00Z",
    )
    parser.add_argument(
        "--end-utc",
        help="Explicit UTC end for chunk pull, e.g. 2026-07-05T00:00:00Z",
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
        "--output-root",
        default="data/raw/databento",
        help="Root output directory. Default: data/raw/databento",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it already exists.",
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


def to_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def date_label(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


def build_window(args: argparse.Namespace) -> tuple[datetime, datetime, str, dict[str, datetime] | None]:
    explicit_start = args.start_utc is not None
    explicit_end = args.end_utc is not None

    if explicit_start or explicit_end:
        if not explicit_start or not explicit_end:
            raise ValueError("Use both --start-utc and --end-utc together.")

        if args.session_date:
            raise ValueError("Use either --session-date or --start-utc/--end-utc, not both.")

        start_utc = parse_utc(args.start_utc)
        end_utc = parse_utc(args.end_utc)

        chunk_label = f"{date_label(start_utc)}_{date_label(end_utc)}"

        return start_utc, end_utc, chunk_label, None

    if not args.session_date:
        raise ValueError("Use either --session-date or both --start-utc and --end-utc.")

    session_date = date.fromisoformat(args.session_date)
    session_tz = ZoneInfo(args.timezone)

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

    session_start_utc = session_start_local.astimezone(timezone.utc)
    session_end_utc = session_end_local.astimezone(timezone.utc)

    # For one-session MBO pulls, start at the UTC midnight before the session
    # so the file can include Databento's synthetic MBO snapshot.
    start_utc = datetime.combine(
        session_start_utc.date(),
        time(0, 0),
        tzinfo=timezone.utc,
    )
    end_utc = session_end_utc

    session_info = {
        "session_start_local": session_start_local,
        "session_end_local": session_end_local,
        "session_start_utc": session_start_utc,
        "session_end_utc": session_end_utc,
    }

    chunk_label = args.session_date

    return start_utc, end_utc, chunk_label, session_info


def main() -> None:
    load_dotenv()

    args = parse_args()

    start_utc, end_utc, chunk_label, session_info = build_window(args)

    if end_utc <= start_utc:
        raise ValueError(f"End must be after start: start={to_z(start_utc)} end={to_z(end_utc)}")

    start = to_z(start_utc)
    end = to_z(end_utc)

    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)

    out_dir = (
        Path(args.output_root)
        / dataset_slug
        / symbol_slug
        / SCHEMA
        / chunk_label
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{symbol_slug}_{chunk_label}_{SCHEMA}.dbn.zst"

    if out_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Output file already exists: {out_path}\n"
            "Use --overwrite if you want to replace it."
        )

    client = db.Historical()

    print("Downloading futures MBO:")
    print(f"  dataset:             {args.dataset}")
    print(f"  symbol:              {args.symbol}")
    print(f"  stype_in:            {args.stype_in}")
    print(f"  schema:              {SCHEMA}")

    if args.session_date:
        print(f"  session_date:        {args.session_date}")

    if session_info:
        print(f"  session_start_local: {session_info['session_start_local']}")
        print(f"  session_end_local:   {session_info['session_end_local']}")
        print(f"  session_start_utc:   {to_z(session_info['session_start_utc'])}")
        print(f"  session_end_utc:     {to_z(session_info['session_end_utc'])}")

    print(f"  request_start_utc:   {start}")
    print(f"  request_end_utc:     {end}")
    print(f"  chunk_label:         {chunk_label}")
    print(f"  out_path:            {out_path}")

    data = client.timeseries.get_range(
        dataset=args.dataset,
        symbols=args.symbol,
        stype_in=args.stype_in,
        schema=SCHEMA,
        start=start,
        end=end,
    )

    data.to_file(out_path)

    size_bytes = out_path.stat().st_size
    size_mb = size_bytes / (1024 ** 2)
    size_gb = size_bytes / (1024 ** 3)

    print()
    print("Saved:")
    print(f"  {out_path}")
    print(f"  file size MB: {size_mb:.2f}")
    print(f"  file size GB: {size_gb:.3f}")

    print()
    print("Symbology:")
    print(data.symbology)


if __name__ == "__main__":
    main()