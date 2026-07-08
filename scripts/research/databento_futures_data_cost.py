from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import databento as db


DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_STYPE_IN = "continuous"
DEFAULT_TZ = "America/Chicago"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate Databento cost/size for futures data."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--stype-in", default=DEFAULT_STYPE_IN)
    parser.add_argument(
        "--schemas",
        nargs="+",
        default=["trades", "mbp-1", "mbo"],
        help="Schemas to estimate, e.g. --schemas trades mbp-1 mbo",
    )

    parser.add_argument(
        "--session-date",
        help="Futures session end date, e.g. 2026-07-06",
    )
    parser.add_argument(
        "--mbo-safe-raw",
        action="store_true",
        help="With --session-date, start request at prior UTC midnight for MBO snapshot safety.",
    )

    parser.add_argument(
        "--start-utc",
        help="Explicit UTC start, e.g. 2026-07-05T00:00:00Z",
    )
    parser.add_argument(
        "--end-utc",
        help="Explicit UTC end, e.g. 2026-07-06T21:00:00Z",
    )

    parser.add_argument("--timezone", default=DEFAULT_TZ)
    parser.add_argument("--session-start-hour", type=int, default=17)
    parser.add_argument("--session-end-hour", type=int, default=16)

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


def build_window(args: argparse.Namespace) -> tuple[datetime, datetime, dict[str, datetime] | None]:
    explicit_start = args.start_utc is not None
    explicit_end = args.end_utc is not None

    if explicit_start or explicit_end:
        if not explicit_start or not explicit_end:
            raise ValueError("Use both --start-utc and --end-utc together.")

        start_utc = parse_utc(args.start_utc)
        end_utc = parse_utc(args.end_utc)
        return start_utc, end_utc, None

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

    if args.mbo_safe_raw:
        start_utc = datetime.combine(
            session_start_utc.date(),
            time(0, 0),
            tzinfo=timezone.utc,
        )
    else:
        start_utc = session_start_utc

    end_utc = session_end_utc

    session_info = {
        "session_start_local": session_start_local,
        "session_end_local": session_end_local,
        "session_start_utc": session_start_utc,
        "session_end_utc": session_end_utc,
    }

    return start_utc, end_utc, session_info


def main() -> None:
    load_dotenv()

    args = parse_args()
    start_utc, end_utc, session_info = build_window(args)

    if end_utc <= start_utc:
        raise ValueError(f"End must be after start: start={to_z(start_utc)} end={to_z(end_utc)}")

    start = to_z(start_utc)
    end = to_z(end_utc)

    client = db.Historical()

    print("Cost check:")
    print(f"  dataset:       {args.dataset}")
    print(f"  symbol:        {args.symbol}")
    print(f"  stype_in:      {args.stype_in}")
    print(f"  schemas:       {', '.join(args.schemas)}")

    if args.session_date:
        print(f"  session_date:  {args.session_date}")
        print(f"  mbo_safe_raw:  {args.mbo_safe_raw}")

    if session_info:
        print(f"  session_start_local: {session_info['session_start_local']}")
        print(f"  session_end_local:   {session_info['session_end_local']}")
        print(f"  session_start_utc:   {to_z(session_info['session_start_utc'])}")
        print(f"  session_end_utc:     {to_z(session_info['session_end_utc'])}")

    print(f"  request_start_utc: {start}")
    print(f"  request_end_utc:   {end}")
    print()

    total_cost = 0.0

    for schema in args.schemas:
        print("=" * 80)
        print(f"Schema: {schema}")

        size_bytes = client.metadata.get_billable_size(
            dataset=args.dataset,
            symbols=args.symbol,
            stype_in=args.stype_in,
            schema=schema,
            start=start,
            end=end,
        )

        cost = client.metadata.get_cost(
            dataset=args.dataset,
            symbols=args.symbol,
            stype_in=args.stype_in,
            schema=schema,
            start=start,
            end=end,
        )

        total_cost += float(cost)

        size_gb = size_bytes / (1024 ** 3)

        print(f"Billable size bytes: {size_bytes:,}")
        print(f"Billable size GB:    {size_gb:.3f}")
        print(f"Estimated cost:      ${cost:.4f}")

    print()
    print("=" * 80)
    print(f"Total estimated cost across schemas: ${total_cost:.4f}")


if __name__ == "__main__":
    main()