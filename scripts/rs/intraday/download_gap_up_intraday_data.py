import argparse
from pathlib import Path

import pandas as pd

from src.pipelines.stocks.run_market_timeframe_refresh import run_market_timeframe_refresh
from src.utils.path_builders import build_market_curated_output_path


TASKS_PATH = Path("data/research/intraday_gap_up/minute_download_tasks_top3.csv")


def output_exists(ticker: str, start_date: str, end_date: str, timeframe: str) -> bool:
    path = build_market_curated_output_path(
        symbol=ticker,
        start_date=start_date,
        end_date=end_date,
        timeframe=timeframe,
    )
    return path.exists()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tasks",
        type=Path,
        default=TASKS_PATH,
        help="CSV of ticker/date intraday download tasks.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of tasks to run for smoke testing.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Redownload/rebuild even if 1m and 15m outputs already exist.",
    )

    args = parser.parse_args()

    tasks = pd.read_csv(args.tasks)

    required = {"ticker", "start_date", "end_date"}
    missing = required - set(tasks.columns)
    if missing:
        raise ValueError(f"Tasks file missing columns: {sorted(missing)}")

    tasks = (
        tasks[["ticker", "start_date", "end_date"]]
        .drop_duplicates()
        .sort_values(["ticker", "start_date", "end_date"])
        .reset_index(drop=True)
    )

    if args.limit is not None:
        tasks = tasks.head(args.limit).copy()

    print("=== Gap-up intraday downloader ===")
    print(f"Tasks file: {args.tasks}")
    print(f"Tasks to process: {len(tasks)}")
    print(f"Force: {args.force}")
    print()

    completed = 0
    skipped = 0
    failed = 0

    for i, row in tasks.iterrows():
        ticker = row["ticker"]
        start_date = row["start_date"]
        end_date = row["end_date"]

        has_1m = output_exists(ticker, start_date, end_date, "1m")
        has_15m = output_exists(ticker, start_date, end_date, "15m")

        if has_1m and has_15m and not args.force:
            skipped += 1
            print(f"[{i + 1}/{len(tasks)}] SKIP {ticker} {start_date} already exists")
            continue

        print(f"[{i + 1}/{len(tasks)}] RUN  {ticker} {start_date}")

        try:
            run_market_timeframe_refresh(
                symbol=ticker,
                start_date=start_date,
                end_date=end_date,
                source_timeframes=("1m",),
                derived_timeframes=("15m",),
            )
            completed += 1
        except Exception as exc:
            failed += 1
            print(f"FAILED {ticker} {start_date}: {exc}")

    print()
    print("=== Done ===")
    print(f"Completed: {completed}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")


if __name__ == "__main__":
    main()
