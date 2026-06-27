from datetime import date, timedelta
from pathlib import Path

from src.pipelines.stocks.batch_market_timeframe_refresh import (
    run_batch_market_timeframe_refresh,
)
from src.pipelines.stocks.build_serving_dataset import build_serving_dataset
from src.utils.path_builders import build_market_serving_output_path
from src.utils.settings import CONFIG_DIR


def refresh_market_universe(
    symbols_file: Path | None = None,
    daily_start_date: str | None = None,
    daily_end_date: str | None = None,
    intraday_start_date: str | None = None,
    intraday_end_date: str | None = None,
    daily_lookback_days: int = 2000,
    intraday_lookback_days: int = 20,
    run_daily: bool = True,
    run_intraday: bool = True,
    build_daily_serving: bool = False,
) -> dict:
    """
    Unified market-universe refresh entrypoint.

    Daily family:
      - source: 1d
      - derived: 1w, 1mo

    Intraday family:
      - source: 1m
      - derived: 5m, 15m, 60m

    Optional:
      - build combined daily serving dataset after daily refresh
    """
    if symbols_file is None:
        symbols_file = CONFIG_DIR / "symbols_intraday_eligible.txt"

    today = date.today()

    if daily_end_date is None:
        daily_end_date = today.isoformat()
    if intraday_end_date is None:
        intraday_end_date = today.isoformat()

    if daily_start_date is None:
        daily_start_date = (
            date.fromisoformat(daily_end_date) - timedelta(days=daily_lookback_days)
        ).isoformat()

    if intraday_start_date is None:
        intraday_start_date = (
            date.fromisoformat(intraday_end_date) - timedelta(days=intraday_lookback_days)
        ).isoformat()

    print("Starting market universe refresh")
    print(f"  Symbols file: {symbols_file}")
    print(f"  Daily window: {daily_start_date} -> {daily_end_date}")
    print(f"  Intraday window: {intraday_start_date} -> {intraday_end_date}")
    print(f"  Run daily: {run_daily}")
    print(f"  Run intraday: {run_intraday}")
    print(f"  Build daily serving: {build_daily_serving}")

    daily_summary = {
        "success_count": 0,
        "failure_count": 0,
        "skipped_count": 0,
    }
    intraday_summary = {
        "success_count": 0,
        "failure_count": 0,
        "skipped_count": 0,
    }
    serving_output_path: str | None = None
 
    if run_daily:
        daily_summary = run_batch_market_timeframe_refresh(
            symbols_file=symbols_file,
            start_date=daily_start_date,
            end_date=daily_end_date,
            source_timeframes=("1d",),
            derived_timeframes=("1w", "1mo"),
        )

        if build_daily_serving:
            output_path = build_market_serving_output_path()
            build_serving_dataset(
                symbols_file=symbols_file,
                start_date=daily_start_date,
                end_date=daily_end_date,
                output_path=output_path,
            )
            serving_output_path = str(output_path)
            print(f"  Daily serving dataset written to {output_path}")

    if run_intraday:
        intraday_summary = run_batch_market_timeframe_refresh(
            symbols_file=symbols_file,
            start_date=intraday_start_date,
            end_date=intraday_end_date,
            source_timeframes=("1m",),
            derived_timeframes=("5m", "15m", "60m"),
        )

    combined_summary = {
        "symbols_file": str(symbols_file),
        "daily": daily_summary,
        "intraday": intraday_summary,
        "serving_output_path": serving_output_path,
        "total_success_count": (
            daily_summary["success_count"] + intraday_summary["success_count"]
        ),
        "total_failure_count": (
            daily_summary["failure_count"] + intraday_summary["failure_count"]
        ),
        "total_skipped_count": (
            daily_summary["skipped_count"] + intraday_summary["skipped_count"]
        ),
    }

    print("Market universe refresh completed")
    print(f"  Total successes: {combined_summary['total_success_count']}")
    print(f"  Total failures: {combined_summary['total_failure_count']}")
    print(f"  Total skipped: {combined_summary['total_skipped_count']}")

    return combined_summary