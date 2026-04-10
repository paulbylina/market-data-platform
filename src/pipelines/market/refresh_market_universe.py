from datetime import date, timedelta
from pathlib import Path

from src.pipelines.market.batch_market_timeframe_refresh import (
    run_batch_market_timeframe_refresh,
)
from src.utils.settings import CONFIG_DIR


def refresh_market_universe(
    symbols_file: Path | None = None,
    daily_start_date: str | None = None,
    daily_end_date: str | None = None,
    intraday_start_date: str | None = None,
    intraday_end_date: str | None = None,
    daily_lookback_days: int = 2000, # Would like to get median/average datapoints for highest tf 1mo to 60 bars
    intraday_lookback_days: int = 20, # Would like to get median/average datapoints for highest intra tf 60m to 60 bars
) -> dict:
    """
    Refresh the market-data universe across daily and intraday timeframe families.

    Daily family:
      - source: 1d
      - derived: 1w, 1mo

    Intraday family:
      - source: 1m
      - derived: 5m, 15m, 60m

    Different lookback windows are used so intraday refreshes stay manageable.
    """
    if symbols_file is None:
        symbols_file = CONFIG_DIR / "symbols.txt"

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

    # 1 - Runs 1D sourced data first
    daily_summary = run_batch_market_timeframe_refresh(
        symbols_file=symbols_file,
        start_date=daily_start_date,
        end_date=daily_end_date,
        source_timeframes=("1d",),
        derived_timeframes=("1w", "1mo"),
    )
    # 2 - Runs 1m sourced data second
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