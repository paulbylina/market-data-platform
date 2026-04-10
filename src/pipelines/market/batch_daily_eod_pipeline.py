from pathlib import Path

from src.pipelines.market.batch_market_timeframe_refresh import (
    run_batch_market_timeframe_refresh,
)


def run_batch_daily_eod_pipeline(
    symbols_file: Path,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Backward-compatible wrapper for batch daily source refresh only.
    """
    return run_batch_market_timeframe_refresh(
        symbols_file=symbols_file,
        start_date=start_date,
        end_date=end_date,
        source_timeframes=("1d",),
        derived_timeframes=(),
    )