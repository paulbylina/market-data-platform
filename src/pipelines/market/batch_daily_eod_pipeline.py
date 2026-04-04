from pathlib import Path

from src.pipelines.market.daily_eod_pipeline import run_daily_eod_pipeline
from src.utils.load_symbols import load_symbols


def run_batch_daily_eod_pipeline(symbols_file: Path, start_date: str, end_date: str) -> None:
    symbols = load_symbols(symbols_file)

    for symbol in symbols:
        print(f"Running pipeline for {symbol}...")
        run_daily_eod_pipeline(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    print("Batch pipeline completed successfully.")