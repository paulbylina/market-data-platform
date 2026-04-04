from pathlib import Path

from src.storage.combine_curated_data import combine_curated_data
from src.utils.load_symbols import load_symbols


def build_serving_dataset(
    symbols_file: Path,
    start_date: str,
    end_date: str,
    output_path: Path,
) -> None:
    symbols = load_symbols(symbols_file)

    combine_curated_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
    )

    print(f"Serving dataset written to {output_path}")