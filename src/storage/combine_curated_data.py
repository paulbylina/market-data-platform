from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


def combine_curated_data(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_path: Path,
) -> None:
    dataframes: list[pd.DataFrame] = []

    for symbol in symbols:
        parquet_path = build_market_curated_output_path(symbol, start_date, end_date)
        if parquet_path.exists():
            dataframes.append(pd.read_parquet(parquet_path))

    if not dataframes:
        raise ValueError("No curated parquet files were found to combine.")

    combined_df = pd.concat(dataframes, ignore_index=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_path, index=False)