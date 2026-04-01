from src.features.generate_daily_features import generate_daily_features
from src.ingestion.fetch_daily_bars import fetch_daily_bars
from src.standardization.standardize_daily_bars import standardize_daily_bars
from src.storage.write_data import write_dataframe_parquet, write_raw_json
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.settings import (
    CURATED_DATA_DIR,
    QUALITY_DATA_DIR,
    RAW_DATA_DIR,
    STAGING_DATA_DIR,
)
from src.validation.validate_daily_bars import validate_daily_bars


def run_daily_eod_pipeline(symbol: str, start_date: str, end_date: str) -> None:
    """
    Run the end-to-end daily EOD pipeline for a single symbol and date range.

    This pipeline fetches raw data, standardizes it, validates it, generates features,
    and writes outputs to the raw, staging, curated, and quality layers.
    """

    raw_response = fetch_daily_bars(symbol, start_date, end_date)

    standardized_df = standardize_daily_bars(raw_response)

    valid_df, failures_df, warnings_df, summary_df = validate_daily_bars(standardized_df)

    featured_df = generate_daily_features(valid_df)

    write_raw_json(
        raw_response,
        RAW_DATA_DIR / f"{symbol}_{start_date}_{end_date}_raw.json",
    )

    write_dataframe_parquet(
        standardized_df,
        STAGING_DATA_DIR / f"{symbol}_{start_date}_{end_date}_staging.parquet",
    )

    write_dataframe_parquet(
        featured_df,
        CURATED_DATA_DIR / f"{symbol}_{start_date}_{end_date}_curated.parquet",
    )

    write_quality_dataframe(
        failures_df,
        QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_failures.parquet",
    )

    write_quality_dataframe(
        warnings_df,
        QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_warnings.parquet",
    )

    write_quality_dataframe(
        summary_df,
        QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_summary.parquet",
    )