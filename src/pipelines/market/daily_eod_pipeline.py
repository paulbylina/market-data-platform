from src.features.market import generate_daily_features
from src.ingestion.massive import fetch_daily_bars
from src.standardization.market import standardize_daily_bars
from src.storage.write_data import write_dataframe_parquet, write_raw_json
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.path_builders import (
    build_massive_raw_output_path,
    build_market_staging_output_path,    
    build_market_curated_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_warnings_output_path,
    build_market_validation_summary_output_path,
)
from src.validation.market import validate_daily_bars


def run_daily_eod_pipeline(symbol: str, start_date: str, end_date: str) -> None:
    """
    Run the end-to-end daily EOD pipeline for a single symbol and date range.

    This pipeline fetches raw data, standardizes it, validates it, generates features,
    and writes outputs to the raw, staging, curated, and quality layers.
    """

    # Get raw data from massive
    raw_response = fetch_daily_bars(symbol, start_date, end_date)
    
    # Standardize df column names and datatypes
    standardized_df = standardize_daily_bars(raw_response)
    
    # Validation/QA
    valid_df, failures_df, warnings_df, summary_df = validate_daily_bars(standardized_df)
    
    # Add features
    featured_df = generate_daily_features(valid_df)
    
    # ---- Staging ---- #
    # Raw
    write_raw_json(
        raw_response,
        build_massive_raw_output_path(symbol, start_date, end_date),
    )
    
    # Standardized
    write_dataframe_parquet(
        standardized_df,
        build_market_staging_output_path(symbol, start_date, end_date),
    )
    
    # Curated
    write_dataframe_parquet(
        featured_df,
        build_market_curated_output_path(symbol, start_date, end_date),
    )
    
    # Failures
    write_quality_dataframe(
        failures_df,
        build_market_validation_failures_output_path(symbol, start_date, end_date),
    )
    
    # Warnings
    write_quality_dataframe(
        warnings_df,
        build_market_validation_warnings_output_path(symbol, start_date, end_date),
    )
    
    # Summary
    write_quality_dataframe(
        summary_df,
        build_market_validation_summary_output_path(symbol, start_date, end_date),
    )