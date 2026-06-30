from src.ingestion.massive import fetch_bars
from src.standardization.market import standardize_bars
from src.storage.write_data import write_dataframe_parquet, write_raw_json
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_standardized_output_path,
    build_massive_raw_output_path
)
from src.validation.market import validate_bars


def run_curated_daily_bars_pipeline(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Run the end-to-end source-native bar pipeline for a single symbol, timeframe,
    and date range.
    """

    # 1a - Download Raw Data
    raw_response = fetch_bars(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
    )
    # 1b - Save Raw data
    write_raw_json(
        raw_response,
        build_massive_raw_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )
        
    # 2a - Standardize data
    standardized_df = standardize_bars(
        raw_response=raw_response,
        timeframe=timeframe,
    )
    # 2b - Save Standardized DF
    write_dataframe_parquet(
        standardized_df,
        build_market_standardized_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    # 3a - Validate Data
    valid_df = validate_bars(
        df=standardized_df, 
        symbol=symbol, 
        start_date=start_date, 
        end_date=end_date, 
        timeframe=timeframe
    )
    # 3b - Save Validated DF
    write_dataframe_parquet(
        valid_df,
        build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

def run_market_bars_pipeline(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Generic market bars pipeline wrapper.

    Kept for callers that use the generic name instead of the older
    run_curated_daily_bars_pipeline name.
    """
    run_curated_daily_bars_pipeline(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
    )
