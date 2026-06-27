import pandas as pd

from src.aggregation.market import build_derived_bars
from src.features.stocks import generate_bar_features
from src.storage.write_data import write_dataframe_parquet
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_staging_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path,
)
from src.utils.timeframes import get_derivation_spec
from src.validation.market import validate_bars


def run_derived_bars_pipeline(
    symbol: str,
    start_date: str,
    end_date: str,
    source_timeframe: str = "1d",
    target_timeframe: str = "1w",
) -> None:
    """
    Build derived bars from an existing lower-granularity curated dataset.
    """
    derivation_spec = get_derivation_spec(target_timeframe)

    if source_timeframe != derivation_spec.source_timeframe:
        raise ValueError(
            f"Invalid derivation {source_timeframe}->{target_timeframe}. "
            f"Expected {derivation_spec.source_timeframe}->{target_timeframe}"
        )

    source_path = build_market_curated_output_path(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        timeframe=source_timeframe,
    )

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source curated dataset does not exist: {source_path}. "
            f"Run the {source_timeframe} source pipeline first."
        )

    source_df = pd.read_parquet(source_path)

    aggregated_df = build_derived_bars(
        source_df=source_df,
        source_timeframe=source_timeframe,
        target_timeframe=target_timeframe,
    )

    valid_df, failures_df, warnings_df, summary_df = validate_bars(aggregated_df)
    featured_df = generate_bar_features(valid_df)

    write_dataframe_parquet(
        aggregated_df,
        build_market_staging_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_dataframe_parquet(
        featured_df,
        build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        failures_df,
        build_market_validation_failures_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        warnings_df,
        build_market_validation_warnings_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        summary_df,
        build_market_validation_summary_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )