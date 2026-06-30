from pathlib import Path

from src.utils.settings import (
    CURATED_DATA_DIR,
    QUALITY_DATA_DIR,
    RAW_DATA_DIR,
    SERVING_DATA_DIR,
    STANDARDIZED_DATA_DIR,
)

def safe_symbol_for_path(symbol: str) -> str:
    return symbol.replace(":", "_").replace("/", "_")

# RAW
def build_massive_raw_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
    asset_class: str = "stocks",
) -> Path:
    safe_symbol = safe_symbol_for_path(symbol)

    return (
        RAW_DATA_DIR
        / "massive"
        / asset_class
        / timeframe
        / f"{safe_symbol}_{start_date}_{end_date}_raw.json"
    )


def build_fred_raw_output_path(series_id: str, start_date: str, end_date: str) -> Path:
    return RAW_DATA_DIR / "fred" / f"{series_id}_{start_date}_{end_date}_raw.json"


# STANDARDIZED
def build_market_standardized_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
    asset_class: str = "stocks",
) -> Path:
    safe_symbol = safe_symbol_for_path(symbol)

    return (
        STANDARDIZED_DATA_DIR
        / "market"
        / asset_class
        / timeframe
        / f"{safe_symbol}_{start_date}_{end_date}_standardized.parquet"
    )


# CURATED
def build_market_curated_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
) -> Path:
    return (
        CURATED_DATA_DIR
        / "market"
        / timeframe
        / f"{symbol}_{start_date}_{end_date}_curated.parquet"
    )


# FAILURES
def build_market_validation_failures_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
) -> Path:
    return (
        QUALITY_DATA_DIR
        / "market"
        / timeframe
        / f"{symbol}_{start_date}_{end_date}_validation_failures.parquet"
    )


# WARNINGS
def build_market_validation_warnings_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
) -> Path:
    return (
        QUALITY_DATA_DIR
        / "market"
        / timeframe
        / f"{symbol}_{start_date}_{end_date}_validation_warnings.parquet"
    )


# QA SUMMARY
def build_market_validation_summary_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
) -> Path:
    return (
        QUALITY_DATA_DIR
        / "market"
        / timeframe
        / f"{symbol}_{start_date}_{end_date}_validation_summary.parquet"
    )


# MARKET SERVING MASTER FILE
def build_market_serving_output_path() -> Path:
    return SERVING_DATA_DIR / "market_universe_master.parquet"

# STAGING
def build_market_staging_output_path(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1d",
) -> Path:
    return (
        STANDARDIZED_DATA_DIR
        / "market"
        / timeframe
        / f"{symbol}_{start_date}_{end_date}_staging.parquet"
    )
