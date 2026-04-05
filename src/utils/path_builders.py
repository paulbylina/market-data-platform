from pathlib import Path

from src.utils.settings import (
    CURATED_DATA_DIR, 
    QUALITY_DATA_DIR, 
    RAW_DATA_DIR, 
    STAGING_DATA_DIR,
    SERVING_DATA_DIR,
)

# RAW
def build_massive_raw_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return RAW_DATA_DIR / "massive" / f"{symbol}_{start_date}_{end_date}_raw.json"

def build_fred_raw_output_path(series_id: str, start_date: str, end_date: str) -> Path:
    return RAW_DATA_DIR / "fred" / f"{series_id}_{start_date}_{end_date}_raw.json"

# STAGING
def build_market_staging_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return STAGING_DATA_DIR / "market" / f"{symbol}_{start_date}_{end_date}_staging.parquet"

# CURATED
def build_market_curated_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return CURATED_DATA_DIR / "market" / f"{symbol}_{start_date}_{end_date}_curated.parquet"

# FAILURES
def build_market_validation_failures_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / "market" / f"{symbol}_{start_date}_{end_date}_validation_failures.parquet"

# WARNINGS
def build_market_validation_warnings_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / "market" / f"{symbol}_{start_date}_{end_date}_validation_warnings.parquet"

# QA SUMMARY
def build_market_validation_summary_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / "market" / f"{symbol}_{start_date}_{end_date}_validation_summary.parquet"

# RELATIVE VOLUME MASTER FILE
def build_relative_volume_serving_output_path() -> Path:
    return SERVING_DATA_DIR / "relative_volume_master.parquet"