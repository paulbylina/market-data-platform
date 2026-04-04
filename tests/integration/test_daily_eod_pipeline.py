from src.pipelines.market.daily_eod_pipeline import run_daily_eod_pipeline
from src.utils.settings import CURATED_DATA_DIR, QUALITY_DATA_DIR, RAW_DATA_DIR, STAGING_DATA_DIR


def test_run_daily_eod_pipeline_writes_expected_outputs() -> None:
    symbol = "AAPL"
    start_date = "2023-10-01"
    end_date = "2024-01-31"

    run_daily_eod_pipeline(symbol, start_date, end_date)

    raw_path = RAW_DATA_DIR / f"{symbol}_{start_date}_{end_date}_raw.json"
    staging_path = STAGING_DATA_DIR / f"{symbol}_{start_date}_{end_date}_staging.parquet"
    curated_path = CURATED_DATA_DIR / f"{symbol}_{start_date}_{end_date}_curated.parquet"
    failures_path = QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_failures.parquet"
    warnings_path = QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_warnings.parquet"
    summary_path = QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_summary.parquet"

    assert raw_path.exists()
    assert staging_path.exists()
    assert curated_path.exists()
    assert failures_path.exists()
    assert warnings_path.exists()
    assert summary_path.exists()