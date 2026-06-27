import pandas as pd

from src.pipelines.stocks.minute_bars_pipeline import run_minute_bars_pipeline
from src.pipelines.stocks.run_derived_bars_pipeline import run_derived_bars_pipeline
from src.pipelines.stocks.run_market_timeframe_refresh import run_market_timeframe_refresh
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_staging_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path,
)


def test_run_derived_5m_pipeline_writes_expected_outputs() -> None:
    symbol = "AAPL"
    start_date = "2024-01-02"
    end_date = "2024-01-02"

    run_minute_bars_pipeline(symbol, start_date, end_date)
    run_derived_bars_pipeline(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_timeframe="1m",
        target_timeframe="5m",
    )

    minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1m"
    )
    five_minute_staging_path = build_market_staging_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )
    five_minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )
    five_minute_failures_path = build_market_validation_failures_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )
    five_minute_warnings_path = build_market_validation_warnings_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )
    five_minute_summary_path = build_market_validation_summary_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )

    assert five_minute_staging_path.exists()
    assert five_minute_curated_path.exists()
    assert five_minute_failures_path.exists()
    assert five_minute_warnings_path.exists()
    assert five_minute_summary_path.exists()

    minute_df = pd.read_parquet(minute_curated_path)
    five_minute_df = pd.read_parquet(five_minute_curated_path)

    assert not minute_df.empty
    assert not five_minute_df.empty
    assert len(five_minute_df) < len(minute_df)
    assert five_minute_df["timeframe"].eq("5m").all()
    assert {
        "symbol",
        "timeframe",
        "bar_start",
        "bar_end",
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }.issubset(five_minute_df.columns)


def test_run_market_timeframe_refresh_writes_expected_intraday_outputs() -> None:
    symbol = "AAPL"
    start_date = "2024-01-02"
    end_date = "2024-01-02"

    run_market_timeframe_refresh(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_timeframes=("1m",),
        derived_timeframes=("5m", "15m", "60m"),
    )

    minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1m"
    )
    five_minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="5m"
    )
    fifteen_minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="15m"
    )
    sixty_minute_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="60m"
    )

    assert minute_curated_path.exists()
    assert five_minute_curated_path.exists()
    assert fifteen_minute_curated_path.exists()
    assert sixty_minute_curated_path.exists()

    minute_df = pd.read_parquet(minute_curated_path)
    five_minute_df = pd.read_parquet(five_minute_curated_path)
    fifteen_minute_df = pd.read_parquet(fifteen_minute_curated_path)
    sixty_minute_df = pd.read_parquet(sixty_minute_curated_path)

    assert not minute_df.empty
    assert not five_minute_df.empty
    assert not fifteen_minute_df.empty
    assert not sixty_minute_df.empty

    assert len(sixty_minute_df) < len(fifteen_minute_df) < len(five_minute_df) < len(minute_df)

    assert five_minute_df["timeframe"].eq("5m").all()
    assert fifteen_minute_df["timeframe"].eq("15m").all()
    assert sixty_minute_df["timeframe"].eq("60m").all()