import json
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.pipelines.stocks.daily_eod_pipeline import run_daily_eod_pipeline
from src.pipelines.stocks.minute_bars_pipeline import run_minute_bars_pipeline
from src.pipelines.stocks.run_derived_bars_pipeline import run_derived_bars_pipeline
from src.pipelines.stocks.run_market_timeframe_refresh import run_market_timeframe_refresh
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_staging_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path,
    build_massive_raw_output_path,
)

CONFIG_PATH = Path(__file__).parent / "integration_test_config.json"


def load_integration_test_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_date_range(lookback_days: int, end_lag_days: int) -> tuple[str, str]:
    end_date = date.today() - timedelta(days=end_lag_days)
    start_date = end_date - timedelta(days=lookback_days)

    return start_date.isoformat(), end_date.isoformat()


def test_run_daily_eod_pipeline_writes_expected_outputs() -> None:
    config = load_integration_test_config()
    symbol = config["stock_symbol"]

    start_date, end_date = build_date_range(
        lookback_days=config["lookback_days"],
        end_lag_days=config["end_lag_days"],
    )

    run_daily_eod_pipeline(symbol, start_date, end_date)

    raw_path = build_massive_raw_output_path(symbol, start_date, end_date, timeframe="1d")
    staging_path = build_market_staging_output_path(symbol, start_date, end_date, timeframe="1d")
    curated_path = build_market_curated_output_path(symbol, start_date, end_date, timeframe="1d")
    failures_path = build_market_validation_failures_output_path(symbol, start_date, end_date, timeframe="1d")
    warnings_path = build_market_validation_warnings_output_path(symbol, start_date, end_date, timeframe="1d")
    summary_path = build_market_validation_summary_output_path(symbol, start_date, end_date, timeframe="1d")

    assert raw_path.exists()
    assert staging_path.exists()
    assert curated_path.exists()
    assert failures_path.exists()
    assert warnings_path.exists()
    assert summary_path.exists()


def test_run_minute_bars_pipeline_writes_expected_outputs() -> None:
    symbol = "AAPL"
    start_date = "2024-01-02"
    end_date = "2024-01-02"

    run_minute_bars_pipeline(symbol, start_date, end_date)

    raw_path = build_massive_raw_output_path(symbol, start_date, end_date, timeframe="1m")
    staging_path = build_market_staging_output_path(symbol, start_date, end_date, timeframe="1m")
    curated_path = build_market_curated_output_path(symbol, start_date, end_date, timeframe="1m")
    failures_path = build_market_validation_failures_output_path(symbol, start_date, end_date, timeframe="1m")
    warnings_path = build_market_validation_warnings_output_path(symbol, start_date, end_date, timeframe="1m")
    summary_path = build_market_validation_summary_output_path(symbol, start_date, end_date, timeframe="1m")

    assert raw_path.exists()
    assert staging_path.exists()
    assert curated_path.exists()
    assert failures_path.exists()
    assert warnings_path.exists()
    assert summary_path.exists()

    minute_df = pd.read_parquet(curated_path)

    assert not minute_df.empty
    assert minute_df["timeframe"].eq("1m").all()
    assert {
        "symbol",
        "bar_start",
        "bar_end",
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }.issubset(minute_df.columns)


def test_run_derived_weekly_pipeline_writes_expected_outputs() -> None:
    symbol = "AAPL"
    start_date = "2024-01-01"
    end_date = "2024-03-31"

    run_daily_eod_pipeline(symbol, start_date, end_date)
    run_derived_bars_pipeline(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_timeframe="1d",
        target_timeframe="1w",
    )

    weekly_staging_path = build_market_staging_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )
    weekly_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )
    weekly_failures_path = build_market_validation_failures_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )
    weekly_warnings_path = build_market_validation_warnings_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )
    weekly_summary_path = build_market_validation_summary_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )

    assert weekly_staging_path.exists()
    assert weekly_curated_path.exists()
    assert weekly_failures_path.exists()
    assert weekly_warnings_path.exists()
    assert weekly_summary_path.exists()

    daily_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1d"
    )

    daily_df = pd.read_parquet(daily_curated_path)
    weekly_df = pd.read_parquet(weekly_curated_path)

    assert not daily_df.empty
    assert not weekly_df.empty
    assert len(weekly_df) < len(daily_df)
    assert {
        "symbol",
        "timeframe",
        "bar_start",
        "bar_end",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }.issubset(weekly_df.columns)
    assert weekly_df["timeframe"].eq("1w").all()


def test_run_market_timeframe_refresh_writes_expected_monthly_outputs() -> None:
    symbol = "AAPL"
    start_date = "2024-01-01"
    end_date = "2024-03-31"

    run_market_timeframe_refresh(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )

    daily_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1d"
    )
    weekly_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1w"
    )
    monthly_staging_path = build_market_staging_output_path(
        symbol, start_date, end_date, timeframe="1mo"
    )
    monthly_curated_path = build_market_curated_output_path(
        symbol, start_date, end_date, timeframe="1mo"
    )
    monthly_failures_path = build_market_validation_failures_output_path(
        symbol, start_date, end_date, timeframe="1mo"
    )
    monthly_warnings_path = build_market_validation_warnings_output_path(
        symbol, start_date, end_date, timeframe="1mo"
    )
    monthly_summary_path = build_market_validation_summary_output_path(
        symbol, start_date, end_date, timeframe="1mo"
    )

    assert daily_curated_path.exists()
    assert weekly_curated_path.exists()
    assert monthly_staging_path.exists()
    assert monthly_curated_path.exists()
    assert monthly_failures_path.exists()
    assert monthly_warnings_path.exists()
    assert monthly_summary_path.exists()

    daily_df = pd.read_parquet(daily_curated_path)
    weekly_df = pd.read_parquet(weekly_curated_path)
    monthly_df = pd.read_parquet(monthly_curated_path)

    assert not daily_df.empty
    assert not weekly_df.empty
    assert not monthly_df.empty

    assert len(monthly_df) < len(weekly_df) < len(daily_df)

    assert {
        "symbol",
        "timeframe",
        "bar_start",
        "bar_end",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }.issubset(monthly_df.columns)
    assert monthly_df["timeframe"].eq("1mo").all()