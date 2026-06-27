import json
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.massive.fetch_bars import fetch_bars
from src.scanners.relative_strength_scanner import build_relative_strength_scan
from src.standardization.market.standardize_bars import standardize_bars
from src.utils.settings import MASSIVE_API_KEY


CONFIG_PATH = Path(__file__).parent / "smoke_test_config.json"


def load_smoke_test_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_date_range(lookback_days: int, end_lag_days: int) -> tuple[str, str]:
    end_date = date.today() - timedelta(days=end_lag_days)
    start_date = end_date - timedelta(days=lookback_days)

    return start_date.isoformat(), end_date.isoformat()


@pytest.mark.smoke
@pytest.mark.massive
def test_rs_scanner_with_real_stock_and_benchmark_daily_bars() -> None:
    """
    Smoke test for the RS scanner using real Massive daily bars.

    This makes 2 API calls:
    1. stock symbol
    2. benchmark symbol
    """
    if not MASSIVE_API_KEY:
        pytest.skip("MASSIVE_API_KEY is not set")

    config = load_smoke_test_config()

    start_date, end_date = build_date_range(
        lookback_days=config["lookback_days"],
        end_lag_days=config["end_lag_days"],
    )

    stock_raw = fetch_bars(
        symbol=config["stock_symbol"],
        timeframe=config["timeframe"],
        start_date=start_date,
        end_date=end_date,
    )

    time.sleep(config["rate_limit_sleep_seconds"])

    benchmark_raw = fetch_bars(
        symbol=config["benchmark_symbol"],
        timeframe=config["timeframe"],
        start_date=start_date,
        end_date=end_date,
    )

    stock_df = standardize_bars(stock_raw, timeframe=config["timeframe"])
    benchmark_df = standardize_bars(benchmark_raw, timeframe=config["timeframe"])

    scan = build_relative_strength_scan(
        stock_df=stock_df,
        benchmark_df=benchmark_df,
        ticker=config["stock_symbol"],
        benchmark=config["benchmark_symbol"],
        date_col="bar_start",
        price_col="close",
        rs_period=config["rs_period"],
    )

    required_columns = [
        "rs_vs_benchmark",
        "close_zscore_50d",
    ]

    valid_scan = scan.dropna(subset=required_columns)

    assert not valid_scan.empty, scan.tail(10).to_string(index=False)

    latest = valid_scan.iloc[-1]

    assert latest["ticker"] == config["stock_symbol"]
    assert latest["benchmark"] == config["benchmark_symbol"]
    assert pd.notna(latest["rs_vs_benchmark"])
    assert pd.notna(latest["close_zscore_50d"])

    assert latest["setup"] in [
        "Strong + Healthy",
        "Strong + Extended",
        "Weak + Breakdown",
        "Mean Reversion Candidate",
        "Neutral",
        "Insufficient Data",
    ]