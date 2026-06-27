import pandas as pd

from src.scanners.relative_strength_scanner import build_relative_strength_scan


def test_build_relative_strength_scan_outputs_expected_columns():
    dates = pd.date_range("2024-01-01", periods=220, freq="D")

    stock_df = pd.DataFrame(
        {
            "date": dates,
            "close": range(100, 320),
        }
    )

    benchmark_df = pd.DataFrame(
        {
            "date": dates,
            "close": range(100, 320),
        }
    )

    result = build_relative_strength_scan(
        stock_df=stock_df,
        benchmark_df=benchmark_df,
        ticker="AAPL",
        benchmark="SPY",
        rs_period=20,
    )

    expected_columns = [
        "ticker",
        "benchmark",
        "date",
        "close",
        "stock_return",
        "benchmark_return",
        "rs_vs_benchmark",
        "close_zscore_50d",
        "close_zscore_200d",
        "setup",
    ]

    assert list(result.columns) == expected_columns
    assert result["ticker"].iloc[-1] == "AAPL"
    assert result["benchmark"].iloc[-1] == "SPY"
    assert result["rs_vs_benchmark"].iloc[-1] == 0
    assert result["setup"].iloc[-1] in [
        "Strong + Healthy",
        "Strong + Extended",
        "Weak + Breakdown",
        "Mean Reversion Candidate",
        "Neutral",
        "Insufficient Data",
    ]