from __future__ import annotations

import numpy as np
import pandas as pd


def add_price_zscores(
    df: pd.DataFrame,
    price_col: str = "close",
    windows: tuple[int, ...] = (50, 200),
) -> pd.DataFrame:
    """
    Add rolling price z-scores.

    Z-score tells us how far price is from its own rolling average,
    measured in standard deviations.

    Example:
        z50 = (close - rolling_mean_50) / rolling_std_50
    """
    result = df.copy()

    for window in windows:
        mean_col = f"close_mean_{window}d"
        std_col = f"close_std_{window}d"
        z_col = f"close_zscore_{window}d"

        result[mean_col] = result[price_col].rolling(window).mean()
        result[std_col] = result[price_col].rolling(window).std()

        result[z_col] = np.where(
            result[std_col] == 0,
            np.nan,
            (result[price_col] - result[mean_col]) / result[std_col],
        )

    return result


def calculate_period_return(
    df: pd.DataFrame,
    price_col: str = "close",
    periods: int = 20,
) -> pd.Series:
    """
    Calculate percentage return over a lookback period.

    Example:
        20-day return = close today / close 20 days ago - 1
    """
    return df[price_col].pct_change(periods=periods)

def classify_setup(
    rs_value: float,
    z50_value: float,
) -> str:
    """
    Classify the stock based on relative strength and z-score extension.
    """
    if pd.isna(rs_value) or pd.isna(z50_value):
        return "Insufficient Data"

    if rs_value > 0 and 0 <= z50_value <= 1.5:
        return "Strong + Healthy"

    if rs_value > 0 and z50_value > 1.5:
        return "Strong + Extended"

    if rs_value < 0 and z50_value < -1:
        return "Weak + Breakdown"

    if rs_value > 0 and z50_value < -2:
        return "Mean Reversion Candidate"

    return "Neutral"

def build_rs_features(
    stock_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    ticker: str,
    benchmark: str = "SPY",
    date_col: str = "date",
    price_col: str = "close",
    rs_period: int = 20,
) -> pd.DataFrame:
    """
    Build a simple relative strength scan for one stock vs one benchmark.

    MVP output:
        ticker
        date
        close
        stock_return
        benchmark_return
        rs_vs_benchmark
        close_zscore_50d
        close_zscore_200d
    """
    stock = stock_df.copy()
    bench = benchmark_df.copy()

    stock = stock.sort_values(date_col)
    bench = bench.sort_values(date_col)

    stock = add_price_zscores(stock, price_col=price_col, windows=(50, 200))

    stock["stock_return"] = calculate_period_return(
        stock,
        price_col=price_col,
        periods=rs_period,
    )

    bench["benchmark_return"] = calculate_period_return(
        bench,
        price_col=price_col,
        periods=rs_period,
    )

    merged = stock.merge(
        bench[[date_col, "benchmark_return"]],
        on=date_col,
        how="left",
    )

    merged["ticker"] = ticker
    merged["benchmark"] = benchmark
    merged["rs_vs_benchmark"] = (
        merged["stock_return"] - merged["benchmark_return"]
    )
    merged["setup"] = merged.apply(
        lambda row: classify_setup(
            row["rs_vs_benchmark"],
            row["close_zscore_50d"],
        ),
        axis=1,
    )

    output_cols = [
        "ticker",
        "benchmark",
        date_col,
        price_col,
        "stock_return",
        "benchmark_return",
        "rs_vs_benchmark",
        "close_zscore_50d",
        "close_zscore_200d",
        "setup",
    ]

    return merged[output_cols]