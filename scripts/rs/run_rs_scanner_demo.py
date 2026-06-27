import pandas as pd

from src.features.stocks.rs_features import build_relative_strength_scan


def main() -> None:
    dates = pd.date_range("2024-01-01", periods=260, freq="D")

    # Fake stock: stronger trend
    stock_df = pd.DataFrame(
        {
            "date": dates,
            "close": [100 + i * 0.8 for i in range(260)],
        }
    )

    # Fake benchmark: slower trend
    benchmark_df = pd.DataFrame(
        {
            "date": dates,
            "close": [100 + i * 0.4 for i in range(260)],
        }
    )

    scan = build_relative_strength_scan(
        stock_df=stock_df,
        benchmark_df=benchmark_df,
        ticker="AAPL",
        benchmark="SPY",
        rs_period=20,
    )

    print(scan.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()