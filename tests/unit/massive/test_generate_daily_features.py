import pandas as pd

from src.features.market.generate_daily_features import generate_daily_features


def test_generate_daily_features_adds_expected_columns() -> None:
    dates = pd.date_range(start="2024-01-01", periods=35, freq="B")

    df = pd.DataFrame(
        {
            "symbol": ["AAPL"] * 35,
            "date": dates,
            "open": [100.0 + i for i in range(35)],
            "high": [101.0 + i for i in range(35)],
            "low": [99.0 + i for i in range(35)],
            "close": [100.0 + i for i in range(35)],
            "volume": [1_000_000 + (i * 10_000) for i in range(35)],
            "vwap": [100.5 + i for i in range(35)],
            "trade_count": [1000 + i for i in range(35)],
            "source": ["massive"] * 35,
            "ingested_at": [pd.Timestamp("2024-02-01T00:00:00Z")] * 35,
            "standardized_at": [pd.Timestamp("2024-02-01T00:00:00Z")] * 35,
        }
    )

    featured_df = generate_daily_features(df)
    print(featured_df.tail())

    assert "volume_mean_30d" in featured_df.columns
    assert "volume_std_30d" in featured_df.columns
    assert "volume_zscore_30d" in featured_df.columns
    assert "close_mean_30d" in featured_df.columns
    assert "close_std_30d" in featured_df.columns
    assert "close_price_zscore_30d" in featured_df.columns

    assert featured_df["volume_zscore_30d"].iloc[28] != featured_df["volume_zscore_30d"].iloc[28]
    assert featured_df["close_price_zscore_30d"].iloc[28] != featured_df["close_price_zscore_30d"].iloc[28]

    assert pd.notna(featured_df["volume_zscore_30d"].iloc[34])
    assert pd.notna(featured_df["close_price_zscore_30d"].iloc[34])