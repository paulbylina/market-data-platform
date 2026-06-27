import pandas as pd

from src.features.stocks.generate_bar_features import generate_bar_features


def generate_daily_features(valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-compatible daily feature generator.

    The canonical feature names are bar-based. Legacy daily aliases are emitted so
    the existing daily serving flow does not break while the platform is refactored.
    """
    df = generate_bar_features(valid_df)

    if df.empty:
        return df

    df["volume_mean_30d"] = df["volume_mean_30bar"]
    df["volume_std_30d"] = df["volume_std_30bar"]
    df["close_mean_30d"] = df["close_mean_30bar"]
    df["close_std_30d"] = df["close_std_30bar"]
    df["volume_zscore_30d"] = df["volume_zscore_30bar"]
    df["close_price_zscore_30d"] = df["close_price_zscore_30bar"]

    return df