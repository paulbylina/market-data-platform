import numpy as np
import pandas as pd


def generate_bar_features(valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 30-bar rolling features.

    Current feature groups:
    - raw volume statistics + z-score
    - log-volume statistics + z-score
    - close-price statistics + z-score
    """
    df = valid_df.copy()

    if df.empty:
        return df

    sort_columns = ["symbol", "bar_start"]
    group_columns = ["symbol"]

    if "timeframe" in df.columns:
        sort_columns = ["symbol", "timeframe", "bar_start"]
        group_columns = ["symbol", "timeframe"]

    df = df.sort_values(sort_columns).reset_index(drop=True)

    grouped = df.groupby(group_columns, group_keys=False)

    # Raw volume features
    df["volume_mean_30bar"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["volume_std_30bar"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )

    # Log-volume features
    df["log_volume"] = np.log1p(df["volume"].astype("float64"))
    df["log_volume_mean_30bar"] = grouped["log_volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["log_volume_std_30bar"] = grouped["log_volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )

    # Close-price features
    df["close_mean_30bar"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["close_std_30bar"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )

    # Z-scores
    df["volume_zscore_30bar"] = (
        (df["volume"] - df["volume_mean_30bar"]) / df["volume_std_30bar"]
    )
    df["log_volume_zscore_30bar"] = (
        (df["log_volume"] - df["log_volume_mean_30bar"]) / df["log_volume_std_30bar"]
    )
    df["close_price_zscore_30bar"] = (
        (df["close"] - df["close_mean_30bar"]) / df["close_std_30bar"]
    )

    df.loc[df["volume_std_30bar"] == 0, "volume_zscore_30bar"] = pd.NA
    df.loc[df["log_volume_std_30bar"] == 0, "log_volume_zscore_30bar"] = pd.NA
    df.loc[df["close_std_30bar"] == 0, "close_price_zscore_30bar"] = pd.NA

    return df