import pandas as pd


def generate_daily_features(valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 30-bar rolling volume and close-price statistics plus z-score features.

    The input is expected to be a validated daily-bars DataFrame sorted by symbol and date.
    """
    df = valid_df.copy()

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    grouped = df.groupby("symbol", group_keys=False)

    df["volume_mean_30d"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["volume_std_30d"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )
    df["close_mean_30d"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["close_std_30d"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )

    df["volume_zscore_30d"] = (
        (df["volume"] - df["volume_mean_30d"]) / df["volume_std_30d"]
    )
    df["close_price_zscore_30d"] = (
        (df["close"] - df["close_mean_30d"]) / df["close_std_30d"]
    )

    df.loc[df["volume_std_30d"] == 0, "volume_zscore_30d"] = pd.NA
    df.loc[df["close_std_30d"] == 0, "close_price_zscore_30d"] = pd.NA

    return df