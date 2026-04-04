import pandas as pd


def validate_daily_bars(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Validate standardized daily bar data and split it into valid rows, failures, warnings, and a summary.

    This function enforces basic staging-layer data quality rules before feature generation.
    """
    
    failure_records: list[dict] = []
    warning_records: list[dict] = []

    required_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]

    for column in required_columns:
        failed_rows = df[df[column].isna()]
        for _, row in failed_rows.iterrows():
            failure_records.append(
                {
                    "symbol": row.get("symbol"),
                    "date": row.get("date"),
                    "rule": f"{column}_is_required",
                    "severity": "failure",
                }
            )

    duplicate_rows = df[df.duplicated(subset=["symbol", "date"], keep=False)]
    for _, row in duplicate_rows.iterrows():
        failure_records.append(
            {
                "symbol": row.get("symbol"),
                "date": row.get("date"),
                "rule": "duplicate_symbol_date",
                "severity": "failure",
            }
        )

    invalid_price_rows = df[
        (df["open"] <= 0)
        | (df["high"] <= 0)
        | (df["low"] <= 0)
        | (df["close"] <= 0)
        | (df["low"] > df["high"])
        | (df["open"] < df["low"])
        | (df["open"] > df["high"])
        | (df["close"] < df["low"])
        | (df["close"] > df["high"])
    ]
    for _, row in invalid_price_rows.iterrows():
        failure_records.append(
            {
                "symbol": row.get("symbol"),
                "date": row.get("date"),
                "rule": "invalid_ohlc_relationship",
                "severity": "failure",
            }
        )

    negative_volume_rows = df[df["volume"] < 0]
    for _, row in negative_volume_rows.iterrows():
        failure_records.append(
            {
                "symbol": row.get("symbol"),
                "date": row.get("date"),
                "rule": "negative_volume",
                "severity": "failure",
            }
        )

    zero_volume_rows = df[df["volume"] == 0]
    for _, row in zero_volume_rows.iterrows():
        warning_records.append(
            {
                "symbol": row.get("symbol"),
                "date": row.get("date"),
                "rule": "zero_volume",
                "severity": "warning",
            }
        )

    failures_df = pd.DataFrame(failure_records)
    warnings_df = pd.DataFrame(warning_records)

    if failures_df.empty:
        valid_df = df.copy()
    else:
        failed_keys = failures_df[["symbol", "date"]].drop_duplicates()
        valid_df = df.merge(failed_keys, on=["symbol", "date"], how="left", indicator=True)
        valid_df = valid_df[valid_df["_merge"] == "left_only"].drop(columns=["_merge"])

    summary_df = pd.DataFrame(
        [
            {
                "total_rows": len(df),
                "valid_rows": len(valid_df),
                "failure_count": len(failures_df),
                "warning_count": len(warnings_df),
            }
        ]
    )

    return valid_df, failures_df, warnings_df, summary_df