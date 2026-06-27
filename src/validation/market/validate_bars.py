import pandas as pd
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.path_builders import (
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path
)

def validate_bars(df: pd.DataFrame, symbol, start_date, end_date, timeframe) -> pd.DataFrame:
    """
    Validate standardized bar data and split it into valid rows, failures,
    warnings, and a summary.

    The canonical uniqueness key is now:
        symbol + timeframe + bar_start
    """
    failure_records: list[dict] = []
    warning_records: list[dict] = []

    required_columns = [
        "symbol",
        "timeframe",
        "bar_start",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    for column in required_columns:
        failed_rows = df[df[column].isna()]
        for _, row in failed_rows.iterrows():
            failure_records.append(
                {
                    "symbol": row.get("symbol"),
                    "timeframe": row.get("timeframe"),
                    "bar_start": row.get("bar_start"),
                    "date": row.get("date"),
                    "rule": f"{column}_is_required",
                    "severity": "failure",
                }
            )

    duplicate_rows = df[
        df.duplicated(subset=["symbol", "timeframe", "bar_start"], keep=False)
    ]
    for _, row in duplicate_rows.iterrows():
        failure_records.append(
            {
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "bar_start": row.get("bar_start"),
                "date": row.get("date"),
                "rule": "duplicate_symbol_timeframe_bar_start",
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
                "timeframe": row.get("timeframe"),
                "bar_start": row.get("bar_start"),
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
                "timeframe": row.get("timeframe"),
                "bar_start": row.get("bar_start"),
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
                "timeframe": row.get("timeframe"),
                "bar_start": row.get("bar_start"),
                "date": row.get("date"),
                "rule": "zero_volume",
                "severity": "warning",
            }
        )

    record_columns = [
        "symbol",
        "timeframe",
        "bar_start",
        "date",
        "rule",
        "severity",
    ]

    failures_df = pd.DataFrame(failure_records, columns=record_columns)
    warnings_df = pd.DataFrame(warning_records, columns=record_columns)

    if failures_df.empty:
        valid_df = df.copy()
    else:
        failed_keys = failures_df[
            ["symbol", "timeframe", "bar_start"]
        ].drop_duplicates()
        valid_df = df.merge(
            failed_keys,
            on=["symbol", "timeframe", "bar_start"],
            how="left",
            indicator=True,
        )
        valid_df = valid_df[valid_df["_merge"] == "left_only"].drop(columns=["_merge"])

    summary_df = pd.DataFrame(
        [
            {
                "timeframe": (
                    df["timeframe"].dropna().iloc[0]
                    if "timeframe" in df.columns and not df.empty
                    else pd.NA
                ),
                "total_rows": len(df),
                "valid_rows": len(valid_df),
                "failure_count": len(failures_df),
                "warning_count": len(warnings_df),
            }
        ]
    )

    # Save validation results
    write_quality_dataframe(
        failures_df,
        build_market_validation_failures_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )
    write_quality_dataframe(
        warnings_df,
        build_market_validation_warnings_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )
    write_quality_dataframe(
        summary_df,
        build_market_validation_summary_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    return valid_df