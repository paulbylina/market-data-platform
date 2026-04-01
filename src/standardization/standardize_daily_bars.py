from datetime import UTC, datetime

import pandas as pd


def standardize_daily_bars(raw_response: dict) -> pd.DataFrame:
    """
    Convert a raw Massive aggregates response into a standardized daily-bars DataFrame.

    The output matches the staging-layer schema used by the pipeline and normalizes
    vendor field names into readable column names.
    """
    results = raw_response.get("results", [])

    columns = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
        "source",
        "ingested_at",
        "standardized_at",
    ]

    if not results:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(results).rename(
        columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "trade_count",
            "t": "timestamp",
        }
    )

    df["symbol"] = raw_response.get("ticker")
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.normalize().dt.tz_localize(None)
    df["source"] = "massive"

    ingested_at = datetime.now(UTC)
    standardized_at = datetime.now(UTC)

    df["ingested_at"] = ingested_at
    df["standardized_at"] = standardized_at

    df["symbol"] = df["symbol"].astype("string")
    df["source"] = df["source"].astype("string")

    numeric_float_cols = ["open", "high", "low", "close", "vwap"]
    for col in numeric_float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.array(pd.to_numeric(df["volume"], errors="coerce"), dtype="Int64")
    df["trade_count"] = pd.array(pd.to_numeric(df["trade_count"], errors="coerce"), dtype="Int64")

    return df[columns]