from pathlib import Path
import sys

import polars as pl


REQUIRED_COLUMNS = {"ts_utc", "instrument", "market_data_type", "price", "size"}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: uv run python src/validation/tick/preview_L1_csv.py <path-to-csv>"
        )

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        raise SystemExit(f"File not found: {csv_path}")

    df = pl.read_csv(csv_path, has_header=False, new_columns=["ts_utc", "instrument", "market_data_type", "price", "size"])

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    typed = df.with_columns(
        [
            pl.col("ts_utc").str.to_datetime(strict=False, time_zone='UTC'),
            pl.col("price").cast(pl.Float64),
            pl.col("size").cast(pl.Int64),
        ]
    )

    print("\nSchema:")
    print(typed.schema)

    print("\nFirst 10 rows:")
    print(typed.head(10))

    print("\nRow count:")
    print(typed.height)

    print("\nMarket data type counts:")
    print(typed.group_by("market_data_type").len().sort("market_data_type"))

    print("\nTime range:")
    print(
        typed.select(
            [
                pl.col("ts_utc").min().alias("min_ts_utc"),
                pl.col("ts_utc").max().alias("max_ts_utc"),
            ]
        )
    )


if __name__ == "__main__":
    main()