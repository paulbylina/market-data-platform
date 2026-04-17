from pathlib import Path
import sys

import polars as pl


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: uv run python src/validation/tick/L1_quality_report.py <path-to-parquet>"
        )

    path = Path(sys.argv[1])
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    pl.Config.set_tbl_rows(50)
    pl.Config.set_tbl_cols(20)

    df = (
        pl.read_parquet(path)
        .sort("ts_utc")
        .with_columns(
            pl.col("ts_utc")
            .dt.convert_time_zone("America/Chicago")
            .alias("ts_central")
        )
        .with_columns(pl.col("ts_central").dt.hour().alias("hour_central"))
    )

    duplicate_groups = (
        df.group_by(["ts_utc", "instrument", "market_data_type", "price", "size"])
        .len()
        .filter(pl.col("len") > 1)
        .sort("len", descending=True)
    )

    gaps = (
        df.select(
            [
                pl.col("ts_utc"),
                pl.col("ts_central"),
                pl.col("market_data_type"),
                pl.col("ts_utc").diff().alias("gap"),
            ]
        )
        .sort("gap", descending=True, nulls_last=True)
    )

    large_gaps = gaps.filter(pl.col("gap") > pl.duration(seconds=5))

    counts_by_type = df.group_by("market_data_type").len().sort("market_data_type")

    counts_by_hour_central = df.group_by("hour_central").len().sort("hour_central")

    summary = df.select(
        [
            pl.len().alias("row_count"),
            pl.col("ts_utc").min().alias("min_ts_utc"),
            pl.col("ts_utc").max().alias("max_ts_utc"),
            pl.col("ts_central").min().alias("min_ts_central"),
            pl.col("ts_central").max().alias("max_ts_central"),
            pl.col("ts_utc").is_null().sum().alias("ts_utc_nulls"),
            pl.col("instrument").is_null().sum().alias("instrument_nulls"),
            pl.col("market_data_type").is_null().sum().alias("market_data_type_nulls"),
            pl.col("price").is_null().sum().alias("price_nulls"),
            pl.col("size").is_null().sum().alias("size_nulls"),
        ]
    )

    print("\n=== SUMMARY ===")
    print(summary)

    print("\n=== COUNTS BY MARKET_DATA_TYPE ===")
    print(counts_by_type)

    print("\n=== COUNTS BY CENTRAL HOUR ===")
    print(counts_by_hour_central)

    print("\n=== DUPLICATE EVENT GROUPS (>1 identical row) ===")
    print(duplicate_groups.head(20))
    print(f"duplicate_group_count={duplicate_groups.height}")

    print("\n=== TOP 20 LARGEST GAPS ===")
    print(gaps.head(20))

    print("\n=== GAPS OVER 5 SECONDS ===")
    print(f"gap_count_over_5s={large_gaps.height}")


if __name__ == "__main__":
    main()