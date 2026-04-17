from pathlib import Path
import sys

import polars as pl


REQUIRED_COLUMNS = {
    "ts_utc",
    "instrument",
    "market_data_type",
    "price",
    "size",
    "instrument_code",
    "trade_date",
}


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: uv run python src/features/tick/build_L1_1m_summary.py "
            "<input-parquet> <output-parquet>"
        )

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = pl.read_parquet(input_path)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    df = df.sort("ts_utc")

    summary = (
        df.group_by_dynamic("ts_utc", every="1m", closed="left", label="left")
        .agg(
            [
                pl.col("instrument").first().alias("instrument"),
                pl.col("instrument_code").first().alias("instrument_code"),
                pl.col("trade_date").first().alias("trade_date"),
                pl.len().alias("event_count"),
                (pl.col("market_data_type") == "Bid").sum().alias("bid_count"),
                (pl.col("market_data_type") == "Ask").sum().alias("ask_count"),
                (pl.col("market_data_type") == "Last").sum().alias("last_count"),
                pl.col("price").first().alias("first_price"),
                pl.col("price").last().alias("last_price"),
                pl.col("price").min().alias("min_price"),
                pl.col("price").max().alias("max_price"),
                pl.when(pl.col("market_data_type") == "Last")
                .then(pl.col("size"))
                .otherwise(0)
                .sum()
                .alias("last_size_total"),
                pl.col("size").sum().alias("all_size_total"),
            ]
        )
        .rename({"ts_utc": "minute_start_utc"})
        .with_columns(
            pl.col("minute_start_utc")
            .dt.convert_time_zone("America/Chicago")
            .alias("minute_start_central")
        )
        .select(
            [
                "minute_start_utc",
                "minute_start_central",
                "trade_date",
                "instrument",
                "instrument_code",
                "event_count",
                "bid_count",
                "ask_count",
                "last_count",
                "first_price",
                "last_price",
                "min_price",
                "max_price",
                "last_size_total",
                "all_size_total",
            ]
        )
        .sort("minute_start_utc")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.write_parquet(output_path)

    print(f"Wrote 1-minute summary parquet: {output_path}")
    print(f"Rows: {summary.height}")
    print("\nFirst 10 rows:")
    print(summary.head(10))
    print("\nSchema:")
    print(summary.schema)


if __name__ == "__main__":
    main()