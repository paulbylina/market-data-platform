from pathlib import Path
import sys

import polars as pl

pl.Config.set_tbl_width_chars(260)
pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(10)
pl.Config.set_fmt_str_lengths(40)

REQUIRED_COLUMNS = {
    "ts_utc",
    "instrument",
    "market_data_type",
    "price",
    "size",
    "instrument_code",
}


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: uv run python src/features/tick/build_L1_1m_trade_quote_summary.py "
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
                pl.len().alias("event_count"),
                (pl.col("market_data_type") == "Bid").sum().alias("bid_count"),
                (pl.col("market_data_type") == "Ask").sum().alias("ask_count"),
                (pl.col("market_data_type") == "Last").sum().alias("trade_count"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Last")
                .first()
                .alias("first_trade_price"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Last")
                .last()
                .alias("last_trade_price"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Last")
                .min()
                .alias("min_trade_price"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Last")
                .max()
                .alias("max_trade_price"),
                pl.col("size")
                .filter(pl.col("market_data_type") == "Last")
                .sum()
                .alias("trade_size_total"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Bid")
                .last()
                .alias("last_bid_price"),
                pl.col("price")
                .filter(pl.col("market_data_type") == "Ask")
                .last()
                .alias("last_ask_price"),
            ]
        )
        .rename({"ts_utc": "minute_start_utc"})
        .with_columns(
            [
                pl.col("minute_start_utc")
                .dt.convert_time_zone("America/Chicago")
                .alias("minute_start_central"),
            ]
        )
        .with_columns(
            [
                pl.col("minute_start_central").dt.date().alias("trade_date_central"),
                pl.col("minute_start_central").dt.hour().alias("hour_central"),
                pl.col("minute_start_central").dt.minute().alias("minute_central"),
                (pl.col("last_ask_price") - pl.col("last_bid_price")).alias(
                    "ending_spread"
                ),
                ((pl.col("last_ask_price") + pl.col("last_bid_price")) / 2).alias(
                    "ending_midprice"
                ),
            ]
        )
        .with_columns(
            [
                (
                    (
                        (pl.col("hour_central") > 8)
                        | (
                            (pl.col("hour_central") == 8)
                            & (pl.col("minute_central") >= 30)
                        )
                    )
                    & (
                        (pl.col("hour_central") < 15)
                        | (
                            (pl.col("hour_central") == 15)
                            & (pl.col("minute_central") == 0)
                        )
                    )
                ).alias("is_rth"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("is_rth"))
                .then(pl.lit("RTH"))
                .otherwise(pl.lit("ETH"))
                .alias("session_name")
            ]
        )
        .select(
            [
                "minute_start_utc",
                "minute_start_central",
                "trade_date_central",
                "instrument",
                "instrument_code",
                "event_count",
                "bid_count",
                "ask_count",
                "trade_count",
                "first_trade_price",
                "last_trade_price",
                "min_trade_price",
                "max_trade_price",
                "trade_size_total",
                "last_bid_price",
                "last_ask_price",
                "ending_spread",
                "ending_midprice",
                "hour_central",
                "minute_central",
                "is_rth",
                "session_name",
            ]
        )
        .sort("minute_start_utc")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.write_parquet(output_path)

    print(f"Wrote 1-minute trade/quote summary parquet: {output_path}")
    print(f"Rows: {summary.height}")
    print("\nFirst 10 rows:")
    print(
        summary.select(
            [
                "minute_start_central",
                "session_name",
                "event_count",
                "bid_count",
                "ask_count",
                "trade_count",
                "trade_size_total",
                "last_bid_price",
                "last_ask_price",
                "ending_spread",
                "ending_midprice",
            ]
        ).head(10)
    )
    print(summary.schema)


if __name__ == "__main__":
    main()