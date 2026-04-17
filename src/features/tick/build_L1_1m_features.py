from pathlib import Path
import sys

import polars as pl


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: uv run python src/features/tick/build_L1_1m_features.py "
            "<input-minute-summary-parquet> <output-parquet>"
        )

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    pl.Config.set_tbl_width_chars(220)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(10)
    pl.Config.set_fmt_str_lengths(40)

    df = pl.read_parquet(input_path).sort("minute_start_utc")

    features = (
        df.with_columns(
            [
                (pl.col("last_trade_price") - pl.col("first_trade_price")).alias("delta_1m"),
                (pl.col("max_trade_price") - pl.col("min_trade_price")).alias("trade_range_1m"),
                (pl.col("trade_count") / pl.col("event_count")).alias("trade_event_share"),
                (pl.col("trade_size_total") / pl.col("trade_count"))
                .cast(pl.Float64)
                .alias("avg_trade_size"),
                (
                    (pl.col("ending_spread") / pl.col("ending_midprice")) * 10000
                ).alias("ending_spread_bps"),
            ]
        )
        .with_columns(
            [
                pl.col("last_trade_price").shift(1).alias("prev_last_trade_price"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.col("last_trade_price") / pl.col("prev_last_trade_price")) - 1
                ).alias("return_1m"),
                (
                    pl.col("last_trade_price").shift(-1) / pl.col("last_trade_price") - 1
                ).alias("forward_return_1m"),
            ]
        )
        .select(
            [
                "minute_start_utc",
                "minute_start_central",
                "trade_date_central",
                "session_name",
                "instrument",
                "instrument_code",
                "event_count",
                "trade_count",
                "trade_size_total",
                "first_trade_price",
                "last_trade_price",
                "min_trade_price",
                "max_trade_price",
                "last_bid_price",
                "last_ask_price",
                "ending_spread",
                "ending_midprice",
                "delta_1m",
                "trade_range_1m",
                "trade_event_share",
                "avg_trade_size",
                "ending_spread_bps",
                "return_1m",
                "forward_return_1m",
            ]
        )
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    features.write_parquet(output_path)

    print(f"Wrote features: {output_path}")
    print(f"Rows: {features.height}")
    print(
        features.select(
            [
                "minute_start_central",
                "session_name",
                "trade_count",
                "last_trade_price",
                "ending_spread",
                "ending_spread_bps",
                "return_1m",
                "forward_return_1m",
            ]
        ).head(10)
    )


if __name__ == "__main__":
    main()