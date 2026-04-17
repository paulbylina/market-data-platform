from pathlib import Path
import sys

import polars as pl


def mean_value(df: pl.DataFrame, column: str) -> float | None:
    if df.height == 0:
        return None
    return df.select(pl.col(column).mean()).item()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: uv run python src/analytics/tick/test_L1_candidate_signals.py <input-minute-summary-parquet>"
        )

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    pl.Config.set_tbl_width_chars(220)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(20)
    pl.Config.set_fmt_str_lengths(40)

    df = (
        pl.read_parquet(input_path)
        .sort("minute_start_utc")
        .with_columns(
            [
                ((pl.col("last_ask_price") + pl.col("last_bid_price")) / 2).alias("midprice"),
                (pl.col("ending_spread") / pl.col("ending_midprice") * 10000).alias("spread_bps"),
                (pl.col("trade_count") / pl.col("event_count")).alias("trade_share"),
                (
                    (pl.col("bid_count") - pl.col("ask_count"))
                    / (pl.col("bid_count") + pl.col("ask_count"))
                ).alias("quote_balance"),
                (
                    (pl.col("last_trade_price") - pl.col("first_trade_price"))
                    / pl.col("first_trade_price")
                ).alias("return_1m"),
                (
                    (pl.col("max_trade_price") - pl.col("min_trade_price"))
                    / pl.col("last_trade_price")
                    * 10000
                ).alias("trade_range_bps"),
                (pl.col("trade_size_total") / pl.col("trade_count")).alias("avg_trade_size"),
            ]
        )
        .with_columns(
            [
                pl.col("trade_count").rolling_mean(window_size=5).alias("trade_count_ma_5"),
                pl.col("trade_size_total").rolling_mean(window_size=5).alias("trade_size_ma_5"),
                pl.col("spread_bps").rolling_mean(window_size=5).alias("spread_bps_ma_5"),
                pl.col("midprice").shift(1).alias("prev_midprice"),
                pl.col("midprice").shift(-1).alias("next_midprice"),
            ]
        )
        .with_columns(
            [
                (pl.col("trade_count") / pl.col("trade_count_ma_5")).alias("trade_count_ratio_5"),
                (pl.col("trade_size_total") / pl.col("trade_size_ma_5")).alias("trade_size_ratio_5"),
                (pl.col("spread_bps") / pl.col("spread_bps_ma_5")).alias("spread_ratio_5"),
                ((pl.col("midprice") / pl.col("prev_midprice")) - 1).alias("mid_return_1m"),
                ((pl.col("next_midprice") / pl.col("midprice")) - 1).alias("forward_return_1m"),
            ]
        )
    )

    candidate_features = [
        "spread_bps",
        "trade_share",
        "bid_count",
        "ask_count",
        "quote_balance",
        "return_1m",
        "trade_range_bps",
        "avg_trade_size",
        "trade_count_ratio_5",
        "trade_size_ratio_5",
        "spread_ratio_5",
        "mid_return_1m",
    ]

    rows: list[dict[str, float | str | int | None]] = []

    for feature in candidate_features:
        subset = df.select(
            [
                "minute_start_central",
                "session_name",
                pl.col(feature).alias("feature"),
                "forward_return_1m",
            ]
        ).drop_nulls()

        if subset.height < 20:
            continue

        corr = subset.select(pl.corr("feature", "forward_return_1m")).item()
        abs_corr = subset.select(
            pl.corr("feature", pl.col("forward_return_1m").abs())
        ).item()

        q20 = subset.select(pl.col("feature").quantile(0.2)).item()
        q80 = subset.select(pl.col("feature").quantile(0.8)).item()

        bottom = subset.filter(pl.col("feature") <= q20)
        top = subset.filter(pl.col("feature") >= q80)

        bottom_mean = mean_value(bottom, "forward_return_1m")
        top_mean = mean_value(top, "forward_return_1m")

        rows.append(
            {
                "feature": feature,
                "rows": subset.height,
                "corr_to_next_return": corr,
                "corr_to_abs_next_return": abs_corr,
                "bottom_20pct_avg_next_return": bottom_mean,
                "top_20pct_avg_next_return": top_mean,
                "top_minus_bottom": (
                    None
                    if top_mean is None or bottom_mean is None
                    else top_mean - bottom_mean
                ),
            }
        )

    result = pl.DataFrame(rows).sort(
        "top_minus_bottom",
        descending=True,
        nulls_last=True,
    )

    print("\n=== CANDIDATE SIGNAL SCREEN ===")
    print(result)

    strongest = result.head(3).get_column("feature").to_list()

    for feature in strongest:
        subset = (
            df.select(
                [
                    "minute_start_central",
                    "session_name",
                    pl.col(feature).alias(feature),
                    "forward_return_1m",
                ]
            )
            .drop_nulls()
            .sort(feature, descending=True)
        )

        print(f"\n=== TOP 10 ROWS FOR {feature} ===")
        print(subset.head(10))


if __name__ == "__main__":
    main()