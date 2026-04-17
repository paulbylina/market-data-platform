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
            "Usage: uv run python src/analytics/tick/test_L1_candidate_signals_1s.py <input-1s-summary-parquet>"
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
        .sort("second_start_utc")
        .with_columns(
            [
                (pl.col("trade_count") / pl.col("event_count")).alias("trade_share"),
                (pl.when((pl.col("bid_count") + pl.col("ask_count")) > 0)
                .then(
                    (pl.col("bid_count") - pl.col("ask_count"))
                    / (pl.col("bid_count") + pl.col("ask_count"))
                )
                .otherwise(None)
                .alias("quote_balance")),
                (pl.col("ending_spread") / pl.col("ending_midprice") * 10000).alias("spread_bps"),
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
                pl.col("trade_count").rolling_mean(window_size=10).alias("trade_count_ma_10"),
                pl.col("trade_size_total").rolling_mean(window_size=10).alias("trade_size_ma_10"),
                pl.col("spread_bps").rolling_mean(window_size=10).alias("spread_bps_ma_10"),
                pl.col("ending_midprice").shift(1).alias("prev_midprice"),
                pl.col("ending_midprice").shift(-1).alias("next_midprice"),
            ]
        )
        .with_columns(
            [
                (pl.col("trade_count") / pl.col("trade_count_ma_10")).alias("trade_count_ratio_10"),
                (pl.col("trade_size_total") / pl.col("trade_size_ma_10")).alias("trade_size_ratio_10"),
                (pl.col("spread_bps") / pl.col("spread_bps_ma_10")).alias("spread_ratio_10"),
                ((pl.col("ending_midprice") / pl.col("prev_midprice")) - 1).alias("mid_return_1s"),
                ((pl.col("next_midprice") / pl.col("ending_midprice")) - 1).alias("forward_return_1s"),
            ]
        )
        .filter(
            pl.col("forward_return_1s").is_not_null()
            & pl.col("event_count").is_not_null()
            & (pl.col("event_count") > 0)
        )
    )

    candidate_features = [
        "spread_bps",
        "trade_share",
        "quote_balance",
        "trade_range_bps",
        "avg_trade_size",
        "trade_count_ratio_10",
        "trade_size_ratio_10",
        "spread_ratio_10",
        "mid_return_1s",
    ]

    rows: list[dict[str, float | str | int | None]] = []

    for feature in candidate_features:
        subset = df.select(
            [
                "second_start_central",
                "session_name",
                pl.col(feature).alias("feature"),
                "forward_return_1s",
            ]
        ).drop_nulls()

        if subset.height < 50:
            continue

        corr = subset.select(pl.corr("feature", "forward_return_1s")).item()
        abs_corr = subset.select(
            pl.corr("feature", pl.col("forward_return_1s").abs())
        ).item()

        q20 = subset.select(pl.col("feature").quantile(0.2)).item()
        q80 = subset.select(pl.col("feature").quantile(0.8)).item()

        bottom = subset.filter(pl.col("feature") <= q20)
        top = subset.filter(pl.col("feature") >= q80)

        bottom_mean = mean_value(bottom, "forward_return_1s")
        top_mean = mean_value(top, "forward_return_1s")

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

    print("\n=== 1-SECOND CANDIDATE SIGNAL SCREEN ===")
    print(result)


if __name__ == "__main__":
    main()