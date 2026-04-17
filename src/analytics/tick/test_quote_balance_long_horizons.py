from pathlib import Path
import sys

import polars as pl


HORIZONS = [1, 2, 5, 10, 20, 30]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: uv run python src/analytics/tick/test_quote_balance_long_horizons.py <input-1s-summary-parquet>"
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
                pl.col("bid_count").cast(pl.Float64).alias("bid_f"),
                pl.col("ask_count").cast(pl.Float64).alias("ask_f"),
            ]
        )
        .with_columns(
            [
                pl.when((pl.col("bid_f") + pl.col("ask_f")) > 0)
                .then(
                    (pl.col("bid_f") - pl.col("ask_f"))
                    / (pl.col("bid_f") + pl.col("ask_f"))
                )
                .otherwise(None)
                .alias("quote_balance")
            ]
        )
    )

    forward_cols = []
    for h in HORIZONS:
        col_name = f"forward_return_{h}s"
        forward_cols.append(
            (pl.col("ending_midprice").shift(-h) / pl.col("ending_midprice") - 1).alias(
                col_name
            )
        )

    df = df.with_columns(forward_cols).filter(
        (pl.col("session_name") == "RTH")
        & (pl.col("ending_spread") == 0.25)
        & (pl.col("trade_count") > 0)
        & pl.col("quote_balance").is_not_null()
    )

    q10 = df.select(pl.col("quote_balance").quantile(0.10)).item()

    signal_df = df.filter(pl.col("quote_balance") <= q10)

    rows = []
    for h in HORIZONS:
        ret_col = f"forward_return_{h}s"
        subset = signal_df.filter(pl.col(ret_col).is_not_null())

        if subset.height == 0:
            continue

        avg_ret_bps = subset.select((pl.col(ret_col).mean() * 10000)).item()
        median_ret_bps = subset.select(pl.col(ret_col).median() * 10000).item()
        win_rate = subset.select((pl.col(ret_col) > 0).mean()).item()

        rows.append(
            {
                "horizon_seconds": h,
                "rows": subset.height,
                "avg_return_bps": avg_ret_bps,
                "median_return_bps": median_ret_bps,
                "win_rate": win_rate,
            }
        )

    result = pl.DataFrame(rows).sort("horizon_seconds")

    print(f"\nquote_balance bottom 10% threshold = {q10:.6f}")
    print("\n=== LONG-ONLY QB BOTTOM-10% BY HORIZON ===")
    print(result)


if __name__ == "__main__":
    main()