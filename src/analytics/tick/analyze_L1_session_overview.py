from pathlib import Path
import sys

import polars as pl


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: uv run python src/analytics/tick/analyze_L1_session_overview.py <input-parquet>"
        )

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    pl.Config.set_tbl_width_chars(220)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(20)
    pl.Config.set_fmt_str_lengths(40)

    df = pl.read_parquet(input_path).sort("minute_start_utc")

    overview = df.select(
        [
            pl.len().alias("minute_rows"),
            pl.col("event_count").sum().alias("total_events"),
            pl.col("trade_count").sum().alias("total_trades"),
            pl.col("bid_count").sum().alias("total_bid_updates"),
            pl.col("ask_count").sum().alias("total_ask_updates"),
            pl.col("trade_size_total").sum().alias("total_trade_size"),
            pl.col("ending_spread").mean().alias("avg_ending_spread"),
            pl.col("ending_spread").median().alias("median_ending_spread"),
            pl.col("ending_spread").max().alias("max_ending_spread"),
            pl.col("minute_start_central").min().alias("start_central"),
            pl.col("minute_start_central").max().alias("end_central"),
        ]
    )

    by_session = (
        df.group_by("session_name")
        .agg(
            [
                pl.len().alias("minute_rows"),
                pl.col("event_count").sum().alias("total_events"),
                pl.col("trade_count").sum().alias("total_trades"),
                pl.col("trade_size_total").sum().alias("total_trade_size"),
                pl.col("ending_spread").mean().alias("avg_ending_spread"),
                pl.col("ending_spread").median().alias("median_ending_spread"),
            ]
        )
        .sort("session_name")
    )

    busiest_minutes = (
        df.select(
            [
                "minute_start_central",
                "session_name",
                "event_count",
                "trade_count",
                "trade_size_total",
                "ending_spread",
                "ending_midprice",
            ]
        )
        .sort("event_count", descending=True)
        .head(10)
    )

    widest_spread_minutes = (
        df.select(
            [
                "minute_start_central",
                "session_name",
                "event_count",
                "trade_count",
                "ending_spread",
                "ending_midprice",
            ]
        )
        .sort("ending_spread", descending=True, nulls_last=True)
        .head(10)
    )

    trade_share = df.select(
        [
            (
                pl.col("trade_count").sum() / pl.col("event_count").sum()
            ).alias("trade_event_share")
        ]
    )

    print("\n=== SESSION OVERVIEW ===")
    print(overview)

    print("\n=== SESSION BREAKDOWN (RTH / ETH) ===")
    print(by_session)

    print("\n=== TRADE SHARE OF ALL EVENTS ===")
    print(trade_share)

    print("\n=== TOP 10 BUSIEST MINUTES ===")
    print(busiest_minutes)

    print("\n=== TOP 10 WIDEST-SPREAD MINUTES ===")
    print(widest_spread_minutes)


if __name__ == "__main__":
    main()