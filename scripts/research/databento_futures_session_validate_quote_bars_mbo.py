from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt


DEFAULT_DATASET = "GLBX.MDP3"
INPUT_NAME = "mbo_quote_1m_session"


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate sessionized Databento futures MBO 1-minute quote bars."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)

    parser.add_argument(
        "--input-root",
        default="data/processed/databento/mbo_quote_bars_1m_sessions",
        help="Input sessionized MBO quote bar root.",
    )
    parser.add_argument(
        "--output-root",
        default="data/research/databento_mbo_validation",
        help="Validation output root.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)

    src_path = (
        Path(args.input_root)
        / dataset_slug
        / symbol_slug
        / f"session_date={args.session_date}"
        / f"{symbol_slug}_{args.session_date}_{INPUT_NAME}.parquet"
    )

    out_dir = (
        Path(args.output_root)
        / dataset_slug
        / symbol_slug
        / f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    con = duckdb.connect()
    base_scan = f"parquet_scan('{src_path}')"

    print("Validating MBO sessionized 1-minute quote bars:")
    print(f"  dataset:      {args.dataset}")
    print(f"  symbol:       {args.symbol}")
    print(f"  session_date: {args.session_date}")
    print(f"  input:        {src_path}")
    print(f"  output_dir:   {out_dir}")

    def save_query(name: str, sql: str):
        df = con.execute(sql).fetchdf()

        parquet_path = out_dir / f"{name}.parquet"
        csv_path = out_dir / f"{name}.csv"

        df.to_parquet(parquet_path, index=False)
        df.to_csv(csv_path, index=False)

        print()
        print(f"=== {name} ===")
        print(df.to_string(index=False))
        print(f"Saved parquet: {parquet_path}")
        print(f"Saved csv:     {csv_path}")

        return df

    summary_df = save_query(
        "summary",
        f"""
        SELECT
            COUNT(*) AS bars,
            MIN(minute) AS min_minute,
            MAX(minute) AS max_minute,

            MIN(session_date) AS session_date,
            MIN(session_start) AS session_start,
            MAX(session_end) AS session_end,

            FIRST(mid_open ORDER BY minute) AS session_mid_open,
            MAX(mid_high) AS session_mid_high,
            MIN(mid_low) AS session_mid_low,
            LAST(mid_close ORDER BY minute) AS session_mid_close,
            LAST(mid_close ORDER BY minute) - FIRST(mid_open ORDER BY minute) AS session_mid_change,

            MIN(bbo_update_count) AS min_bbo_updates,
            QUANTILE_CONT(bbo_update_count, 0.50) AS median_bbo_updates,
            QUANTILE_CONT(bbo_update_count, 0.90) AS p90_bbo_updates,
            MAX(bbo_update_count) AS max_bbo_updates,
            SUM(bbo_update_count) AS total_bbo_updates,

            SUM(source_add_rows) AS source_add_rows,
            SUM(source_cancel_rows) AS source_cancel_rows,
            SUM(source_modify_rows) AS source_modify_rows,
            SUM(source_trade_rows) AS source_trade_rows,
            SUM(source_fill_rows) AS source_fill_rows,
            SUM(snapshot_ready_rows) AS snapshot_ready_rows,

            MIN(spread_ticks_avg) AS min_avg_spread_ticks,
            QUANTILE_CONT(spread_ticks_avg, 0.50) AS median_avg_spread_ticks,
            QUANTILE_CONT(spread_ticks_avg, 0.99) AS p99_avg_spread_ticks,
            MAX(spread_ticks_max) AS max_spread_ticks_seen,

            MIN(imbalance_avg) AS min_imbalance_avg,
            QUANTILE_CONT(imbalance_avg, 0.50) AS median_imbalance_avg,
            MAX(imbalance_avg) AS max_imbalance_avg,

            SUM(CASE WHEN spread_ticks_max < 1 THEN 1 ELSE 0 END) AS bars_bad_low_spread,
            SUM(CASE WHEN spread_ticks_max > 20 THEN 1 ELSE 0 END) AS bars_spread_gt_20
        FROM {base_scan}
        """,
    )

    save_query(
        "widest_spread_minutes",
        f"""
        SELECT
            minute,
            bbo_update_count,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_median,
            spread_ticks_max,
            imbalance_avg,
            bid_sz_median,
            ask_sz_median
        FROM {base_scan}
        ORDER BY spread_ticks_max DESC, spread_ticks_avg DESC, bbo_update_count DESC
        LIMIT 50
        """,
    )

    save_query(
        "highest_bbo_update_minutes",
        f"""
        SELECT
            minute,
            bbo_update_count,
            source_add_rows,
            source_cancel_rows,
            source_modify_rows,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg
        FROM {base_scan}
        ORDER BY bbo_update_count DESC
        LIMIT 50
        """,
    )

    save_query(
        "largest_mid_moves",
        f"""
        SELECT
            minute,
            bbo_update_count,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_close - mid_open AS mid_change,
            ABS(mid_close - mid_open) AS abs_mid_change,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg
        FROM {base_scan}
        ORDER BY abs_mid_change DESC
        LIMIT 50
        """,
    )

    save_query(
        "largest_mid_ranges",
        f"""
        SELECT
            minute,
            bbo_update_count,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg,
            bid_sz_avg,
            ask_sz_avg
        FROM {base_scan}
        ORDER BY mid_range DESC
        LIMIT 50
        """,
    )

    save_query(
        "cash_open_sample",
        f"""
        SELECT *
        FROM {base_scan}
        WHERE CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) >= TIME '08:25:00'
          AND CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) <  TIME '08:45:00'
        ORDER BY minute
        """,
    )

    save_query(
        "news_window_0730_sample",
        f"""
        SELECT *
        FROM {base_scan}
        WHERE CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) >= TIME '07:25:00'
          AND CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) <  TIME '07:35:00'
        ORDER BY minute
        """,
    )

    save_query(
        "session_open_sample",
        f"""
        SELECT *
        FROM {base_scan}
        WHERE minute >= (
            SELECT MIN(minute)
            FROM {base_scan}
        )
        ORDER BY minute
        LIMIT 30
        """,
    )

    chart_df = con.execute(
        f"""
        SELECT
            minute,
            mid_close,
            spread_ticks_avg,
            bbo_update_count
        FROM {base_scan}
        ORDER BY minute
        """
    ).fetchdf()

    if not chart_df.empty:
        chart_path = out_dir / "mid_close_chart.png"

        plt.figure(figsize=(14, 6))
        plt.plot(chart_df["minute"], chart_df["mid_close"])
        plt.title(f"{args.symbol} {args.session_date} MBO Quote 1m Mid Close")
        plt.xlabel("Minute")
        plt.ylabel("Mid Close")
        plt.xticks(rotation=30)
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        print()
        print(f"Saved chart: {chart_path}")

    print()
    print("Done.")
    print(f"Summary rows: {len(summary_df)}")
    print(f"Outputs saved to: {out_dir}")


if __name__ == "__main__":
    main()