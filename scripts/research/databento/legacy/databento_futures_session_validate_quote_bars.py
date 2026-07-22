from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt


def symbol_to_slug(symbol: str) -> str:
    s = symbol.lower()
    s = s.replace(".v.", "_v")
    s = s.replace(".", "_")
    s = s.replace("-", "_")
    s = s.replace("/", "_")
    return s


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Databento MBP-1 session 1-minute quote bars."
    )
    parser.add_argument("--symbol", default="ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--schema", default="mbp-1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbol_slug = symbol_to_slug(args.symbol)

    src_path = Path(
        f"data/processed/databento/quote_bars_1m/{symbol_slug}/"
        f"session_date={args.session_date}/"
        f"{symbol_slug}_{args.session_date}_{args.schema}_quote_1m.parquet"
    )

    out_dir = Path(
        f"data/research/databento_validation/{symbol_slug}/"
        f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    con = duckdb.connect()

    print(f"Reading: {src_path}")
    print(f"Writing outputs to: {out_dir}")

    base_scan = f"parquet_scan('{src_path}')"

    def save_query(name: str, sql: str):
        df = con.execute(sql).fetchdf()
        out_path = out_dir / f"{name}.parquet"
        df.to_parquet(out_path, index=False)

        print()
        print(f"=== {name} ===")
        print(df)
        print(f"Saved: {out_path}")

        return df

    summary_df = con.execute(
        f"""
        SELECT
            COUNT(*) AS bars,
            MIN(minute) AS min_minute,
            MAX(minute) AS max_minute,

            MIN(mid_low) AS session_mid_low,
            MAX(mid_high) AS session_mid_high,
            FIRST(mid_open ORDER BY minute) AS session_mid_open,
            LAST(mid_close ORDER BY minute) AS session_mid_close,
            LAST(mid_close ORDER BY minute) - FIRST(mid_open ORDER BY minute) AS session_mid_change,

            MIN(event_count) AS min_events,
            QUANTILE_CONT(event_count, 0.50) AS median_events,
            QUANTILE_CONT(event_count, 0.90) AS p90_events,
            MAX(event_count) AS max_events,

            SUM(trade_events) AS total_trade_events,
            SUM(trade_size_sum) AS total_trade_size,

            MIN(spread_ticks_avg) AS min_avg_spread_ticks,
            QUANTILE_CONT(spread_ticks_avg, 0.50) AS median_avg_spread_ticks,
            QUANTILE_CONT(spread_ticks_avg, 0.99) AS p99_avg_spread_ticks,
            MAX(spread_ticks_max) AS max_spread_ticks_seen,

            MIN(imbalance_avg) AS min_imbalance_avg,
            QUANTILE_CONT(imbalance_avg, 0.50) AS median_imbalance_avg,
            MAX(imbalance_avg) AS max_imbalance_avg
        FROM {base_scan}
        """
    ).fetchdf()

    print()
    print("=== summary ===")
    print(summary_df)

    summary_path = out_dir / "summary.parquet"
    summary_df.to_parquet(summary_path, index=False)
    print(f"Saved: {summary_path}")

    save_query(
        "widest_spread_minutes",
        f"""
        SELECT
            minute,
            event_count,
            trade_events,
            trade_size_sum,
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
        ORDER BY spread_ticks_max DESC, spread_ticks_avg DESC, event_count DESC
        LIMIT 50
        """,
    )

    save_query(
        "highest_event_minutes",
        f"""
        SELECT
            minute,
            event_count,
            trade_events,
            trade_size_sum,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg
        FROM {base_scan}
        ORDER BY event_count DESC
        LIMIT 50
        """,
    )

    save_query(
        "highest_trade_size_minutes",
        f"""
        SELECT
            minute,
            event_count,
            trade_events,
            trade_size_sum,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg
        FROM {base_scan}
        ORDER BY trade_size_sum DESC
        LIMIT 50
        """,
    )

    save_query(
        "largest_mid_moves",
        f"""
        SELECT
            minute,
            event_count,
            trade_events,
            trade_size_sum,
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
            event_count,
            trade_events,
            trade_size_sum,
            mid_open,
            mid_high,
            mid_low,
            mid_close,
            mid_high - mid_low AS mid_range,
            spread_ticks_avg,
            spread_ticks_max,
            imbalance_avg
        FROM {base_scan}
        ORDER BY mid_range DESC
        LIMIT 50
        """,
    )

    save_query(
        "us_cash_open_sample",
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
        "globex_open_sample",
        f"""
        SELECT *
        FROM {base_scan}
        WHERE CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) >= TIME '17:00:00'
          AND CAST(minute AT TIME ZONE 'America/Chicago' AS TIME) <  TIME '17:15:00'
        ORDER BY minute
        """,
    )

    bars = con.execute(
        f"""
        SELECT
            minute,
            mid_close,
            event_count,
            spread_ticks_avg,
            spread_ticks_max,
            trade_size_sum,
            imbalance_avg
        FROM {base_scan}
        ORDER BY minute
        """
    ).fetchdf()

    def save_line_chart(y_col: str, title: str, ylabel: str, filename: str) -> None:
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(bars["minute"], bars[y_col])
        ax.set_title(title)
        ax.set_xlabel("Minute")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()

        out_path = out_dir / filename
        fig.savefig(out_path, dpi=140)
        plt.close(fig)

        print(f"Saved chart: {out_path}")

    save_line_chart("mid_close", f"{args.symbol} 1m Mid Close", "Mid price", "mid_close_chart.png")
    save_line_chart("event_count", f"{args.symbol} 1m Event Count", "Events", "event_count_chart.png")
    save_line_chart("spread_ticks_avg", f"{args.symbol} 1m Average Spread", "Spread ticks", "spread_avg_chart.png")
    save_line_chart("spread_ticks_max", f"{args.symbol} 1m Max Spread", "Spread ticks", "spread_max_chart.png")
    save_line_chart("trade_size_sum", f"{args.symbol} 1m Trade Size", "Contracts", "trade_size_chart.png")
    save_line_chart("imbalance_avg", f"{args.symbol} 1m Average Imbalance", "Bid size / total size", "imbalance_chart.png")

    print()
    print("Done.")
    print(f"Output directory: {out_dir}")


if __name__ == "__main__":
    main()
