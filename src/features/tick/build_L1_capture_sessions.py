from pathlib import Path
import sys

import polars as pl

pl.Config.set_tbl_width_chars(150)
pl.Config.set_fmt_str_lengths(40)
pl.Config.set_tbl_cols(30)
pl.Config.set_tbl_rows(20)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: uv run python src/features/tick/build_L1_capture_sessions.py "
            "<input-parquet> <output-parquet>"
        )

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = pl.read_parquet(input_path).sort("minute_start_utc")

    out = (
        df.with_columns(
            pl.col("minute_start_utc").diff().alias("minute_gap")
        )
        .with_columns(
            (
                pl.col("minute_gap").is_null()
                | (pl.col("minute_gap") > pl.duration(minutes=1))
            )
            .cast(pl.Int64)
            .cum_sum()
            .alias("capture_session_id")
        )
    )

    session_summary = (
        out.group_by("capture_session_id")
        .agg(
            [
                pl.col("minute_start_utc").min().alias("session_start_utc"),
                pl.col("minute_start_utc").max().alias("session_end_utc"),
                pl.len().alias("minute_count"),
                pl.col("event_count").sum().alias("event_count_total"),
            ]
        )
        .sort("capture_session_id")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(output_path)

    print("Session summary:")
    print(session_summary)
    print(f"\nWrote: {output_path}")


if __name__ == "__main__":
    main()