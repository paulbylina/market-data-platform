from pathlib import Path
import sys

import matplotlib.pyplot as plt
import polars as pl


DEFAULT_COLUMN = "last_trade_price"
VALID_COLUMNS = {
    "last_trade_price",
    "trade_count",
    "trade_size_total",
    "ending_spread",
    "ending_midprice",
    "event_count",
}


def main() -> None:
    if len(sys.argv) not in {2, 3}:
        raise SystemExit(
            "Usage: uv run python src/analytics/tick/plot_L1_1m_series.py "
            "<input-parquet> [column]"
        )

    input_path = Path(sys.argv[1])
    column = sys.argv[2] if len(sys.argv) == 3 else DEFAULT_COLUMN

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    if column not in VALID_COLUMNS:
        raise SystemExit(
            f"Invalid column: {column}. Valid options: {sorted(VALID_COLUMNS)}"
        )

    df = (
        pl.read_parquet(input_path)
        .sort("minute_start_utc")
        .select(["minute_start_central", "session_name", column])
        .drop_nulls()
    )

    if df.height == 0:
        raise SystemExit("No rows available to plot after dropping nulls.")

    x = df.get_column("minute_start_central").to_list()
    y = df.get_column(column).to_list()

    plt.figure(figsize=(14, 6))
    plt.plot(x, y)
    plt.title(f"{column} over time (1-minute)")
    plt.xlabel("Central time")
    plt.ylabel(column)
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_dir = Path("output/plots")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{input_path.stem}__{column}.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.show()

    print(f"Saved plot: {output_path}")


if __name__ == "__main__":
    main()