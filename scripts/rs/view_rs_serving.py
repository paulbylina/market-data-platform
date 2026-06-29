import argparse
import json
from pathlib import Path

import pandas as pd


DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_rs_serving_output_path(config: dict) -> Path:
    filename = (
        f"{config['stock_symbol']}_vs_{config['benchmark_symbol']}_"
        f"{config['start_date']}_{config['end_date']}_rs_scan.parquet"
    )

    return (
        Path(config["data_root"])
        / "serving"
        / "scanners"
        / "rs"
        / filename
    )


def format_for_display(df: pd.DataFrame, rows: int) -> pd.DataFrame:
    output = (
        df.sort_values("date", ascending=False)
        .head(rows)
        .reset_index(drop=True)
        .copy()
    )

    output["stock_return_pct"] = output["stock_return_pct"].round(2)
    output["benchmark_return_pct"] = output["benchmark_return_pct"].round(2)
    output["rs_pct"] = output["rs_vs_benchmark_pct"].round(2)

    output["close"] = output["close"].round(2)
    output["close_zscore_50d"] = output["close_zscore_50d"].round(2)
    output["close_zscore_200d"] = output["close_zscore_200d"].round(2)

    output["stock_return_1d"] = output["stock_return_pct"].shift(1)
    output["benchmark_return_1d"] = output["benchmark_return_pct"].shift(1)
    output["rs_pct_1d"] = output["rs_pct"].shift(1)

    columns = [
        "ticker",
        "benchmark",
        "date",
        "close",
        "stock_return_pct",
        "benchmark_return_pct",
        "rs_pct",
        "close_zscore_50d",
        "close_zscore_200d",
        "stock_return_1d",
        "benchmark_return_1d",
        "rs_pct_1d",

    ]

    return output[columns].dropna()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to RS scanner config JSON.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=20,
        help="Number of newest rows to display.",
    )

    args = parser.parse_args()
    config = load_config(args.config)

    path = build_rs_serving_output_path(config)

    if not path.exists():
        raise FileNotFoundError(
            f"Missing RS serving file: {path}\n"
            "Run `make rs-run` first to create it."
        )

    df = pd.read_parquet(path)
    output = format_for_display(df, rows=args.rows)

    print(f"\nRS serving file: {path}\n")
    print(output.info())
    print(output.to_string(index=False))


if __name__ == "__main__":
    main()