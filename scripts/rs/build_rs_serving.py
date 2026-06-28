import argparse
import json
from pathlib import Path

import pandas as pd

from src.features.stocks.rs_features import build_rs_features


DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_curated_dir(config: dict) -> Path:
    return (
        Path(config["data_root"])
        / "curated"
        / "market"
        / config["timeframe"]
    )


def build_rs_serving_output_path(config: dict) -> Path:
    output_dir = (
        Path(config["data_root"])
        / "serving"
        / "scanners"
        / "rs"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    filename = (
        f"{config['stock_symbol']}_vs_{config['benchmark_symbol']}_"
        f"{config['start_date']}_{config['end_date']}_rs_scan.parquet"
    )

    return output_dir / filename


def load_curated_bars(symbol: str, config: dict) -> pd.DataFrame:
    curated_dir = build_curated_dir(config)

    path = (
        curated_dir
        / f"{symbol}_{config['start_date']}_{config['end_date']}_curated.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(f"Missing curated file: {path}")

    return pd.read_parquet(path)


def format_display_output(scan: pd.DataFrame, display_rows: int) -> pd.DataFrame:
    output = (
        scan
        .tail(display_rows)
        .sort_values("date", ascending=False)
        .reset_index(drop=True)
        .copy()
    )

    output["stock_return_pct"] = output["stock_return_pct"].round(2)
    output["benchmark_return_pct"] = output["benchmark_return_pct"].round(2)
    output["rs_vs_benchmark_pct"] = output["rs_vs_benchmark_pct"].round(2)
    output["close"] = output["close"].round(2)
    output["close_zscore_50d"] = output["close_zscore_50d"].round(2)
    output["close_zscore_200d"] = output["close_zscore_200d"].round(2)

    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to RS scanner config JSON.",
    )

    args = parser.parse_args()
    config = load_config(args.config)

    stock_df = load_curated_bars(config["stock_symbol"], config)
    benchmark_df = load_curated_bars(config["benchmark_symbol"], config)

    scan = build_rs_features(
        stock_df=stock_df,
        benchmark_df=benchmark_df,
        ticker=config["stock_symbol"],
        benchmark=config["benchmark_symbol"],
        date_col="date",
        price_col="close",
        rs_period=config["rs_period"],
    )

    valid_scan = scan.dropna(subset=["rs_vs_benchmark", "close_zscore_50d"])

    serving_scan = valid_scan.copy()

    serving_scan["stock_return"] = (serving_scan["stock_return"] * 100).round(2)
    serving_scan["benchmark_return"] = (serving_scan["benchmark_return"] * 100).round(2)
    serving_scan["rs_vs_benchmark"] = (serving_scan["rs_vs_benchmark"] * 100).round(2)

    serving_scan = serving_scan.rename(
        columns={
            "stock_return": "stock_return_pct",
            "benchmark_return": "benchmark_return_pct",
            "rs_vs_benchmark": "rs_vs_benchmark_pct",
        }
    )

    output_path = build_rs_serving_output_path(config)
    serving_scan.to_parquet(output_path, index=False)

    display_output = format_display_output(
        scan=serving_scan,
        display_rows=config["display_rows"],
    )

    print(display_output.to_string(index=False))
    print(f"\nSaved RS scanner output to: {output_path}")


if __name__ == "__main__":
    main()