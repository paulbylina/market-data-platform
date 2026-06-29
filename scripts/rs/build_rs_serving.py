import argparse
import json
from pathlib import Path

import pandas as pd

from src.features.stocks.rs_features import build_rs_features


DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def get_stock_symbols(config: dict) -> list[str]:
    if "stock_symbols" in config:
        return config["stock_symbols"]

    if "stock_symbol" in config:
        return [config["stock_symbol"]]

    raise KeyError("Config must contain either 'stock_symbols' or 'stock_symbol'.")


def build_curated_dir(config: dict) -> Path:
    return (
        Path(config["data_root"])
        / "curated"
        / "market"
        / config["timeframe"]
    )


def build_rs_serving_output_path(
    stock_symbol: str,
    benchmark_symbol: str,
    config: dict,
) -> Path:
    output_dir = (
        Path(config["data_root"])
        / "serving"
        / "scanners"
        / "rs"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    filename = (
        f"{stock_symbol}_vs_{benchmark_symbol}_"
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


def build_serving_scan(
    stock_symbol: str,
    benchmark_symbol: str,
    stock_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    scan = build_rs_features(
        stock_df=stock_df,
        benchmark_df=benchmark_df,
        ticker=stock_symbol,
        benchmark=benchmark_symbol,
        date_col="date",
        price_col="close",
        rs_period=config["rs_period"],
    )

    valid_scan = scan.dropna(subset=["rs_vs_benchmark", "close_zscore_50d"])

    serving_scan = valid_scan.copy()

    serving_scan["stock_return"] = serving_scan["stock_return"] * 100
    serving_scan["benchmark_return"] = serving_scan["benchmark_return"] * 100
    serving_scan["rs_vs_benchmark"] = serving_scan["rs_vs_benchmark"] * 100

    serving_scan = serving_scan.rename(
        columns={
            "stock_return": "stock_return_pct",
            "benchmark_return": "benchmark_return_pct",
            "rs_vs_benchmark": "rs_vs_benchmark_pct",
        }
    )

    return serving_scan


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

    stock_symbols = get_stock_symbols(config)
    benchmark_symbol = config["benchmark_symbol"]

    benchmark_df = load_curated_bars(benchmark_symbol, config)

    saved_paths = []

    for stock_symbol in stock_symbols:
        if stock_symbol == benchmark_symbol:
            print(f"Skipping {stock_symbol}: same as benchmark.")
            continue

        print(f"\n=== Building {stock_symbol} vs {benchmark_symbol} ===")

        stock_df = load_curated_bars(stock_symbol, config)

        serving_scan = build_serving_scan(
            stock_symbol=stock_symbol,
            benchmark_symbol=benchmark_symbol,
            stock_df=stock_df,
            benchmark_df=benchmark_df,
            config=config,
        )

        output_path = build_rs_serving_output_path(
            stock_symbol=stock_symbol,
            benchmark_symbol=benchmark_symbol,
            config=config,
        )

        serving_scan.to_parquet(output_path, index=False)
        saved_paths.append(output_path)

        display_output = format_display_output(
            scan=serving_scan,
            display_rows=config["display_rows"],
        )

        print(display_output.to_string(index=False))
        print(f"\nSaved RS scanner output to: {output_path}")

    print("\n=== Saved RS scanner files ===")
    for path in saved_paths:
        print(path)


if __name__ == "__main__":
    main()