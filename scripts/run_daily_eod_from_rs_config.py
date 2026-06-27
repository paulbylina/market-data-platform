import argparse
import json
from pathlib import Path

from src.pipelines.market.daily_eod_pipeline import run_daily_eod_pipeline


DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target",
        choices=["stock", "benchmark", "all"],
        help="Which RS scanner symbol to download.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to RS scanner config JSON.",
    )

    args = parser.parse_args()
    config = load_config(args.config)

    if args.target == "stock":
        symbols = [config["stock_symbol"]]
    elif args.target == "benchmark":
        symbols = [config["benchmark_symbol"]]
    else:
        symbols = [config["stock_symbol"], config["benchmark_symbol"]]

    for symbol in symbols:
        print(
            f"Running daily EOD pipeline for {symbol} "
            f"{config['start_date']} to {config['end_date']}..."
        )

        run_daily_eod_pipeline(
            symbol=symbol,
            start_date=config["start_date"],
            end_date=config["end_date"],
        )


if __name__ == "__main__":
    main()