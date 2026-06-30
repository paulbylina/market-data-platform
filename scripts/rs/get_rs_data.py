import argparse
import json
from pathlib import Path

from src.pipelines.stocks.curated_daily_bars_pipeline import run_curated_daily_bars_pipeline


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


def get_sector_etf_symbols(config: dict) -> list[str]:
    sector_map = config.get("sector_etf_by_symbol", {})
    return list(sector_map.values())


def dedupe_preserve_order(symbols: list[str]) -> list[str]:
    return list(dict.fromkeys(symbols))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target",
        choices=["stock", "benchmark", "sector", "all"],
        help="Which RS scanner symbol group to download.",
    )
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
    sector_symbols = get_sector_etf_symbols(config)

    if args.target == "stock":
        symbols = stock_symbols
    elif args.target == "benchmark":
        symbols = [benchmark_symbol]
    elif args.target == "sector":
        symbols = sector_symbols
    else:
        symbols = stock_symbols + [benchmark_symbol] + sector_symbols

    symbols = dedupe_preserve_order(symbols)

    total_symbols = len(symbols)

    for index, symbol in enumerate(symbols, start=1):
        print(
            f"[{index}/{total_symbols}] Running daily bar pipeline for {symbol} "
            f"{config['start_date']} to {config['end_date']}..."
        )

        run_curated_daily_bars_pipeline(
            symbol=symbol,
            timeframe=config["timeframe"],
            start_date=config["start_date"],
            end_date=config["end_date"],
        )


if __name__ == "__main__":
    main()
