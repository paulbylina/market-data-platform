from pathlib import Path
import time

from src.ingestion.massive.fetch_bars import fetch_bars
from src.storage.write_data import write_raw_json
from src.utils.load_symbols import load_symbols


def safe_symbol_for_path(symbol: str) -> str:
    return symbol.replace(":", "_").replace("/", "_")


def download_raw_forex_bars(
    symbols_file: Path,
    start_date: str,
    end_date: str,
    output_dir: Path = Path("data/raw/massive/forex/1d"),
    timeframe: str = "1d",
    sleep_seconds: int = 15,
    skip_existing: bool = True,
) -> dict:
    symbols = load_symbols(symbols_file)

    successes: list[str] = []
    skipped: list[str] = []
    failures: list[dict] = []

    for index, symbol in enumerate(symbols, start=1):
        safe_symbol = safe_symbol_for_path(symbol)
        output_path = output_dir / f"{safe_symbol}_{start_date}_{end_date}_raw.json"

        if skip_existing and output_path.exists():
            skipped.append(symbol)
            print(f"[{index}/{len(symbols)}] Skipping existing file: {symbol}")
            continue

        print(f"[{index}/{len(symbols)}] Downloading raw forex data: {symbol}")

        try:
            raw = fetch_bars(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            write_raw_json(raw, output_path)
            successes.append(symbol)
            print(f"  saved -> {output_path}")

        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
            print(f"  failed -> {exc}")

        if index < len(symbols):
            print(f"  sleeping {sleep_seconds} seconds to respect API limit...")
            time.sleep(sleep_seconds)

    summary = {
        "symbols_file": str(symbols_file),
        "start_date": start_date,
        "end_date": end_date,
        "success_count": len(successes),
        "skipped_count": len(skipped),
        "failure_count": len(failures),
        "successes": successes,
        "skipped": skipped,
        "failures": failures,
    }

    print()
    print("Raw forex download completed")
    print(f"  Successes: {summary['success_count']}")
    print(f"  Skipped: {summary['skipped_count']}")
    print(f"  Failures: {summary['failure_count']}")

    return summary


if __name__ == "__main__":
    download_raw_forex_bars(
        symbols_file=Path("config/symbols_forex_major.txt"),
        start_date="2025-01-01",
        end_date="2025-01-31",
    )

