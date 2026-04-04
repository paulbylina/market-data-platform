from pathlib import Path


def load_symbols(symbols_file: Path) -> list[str]:
    with symbols_file.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]