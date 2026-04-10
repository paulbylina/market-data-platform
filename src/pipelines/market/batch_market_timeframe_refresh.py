from collections.abc import Sequence
from pathlib import Path

from src.pipelines.market.run_derived_bars_pipeline import run_derived_bars_pipeline
from src.pipelines.market.run_market_bars_pipeline import run_market_bars_pipeline
from src.utils.load_symbols import load_symbols
from src.utils.timeframes import (
    get_derivation_spec,
    list_derived_timeframes,
    list_source_timeframes,
)


def _progress_bar(current: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "[----------------------------] 0/0"
    filled = int(width * current / total)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {current}/{total}"


def run_batch_market_timeframe_refresh(
    symbols_file: Path,
    start_date: str,
    end_date: str,
    source_timeframes: Sequence[str] = ("1d",),
    derived_timeframes: Sequence[str] = ("1w", "1mo"),
) -> dict:
    """
    Run source-native and derived timeframe refreshes for every symbol in a symbols file.

    Behavior:
    - source timeframes run first per symbol
    - derived timeframes run only if their required source timeframe succeeded
    - failures are captured per symbol/timeframe without stopping the full batch
    - a structured summary is returned at the end
    """
    supported_source_timeframes = set(list_source_timeframes())
    supported_derived_timeframes = set(list_derived_timeframes())

    invalid_source = sorted(set(source_timeframes) - supported_source_timeframes)
    if invalid_source:
        invalid = ", ".join(invalid_source)
        supported = ", ".join(list_source_timeframes())
        raise ValueError(
            f"Unsupported source timeframes: {invalid}. Supported: {supported}"
        )

    invalid_derived = sorted(set(derived_timeframes) - supported_derived_timeframes)
    if invalid_derived:
        invalid = ", ".join(invalid_derived)
        supported = ", ".join(list_derived_timeframes())
        raise ValueError(
            f"Unsupported derived timeframes: {invalid}. Supported: {supported}"
        )

    symbols = load_symbols(symbols_file)
    total_symbols = len(symbols)

    successes: list[dict] = []
    failures: list[dict] = []
    skipped: list[dict] = []

    print("Starting batch market timeframe refresh")
    print(f"Symbols file: {symbols_file}")
    print(f"Date range: {start_date} -> {end_date}")
    print(f"Source timeframes: {', '.join(source_timeframes) if source_timeframes else '(none)'}")
    print(f"Derived timeframes: {', '.join(derived_timeframes) if derived_timeframes else '(none)'}")

    for index, symbol in enumerate(symbols, start=1):
        print()
        print(f"{_progress_bar(index, total_symbols)}  {symbol}")

        completed_source_timeframes: set[str] = set()

        for timeframe in source_timeframes:
            print(f"  Source {timeframe}: starting")

            try:
                run_market_bars_pipeline(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                )
                completed_source_timeframes.add(timeframe)
                successes.append(
                    {
                        "symbol": symbol,
                        "run_type": "source",
                        "timeframe": timeframe,
                    }
                )
                print(f"  Source {timeframe}: success")
            except Exception as exc:
                failures.append(
                    {
                        "symbol": symbol,
                        "run_type": "source",
                        "timeframe": timeframe,
                        "error": str(exc),
                    }
                )
                print(f"  Source {timeframe}: failed -> {exc}")

        for target_timeframe in derived_timeframes:
            derivation_spec = get_derivation_spec(target_timeframe)
            required_source_timeframe = derivation_spec.source_timeframe

            if required_source_timeframe not in completed_source_timeframes:
                skipped.append(
                    {
                        "symbol": symbol,
                        "run_type": "derived",
                        "timeframe": target_timeframe,
                        "required_source_timeframe": required_source_timeframe,
                        "reason": (
                            f"required source timeframe '{required_source_timeframe}' "
                            f"did not complete successfully for this symbol"
                        ),
                    }
                )
                print(
                    f"  Derived {target_timeframe}: skipped "
                    f"(missing successful source {required_source_timeframe})"
                )
                continue

            print(
                f"  Derived {target_timeframe} from {required_source_timeframe}: starting"
            )

            try:
                run_derived_bars_pipeline(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    source_timeframe=required_source_timeframe,
                    target_timeframe=target_timeframe,
                )
                successes.append(
                    {
                        "symbol": symbol,
                        "run_type": "derived",
                        "timeframe": target_timeframe,
                        "required_source_timeframe": required_source_timeframe,
                    }
                )
                print(f"  Derived {target_timeframe}: success")
            except Exception as exc:
                failures.append(
                    {
                        "symbol": symbol,
                        "run_type": "derived",
                        "timeframe": target_timeframe,
                        "required_source_timeframe": required_source_timeframe,
                        "error": str(exc),
                    }
                )
                print(f"  Derived {target_timeframe}: failed -> {exc}")

    summary = {
        "symbols_file": str(symbols_file),
        "symbol_count": len(symbols),
        "source_timeframes": list(source_timeframes),
        "derived_timeframes": list(derived_timeframes),
        "success_count": len(successes),
        "failure_count": len(failures),
        "skipped_count": len(skipped),
        "successes": successes,
        "failures": failures,
        "skipped": skipped,
    }

    print()
    print("Batch market timeframe refresh completed")
    print(f"  Symbols processed: {summary['symbol_count']}")
    print(f"  Successes: {summary['success_count']}")
    print(f"  Failures: {summary['failure_count']}")
    print(f"  Skipped: {summary['skipped_count']}")

    return summary