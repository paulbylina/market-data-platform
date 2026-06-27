from collections.abc import Sequence

from src.pipelines.stocks.run_derived_bars_pipeline import run_derived_bars_pipeline
from src.pipelines.stocks.curated_daily_bars_pipeline import run_market_bars_pipeline
from src.utils.timeframes import (
    get_derivation_spec,
    list_derived_timeframes,
    list_source_timeframes,
)


def run_market_timeframe_refresh(
    symbol: str,
    start_date: str,
    end_date: str,
    source_timeframes: Sequence[str] = ("1d",),
    derived_timeframes: Sequence[str] = ("1w", "1mo"),
) -> None:
    """
    Refresh source-native and derived market bar datasets for a symbol/date range.
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

    for timeframe in source_timeframes:
        run_market_bars_pipeline(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
        )

    for target_timeframe in derived_timeframes:
        derivation_spec = get_derivation_spec(target_timeframe)

        run_derived_bars_pipeline(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            source_timeframe=derivation_spec.source_timeframe,
            target_timeframe=target_timeframe,
        )