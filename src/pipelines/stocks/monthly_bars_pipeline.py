from src.pipelines.stocks.run_derived_bars_pipeline import run_derived_bars_pipeline


def run_monthly_bars_pipeline(symbol: str, start_date: str, end_date: str) -> None:
    """
    Convenience wrapper for the initial supported monthly derivation:
        1d -> 1mo
    """
    run_derived_bars_pipeline(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_timeframe="1d",
        target_timeframe="1mo",
    )