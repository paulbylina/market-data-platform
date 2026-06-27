from src.pipelines.stocks.curated_daily_bars_pipeline import run_curated_daily_bars_pipeline


def run_daily_eod_pipeline(symbol: str, start_date: str, end_date: str) -> None:
    """
    Backward-compatible wrapper around the generic market bars pipeline.
    """
    run_curated_daily_bars_pipeline(
        symbol=symbol,
        timeframe="1d",
        start_date=start_date,
        end_date=end_date,
    )