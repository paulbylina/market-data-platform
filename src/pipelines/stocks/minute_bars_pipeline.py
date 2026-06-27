from src.pipelines.stocks.curated_daily_bars_pipeline import run_market_bars_pipeline


def run_minute_bars_pipeline(symbol: str, start_date: str, end_date: str) -> None:
    """
    Convenience wrapper for 1-minute source bars.
    """
    run_market_bars_pipeline(
        symbol=symbol,
        timeframe="1m",
        start_date=start_date,
        end_date=end_date,
    )