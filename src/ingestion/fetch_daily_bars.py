from src.ingestion.massive_client import MassiveClient


def fetch_daily_bars(
    symbol: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 5000,
) -> dict:
    """
    Fetch daily OHLCV aggregate bars for a single stock symbol from the Massive API.

    Args:
        symbol: Stock ticker symbol, for example 'AAPL'.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        adjusted: Whether to request adjusted price data.
        sort: Sort order returned by the API. Use 'asc' for oldest-to-newest.
        limit: Maximum number of aggregate bars to request.

    Returns:
        The raw JSON response from the Massive aggregates endpoint.
    """
    client = MassiveClient()

    path = f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"

    params = {
        "adjusted": str(adjusted).lower(),
        "sort": sort,
        "limit": limit,
    }

    return client._get(path, params=params)