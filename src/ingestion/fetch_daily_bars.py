from src.ingestion.massive_client import MassiveClient


def fetch_daily_bars(
    symbol: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 5000,
) -> dict:
    client = MassiveClient()

    path = f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"

    params = {
        "adjusted": str(adjusted).lower(),
        "sort": sort,
        "limit": limit,
    }

    return client._get(path, params=params)