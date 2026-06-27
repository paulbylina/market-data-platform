import pytest

from src.ingestion.massive.fetch_bars import fetch_bars
from src.utils.settings import MASSIVE_API_KEY


@pytest.mark.smoke
@pytest.mark.massive
def test_can_fetch_raw_aapl_daily_bars_from_massive() -> None:
    """
    Smoke test: verify we can download a small raw daily-bar dataset.

    This makes only 1 API call.
    """
    if not MASSIVE_API_KEY:
        pytest.skip("MASSIVE_API_KEY is not set")

    raw_response = fetch_bars(
        symbol="AAPL",
        timeframe="1d",
        start_date="2025-07-01",
        end_date="2026-06-20",
    )

    assert isinstance(raw_response, dict)
    assert "results" in raw_response
    assert isinstance(raw_response["results"], list)
    assert len(raw_response["results"]) > 0

    first_bar = raw_response["results"][0]

    expected_raw_bar_fields = ["o", "h", "l", "c", "v", "t"]

    for field in expected_raw_bar_fields:
        assert field in first_bar