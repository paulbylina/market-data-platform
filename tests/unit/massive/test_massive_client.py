from unittest.mock import patch

import pytest

from src.clients.massive.massive_client import MassiveClient


def test_massive_client_initializes_with_settings() -> None:
    client = MassiveClient()

    assert client.base_url == "https://api.massive.com"
    assert client.api_key is not None
    assert client.timeout == 30


def test_massive_client_raises_when_api_key_is_missing() -> None:
    client = MassiveClient()

    with patch.object(client, "api_key", None):
        with pytest.raises(ValueError, match="MASSIVE_API_KEY is not set."):
            client._get("/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-31")