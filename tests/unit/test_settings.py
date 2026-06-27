from src.utils import settings


def test_massive_api_key_is_loaded() -> None:
    assert settings.MASSIVE_API_KEY is not None
    assert settings.MASSIVE_API_KEY != ""