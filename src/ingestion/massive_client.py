import httpx

from src.utils.settings import MASSIVE_API_KEY, MASSIVE_BASE_URL, REQUEST_TIMEOUT_SECONDS


class MassiveClient:
    def __init__(self) -> None:
        self.base_url = MASSIVE_BASE_URL
        self.api_key = MASSIVE_API_KEY
        self.timeout = REQUEST_TIMEOUT_SECONDS

    def _get(self, path: str, params: dict | None = None) -> dict:
        if not self.api_key:
            raise ValueError("MASSIVE_API_KEY is not set.")

        request_params = params.copy() if params else {}
        request_params["apiKey"] = self.api_key

        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get(path, params=request_params)
            response.raise_for_status()
            return response.json()