import time

import httpx

from src.utils.settings import MASSIVE_API_KEY, MASSIVE_BASE_URL, REQUEST_TIMEOUT_SECONDS


class MassiveClient:
    """Small HTTP client wrapper for interacting with the Massive REST API."""

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self) -> None:
        self.base_url = MASSIVE_BASE_URL
        self.api_key = MASSIVE_API_KEY
        self.timeout = REQUEST_TIMEOUT_SECONDS

    def _get(
        self,
        path: str,
        params: dict | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
    ) -> dict:
        """
        Send a GET request to the Massive API and return the parsed JSON response.

        Retries are applied for transient failures such as:
        - HTTP 429
        - HTTP 500 / 502 / 503 / 504
        - network / timeout errors

        Args:
            path: Endpoint path relative to the Massive API base URL.
            params: Optional query parameters to include in the request.
            max_attempts: Total attempts before failing.
            backoff_seconds: Initial retry delay in seconds.

        Returns:
            Parsed JSON response from the API.

        Raises:
            ValueError: If MASSIVE_API_KEY is not set.
            httpx.HTTPStatusError: If the API returns a non-retryable status code,
                or a retryable status code that still fails after all attempts.
            httpx.RequestError: If the request keeps failing due to network issues.
        """
        if not self.api_key:
            raise ValueError("MASSIVE_API_KEY is not set.")

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1.")

        request_params = params.copy() if params else {}
        request_params["apiKey"] = self.api_key

        last_exception: Exception | None = None

        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            for attempt in range(1, max_attempts + 1):
                try:
                    response = client.get(path, params=request_params)
                    response.raise_for_status()
                    return response.json()

                except httpx.HTTPStatusError as exc:
                    last_exception = exc
                    status_code = exc.response.status_code

                    should_retry = (
                        status_code in self.RETRYABLE_STATUS_CODES
                        and attempt < max_attempts
                    )

                    print(
                        f"Massive GET failed with HTTP {status_code} "
                        f"on attempt {attempt}/{max_attempts} for path={path}"
                    )

                    if not should_retry:
                        raise

                except httpx.RequestError as exc:
                    last_exception = exc
                    should_retry = attempt < max_attempts

                    print(
                        f"Massive GET request error on attempt {attempt}/{max_attempts} "
                        f"for path={path}: {exc}"
                    )

                    if not should_retry:
                        raise

                sleep_seconds = backoff_seconds * (2 ** (attempt - 1))
                print(f"Retrying in {sleep_seconds:.1f}s...")
                time.sleep(sleep_seconds)

        if last_exception is not None:
            raise last_exception

        raise RuntimeError("Massive GET failed unexpectedly without an exception.")