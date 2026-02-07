# integrations/meta_graph_client.py
import time
from typing import Any, Dict, Generator, Optional
import requests

from logs.logger import logger
from config.config import META_GRAPH_VERSION


class MetaObjectAccessError(Exception):
    """
    Raised when Meta API returns:
    code=100 & error_subcode=33
    (Object does not exist or no permission)
    """
    pass


class MetaPermissionError(Exception):
    """Missing ads_read / ads_management permissions"""
    pass


class MetaRateLimitError(Exception):
    """User request limit reached / throttling"""
    pass


class MetaGraphClient:
    BASE_URL = f"https://graph.facebook.com/{META_GRAPH_VERSION}"

    def __init__(
        self,
        access_token: str,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        self.access_token = access_token
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    # -------------------------
    # internal helpers
    # -------------------------
    def _safe_json(self, r: requests.Response, url: str) -> Dict[str, Any]:
        """
        Meta sometimes returns non-JSON (proxy/html) or empty body.
        Prevents: Expecting value: line 1 column 1 (char 0)
        """
        try:
            return r.json()
        except Exception:
            txt = (r.text or "").strip()
            logger.error(f"Meta API non-JSON response status={r.status_code} url={url} body_snip={txt[:200]}")
            # Raise generic error so retries happen
            raise Exception("Meta API returned non-JSON response")

    def _sleep_backoff(self, attempt: int, url: str) -> None:
        sleep_s = self.retry_delay * (attempt + 1)
        logger.warning(f"Rate limit / retry. sleeping={sleep_s}s url={url}")
        time.sleep(sleep_s)

    # -------------------------
    # public methods
    # -------------------------
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Simple GET to a single endpoint (returns dict).
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        params["access_token"] = self.access_token

        attempt = 0
        while attempt < self.max_retries:
            try:
                r = requests.get(url, params=params, timeout=self.timeout)
                data = self._safe_json(r, url)

                if r.status_code != 200:
                    self._handle_meta_error(data)

                return data

            except MetaRateLimitError:
                self._sleep_backoff(attempt, url)
                attempt += 1

            except requests.exceptions.Timeout:
                logger.warning(f"Meta API timeout attempt={attempt+1} url={url}")
                time.sleep(self.retry_delay)
                attempt += 1

            except Exception as e:
                attempt += 1
                logger.error(f"Meta API error attempt={attempt} endpoint={endpoint}: {e}")
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_delay)

        return {}

    def get_object(self, object_id_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Compatibility helper used by services:
        - object_id_or_endpoint can be:
            "123456789"
            "123456789?fields=..."
            "/123456789"
        """
        return self.get(object_id_or_endpoint, params=params)

    def get_paged(self, endpoint: str, params: dict) -> Generator[Dict[str, Any], None, None]:
        """
        Generator that yields items from a paged endpoint.
        Adds retry/backoff for rate limits + timeouts.
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        params["access_token"] = self.access_token

        next_url = url
        next_params = params

        while next_url:
            attempt = 0
            while attempt < self.max_retries:
                try:
                    r = requests.get(next_url, params=next_params, timeout=self.timeout)
                    data = self._safe_json(r, next_url)

                    if r.status_code != 200:
                        self._handle_meta_error(data)

                    for item in data.get("data", []):
                        yield item

                    next_url = data.get("paging", {}).get("next")
                    next_params = None  # next already contains params
                    break  # success -> break retry loop

                except MetaRateLimitError:
                    self._sleep_backoff(attempt, next_url)
                    attempt += 1

                except requests.exceptions.Timeout:
                    logger.warning(f"Meta API timeout attempt={attempt+1} url={next_url}")
                    time.sleep(self.retry_delay)
                    attempt += 1

                except Exception as e:
                    attempt += 1
                    logger.error(f"Meta paged error attempt={attempt} url={next_url}: {e}")
                    if attempt >= self.max_retries:
                        raise
                    time.sleep(self.retry_delay)

            # If we exhausted retries inside while attempt loop, it would raise.
            # Otherwise continue outer loop to next page.

    # -------------------------
    # error handling
    # -------------------------
    def _handle_meta_error(self, err: dict) -> None:
        error = (err or {}).get("error", {})
        code = error.get("code")
        subcode = error.get("error_subcode")
        message = error.get("message", "Unknown Meta API error")

        # Object not accessible
        if code == 100 and subcode == 33:
            raise MetaObjectAccessError(message)

        # Missing permissions
        if code == 200:
            raise MetaPermissionError(message)

        # Rate limit / throttling
        # Meta can return:
        # - code 17 (User request limit reached)
        # - code 4 (Application request limit reached) sometimes
        # - code 80004 (common "User request limit reached")
        if code in (17, 4, 80004):
            raise MetaRateLimitError(message)

        raise Exception(message)
