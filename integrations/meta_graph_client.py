# integrations/meta_graph_client.py
import code
from email import message
import time
from typing import Any, Dict, Generator, Optional
import requests

from logs.logger import logger
from db.config_store import get_config  # Dynamic DB config


class MetaObjectAccessError(Exception):
    """Raised when code=100 & error_subcode=33"""
    pass


class MetaPermissionError(Exception):
    """Missing ads_read / ads_management permissions"""
    pass


class MetaRateLimitError(Exception):
    """User request limit reached / throttling"""
    pass
class MetaInvalidFieldError(Exception):
    """Raised when code=100 and the message contains 'nonexisting field'"""
    pass

class MetaGraphClient:
    def __init__(
        self,
        access_token: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        # Dynamically fetch from DB if not provided
        self.access_token = access_token or get_config("META_USER_TOKEN")
        
        # Pull version from DB, fallback to v24.0
        version = get_config("META_GRAPH_VERSION") or "v24.0"
        self.BASE_URL = f"https://graph.facebook.com/{version}"
        
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    # -------------------------
    # internal helpers
    # -------------------------
    def _safe_json(self, r: requests.Response, url: str) -> Dict[str, Any]:
        try:
            return r.json()
        except Exception:
            txt = (r.text or "").strip()
            logger.error(f"Meta API non-JSON response status={r.status_code} url={url} body_snip={txt[:200]}")
            raise Exception("Meta API returned non-JSON response")

    def _sleep_backoff(self, attempt: int, url: str) -> None:
        sleep_s = self.retry_delay * (attempt + 1)
        logger.warning(f"Rate limit / retry. sleeping={sleep_s}s url={url}")
        time.sleep(sleep_s)

    # -------------------------
    # public methods
    # -------------------------
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Simple GET to a single endpoint."""
        # Check if endpoint is already a full URL (from paging)
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        params = dict(params or {})
        if "access_token" not in params:
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
            except (MetaInvalidFieldError, MetaObjectAccessError, MetaPermissionError):
                # CRITICAL: Do NOT retry if the field is missing or permission is denied
                raise    
            except requests.exceptions.Timeout:
                attempt += 1
                logger.warning(f"Meta API timeout attempt={attempt} url={url}")
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_delay)
            except Exception as e:
                attempt += 1
                logger.error(f"Meta API error attempt={attempt} endpoint={endpoint}: {e}")
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_delay)

        return {}

    def get_object(self, object_id_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.get(object_id_or_endpoint, params=params)

    def get_paged(self, endpoint: str, params: dict) -> Generator[Dict[str, Any], None, None]:
        """Generator that yields items from a paged endpoint."""
        next_url = endpoint
        next_params = dict(params or {})
        if "access_token" not in next_params:
            next_params["access_token"] = self.access_token

        while next_url:
            attempt = 0
            success = False
            while attempt < self.max_retries:
                try:
                    # 'get' handles the URL construction/full URL check
                    data = self.get(next_url, params=next_params)
                    
                    for item in data.get("data", []):
                        yield item

                    next_url = data.get("paging", {}).get("next")
                    next_params = None  # next_url already has tokens/params
                    success = True
                    break

                except MetaRateLimitError:
                    self._sleep_backoff(attempt, next_url)
                    attempt += 1
                except Exception as e:
                    attempt += 1
                    if attempt >= self.max_retries:
                        raise
                    time.sleep(self.retry_delay)
            
            if not success or not next_url:
                break

    def _handle_meta_error(self, err: dict) -> None:
        error = (err or {}).get("error", {})
        code = error.get("code")
        subcode = error.get("error_subcode")
        message = error.get("message", "Unknown Meta API error")

        if code == 100 and "nonexisting field" in message:
            raise MetaInvalidFieldError(message)
        if code == 100 and subcode == 33:
            raise MetaObjectAccessError(message)
        if code == 200:
            raise MetaPermissionError(message)
        if code in (17, 4, 80004):
            raise MetaRateLimitError(message)
        if code in (190, 102):
            logger.critical(f"🛑 AUTH FAILURE: Token is dead! {message}")
            raise Exception(f"AUTH_FAILURE: {message}")

        raise Exception(message)

# # integrations/meta_graph_client.py
# import time
# from typing import Any, Dict, Generator, Optional
# import requests

# from logs.logger import logger
# from config.config import META_GRAPH_VERSION


# class MetaObjectAccessError(Exception):
#     """
#     Raised when Meta API returns:
#     code=100 & error_subcode=33
#     (Object does not exist or no permission)
#     """
#     pass


# class MetaPermissionError(Exception):
#     """Missing ads_read / ads_management permissions"""
#     pass


# class MetaRateLimitError(Exception):
#     """User request limit reached / throttling"""
#     pass


# class MetaGraphClient:
#     BASE_URL = f"https://graph.facebook.com/{META_GRAPH_VERSION}"

#     def __init__(
#         self,
#         access_token: str,
#         timeout: int = 30,
#         max_retries: int = 3,
#         retry_delay: int = 5,
#     ):
#         self.access_token = access_token
#         self.timeout = timeout
#         self.max_retries = max_retries
#         self.retry_delay = retry_delay

#     # -------------------------
#     # internal helpers
#     # -------------------------
#     def _safe_json(self, r: requests.Response, url: str) -> Dict[str, Any]:
#         """
#         Meta sometimes returns non-JSON (proxy/html) or empty body.
#         Prevents: Expecting value: line 1 column 1 (char 0)
#         """
#         try:
#             return r.json()
#         except Exception:
#             txt = (r.text or "").strip()
#             logger.error(f"Meta API non-JSON response status={r.status_code} url={url} body_snip={txt[:200]}")
#             # Raise generic error so retries happen
#             raise Exception("Meta API returned non-JSON response")

#     def _sleep_backoff(self, attempt: int, url: str) -> None:
#         sleep_s = self.retry_delay * (attempt + 1)
#         logger.warning(f"Rate limit / retry. sleeping={sleep_s}s url={url}")
#         time.sleep(sleep_s)

#     # -------------------------
#     # public methods
#     # -------------------------
#     def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
#         """
#         Simple GET to a single endpoint (returns dict).
#         """
#         url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
#         params = dict(params or {})
#         params["access_token"] = self.access_token

#         attempt = 0
#         while attempt < self.max_retries:
#             try:
#                 r = requests.get(url, params=params, timeout=self.timeout)
#                 data = self._safe_json(r, url)

#                 if r.status_code != 200:
#                     self._handle_meta_error(data)

#                 return data

#             except MetaRateLimitError:
#                 self._sleep_backoff(attempt, url)
#                 attempt += 1

#             except requests.exceptions.Timeout:
#                 logger.warning(f"Meta API timeout attempt={attempt+1} url={url}")
#                 time.sleep(self.retry_delay)
#                 attempt += 1

#             except Exception as e:
#                 attempt += 1
#                 logger.error(f"Meta API error attempt={attempt} endpoint={endpoint}: {e}")
#                 if attempt >= self.max_retries:
#                     raise
#                 time.sleep(self.retry_delay)

#         return {}

#     def get_object(self, object_id_or_endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
#         """
#         Compatibility helper used by services:
#         - object_id_or_endpoint can be:
#             "123456789"
#             "123456789?fields=..."
#             "/123456789"
#         """
#         return self.get(object_id_or_endpoint, params=params)

#     def get_paged(self, endpoint: str, params: dict) -> Generator[Dict[str, Any], None, None]:
#         """
#         Generator that yields items from a paged endpoint.
#         Adds retry/backoff for rate limits + timeouts.
#         """
#         url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
#         params = dict(params or {})
#         params["access_token"] = self.access_token

#         next_url = url
#         next_params = params

#         while next_url:
#             attempt = 0
#             while attempt < self.max_retries:
#                 try:
#                     r = requests.get(next_url, params=next_params, timeout=self.timeout)
#                     data = self._safe_json(r, next_url)

#                     if r.status_code != 200:
#                         self._handle_meta_error(data)

#                     for item in data.get("data", []):
#                         yield item

#                     next_url = data.get("paging", {}).get("next")
#                     next_params = None  # next already contains params
#                     break  # success -> break retry loop

#                 except MetaRateLimitError:
#                     self._sleep_backoff(attempt, next_url)
#                     attempt += 1

#                 # except requests.exceptions.Timeout:
#                 #     logger.warning(f"Meta API timeout attempt={attempt+1} url={next_url}")
#                 #     time.sleep(self.retry_delay)
#                 #     attempt += 1
# # integrations/meta_graph_client.py -> inside get_paged while loop

#                 except requests.exceptions.Timeout:
#                     attempt += 1
#                     logger.warning(f"Meta API timeout attempt={attempt} url={next_url}")
#                     if attempt >= self.max_retries:
#                         # RAISE a specific error so the worker knows to give up on this account
#                         raise Exception(f"CRITICAL_TIMEOUT: Meta stopped responding after {self.max_retries} attempts.")
#                     time.sleep(self.retry_delay)
            
#                 except Exception as e:
#                     attempt += 1
#                     logger.error(f"Meta paged error attempt={attempt} url={next_url}: {e}")
#                     if attempt >= self.max_retries:
#                         raise
#                     time.sleep(self.retry_delay)

#             # If we exhausted retries inside while attempt loop, it would raise.
#             # Otherwise continue outer loop to next page.

#     # -------------------------
#     # error handling
#     # -------------------------
#     def _handle_meta_error(self, err: dict) -> None:
#         error = (err or {}).get("error", {})
#         code = error.get("code")
#         subcode = error.get("error_subcode")
#         message = error.get("message", "Unknown Meta API error")

#         # Object not accessible
#         if code == 100 and subcode == 33:
#             raise MetaObjectAccessError(message)

#         # Missing permissions
#         if code == 200:
#             raise MetaPermissionError(message)

#         # Rate limit / throttling
#         # Meta can return:
#         # - code 17 (User request limit reached)
#         # - code 4 (Application request limit reached) sometimes
#         # - code 80004 (common "User request limit reached")
#         if code in (17, 4, 80004):
#             raise MetaRateLimitError(message)

#         raise Exception(message)
