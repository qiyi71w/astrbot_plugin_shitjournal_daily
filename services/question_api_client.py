from __future__ import annotations

from typing import Any, Awaitable, Callable
from urllib.parse import quote

import httpx

from .question_constants import (
    DEFAULT_QUESTION_API_BASE_URL,
    QUESTION_ALLOWED_SORTS,
    QUESTION_DEFAULT_SORTS,
    normalize_question_payload,
    to_text,
)

MAX_FETCH_LIMIT = 20
MIN_FETCH_LIMIT = 1
MIN_FETCH_OFFSET = 0
HTTP_TIMEOUT_DEFAULT_SEC = 20
HTTP_TIMEOUT_MIN_SEC = 5


class QuestionConfigError(ValueError):
    pass


class QuestionApiClient:
    def __init__(
        self,
        *,
        cfg_getter: Callable[[str, Any], Any],
        cfg_int_getter: Callable[..., int],
        request_json: Callable[..., Awaitable[Any]] | None = None,
        default_api_base_url: str = DEFAULT_QUESTION_API_BASE_URL,
    ):
        self._cfg = cfg_getter
        self._cfg_int = cfg_int_getter
        self._request_json_override = request_json
        self._default_api_base_url = default_api_base_url

    async def fetch_latest_questions(
        self,
        zone: str,
        limit: int,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        safe_limit = max(MIN_FETCH_LIMIT, min(int(limit), MAX_FETCH_LIMIT))
        safe_offset = max(MIN_FETCH_OFFSET, int(offset))
        page_index, page_offset = divmod(safe_offset, safe_limit)
        params = {
            "zone": zone,
            "sort": self._resolve_zone_sort(zone),
            "page": str(page_index + 1),
            "limit": str(safe_limit),
        }
        data = await self._request_json("GET", f"{self._api_base_url()}/api/questions/", params=params)
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            return []
        normalized = [normalize_question_payload(item, zone) for item in items if isinstance(item, dict)]
        return normalized[page_offset:]

    async def fetch_question_detail(self, question_id: str) -> dict[str, Any]:
        encoded_question_id = quote(str(question_id).strip(), safe="")
        data = await self._request_json("GET", f"{self._api_base_url()}/api/questions/{encoded_question_id}")
        if not isinstance(data, dict):
            return {}
        return normalize_question_payload(data)

    def _resolve_zone_sort(self, zone: str) -> str:
        normalized_zone = to_text(zone).lower()
        allowed = QUESTION_ALLOWED_SORTS.get(normalized_zone)
        if not allowed:
            raise QuestionConfigError(f"课题分区不受支持：zone={normalized_zone}")
        configured = to_text(self._cfg(f"questions_sort_{normalized_zone}", QUESTION_DEFAULT_SORTS[normalized_zone]))
        if configured in allowed:
            return configured
        allowed_text = ", ".join(allowed)
        raise QuestionConfigError(
            f"课题排序配置非法：zone={normalized_zone} sort={configured} allowed={allowed_text}",
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if self._request_json_override is not None:
            return await self._request_json_override(method, url, params=params, json_body=json_body)
        timeout = self._cfg_int("http_timeout_sec", HTTP_TIMEOUT_DEFAULT_SEC, min_value=HTTP_TIMEOUT_MIN_SEC)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
            response = await client.request(method, url, params=params, json=json_body, headers=self._json_headers())
            response.raise_for_status()
            return response.json()

    def _api_base_url(self) -> str:
        configured = to_text(self._cfg("api_base_url", self._default_api_base_url))
        return configured.rstrip("/") or DEFAULT_QUESTION_API_BASE_URL

    def _json_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
