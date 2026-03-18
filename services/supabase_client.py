from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit

import httpx
from astrbot.api import logger

from .http_executor import HttpExecutor, HttpRequestOptions
from .sensitive import mask_sensitive_text

MAX_FETCH_LIMIT = 20
MIN_FETCH_LIMIT = 1
MIN_FETCH_OFFSET = 0
SIGNED_URL_EXPIRES_SECONDS = 3600
DEFAULT_PDF_MAX_SIZE_MB = 50
MIN_PDF_MAX_SIZE_MB = 1
MAX_PDF_MAX_SIZE_MB = 512
BYTES_PER_MB = 1024 * 1024
HTTP_TIMEOUT_DEFAULT_SEC = 20
HTTP_TIMEOUT_MIN_SEC = 5
HTTP_RETRY_DEFAULT = 3
HTTP_RETRY_MIN = 1
MAX_CONNECTIONS = 16
MAX_KEEPALIVE_CONNECTIONS = 16
BACKOFF_BASE_SECONDS = 2
BACKOFF_MAX_SECONDS = 8


class SupabaseClient:
    def __init__(
        self,
        cfg_getter: Callable[[str, Any], Any],
        cfg_int_getter: Callable[..., int],
        key_getter: Callable[[], str],
        default_url: str,
        default_bucket: str,
    ):
        self._cfg = cfg_getter
        self._cfg_int = cfg_int_getter
        self._get_supabase_key = key_getter
        self._default_url = default_url
        self._default_bucket = default_bucket
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()
        self._http_executor = HttpExecutor(
            get_client=lambda: self._get_http_client(),
            backoff_sleep=lambda attempt: self._backoff_sleep(attempt),
            mask_text=mask_sensitive_text,
            mask_url=lambda url: self._mask_url(url),
        )

    async def close(self) -> None:
        async with self._client_lock:
            client = self._client
            self._client = None
            if client is None:
                return
            try:
                await client.aclose()
            except Exception:
                logger.warning("关闭 HTTP 客户端失败。", exc_info=True)

    async def fetch_latest_submissions(
        self,
        zone: str,
        limit: int,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        safe_limit = max(MIN_FETCH_LIMIT, min(int(limit), MAX_FETCH_LIMIT))
        safe_offset = max(MIN_FETCH_OFFSET, int(offset))
        supabase_url = str(self._cfg("supabase_url", self._default_url)).strip()
        select_fields = (
            "id,manuscript_title,author_name,institution,viscosity,discipline,"
            "created_at,avg_score,rating_count,weighted_score,co_authors,"
            "solicited_topic,comment_count,unique_commenters,promoted_to_septic_at,pdf_path,zone"
        )
        params = {
            "select": select_fields,
            "zone": f"eq.{zone}",
            "limit": str(safe_limit),
            "offset": str(safe_offset),
        }
        params["order"] = "promoted_to_septic_at.desc.nullslast" if zone == "septic" else "created_at.desc"

        url = f"{supabase_url}/rest/v1/preprints_with_ratings_mat"
        data = await self._request_json("GET", url, params=params)
        if not isinstance(data, list) or not data:
            return []
        return [item for item in data if isinstance(item, dict)]

    async def fetch_submission_detail(self, paper_id: str) -> dict[str, Any]:
        supabase_url = str(self._cfg("supabase_url", self._default_url)).strip()
        url = f"{supabase_url}/rest/v1/preprints_with_ratings_mat"
        params = {
            "select": "*",
            "id": f"eq.{paper_id}",
            "limit": "1",
        }
        data = await self._request_json("GET", url, params=params)
        if not isinstance(data, list) or not data:
            return {}
        return data[0]

    async def create_signed_pdf_url(self, pdf_path: str) -> str:
        supabase_url = str(self._cfg("supabase_url", self._default_url)).strip()
        bucket = str(self._cfg("supabase_bucket", self._default_bucket)).strip()
        escaped = quote(pdf_path, safe="/")
        url = f"{supabase_url}/storage/v1/object/sign/{bucket}/{escaped}"
        data = await self._request_json(
            "POST",
            url,
            json_body={"expiresIn": SIGNED_URL_EXPIRES_SECONDS},
        )
        signed = str((data or {}).get("signedURL", "")).strip()
        if not signed:
            logger.error("响应中缺少 signedURL 字段：地址=%s", self._mask_url(url))
            raise RuntimeError("响应中缺少 signedURL")
        return self._resolve_signed_url(supabase_url, signed)

    async def download_pdf_file(self, signed_url: str, target_path: Path) -> tuple[int, str]:
        valid_signed_url = self._validate_signed_url(signed_url)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        max_pdf_mb = self._cfg_int(
            "pdf_max_size_mb",
            DEFAULT_PDF_MAX_SIZE_MB,
            min_value=MIN_PDF_MAX_SIZE_MB,
            max_value=MAX_PDF_MAX_SIZE_MB,
        )
        max_pdf_bytes = max_pdf_mb * BYTES_PER_MB
        timeout, retry, _ = self._http_request_options()
        options = HttpRequestOptions(timeout=timeout, retry=retry)
        return await self._http_executor.download_pdf(
            url=valid_signed_url,
            target_path=target_path,
            headers=self._build_download_headers(),
            max_bytes=max_pdf_bytes,
            options=options,
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        timeout, retry, headers = self._http_request_options()
        options = HttpRequestOptions(timeout=timeout, retry=retry)
        return await self._http_executor.request_json(
            method=method,
            url=url,
            params=params,
            json_body=json_body,
            headers=headers,
            options=options,
        )

    def _http_request_options(self) -> tuple[int, int, dict[str, str]]:
        timeout = self._cfg_int("http_timeout_sec", HTTP_TIMEOUT_DEFAULT_SEC, min_value=HTTP_TIMEOUT_MIN_SEC)
        retry = self._cfg_int("http_retry", HTTP_RETRY_DEFAULT, min_value=HTTP_RETRY_MIN)
        headers = self._build_supabase_headers()
        return timeout, retry, headers

    async def _backoff_sleep(self, attempt: int) -> None:
        delay = min(BACKOFF_BASE_SECONDS ** (attempt - 1), BACKOFF_MAX_SECONDS)
        await asyncio.sleep(delay)

    async def _get_http_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            client = self._client
            if client is not None:
                return client

            limits = httpx.Limits(
                max_connections=MAX_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
            )
            self._client = httpx.AsyncClient(limits=limits, follow_redirects=False)
            return self._client

    def _build_supabase_headers(self) -> dict[str, str]:
        key = self._get_supabase_key()
        return {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _build_download_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/pdf",
        }

    def _resolve_signed_url(self, supabase_url: str, signed: str) -> str:
        parsed = urlsplit(signed)
        if parsed.scheme and parsed.netloc:
            return signed
        base = str(supabase_url).rstrip("/")
        if signed.startswith("/storage/v1/"):
            return f"{base}{signed}"
        if signed.startswith("/"):
            return f"{base}/storage/v1{signed}"
        if signed.startswith("storage/v1/"):
            return f"{base}/{signed}"
        return f"{base}/storage/v1/{signed.lstrip('/')}"

    def _validate_signed_url(self, signed_url: str) -> str:
        parsed = urlsplit(str(signed_url).strip())
        if parsed.scheme.lower() != "https":
            raise RuntimeError("签名 URL 非法：协议必须为 https")

        expected = urlsplit(str(self._cfg("supabase_url", self._default_url)).strip())
        expected_host = (expected.hostname or "").lower()
        actual_host = (parsed.hostname or "").lower()
        if (not expected_host) or actual_host != expected_host:
            raise RuntimeError("签名 URL 非法：主机名不匹配")
        return signed_url

    def _mask_url(self, url: str) -> str:
        text = str(url).strip()
        parsed = urlsplit(text)
        if not parsed.scheme and not parsed.netloc:
            return mask_sensitive_text(text)
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            return f"{base}?<已隐藏>"
        return base
