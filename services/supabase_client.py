from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit

import aiofiles
import httpx
from astrbot.api import logger

from .sensitive import mask_sensitive_text


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

    async def close(self) -> None:
        async with self._client_lock:
            client = self._client
            self._client = None

        if client is None:
            return

        try:
            await client.aclose()
        except Exception:
            logger.warning("close http client failed", exc_info=True)

    async def fetch_latest_submission(self, zone: str) -> dict[str, Any] | None:
        latest_list = await self.fetch_latest_submissions(zone=zone, limit=1)
        if not latest_list:
            return None
        return latest_list[0]

    async def fetch_latest_submissions(
        self,
        zone: str,
        limit: int,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 20))
        safe_offset = max(0, int(offset))
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
        if zone == "septic":
            params["order"] = "promoted_to_septic_at.desc.nullslast"
        else:
            params["order"] = "created_at.desc"

        url = f"{supabase_url}/rest/v1/preprints_with_ratings_mat"
        data = await self._request_json("GET", url, params=params)
        if not isinstance(data, list) or not data:
            return []
        result: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                result.append(item)
        return result

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
        data = await self._request_json("POST", url, json_body={"expiresIn": 3600})
        signed = str((data or {}).get("signedURL", "")).strip()
        if not signed:
            logger.error("signedURL missing in response: url=%s", self._mask_url(url))
            raise RuntimeError("signedURL missing in response")
        return self._resolve_signed_url(supabase_url, signed)

    async def download_pdf_file(self, signed_url: str, target_path: Path) -> tuple[int, str]:
        signed_url = self._validate_signed_url(signed_url)
        safe_url = self._mask_url(signed_url)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        timeout, retry, _ = self._http_request_options()
        client = await self._get_http_client()
        last_error: Exception | None = None
        status_code = 0
        content_type = ""

        for attempt in range(1, retry + 1):
            try:
                async with client.stream(
                    method="GET",
                    url=signed_url,
                    headers=self._build_download_headers(),
                    follow_redirects=True,
                    timeout=timeout,
                ) as response:
                    status_code = response.status_code
                    content_type = str(response.headers.get("content-type", ""))
                    if status_code >= 500 and attempt < retry:
                        logger.warning(
                            "pdf download retry due to status=%s attempt=%s/%s url=%s",
                            status_code,
                            attempt,
                            retry,
                            safe_url,
                        )
                        await self._backoff_sleep(attempt)
                        continue
                    if status_code >= 400:
                        body_preview = await self._read_response_preview(response)
                        logger.warning(
                            "pdf download http error: status=%s url=%s body=%s",
                            status_code,
                            safe_url,
                            body_preview,
                        )
                        raise RuntimeError(
                            f"http error {status_code} while downloading pdf",
                        )
                    if "application/pdf" not in content_type.lower():
                        logger.warning(
                            "pdf content-type is not application/pdf: %s (will validate header)",
                            content_type,
                        )

                    async with aiofiles.open(target_path, "wb") as fp:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 64):
                            if chunk:
                                await fp.write(chunk)
                break
            except Exception as exc:
                last_error = exc
                if attempt < retry:
                    logger.warning(
                        "pdf download retry due to exception attempt=%s/%s url=%s err=%s",
                        attempt,
                        retry,
                        safe_url,
                        mask_sensitive_text(str(exc)),
                    )
                    await self._backoff_sleep(attempt)
                    continue
                break

        if (not target_path.exists()) or target_path.stat().st_size <= 0:
            if last_error is not None:
                raise last_error
            raise RuntimeError("downloaded pdf file missing")

        with target_path.open("rb") as fp:
            header = fp.read(5)
        if header != b"%PDF-":
            raise RuntimeError("downloaded file is not a valid PDF")

        return status_code, content_type

    async def _read_response_preview(
        self,
        response: httpx.Response,
        *,
        max_bytes: int = 1024,
        chunk_size: int = 256,
        preview_chars: int = 300,
    ) -> str:
        preview = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=chunk_size):
            if not chunk:
                continue
            remaining = max_bytes - len(preview)
            if remaining <= 0:
                break
            preview.extend(chunk[:remaining])
            if len(preview) >= max_bytes:
                break
        return mask_sensitive_text(bytes(preview[:preview_chars]).decode("utf-8", errors="replace"))

    async def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        safe_url = self._mask_url(url)
        timeout, retry, headers = self._http_request_options()
        client = await self._get_http_client()
        last_error: Exception | None = None
        for attempt in range(1, retry + 1):
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    follow_redirects=False,
                    timeout=timeout,
                )
                if 300 <= response.status_code < 400:
                    location = self._mask_url(str(response.headers.get("location", "")).strip())
                    logger.warning(
                        "supabase request redirect blocked: status=%s url=%s location=%s",
                        response.status_code,
                        safe_url,
                        location or "<missing>",
                    )
                    raise RuntimeError("unexpected redirect for supabase request")
                if response.status_code >= 500 and attempt < retry:
                    logger.warning(
                        "http retrying due to status=%s attempt=%s/%s url=%s",
                        response.status_code,
                        attempt,
                        retry,
                        safe_url,
                    )
                    await self._backoff_sleep(attempt)
                    continue
                if response.status_code >= 400:
                    body_preview = mask_sensitive_text(response.text[:300])
                    logger.warning(
                        "supabase request http error: status=%s url=%s body=%s",
                        response.status_code,
                        safe_url,
                        body_preview,
                    )
                    raise RuntimeError(
                        f"http error {response.status_code} for supabase request",
                    )
                try:
                    return response.json()
                except Exception:
                    body_preview = mask_sensitive_text(response.text[:300])
                    logger.warning(
                        "supabase json decode failed: url=%s body=%s",
                        safe_url,
                        body_preview,
                    )
                    raise RuntimeError(
                        "json decode failed for supabase response",
                    )
            except Exception as exc:
                last_error = exc
                if attempt < retry:
                    logger.warning(
                        "http retrying due to exception attempt=%s/%s url=%s err=%s",
                        attempt,
                        retry,
                        safe_url,
                        mask_sensitive_text(str(exc)),
                    )
                    await self._backoff_sleep(attempt)
                    continue
                break

        assert last_error is not None
        raise last_error

    def _http_request_options(self) -> tuple[int, int, dict[str, str]]:
        timeout = self._cfg_int("http_timeout_sec", 20, min_value=5)
        retry = self._cfg_int("http_retry", 3, min_value=1)
        headers = self._build_supabase_headers()
        return timeout, retry, headers

    async def _backoff_sleep(self, attempt: int) -> None:
        delay = min(2 ** (attempt - 1), 8)
        await asyncio.sleep(delay)

    async def _get_http_client(self) -> httpx.AsyncClient:
        client = self._client
        if client is not None:
            return client

        async with self._client_lock:
            client = self._client
            if client is not None:
                return client

            limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
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
            raise RuntimeError("invalid signed url: scheme must be https")

        expected = urlsplit(str(self._cfg("supabase_url", self._default_url)).strip())
        expected_host = (expected.hostname or "").lower()
        actual_host = (parsed.hostname or "").lower()
        if (not expected_host) or actual_host != expected_host:
            raise RuntimeError("invalid signed url: unexpected host")
        return signed_url

    def _mask_url(self, url: str) -> str:
        text = str(url).strip()
        parsed = urlsplit(text)
        if not parsed.scheme and not parsed.netloc:
            return mask_sensitive_text(text)
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            return f"{base}?<redacted>"
        return base
