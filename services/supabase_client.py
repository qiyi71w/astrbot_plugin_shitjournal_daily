from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit

import aiofiles
import httpx
from astrbot.api import logger

from .sensitive import mask_sensitive_text


class _NonRetryableRequestError(RuntimeError):
    pass


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
                logger.warning("关闭 HTTP 客户端失败。", exc_info=True)

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
            logger.error("响应中缺少 signedURL 字段：地址=%s", self._mask_url(url))
            raise RuntimeError("响应中缺少 signedURL")
        return self._resolve_signed_url(supabase_url, signed)

    async def download_pdf_file(self, signed_url: str, target_path: Path) -> tuple[int, str]:
        signed_url = self._validate_signed_url(signed_url)
        safe_url = self._mask_url(signed_url)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        max_pdf_mb = self._cfg_int("pdf_max_size_mb", 50, min_value=1, max_value=512)
        max_pdf_bytes = max_pdf_mb * 1024 * 1024
        timeout, retry, _ = self._http_request_options()
        client = await self._get_http_client()
        last_error: Exception | None = None
        status_code = 0
        content_type = ""

        for attempt in range(1, retry + 1):
            should_retry_after_release = False
            try:
                async with client.stream(
                    method="GET",
                    url=signed_url,
                    headers=self._build_download_headers(),
                    follow_redirects=False,
                    timeout=timeout,
                ) as response:
                    status_code = response.status_code
                    content_type = str(response.headers.get("content-type", ""))
                    if 300 <= status_code < 400:
                        location = self._mask_url(str(response.headers.get("location", "")).strip())
                        logger.warning(
                            "PDF 下载出现重定向，已阻止：状态码=%s 地址=%s 跳转=%s",
                            status_code,
                            safe_url,
                            location or "<缺失>",
                        )
                        raise _NonRetryableRequestError("下载 PDF 时发生未预期的重定向")
                    if status_code >= 500 and attempt < retry:
                        logger.warning(
                            "PDF 下载因状态码触发重试：状态码=%s 尝试=%s/%s 地址=%s",
                            status_code,
                            attempt,
                            retry,
                            safe_url,
                        )
                        should_retry_after_release = True
                    elif status_code >= 400:
                        body_preview = await self._read_response_preview(response)
                        logger.warning(
                            "PDF 下载 HTTP 错误：状态码=%s 地址=%s 响应=%s",
                            status_code,
                            safe_url,
                            body_preview,
                        )
                        raise _NonRetryableRequestError(
                            f"下载 PDF 时出现 HTTP 错误：{status_code}",
                        )
                    if "application/pdf" not in content_type.lower():
                        logger.warning(
                            "PDF 响应的内容类型不是 application/pdf：%s（将继续校验文件头）",
                            content_type,
                        )

                    if not should_retry_after_release:
                        header_preview = bytearray()
                        downloaded_bytes = 0
                        async with aiofiles.open(target_path, "wb") as fp:
                            async for chunk in response.aiter_bytes(chunk_size=1024 * 64):
                                if not chunk:
                                    continue
                                downloaded_bytes += len(chunk)
                                if downloaded_bytes > max_pdf_bytes:
                                    raise _NonRetryableRequestError(
                                        f"PDF 文件过大：{downloaded_bytes} 字节，超过配置上限 {max_pdf_bytes} 字节",
                                    )
                                if len(header_preview) < 5:
                                    missing = 5 - len(header_preview)
                                    header_preview.extend(chunk[:missing])
                                    if len(header_preview) >= 5 and bytes(header_preview[:5]) != b"%PDF-":
                                        raise _NonRetryableRequestError(
                                            "下载的文件不是有效的 PDF",
                                        )
                                await fp.write(chunk)
                        if bytes(header_preview[:5]) != b"%PDF-":
                            raise _NonRetryableRequestError("下载的文件不是有效的 PDF")
                if should_retry_after_release:
                    await self._backoff_sleep(attempt)
                    continue
                break
            except Exception as exc:
                last_error = exc
                self._remove_partial_file(target_path)
                if isinstance(exc, _NonRetryableRequestError):
                    break
                if attempt < retry:
                    logger.warning(
                        "PDF 下载因异常触发重试：尝试=%s/%s 地址=%s 错误=%s",
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
            raise RuntimeError("下载后的 PDF 文件不存在")

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
        return self._decode_preview_bytes(preview, max_bytes=max_bytes, preview_chars=preview_chars)

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
                        "Supabase 请求被重定向，已阻止：状态码=%s 地址=%s 跳转=%s",
                        response.status_code,
                        safe_url,
                        location or "<缺失>",
                    )
                    raise _NonRetryableRequestError("Supabase 请求发生未预期的重定向")
                if response.status_code >= 500 and attempt < retry:
                    logger.warning(
                        "HTTP 请求因状态码触发重试：状态码=%s 尝试=%s/%s 地址=%s",
                        response.status_code,
                        attempt,
                        retry,
                        safe_url,
                    )
                    await self._backoff_sleep(attempt)
                    continue
                if response.status_code >= 400:
                    body_preview = self._decode_preview_bytes(response.content)
                    logger.warning(
                        "Supabase 请求 HTTP 错误：状态码=%s 地址=%s 响应=%s",
                        response.status_code,
                        safe_url,
                        body_preview,
                    )
                    raise _NonRetryableRequestError(
                        f"Supabase 请求返回 HTTP 错误：{response.status_code}",
                    )
                try:
                    return response.json()
                except Exception:
                    body_preview = self._decode_preview_bytes(response.content)
                    logger.warning(
                        "Supabase 响应 JSON 解析失败：地址=%s 响应=%s",
                        safe_url,
                        body_preview,
                    )
                    raise _NonRetryableRequestError(
                        "Supabase 响应 JSON 解析失败",
                    )
            except Exception as exc:
                last_error = exc
                if isinstance(exc, _NonRetryableRequestError):
                    break
                if attempt < retry:
                    logger.warning(
                        "HTTP 请求因异常触发重试：尝试=%s/%s 地址=%s 错误=%s",
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

    def _decode_preview_bytes(
        self,
        data: bytes | bytearray,
        *,
        max_bytes: int = 1024,
        preview_chars: int = 300,
    ) -> str:
        preview_bytes = bytes(data[:max_bytes])
        return mask_sensitive_text(preview_bytes[:preview_chars].decode("utf-8", errors="replace"))

    def _remove_partial_file(self, target_path: Path) -> None:
        try:
            target_path.unlink(missing_ok=True)
        except Exception:
            logger.warning("删除未完成的下载文件失败：%s", str(target_path), exc_info=True)

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
