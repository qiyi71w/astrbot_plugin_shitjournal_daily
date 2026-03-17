from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiofiles
import httpx
from astrbot.api import logger

PDF_SIGNATURE = b"%PDF-"
PREVIEW_MAX_BYTES = 1024
PREVIEW_CHARS = 300
PREVIEW_CHUNK_BYTES = 256
DOWNLOAD_CHUNK_BYTES = 1024 * 64

class NonRetryableRequestError(RuntimeError):
    pass

@dataclass(frozen=True)
class HttpRequestOptions:
    timeout: int
    retry: int


@dataclass(frozen=True)
class HttpStatusPolicy:
    redirect_message: str
    redirect_error: str
    retry_message: str
    error_message: str
    error_prefix: str


JSON_STATUS_POLICY = HttpStatusPolicy(
    redirect_message="Supabase 请求被重定向，已阻止：状态码=%s 地址=%s 跳转=%s",
    redirect_error="Supabase 请求发生未预期的重定向",
    retry_message="HTTP 请求因状态码触发重试：状态码=%s 尝试=%s/%s 地址=%s",
    error_message="Supabase 请求 HTTP 错误：状态码=%s 地址=%s 响应=%s",
    error_prefix="Supabase 请求返回 HTTP 错误",
)
PDF_STATUS_POLICY = HttpStatusPolicy(
    redirect_message="PDF 下载出现重定向，已阻止：状态码=%s 地址=%s 跳转=%s",
    redirect_error="下载 PDF 时发生未预期的重定向",
    retry_message="PDF 下载因状态码触发重试：状态码=%s 尝试=%s/%s 地址=%s",
    error_message="PDF 下载 HTTP 错误：状态码=%s 地址=%s 响应=%s",
    error_prefix="下载 PDF 时出现 HTTP 错误",
)


class HttpExecutor:
    def __init__(
        self,
        *,
        get_client: Callable[[], Awaitable[httpx.AsyncClient]],
        backoff_sleep: Callable[[int], Awaitable[None]],
        mask_text: Callable[[str], str],
        mask_url: Callable[[str], str],
    ):
        self._get_client = get_client
        self._backoff_sleep = backoff_sleep
        self._mask_text = mask_text
        self._mask_url = mask_url

    async def request_json(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None,
        headers: dict[str, str],
        options: HttpRequestOptions,
    ) -> Any:
        safe_url = self._mask_url(url)
        return await self._run_with_retry(
            retry=options.retry,
            safe_url=safe_url,
            retry_log_label="HTTP 请求",
            run_attempt=lambda client, attempt, retry: self._request_json_once(
                client=client,
                method=method,
                url=url,
                params=params,
                json_body=json_body,
                headers=headers,
                timeout=options.timeout,
                safe_url=safe_url,
                attempt=attempt,
                retry=retry,
            ),
        )

    async def download_pdf(
        self,
        *,
        url: str,
        target_path: Path,
        headers: dict[str, str],
        max_bytes: int,
        options: HttpRequestOptions,
    ) -> tuple[int, str]:
        safe_url = self._mask_url(url)
        result = await self._run_with_retry(
            retry=options.retry,
            safe_url=safe_url,
            retry_log_label="PDF 下载",
            on_attempt_error=lambda: self._remove_partial_file(target_path),
            run_attempt=lambda client, attempt, retry: self._download_pdf_once(
                client=client,
                url=url,
                target_path=target_path,
                headers=headers,
                max_bytes=max_bytes,
                timeout=options.timeout,
                safe_url=safe_url,
                attempt=attempt,
                retry=retry,
            ),
        )
        if (not target_path.exists()) or target_path.stat().st_size <= 0:
            raise RuntimeError("下载后的 PDF 文件不存在")
        return result

    async def _run_with_retry(
        self,
        *,
        retry: int,
        safe_url: str,
        retry_log_label: str,
        run_attempt: Callable[[httpx.AsyncClient, int, int], Awaitable[tuple[bool, Any]]],
        on_attempt_error: Callable[[], None] | None = None,
    ) -> Any:
        client = await self._get_client()
        last_error: Exception | None = None
        for attempt in range(1, retry + 1):
            try:
                should_retry, value = await run_attempt(client, attempt, retry)
            except Exception as exc:
                last_error = exc
                if on_attempt_error is not None:
                    on_attempt_error()
                if isinstance(exc, NonRetryableRequestError):
                    break
                if attempt < retry:
                    logger.warning(
                        "%s因异常触发重试：尝试=%s/%s 地址=%s 错误=%s",
                        retry_log_label,
                        attempt,
                        retry,
                        safe_url,
                        self._mask_text(str(exc)),
                    )
                    await self._backoff_sleep(attempt)
                    continue
                break
            if should_retry and attempt < retry:
                await self._backoff_sleep(attempt)
                continue
            return value
        assert last_error is not None
        raise last_error

    async def _request_json_once(self, *, client: httpx.AsyncClient, method: str, url: str, params: dict[str, Any] | None, json_body: dict[str, Any] | None, headers: dict[str, str], timeout: int, safe_url: str, attempt: int, retry: int) -> tuple[bool, Any]:
        response = await client.request(method=method, url=url, params=params, json=json_body, headers=headers, follow_redirects=False, timeout=timeout)
        if await self._evaluate_response(response=response, safe_url=safe_url, attempt=attempt, retry=retry, policy=JSON_STATUS_POLICY, preview_reader=self._read_loaded_preview):
            return True, None
        try:
            return False, response.json()
        except Exception:
            logger.warning("Supabase 响应 JSON 解析失败：地址=%s 响应=%s", safe_url, self._decode_preview_bytes(response.content))
            raise NonRetryableRequestError("Supabase 响应 JSON 解析失败")

    async def _download_pdf_once(self, *, client: httpx.AsyncClient, url: str, target_path: Path, headers: dict[str, str], max_bytes: int, timeout: int, safe_url: str, attempt: int, retry: int) -> tuple[bool, tuple[int, str]]:
        async with client.stream(method="GET", url=url, headers=headers, follow_redirects=False, timeout=timeout) as response:
            status_code = response.status_code
            content_type = str(response.headers.get("content-type", ""))
            if await self._evaluate_response(response=response, safe_url=safe_url, attempt=attempt, retry=retry, policy=PDF_STATUS_POLICY, preview_reader=self._read_response_preview):
                return True, (status_code, content_type)
            if "application/pdf" not in content_type.lower():
                logger.warning("PDF 响应的内容类型不是 application/pdf：%s（将继续校验文件头）", content_type)
            await self._stream_pdf_to_file(response=response, target_path=target_path, max_bytes=max_bytes)
            return False, (status_code, content_type)

    async def _evaluate_response(
        self,
        *,
        response: httpx.Response,
        safe_url: str,
        attempt: int,
        retry: int,
        policy: HttpStatusPolicy,
        preview_reader: Callable[[httpx.Response], Awaitable[str]],
    ) -> bool:
        status_code = response.status_code
        if 300 <= status_code < 400:
            self._raise_redirect_error(status_code, safe_url, response.headers.get("location", ""), policy.redirect_message, policy.redirect_error)
        if status_code >= 500 and attempt < retry:
            logger.warning(policy.retry_message, status_code, attempt, retry, safe_url)
            return True
        if status_code >= 400:
            logger.warning(policy.error_message, status_code, safe_url, await preview_reader(response))
            raise NonRetryableRequestError(f"{policy.error_prefix}：{status_code}")
        return False

    async def _read_loaded_preview(self, response: httpx.Response) -> str:
        return self._decode_preview_bytes(response.content)

    async def _read_response_preview(self, response: httpx.Response) -> str:
        preview = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=PREVIEW_CHUNK_BYTES):
            if not chunk:
                continue
            remaining = PREVIEW_MAX_BYTES - len(preview)
            if remaining <= 0:
                break
            preview.extend(chunk[:remaining])
            if len(preview) >= PREVIEW_MAX_BYTES:
                break
        return self._decode_preview_bytes(preview)

    async def _stream_pdf_to_file(
        self,
        *,
        response: httpx.Response,
        target_path: Path,
        max_bytes: int,
    ) -> None:
        header_preview = bytearray()
        downloaded_bytes = 0
        async with aiofiles.open(target_path, "wb") as fp:
            async for chunk in response.aiter_bytes(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                downloaded_bytes += len(chunk)
                if downloaded_bytes > max_bytes:
                    raise NonRetryableRequestError(
                        f"PDF 文件过大：{downloaded_bytes} 字节，超过配置上限 {max_bytes} 字节",
                    )
                if len(header_preview) < len(PDF_SIGNATURE):
                    missing = len(PDF_SIGNATURE) - len(header_preview)
                    header_preview.extend(chunk[:missing])
                    if len(header_preview) >= len(PDF_SIGNATURE):
                        if bytes(header_preview[: len(PDF_SIGNATURE)]) != PDF_SIGNATURE:
                            raise NonRetryableRequestError("下载的文件不是有效的 PDF")
                await fp.write(chunk)
        if bytes(header_preview[: len(PDF_SIGNATURE)]) != PDF_SIGNATURE:
            raise NonRetryableRequestError("下载的文件不是有效的 PDF")

    def _decode_preview_bytes(self, data: bytes | bytearray) -> str:
        preview_bytes = bytes(data[:PREVIEW_MAX_BYTES])
        text = preview_bytes[:PREVIEW_CHARS].decode("utf-8", errors="replace")
        return self._mask_text(text)

    def _raise_redirect_error(self, status_code: int, safe_url: str, location: Any, message: str, error_text: str) -> None:
        masked_location = self._mask_url(str(location).strip())
        logger.warning(message, status_code, safe_url, masked_location or "<缺失>")
        raise NonRetryableRequestError(error_text)

    def _remove_partial_file(self, target_path: Path) -> None:
        try:
            target_path.unlink(missing_ok=True)
        except Exception:
            logger.warning("删除未完成的下载文件失败：%s", str(target_path), exc_info=True)
