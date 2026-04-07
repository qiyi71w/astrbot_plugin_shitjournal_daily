from __future__ import annotations

import asyncio
import posixpath
from pathlib import Path
from typing import Any, Callable
from urllib.parse import SplitResult, quote, urlsplit, urlunsplit

import httpx
from astrbot.api import logger

from .http_executor import HttpExecutor, HttpRequestOptions
from .sensitive import mask_sensitive_text

DEFAULT_SITE_API_BASE_URL = "https://api.shitjournal.org"
DEFAULT_PDF_BASE_URL = "https://files.shitjournal.org"
MAX_FETCH_LIMIT = 20
MIN_FETCH_LIMIT = 1
MIN_FETCH_OFFSET = 0
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
SUPPORTED_PROXY_SCHEMES = {"http", "https"}
UNSUPPORTED_PROXY_SCHEME = "socks5"


class SiteApiClient:
    def __init__(
        self,
        cfg_getter: Callable[[str, Any], Any],
        cfg_int_getter: Callable[..., int],
        default_api_base_url: str = DEFAULT_SITE_API_BASE_URL,
        default_pdf_base_url: str = DEFAULT_PDF_BASE_URL,
    ):
        self._cfg = cfg_getter
        self._cfg_int = cfg_int_getter
        self._default_api_base_url = default_api_base_url
        self._default_pdf_base_url = default_pdf_base_url
        self._client: httpx.AsyncClient | None = None
        self._client_proxy_url: str | None = None
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
            self._client_proxy_url = None
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
        page_index, page_offset = divmod(safe_offset, safe_limit)
        url = f"{self._get_api_base_url()}/api/articles/"
        params = {
            "zone": zone,
            "sort": "newest",
            "discipline": "all",
            "page": str(page_index + 1),
            "limit": str(safe_limit),
        }
        data = await self._request_json("GET", url, params=params)
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            return []
        normalized = [
            self._normalize_site_article(item)
            for item in items
            if isinstance(item, dict)
        ]
        return normalized[page_offset:]

    async def fetch_submission_detail(self, paper_id: str) -> dict[str, Any]:
        encoded_paper_id = quote(str(paper_id).strip(), safe="")
        url = f"{self._get_api_base_url()}/api/articles/{encoded_paper_id}"
        data = await self._request_json("GET", url)
        article = data.get("article") if isinstance(data, dict) else None
        if not isinstance(article, dict):
            return {}
        return self._normalize_site_article(article)

    async def resolve_pdf_download_url(self, pdf_url: str) -> str:
        text = str(pdf_url).strip()
        if not text:
            raise RuntimeError("PDF URL 为空")
        parsed_pdf = urlsplit(text)
        parsed_base = self._parse_pdf_base_url()
        normalized = self._resolve_pdf_download_url(parsed_pdf, parsed_base)
        return self._validate_download_url(normalized)

    async def download_pdf_file(self, download_url: str, target_path: Path) -> tuple[int, str]:
        valid_download_url = self._validate_download_url(download_url)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        max_pdf_mb = self._cfg_int(
            "pdf_max_size_mb",
            DEFAULT_PDF_MAX_SIZE_MB,
            min_value=MIN_PDF_MAX_SIZE_MB,
            max_value=MAX_PDF_MAX_SIZE_MB,
        )
        max_pdf_bytes = max_pdf_mb * BYTES_PER_MB
        timeout, retry = self._http_request_options()
        options = HttpRequestOptions(timeout=timeout, retry=retry)
        return await self._http_executor.download_pdf(
            url=valid_download_url,
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
        timeout, retry = self._http_request_options()
        options = HttpRequestOptions(timeout=timeout, retry=retry)
        return await self._http_executor.request_json(
            method=method,
            url=url,
            params=params,
            json_body=json_body,
            headers=self._build_json_headers(),
            options=options,
        )

    def _http_request_options(self) -> tuple[int, int]:
        timeout = self._cfg_int(
            "http_timeout_sec",
            HTTP_TIMEOUT_DEFAULT_SEC,
            min_value=HTTP_TIMEOUT_MIN_SEC,
        )
        retry = self._cfg_int("http_retry", HTTP_RETRY_DEFAULT, min_value=HTTP_RETRY_MIN)
        return timeout, retry

    async def _backoff_sleep(self, attempt: int) -> None:
        delay = min(BACKOFF_BASE_SECONDS ** (attempt - 1), BACKOFF_MAX_SECONDS)
        await asyncio.sleep(delay)

    async def _get_http_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            configured_proxy = self._get_configured_proxy_url()
            client = self._client
            if client is not None and configured_proxy == self._client_proxy_url:
                return client
            if client is not None:
                self._client = None
                self._client_proxy_url = None
                try:
                    await client.aclose()
                except Exception:
                    logger.warning("关闭 HTTP 客户端失败。", exc_info=True)
            limits = httpx.Limits(
                max_connections=MAX_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
            )
            client_options: dict[str, Any] = {
                "limits": limits,
                "follow_redirects": False,
            }
            if configured_proxy:
                client_options["proxy"] = configured_proxy
                client_options["trust_env"] = False
            self._client = httpx.AsyncClient(**client_options)
            self._client_proxy_url = configured_proxy
            return self._client

    def _normalize_site_article(self, payload: dict[str, Any]) -> dict[str, Any]:
        author = payload.get("author")
        zone = self._resolve_article_zone(payload)
        author_name = self._read_site_author_field(author, "display_name")
        institution = self._read_site_author_field(author, "institution")
        normalized = dict(payload)
        normalized["zone"] = zone
        if "manuscript_title" not in normalized:
            normalized["manuscript_title"] = payload.get("title")
        if "author_name" not in normalized:
            normalized["author_name"] = author_name
        if "institution" not in normalized:
            normalized["institution"] = institution
        return normalized

    def _resolve_article_zone(self, payload: dict[str, Any]) -> str:
        zone = self._to_zone_text(payload.get("zone"))
        if zone:
            return zone
        zones = payload.get("zones")
        if isinstance(zones, list):
            for item in zones:
                candidate = self._to_zone_text(item)
                if candidate:
                    return candidate
            return ""
        return self._to_zone_text(zones)

    def _to_zone_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value).strip()
        return ""

    def _read_site_author_field(self, author: Any, field: str) -> str:
        if not isinstance(author, dict):
            return ""
        return str(author.get(field) or "").strip()

    def _build_json_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _build_download_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/pdf",
        }

    def _resolve_pdf_download_url(
        self,
        parsed_pdf: SplitResult,
        parsed_base: SplitResult,
    ) -> str:
        if parsed_pdf.scheme or parsed_pdf.netloc:
            if parsed_pdf.scheme.lower() != "https":
                raise RuntimeError("PDF URL 非法：协议必须为 https")
            if self._is_same_pdf_scope_origin(parsed_pdf, parsed_base):
                normalized_path = self._normalize_url_path(parsed_pdf.path)
                return urlunsplit(
                    (
                        parsed_base.scheme,
                        parsed_base.netloc,
                        normalized_path,
                        parsed_pdf.query,
                        "",
                    ),
                )
        relative_path = self._normalize_pdf_relative_path(parsed_pdf.path)
        return self._build_pdf_download_url(parsed_base, relative_path, parsed_pdf.query)

    def _is_same_pdf_scope_origin(self, parsed_pdf: SplitResult, parsed_base: SplitResult) -> bool:
        expected_host = (parsed_base.hostname or "").lower()
        actual_host = (parsed_pdf.hostname or "").lower()
        if (not expected_host) or actual_host != expected_host:
            return False
        expected_port = self._effective_port(parsed_base, "pdf_base_url")
        actual_port = self._effective_port(parsed_pdf, "PDF URL")
        return actual_port == expected_port

    def _build_pdf_download_url(
        self,
        parsed_base: SplitResult,
        relative_path: str,
        query: str,
    ) -> str:
        scope_path = self._normalize_pdf_scope_path(parsed_base.path)
        normalized_path = self._join_scope_path(scope_path, relative_path)
        return urlunsplit(
            (
                parsed_base.scheme,
                parsed_base.netloc,
                normalized_path,
                query,
                "",
            ),
        )

    def _normalize_pdf_relative_path(self, path: str) -> str:
        return str(path or "").strip().lstrip("/")

    def _normalize_pdf_scope_path(self, path: str) -> str:
        return self._normalize_url_path(path)

    def _join_scope_path(self, scope_path: str, relative_path: str) -> str:
        if scope_path == "/":
            return f"/{relative_path}" if relative_path else "/"
        if not relative_path:
            return scope_path
        return f"{scope_path}/{relative_path}"

    def _validate_download_url(self, url: str) -> str:
        parsed = urlsplit(str(url).strip())
        if parsed.scheme.lower() != "https":
            raise RuntimeError("PDF URL 非法：协议必须为 https")
        expected = self._parse_pdf_base_url()
        expected_host = (expected.hostname or "").lower()
        actual_host = (parsed.hostname or "").lower()
        if (not expected_host) or actual_host != expected_host:
            raise RuntimeError("PDF URL 非法：主机名不匹配")
        expected_port = self._effective_port(expected, "pdf_base_url")
        actual_port = self._effective_port(parsed, "PDF URL")
        if actual_port != expected_port:
            raise RuntimeError("PDF URL 非法：端口不匹配")
        expected_scope_path = self._normalize_pdf_scope_path(expected.path)
        normalized_path = self._normalize_url_path(parsed.path)
        if not self._is_path_in_scope(normalized_path, expected_scope_path):
            raise RuntimeError("PDF URL 非法：路径超出 pdf_base_url 范围")
        return urlunsplit((expected.scheme, expected.netloc, normalized_path, parsed.query, ""))

    def _normalize_url_path(self, path: str) -> str:
        text = str(path or "").strip()
        if not text:
            return "/"
        normalized = posixpath.normpath(text)
        if normalized in {"", "."}:
            return "/"
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        if normalized.startswith("//"):
            normalized = f"/{normalized.lstrip('/')}"
        return normalized

    def _is_path_in_scope(self, path: str, expected_scope_path: str) -> bool:
        if expected_scope_path == "/":
            return path.startswith("/")
        if path == expected_scope_path:
            return True
        return path.startswith(f"{expected_scope_path}/")

    def _effective_port(self, parsed: SplitResult, display_name: str) -> int:
        try:
            port = parsed.port
        except ValueError as exc:
            raise RuntimeError(f"{display_name} 非法：端口格式错误") from exc
        if port is not None:
            return port
        if parsed.scheme.lower() == "https":
            return 443
        return 80

    def _get_api_base_url(self) -> str:
        parsed = self._parse_configured_https_url(
            config_key="api_base_url",
            default_value=self._default_api_base_url,
            display_name="api_base_url",
        )
        path = parsed.path.rstrip("/")
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))

    def _parse_pdf_base_url(self) -> SplitResult:
        return self._parse_configured_https_url(
            config_key="pdf_base_url",
            default_value=self._default_pdf_base_url,
            display_name="pdf_base_url",
        )

    def _parse_configured_https_url(
        self,
        *,
        config_key: str,
        default_value: str,
        display_name: str,
    ) -> SplitResult:
        raw_value = str(self._cfg(config_key, default_value)).strip()
        parsed = urlsplit(raw_value)
        if (not parsed.scheme) or (not parsed.netloc):
            raise RuntimeError(f"{display_name} 配置非法：必须为绝对 URL")
        if parsed.scheme.lower() != "https":
            raise RuntimeError(f"{display_name} 配置非法：协议必须为 https")
        if parsed.query or parsed.fragment:
            raise RuntimeError(f"{display_name} 配置非法：不能包含 query 或 fragment")
        self._effective_port(parsed, f"{display_name} 配置")
        return SplitResult(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc,
            path=parsed.path,
            query="",
            fragment="",
        )

    def _mask_url(self, url: str) -> str:
        text = str(url).strip()
        parsed = urlsplit(text)
        if not parsed.scheme and not parsed.netloc:
            return mask_sensitive_text(text)
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            return f"{base}?<已隐藏>"
        return base

    def _get_configured_proxy_url(self) -> str | None:
        raw_proxy = str(self._cfg("proxy_url", "") or "").strip()
        if not raw_proxy:
            return None
        parsed = urlsplit(raw_proxy)
        scheme = parsed.scheme.lower()
        if scheme == UNSUPPORTED_PROXY_SCHEME:
            raise RuntimeError("proxy_url 配置非法：暂不支持 socks5:// 代理，仅支持 http/https")
        if scheme not in SUPPORTED_PROXY_SCHEMES:
            raise RuntimeError("proxy_url 配置非法：仅支持 http/https 代理")
        if not parsed.netloc:
            raise RuntimeError("proxy_url 配置非法：必须为绝对代理 URL")
        return urlunsplit(
            (
                scheme,
                parsed.netloc,
                parsed.path,
                parsed.query,
                parsed.fragment,
            ),
        )
