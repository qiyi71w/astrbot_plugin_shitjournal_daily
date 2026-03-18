from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

DETAIL_URL_BASE = "https://shitjournal.org"
SCORE_PRECISION = 4
META_PREVIEW_KEYS = (
    "id",
    "manuscript_title",
    "author_name",
    "institution",
    "discipline",
    "viscosity",
    "created_at",
    "avg_score",
    "rating_count",
    "weighted_score",
    "pdf_path",
    "zone",
)
DEFAULT_DISCIPLINE_LABELS: dict[str, tuple[str, str]] = {
    "interdisciplinary": ("交叉", "Interdisciplinary"),
    "science": ("理", "Science"),
    "engineering": ("工", "Engineering"),
    "medical": ("医", "Medical"),
    "agriculture": ("农", "Agriculture"),
    "law_social": ("法社", "Law & Social"),
    "humanities": ("文", "Humanities"),
}
DEFAULT_VISCOSITY_LABELS: dict[str, tuple[str, str]] = {
    "stringy": ("拉丝型", "Stringy"),
    "semi": ("半固态", "Semi-solid"),
    "high-entropy": ("高熵态", "High-Entropy"),
}
DEFAULT_ZONE_LABELS: dict[str, tuple[str, str]] = {
    "latrine": ("旱厕", "The Latrine"),
    "septic": ("化粪池", "Septic Tank"),
    "stone": ("构石", "The Stone"),
    "sediment": ("沉淀区", "Sediment"),
}


class AssetPipeline:
    def __init__(
        self,
        *,
        fetch_submission_detail: Callable[[str], Awaitable[dict[str, Any]]],
        create_signed_pdf_url: Callable[[str], Awaitable[str]],
        download_pdf_file: Callable[[str, Path], Awaitable[tuple[int, str]]],
        build_output_paths: Callable[[str], tuple[Path, Path]],
        mark_in_use: Callable[[Path, Path], Awaitable[None]],
        release_temp_files: Callable[[Path | None, Path | None], Awaitable[None]],
        ensure_pdf_size_limit: Callable[[Path], None],
        export_first_page_png: Callable[[Path, Path], None],
        logger: Any,
        mask_sensitive_text: Callable[[str], str],
        detail_url_base: str = DETAIL_URL_BASE,
        detail_hide_domain: Callable[[], bool] | None = None,
        discipline_labels: dict[str, tuple[str, str]] | None = None,
        viscosity_labels: dict[str, tuple[str, str]] | None = None,
        zone_labels: dict[str, tuple[str, str]] | None = None,
    ):
        self._fetch_submission_detail = fetch_submission_detail
        self._create_signed_pdf_url = create_signed_pdf_url
        self._download_pdf_file = download_pdf_file
        self._build_output_paths = build_output_paths
        self._mark_in_use = mark_in_use
        self._release_temp_files = release_temp_files
        self._ensure_pdf_size_limit = ensure_pdf_size_limit
        self._export_first_page_png = export_first_page_png
        self._logger = logger
        self._mask_sensitive_text = mask_sensitive_text
        self._detail_url_base = str(detail_url_base).strip() or DETAIL_URL_BASE
        self._detail_hide_domain = detail_hide_domain or (lambda: False)
        self._discipline_labels = discipline_labels or DEFAULT_DISCIPLINE_LABELS
        self._viscosity_labels = viscosity_labels or DEFAULT_VISCOSITY_LABELS
        self._zone_labels = zone_labels or DEFAULT_ZONE_LABELS

    async def load_submission_payload(
        self,
        latest: dict[str, Any],
        paper_id: str,
    ) -> dict[str, Any]:
        payload = dict(latest)
        if self._extract_pdf_key(payload):
            return payload

        try:
            detail = await self._fetch_submission_detail(paper_id)
        except Exception:
            self._logger.warning("获取论文详情失败，将回退为列表页载荷。", exc_info=True)
            detail = {}
        return {**payload, **(detail or {})}

    async def prepare_pdf_assets(self, payload: dict[str, Any], paper_id: str) -> tuple[Path, Path, str]:
        pdf_key = self._extract_pdf_key(payload)
        if not pdf_key:
            raise RuntimeError("PDF 路径缺失")

        signed_url = await self._load_signed_url(pdf_key)
        if not signed_url:
            raise RuntimeError("签名 URL 为空")

        self._logger.info("已取得签名 PDF 地址：%s", self._mask_token(signed_url))
        pdf_file, png_file = self._build_output_paths(paper_id)
        await self._mark_in_use(pdf_file, png_file)

        try:
            await self._download_pdf(signed_url, pdf_file)
            self._ensure_pdf_size_limit(pdf_file)
            await self._export_first_page(pdf_file, png_file)
            png_size = png_file.stat().st_size if png_file.exists() else 0
            self._logger.info("PDF 首页预览图导出完成：文件=%s 字节数=%s", str(png_file), png_size)
            return pdf_file, png_file, signed_url
        except Exception:
            await self._release_temp_files(pdf_file, png_file)
            raise

    def build_meta_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {key: payload.get(key) for key in META_PREVIEW_KEYS}

    def build_preprint_detail_url(self, paper_id: str) -> str:
        normalized_paper_id = str(paper_id).strip()
        return f"{self._detail_url_base}/preprints/{normalized_paper_id}"

    def build_push_text(
        self,
        payload: dict[str, Any],
        detail_url: str,
        *,
        zone: str = "",
    ) -> str:
        title = self._fallback_text(payload.get("manuscript_title"))
        author = self._fallback_text(payload.get("author_name"))
        institution = self._fallback_text(payload.get("institution"))
        zone_text = self._format_bilingual_label(zone or payload.get("zone"), self._zone_labels)
        submitted = self._format_datetime(payload.get("created_at"))
        discipline = self._format_bilingual_label(payload.get("discipline"), self._discipline_labels)
        viscosity = self._format_bilingual_label(payload.get("viscosity"), self._viscosity_labels)
        avg_score = self._format_number(payload.get("avg_score"))
        weighted_score = self._format_number(payload.get("weighted_score"))
        rating_count = self._fallback_text(payload.get("rating_count"))
        detail_text = self._format_detail_text(detail_url)
        lines = [
            "S.H.I.T Journal 论文推送",
            f"分区: {zone_text}",
            f"标题: {title}",
            f"作者: {author}",
            f"单位: {institution}",
            f"提交时间: {submitted}",
            f"学科: {discipline}",
            f"粘度: {viscosity}",
            f"评分: 平均={avg_score}, 加权={weighted_score}, 票数={rating_count}",
            f"详情: {detail_text}",
        ]
        return "\n".join(lines)

    def _format_detail_text(self, detail_url: str) -> str:
        detail_text = str(detail_url or "").strip()
        if not detail_text:
            return detail_text
        if not self._detail_hide_domain():
            return detail_text
        if detail_text.startswith(self._detail_url_base):
            path = detail_text[len(self._detail_url_base) :].strip()
            return path if path.startswith("/") else f"/{path}"
        return detail_text

    async def _load_signed_url(self, pdf_key: str) -> str:
        try:
            return await self._create_signed_pdf_url(pdf_key)
        except Exception as exc:
            self._logger.error(
                "生成签名 URL 失败：%s",
                self._mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            raise RuntimeError("生成签名 URL 失败") from exc

    async def _download_pdf(self, signed_url: str, pdf_file: Path) -> None:
        try:
            pdf_status, pdf_type = await self._download_pdf_file(signed_url, pdf_file)
            self._logger.info("PDF 下载完成：状态码=%s 内容类型=%s", pdf_status, pdf_type)
        except Exception as exc:
            self._logger.error(
                "下载 PDF 失败：%s",
                self._mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            raise RuntimeError("下载 PDF 失败") from exc

    async def _export_first_page(self, pdf_file: Path, png_file: Path) -> None:
        try:
            await asyncio.to_thread(self._export_first_page_png, pdf_file, png_file)
        except Exception as exc:
            self._logger.error(
                "导出 PDF 首页预览图失败：%s",
                self._mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            raise RuntimeError("导出 PDF 首页预览图失败") from exc

    def _extract_pdf_key(self, payload: dict[str, Any]) -> str:
        return str(payload.get("pdf_path") or payload.get("file_path") or "").strip()

    def _format_datetime(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return "无"
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return text

    def _format_number(self, value: Any) -> str:
        if value is None or value == "":
            return "无"
        try:
            return f"{float(value):.{SCORE_PRECISION}f}"
        except Exception:
            return str(value)

    def _fallback_text(self, value: Any) -> str:
        text = str(value or "").strip()
        return text if text else "无"

    def _format_bilingual_label(
        self,
        raw_value: Any,
        mapping: dict[str, tuple[str, str]],
    ) -> str:
        text = str(raw_value or "").strip()
        if not text:
            return "无"
        key = text.lower()
        if key in mapping:
            cn, en = mapping[key]
            return f"{cn} / {en}"
        return text

    def _mask_token(self, url: str) -> str:
        if "token=" not in url:
            return url
        head, _, _ = url.partition("token=")
        return head + "token=<已隐藏>"
