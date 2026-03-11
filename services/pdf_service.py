from __future__ import annotations

from pathlib import Path
from typing import Callable

import fitz


class PdfService:
    def __init__(self, cfg_int_getter: Callable[..., int]):
        self._cfg_int = cfg_int_getter

    def ensure_pdf_size_limit(self, pdf_file: Path) -> None:
        if not pdf_file.exists():
            raise RuntimeError(f"PDF 文件不存在：{pdf_file}")
        if not pdf_file.is_file():
            raise RuntimeError(f"PDF 路径不是文件：{pdf_file}")

        max_pdf_mb = self._cfg_int("pdf_max_size_mb", 50, min_value=1, max_value=512)
        max_pdf_bytes = max_pdf_mb * 1024 * 1024
        pdf_size = pdf_file.stat().st_size
        if pdf_size > max_pdf_bytes:
            raise RuntimeError(
                f"PDF 文件过大：{pdf_size} 字节，超过配置上限 {max_pdf_bytes} 字节",
            )

    def export_first_page_png(self, pdf_file: Path, png_file: Path) -> None:
        dpi = self._cfg_int("pdf_dpi", 170, min_value=72, max_value=300)
        pdf = fitz.open(str(pdf_file))
        try:
            if pdf.page_count < 1:
                raise RuntimeError("PDF 没有任何页面")
            page = pdf.load_page(0)
            scale = dpi / 72.0
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            pix.save(str(png_file))
        finally:
            pdf.close()
