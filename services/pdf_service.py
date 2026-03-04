from __future__ import annotations

from pathlib import Path
from typing import Callable

import fitz


class PdfService:
    def __init__(self, cfg_int_getter: Callable[..., int]):
        self._cfg_int = cfg_int_getter

    def ensure_pdf_size_limit(self, pdf_file: Path) -> None:
        max_pdf_mb = self._cfg_int("pdf_max_size_mb", 50, min_value=1, max_value=512)
        max_pdf_bytes = max_pdf_mb * 1024 * 1024
        pdf_size = pdf_file.stat().st_size if pdf_file.exists() else 0
        if pdf_size > max_pdf_bytes:
            raise RuntimeError(
                f"pdf too large: {pdf_size} bytes exceeds configured limit {max_pdf_bytes} bytes",
            )

    def export_first_page_png(self, pdf_file: Path, png_file: Path) -> None:
        dpi = self._cfg_int("pdf_dpi", 170, min_value=72, max_value=300)
        pdf = fitz.open(str(pdf_file))
        try:
            if pdf.page_count < 1:
                raise RuntimeError("pdf has no pages")
            page = pdf.load_page(0)
            scale = dpi / 72.0
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            pix.save(str(png_file))
        finally:
            pdf.close()
