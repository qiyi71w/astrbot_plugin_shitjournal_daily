from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable

from astrbot.api import logger


class TempFileManager:
    def __init__(self, temp_dir: Path, cfg_int_getter: Callable[..., int]):
        self._temp_dir = temp_dir
        self._cfg_int = cfg_int_getter
        self._temp_files_lock = asyncio.Lock()
        self._active_temp_files: set[str] = set()

    def set_temp_dir(self, temp_dir: Path) -> None:
        self._temp_dir = temp_dir

    def build_output_paths(self, paper_id: str) -> tuple[Path, Path]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        nonce = uuid.uuid4().hex[:8]
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", paper_id)
        pdf_file = self._temp_dir / f"{ts}_{safe_id}_{nonce}.pdf"
        png_file = self._temp_dir / f"{ts}_{safe_id}_{nonce}_p1.png"
        return pdf_file, png_file

    async def mark_in_use(self, *paths: Path) -> None:
        async with self._temp_files_lock:
            for path in paths:
                self._active_temp_files.add(self._temp_file_key(path))

    async def release(self, *paths: Path | None) -> None:
        valid_paths = [path for path in paths if path is not None]
        if not valid_paths:
            return
        async with self._temp_files_lock:
            for path in valid_paths:
                self._active_temp_files.discard(self._temp_file_key(path))

    async def trim(self) -> None:
        keep = self._cfg_int("temp_keep_files", 30, min_value=0)
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        async with self._temp_files_lock:
            active_keys = set(self._active_temp_files)
        files: list[tuple[float, Path]] = []
        for file_path in self._temp_dir.iterdir():
            if not file_path.is_file():
                continue
            try:
                files.append((file_path.stat().st_mtime, file_path))
            except FileNotFoundError:
                continue
        files.sort(key=lambda item: item[0], reverse=True)

        delete_candidates: list[Path] = []
        kept_inactive = 0
        for _, file_path in files:
            file_key = self._temp_file_key(file_path)
            if file_key in active_keys:
                continue
            if kept_inactive < keep:
                kept_inactive += 1
                continue
            delete_candidates.append(file_path)

        for file_path in delete_candidates:
            file_key = self._temp_file_key(file_path)
            async with self._temp_files_lock:
                if file_key in self._active_temp_files:
                    continue
                try:
                    file_path.unlink(missing_ok=True)
                except FileNotFoundError:
                    continue
                except Exception:
                    logger.warning("删除临时文件失败：%s", str(file_path), exc_info=True)

    def _temp_file_key(self, path: Path) -> str:
        try:
            return str(path.resolve())
        except Exception:
            return str(path)
