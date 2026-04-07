from __future__ import annotations

import base64
import hashlib
import inspect
import uuid
from pathlib import Path
from typing import Any, Protocol


DEFAULT_CHUNK_SIZE_BYTES = 64 * 1024
DEFAULT_FILE_RETENTION_MS = 30 * 1000
STREAM_ACTION_NAME = "upload_file_stream"
STREAM_CHUNK_SUCCESS_STATUSES = frozenset({"chunk_received", "ok"})
STREAM_COMPLETE_SUCCESS_STATUSES = frozenset({"file_complete", "ok"})
STREAM_SUCCESS_TYPES = frozenset({"stream", "response"})


class OneBotCallAction(Protocol):
    def __call__(self, action: str, **params: Any) -> Any: ...


class NapCatStreamUploader:
    def __init__(
        self,
        *,
        chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
        file_retention_ms: int = DEFAULT_FILE_RETENTION_MS,
    ):
        self._chunk_size_bytes = max(1024, int(chunk_size_bytes))
        self._file_retention_ms = max(0, int(file_retention_ms))

    async def upload_pdf(self, *, call_action: OneBotCallAction, pdf_file: Path) -> str:
        if not pdf_file.exists() or not pdf_file.is_file():
            raise RuntimeError(f"Stream 上传文件不存在：{pdf_file}")
        file_size = pdf_file.stat().st_size
        if file_size <= 0:
            raise RuntimeError(f"Stream 上传文件为空：{pdf_file}")
        expected_sha256 = self._compute_sha256(pdf_file)
        total_chunks = self._calc_total_chunks(file_size)
        stream_id = str(uuid.uuid4())
        await self._upload_chunks(
            call_action=call_action,
            pdf_file=pdf_file,
            stream_id=stream_id,
            file_size=file_size,
            total_chunks=total_chunks,
            expected_sha256=expected_sha256,
        )
        response = await self._call_action(
            call_action,
            STREAM_ACTION_NAME,
            stream_id=stream_id,
            is_complete=True,
        )
        data = self._extract_complete_data(response)
        file_path = self._extract_file_path(data)
        if not file_path:
            raise RuntimeError(f"Stream 完成响应缺少 file_path：{data!r}")
        return file_path

    async def _upload_chunks(
        self,
        *,
        call_action: OneBotCallAction,
        pdf_file: Path,
        stream_id: str,
        file_size: int,
        total_chunks: int,
        expected_sha256: str,
    ) -> None:
        chunk_index = 0
        with pdf_file.open("rb") as f:
            while True:
                chunk = f.read(self._chunk_size_bytes)
                if not chunk:
                    break
                response = await self._call_action(
                    call_action,
                    STREAM_ACTION_NAME,
                    stream_id=stream_id,
                    chunk_data=base64.b64encode(chunk).decode("ascii"),
                    chunk_index=chunk_index,
                    total_chunks=total_chunks,
                    file_size=file_size,
                    expected_sha256=expected_sha256,
                    filename=pdf_file.name,
                    file_retention=self._file_retention_ms,
                )
                self._extract_chunk_data(response, stage=f"chunk:{chunk_index}")
                chunk_index += 1
        if chunk_index != total_chunks:
            raise RuntimeError(f"Stream 分片数量不一致：expected={total_chunks} actual={chunk_index}")

    async def _call_action(
        self,
        call_action: OneBotCallAction,
        action: str,
        **params: Any,
    ) -> Any:
        result = call_action(action, **params)
        if inspect.isawaitable(result):
            return await result
        return result

    def _compute_sha256(self, pdf_file: Path) -> str:
        hasher = hashlib.sha256()
        with pdf_file.open("rb") as f:
            while True:
                chunk = f.read(self._chunk_size_bytes)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    def _calc_total_chunks(self, file_size: int) -> int:
        full, remainder = divmod(file_size, self._chunk_size_bytes)
        return full + (1 if remainder else 0)

    def _extract_chunk_data(self, response: Any, *, stage: str) -> dict[str, Any]:
        payload = self._extract_payload(response, stage=stage)
        if self._is_chunk_success_payload(payload):
            return payload
        raise RuntimeError(f"Stream 请求失败：stage={stage} response={response!r}")

    def _extract_complete_data(self, response: Any) -> dict[str, Any]:
        payload = self._extract_payload(response, stage="complete")
        if self._is_complete_success_payload(payload):
            return payload
        raise RuntimeError(f"Stream 请求失败：stage=complete response={response!r}")

    def _extract_payload(self, response: Any, *, stage: str) -> dict[str, Any]:
        if not isinstance(response, dict):
            raise RuntimeError(f"Stream 响应不是字典：stage={stage} response={response!r}")
        if str(response.get("status", "")).lower() == "ok":
            data = response.get("data")
            if isinstance(data, dict):
                return data
            return {}
        return response

    def _is_chunk_success_payload(self, payload: dict[str, Any]) -> bool:
        status = str(payload.get("status", "")).lower()
        if status in STREAM_CHUNK_SUCCESS_STATUSES:
            return True
        if "received_chunks" in payload and "total_chunks" in payload:
            payload_type = str(payload.get("type", "")).lower()
            return payload_type in STREAM_SUCCESS_TYPES or not payload_type
        return False

    def _is_complete_success_payload(self, payload: dict[str, Any]) -> bool:
        if self._extract_file_path(payload):
            return True
        status = str(payload.get("status", "")).lower()
        if status in STREAM_COMPLETE_SUCCESS_STATUSES:
            return True
        payload_type = str(payload.get("type", "")).lower()
        return payload_type in STREAM_SUCCESS_TYPES and status == "complete"

    def _extract_file_path(self, data: dict[str, Any]) -> str:
        file_path = str(data.get("file_path", "")).strip()
        if file_path:
            return file_path
        payload = data.get("data")
        if isinstance(payload, dict):
            return str(payload.get("file_path", "")).strip()
        return ""
