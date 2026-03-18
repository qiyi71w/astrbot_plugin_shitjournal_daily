from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class RunStatus(str, Enum):
    __str__ = str.__str__

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"


class RunReason(str, Enum):
    __str__ = str.__str__

    RUN_IN_PROGRESS = "RUN_IN_PROGRESS"
    NO_TARGET_SESSION_CONFIGURED = "NO_TARGET_SESSION_CONFIGURED"
    FETCH_LATEST_FAILED = "FETCH_LATEST_FAILED"
    ALREADY_DELIVERED = "ALREADY_DELIVERED"
    LATEST_NOT_FOUND = "LATEST_NOT_FOUND"
    EMPTY_PAPER_ID = "EMPTY_PAPER_ID"
    DELIVERY_STATE_WRITE_FAILED = "DELIVERY_STATE_WRITE_FAILED"
    PREPARE_ASSETS_FAILED = "PREPARE_ASSETS_FAILED"
    PUSHED_SUCCESSFULLY = "PUSHED_SUCCESSFULLY"
    PUSHED_PARTIALLY = "PUSHED_PARTIALLY"
    ALL_SENDS_FAILED = "ALL_SENDS_FAILED"
    UNKNOWN = "UNKNOWN"


def _coerce_status(value: Any, default: RunStatus = RunStatus.UNKNOWN) -> RunStatus:
    if isinstance(value, RunStatus):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    try:
        return RunStatus(text)
    except ValueError:
        return default


def _coerce_reason(value: Any, default: RunReason = RunReason.UNKNOWN) -> RunReason:
    if isinstance(value, RunReason):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return RunReason(text)
    except ValueError:
        return default


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


@dataclass(slots=True)
class RunBatch:
    zone: str = ""
    latest: dict[str, Any] = field(default_factory=dict)
    paper_id: str = ""
    detail_url: str = ""
    targets: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "zone": self.zone,
            "latest": dict(self.latest),
            "paper_id": self.paper_id,
            "detail_url": self.detail_url,
            "targets": list(self.targets),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | RunBatch) -> RunBatch:
        if isinstance(payload, RunBatch):
            return payload
        return cls(
            zone=str(payload.get("zone", "")),
            latest=dict(payload.get("latest", {}) or {}),
            paper_id=str(payload.get("paper_id", "")),
            detail_url=str(payload.get("detail_url", "")),
            targets=[str(item) for item in payload.get("targets", []) or []],
        )


@dataclass(slots=True)
class RunBatchReport:
    status: RunStatus = RunStatus.FAILED
    reason_code: RunReason = RunReason.UNKNOWN
    zone: str = ""
    paper_id: str = ""
    detail_url: str = ""
    sent_ok: int = 0
    sent_total: int = 0
    debug_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "reason_code": self.reason_code.value,
            "zone": self.zone,
            "paper_id": self.paper_id,
            "detail_url": self.detail_url,
            "sent_ok": self.sent_ok,
            "sent_total": self.sent_total,
            "debug_reason": self.debug_reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | RunBatchReport) -> RunBatchReport:
        if isinstance(payload, RunBatchReport):
            return payload
        return cls(
            status=_coerce_status(payload.get("status"), default=RunStatus.FAILED),
            reason_code=_coerce_reason(payload.get("reason_code"), default=RunReason.UNKNOWN),
            zone=str(payload.get("zone", "")),
            paper_id=str(payload.get("paper_id", "")),
            detail_url=str(payload.get("detail_url", "")),
            sent_ok=int(payload.get("sent_ok", 0) or 0),
            sent_total=int(payload.get("sent_total", 0) or 0),
            debug_reason=str(payload.get("debug_reason", "")),
        )


@dataclass(slots=True)
class RunReport:
    status: RunStatus = RunStatus.FAILED
    source: str = ""
    zone: str = ""
    requested_zone: str = ""
    force: bool = False
    paper_id: str = ""
    detail_url: str = ""
    sent_ok: int = 0
    sent_total: int = 0
    reason_code: RunReason = RunReason.UNKNOWN
    debug_reason: str = ""
    latest_only: bool = False
    batches: list[RunBatchReport] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "source": self.source,
            "zone": self.zone,
            "requested_zone": self.requested_zone,
            "force": self.force,
            "paper_id": self.paper_id,
            "detail_url": self.detail_url,
            "sent_ok": self.sent_ok,
            "sent_total": self.sent_total,
            "reason_code": self.reason_code.value,
            "debug_reason": self.debug_reason,
            "latest_only": self.latest_only,
            "batches": [batch.to_dict() for batch in self.batches],
            "warnings": list(self.warnings),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | RunReport) -> RunReport:
        if isinstance(payload, RunReport):
            return payload
        return cls(
            status=_coerce_status(payload.get("status"), default=RunStatus.FAILED),
            source=str(payload.get("source", "")),
            zone=str(payload.get("zone", "")),
            requested_zone=str(payload.get("requested_zone", "")),
            force=_coerce_bool(payload.get("force"), default=False),
            paper_id=str(payload.get("paper_id", "")),
            detail_url=str(payload.get("detail_url", "")),
            sent_ok=int(payload.get("sent_ok", 0) or 0),
            sent_total=int(payload.get("sent_total", 0) or 0),
            reason_code=_coerce_reason(payload.get("reason_code"), default=RunReason.UNKNOWN),
            debug_reason=str(payload.get("debug_reason", "")),
            latest_only=_coerce_bool(payload.get("latest_only"), default=False),
            batches=[
                RunBatchReport.from_dict(item)
                for item in payload.get("batches", []) or []
            ],
            warnings=[str(item) for item in payload.get("warnings", []) or []],
        )


@dataclass(slots=True)
class PushRequest:
    targets: list[str] = field(default_factory=list)
    text: str = ""
    png_file: Path | None = None
    pdf_file: Path | None = None
    pdf_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "targets": list(self.targets),
            "text": self.text,
            "png_file": str(self.png_file) if self.png_file else "",
            "pdf_file": str(self.pdf_file) if self.pdf_file else "",
            "pdf_url": self.pdf_url,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | PushRequest) -> PushRequest:
        if isinstance(payload, PushRequest):
            return payload
        png_file = str(payload.get("png_file", "")).strip()
        pdf_file = str(payload.get("pdf_file", "")).strip()
        return cls(
            targets=[str(item) for item in payload.get("targets", []) or []],
            text=str(payload.get("text", "")),
            png_file=Path(png_file) if png_file else None,
            pdf_file=Path(pdf_file) if pdf_file else None,
            pdf_url=str(payload.get("pdf_url", "")),
        )
