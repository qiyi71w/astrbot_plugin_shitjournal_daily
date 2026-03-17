from __future__ import annotations

from typing import Any, Callable

from .models import RunReport

DETAIL_URL_BASE = "https://shitjournal.org"


class ReportRenderer:
    def __init__(
        self,
        *,
        status_labels: dict[str, str],
        reason_labels: dict[str, str],
        mask_sensitive_text: Callable[[str], str],
        detail_url_base: str = DETAIL_URL_BASE,
        detail_hide_domain: Callable[[], bool] | None = None,
    ):
        self._status_labels = dict(status_labels)
        self._reason_labels = dict(reason_labels)
        self._mask_sensitive_text = mask_sensitive_text
        self._detail_url_base = str(detail_url_base).strip() or DETAIL_URL_BASE
        self._detail_hide_domain = detail_hide_domain or (lambda: False)

    def render_report(self, report: RunReport | dict[str, Any], include_debug: bool = False) -> str:
        report_dict = self._to_run_report_dict(report)
        status = str(report_dict.get("status", "unknown") or "unknown")
        reason_code = str(report_dict.get("reason_code") or report_dict.get("reason") or "UNKNOWN")
        debug_reason = self._mask_sensitive_text(str(report_dict.get("debug_reason", "")).strip())
        latest_only = self._to_bool(report_dict.get("latest_only"), False)
        requested_zone = str(report_dict.get("requested_zone", "")).strip()
        warnings = self._clean_warning_list(report_dict.get("warnings", []))
        status_text = self._render_status_text(status)
        reason_text = self._render_reason_text(reason_code)
        batches = list(report_dict.get("batches", []))
        if len(batches) > 1:
            return self._render_multi_batch_report(
                report=report_dict,
                status_text=status_text,
                reason_text=reason_text,
                latest_only=latest_only,
                requested_zone=requested_zone,
                warnings=warnings,
                debug_reason=debug_reason,
                include_debug=include_debug,
            )
        if len(batches) == 1:
            return self._render_single_batch_report(
                report=report_dict,
                batch=batches[0],
                status_text=status_text,
                reason_text=reason_text,
                latest_only=latest_only,
                requested_zone=requested_zone,
                warnings=warnings,
                debug_reason=debug_reason,
                include_debug=include_debug,
            )
        return self._render_single_batch_report(
            report=report_dict,
            batch=self._build_empty_batch(report_dict),
            status_text=status_text,
            reason_text=reason_text,
            latest_only=latest_only,
            requested_zone=requested_zone,
            warnings=warnings,
            debug_reason=debug_reason,
            include_debug=include_debug,
        )

    def _build_empty_batch(self, report: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": str(report.get("status", "unknown") or "unknown"),
            "zone": str(report.get("zone", "")).strip(),
            "paper_id": str(report.get("paper_id", "")).strip(),
            "detail_url": str(report.get("detail_url", "")).strip(),
            "sent_ok": report.get("sent_ok", 0),
            "sent_total": report.get("sent_total", 0),
        }

    def _to_run_report_dict(self, report: RunReport | dict[str, Any]) -> dict[str, Any]:
        if isinstance(report, RunReport):
            return report.to_dict()
        return dict(report)

    def _to_bool(self, value: Any, default: bool = False) -> bool:
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

    def _render_status_text(self, status: str) -> str:
        return self._status_labels.get(status, status)

    def _render_reason_text(self, reason_code: str) -> str:
        return self._reason_labels.get(reason_code, reason_code)

    def _clean_warning_list(self, raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        warnings: list[str] = []
        seen: set[str] = set()
        for item in raw:
            text = self._mask_sensitive_text(str(item).strip())
            if not text or text in seen:
                continue
            seen.add(text)
            warnings.append(text)
        return warnings

    def _append_warning_lines(self, lines: list[str], warnings: list[str]) -> None:
        for index, warning in enumerate(self._clean_warning_list(warnings), start=1):
            lines.append(f"告警{index}: {warning}")

    def _render_run_mode_text(self, latest_only: bool) -> str:
        if latest_only:
            return "模式: 仅检查最新一篇"
        return ""

    def _render_report_header(self, status_text: str, reason_text: str) -> list[str]:
        return [
            f"[shitjournal 执行结果] 状态: {status_text}",
            f"原因: {reason_text}",
        ]

    def _render_report_batch_line(self, index: int, batch: dict[str, Any]) -> list[str]:
        detail_text = self._format_detail_text(str(batch.get("detail_url", "")))
        batch_status = str(batch.get("status", "unknown") or "unknown")
        batch_status_text = self._render_status_text(batch_status)
        batch_reason_code = str(batch.get("reason_code", "")).strip()
        batch_reason_text = self._render_reason_text(batch_reason_code) if batch_reason_code else ""
        suffix = f" 原因={batch_reason_text}" if batch_status == "failed" and batch_reason_text else ""
        return [
            (
                f"第{index}批: 状态={batch_status_text} 分区={batch.get('zone', '')} "
                f"论文ID={batch.get('paper_id', '')} "
                f"推送={batch.get('sent_ok', 0)}/{batch.get('sent_total', 0)} "
                f"详情={detail_text}{suffix}"
            ),
        ]

    def _append_report_suffix(
        self,
        *,
        lines: list[str],
        latest_only: bool,
        requested_zone: str,
        report_zone: str,
        warnings: list[str],
        debug_reason: str,
        include_debug: bool,
    ) -> str:
        mode_text = self._render_run_mode_text(latest_only)
        if mode_text:
            lines.append(mode_text)
        if requested_zone and requested_zone != report_zone:
            lines.append(f"请求分区: {requested_zone}")
        self._append_warning_lines(lines, warnings)
        if include_debug and debug_reason:
            lines.append(f"调试信息: {debug_reason}")
        return "\n".join(lines)

    def _render_single_batch_report(
        self,
        *,
        report: dict[str, Any],
        batch: dict[str, Any],
        status_text: str,
        reason_text: str,
        latest_only: bool,
        requested_zone: str,
        warnings: list[str],
        debug_reason: str,
        include_debug: bool,
    ) -> str:
        lines = self._render_report_header(status_text, reason_text)
        batch_reason_code = str(batch.get("reason_code", "")).strip()
        lines.extend(
            [
                f"批次状态: {self._render_status_text(str(batch.get('status', 'unknown') or 'unknown'))}",
                f"分区: {batch.get('zone', '')}",
                f"论文 ID: {batch.get('paper_id', '')}",
                f"推送: {batch.get('sent_ok', 0)}/{batch.get('sent_total', 0)}",
                f"详情: {self._format_detail_text(str(batch.get('detail_url', '')))}",
            ]
        )
        if batch_reason_code and batch_reason_code != "PUSHED_SUCCESSFULLY":
            lines.append(f"批次原因: {self._render_reason_text(batch_reason_code)}")
        return self._append_report_suffix(
            lines=lines,
            latest_only=latest_only,
            requested_zone=requested_zone,
            report_zone=str(report.get("zone", "")).strip(),
            warnings=warnings,
            debug_reason=debug_reason,
            include_debug=include_debug,
        )

    def _render_multi_batch_report(
        self,
        *,
        report: dict[str, Any],
        status_text: str,
        reason_text: str,
        latest_only: bool,
        requested_zone: str,
        warnings: list[str],
        debug_reason: str,
        include_debug: bool,
    ) -> str:
        batches = list(report.get("batches", []))
        lines = self._render_report_header(status_text, reason_text)
        lines.append(f"总批次: {len(batches)}")
        lines.append(f"总推送: {report.get('sent_ok', 0)}/{report.get('sent_total', 0)}")
        for index, batch in enumerate(batches, start=1):
            lines.extend(self._render_report_batch_line(index, batch))
        return self._append_report_suffix(
            lines=lines,
            latest_only=latest_only,
            requested_zone=requested_zone,
            report_zone=str(report.get("zone", "")).strip(),
            warnings=warnings,
            debug_reason=debug_reason,
            include_debug=include_debug,
        )

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
