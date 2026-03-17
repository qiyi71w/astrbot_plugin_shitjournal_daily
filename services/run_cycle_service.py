from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from .models import RunBatch, RunBatchReport, RunReason, RunReport, RunStatus
from .run_selector import RunSelectionError, RunSelectionResult
from .warning_text import join_warning_text


class RunCycleService:
    def __init__(
        self,
        *,
        cfg_getter: Callable[[str, Any], Any],
        run_selector: Any,
        history_store: Any,
        send_batches: Callable[..., Awaitable[list[RunBatchReport | dict[str, Any]]]],
        coerce_run_batch_report: Callable[[RunBatchReport | dict[str, Any]], RunBatchReport],
        resolve_run_batch_reports: Callable[[list[RunBatchReport], int, int], tuple[RunStatus, RunReason]],
        get_primary_zone: Callable[[], str],
        get_candidate_zones: Callable[[str], list[str]],
        get_configured_send_concurrency: Callable[[], int],
        maybe_trim_temp_files: Callable[[], Awaitable[None]],
        logger: Any,
        mask_sensitive_text: Callable[[str], str],
        run_fetch_page_size: int,
    ):
        self._cfg_getter = cfg_getter
        self._run_selector = run_selector
        self._history_store = history_store
        self._send_batches = send_batches
        self._coerce_run_batch_report = coerce_run_batch_report
        self._resolve_run_batch_reports = resolve_run_batch_reports
        self._get_primary_zone = get_primary_zone
        self._get_candidate_zones = get_candidate_zones
        self._get_configured_send_concurrency = get_configured_send_concurrency
        self._maybe_trim_temp_files = maybe_trim_temp_files
        self._logger = logger
        self._mask_sensitive_text = mask_sensitive_text
        self._run_fetch_page_size = int(run_fetch_page_size)
        self._run_lock = asyncio.Lock()

    async def run_cycle(self, *, force: bool, source: str, latest_only: bool = False) -> RunReport:
        if self._run_lock.locked():
            return self._build_run_in_progress_report(source=source, latest_only=latest_only)
        async with self._run_lock:
            return await self._run_cycle_locked(force=force, source=source, latest_only=latest_only)

    async def _run_cycle_locked(self, *, force: bool, source: str, latest_only: bool) -> RunReport:
        primary_zone = self._get_primary_zone()
        zone_order = self._get_candidate_zones(primary_zone)
        report = self._build_run_cycle_report(
            source=source,
            primary_zone=primary_zone,
            force=force,
            latest_only=latest_only,
        )
        self._logger.info(
            "开始执行推送：来源=%s 主分区=%s 分区顺序=%s 强制=%s 仅最新=%s",
            source,
            primary_zone,
            zone_order,
            force,
            latest_only,
        )
        try:
            targets = await self._history_store.get_all_target_sessions(self._cfg_getter("target_sessions", []))
            if not targets:
                report.reason_code = RunReason.NO_TARGET_SESSION_CONFIGURED
                return report
            previous_last_seen, selection = await self._select_run_cycle_batches(
                report=report,
                zone_order=zone_order,
                targets=targets,
                force=force,
                latest_only=latest_only,
            )
            if selection is None:
                return report
            return await self._apply_selection(report, primary_zone, previous_last_seen, selection)
        finally:
            await self._trim_temp_files_after_run()

    async def _apply_selection(
        self,
        report: RunReport,
        primary_zone: str,
        previous_last_seen: dict[str, str],
        selection: RunSelectionResult,
    ) -> RunReport:
        if not selection.batches:
            return await self._finalize_empty_run_selection(
                report=report,
                selection=selection,
                previous_last_seen=previous_last_seen,
            )
        batch_reports = await self._deliver_selected_run_batches(selection.batches)
        self._apply_run_batch_reports_to_report(report, batch_reports, primary_zone)
        await self._persist_last_seen_map_if_changed(previous_last_seen, selection.last_seen_map)
        report.status, report.reason_code = self._resolve_run_batch_reports(
            batch_reports,
            report.sent_ok,
            report.sent_total,
        )
        return report

    async def _trim_temp_files_after_run(self) -> None:
        try:
            await self._maybe_trim_temp_files()
        except Exception:
            self._logger.warning("执行推送后清理临时文件失败。", exc_info=True)

    def _build_run_in_progress_report(self, *, source: str, latest_only: bool) -> RunReport:
        return RunReport(
            status=RunStatus.SKIPPED,
            reason_code=RunReason.RUN_IN_PROGRESS,
            source=source,
            debug_reason="",
            latest_only=latest_only,
        )

    def _build_run_cycle_report(
        self,
        *,
        source: str,
        primary_zone: str,
        force: bool,
        latest_only: bool,
    ) -> RunReport:
        return RunReport(
            status=RunStatus.FAILED,
            source=source,
            zone=primary_zone,
            requested_zone=primary_zone,
            force=force,
            latest_only=latest_only,
        )

    async def _select_run_cycle_batches(
        self,
        *,
        report: RunReport,
        zone_order: list[str],
        targets: list[str],
        force: bool,
        latest_only: bool,
    ) -> tuple[dict[str, str], RunSelectionResult | None]:
        previous_last_seen = await self._history_store.get_last_seen_map()
        try:
            selection = await self._run_selector.select_run_batches(
                zone_order=zone_order,
                targets=targets,
                last_seen_map=previous_last_seen,
                force=force,
                latest_only=latest_only,
                fetch_page_size=self._run_fetch_page_size,
            )
        except RunSelectionError as exc:
            self._apply_run_selection_error(report, self._coerce_run_reason(exc.reason_code), str(exc).strip())
            return previous_last_seen, None
        except RuntimeError as exc:
            self._apply_run_selection_error(report, RunReason.FETCH_LATEST_FAILED, str(exc).strip())
            return previous_last_seen, None
        report.warnings = selection.warnings
        return previous_last_seen, selection

    def _apply_run_selection_error(self, report: RunReport, reason_code: RunReason, message: str) -> None:
        report.reason_code = reason_code
        self._logger.error("获取最新论文失败：%s", self._mask_sensitive_text(message), exc_info=True)
        report.debug_reason = self._mask_sensitive_text(message)

    async def _finalize_empty_run_selection(
        self,
        *,
        report: RunReport,
        selection: RunSelectionResult,
        previous_last_seen: dict[str, str],
    ) -> RunReport:
        if selection.warnings:
            report.status = RunStatus.FAILED
            report.reason_code = RunReason.FETCH_LATEST_FAILED
            report.debug_reason = join_warning_text(selection.warnings, self._mask_sensitive_text)
        else:
            report.status = RunStatus.SKIPPED
            report.reason_code = RunReason.ALREADY_DELIVERED if selection.saw_submission else RunReason.LATEST_NOT_FOUND
        await self._persist_last_seen_map_if_changed(previous_last_seen, selection.last_seen_map)
        return report

    async def _deliver_selected_run_batches(self, batches: list[RunBatch]) -> list[RunBatchReport]:
        raw_reports = await self._send_batches(
            batches,
            send_semaphore=asyncio.Semaphore(self._get_configured_send_concurrency()),
        )
        return [self._coerce_run_batch_report(item) for item in raw_reports]

    def _apply_run_batch_reports_to_report(
        self,
        report: RunReport,
        batch_reports: list[RunBatchReport],
        primary_zone: str,
    ) -> None:
        report.batches = batch_reports
        report.sent_ok = sum(batch.sent_ok for batch in batch_reports)
        report.sent_total = sum(batch.sent_total for batch in batch_reports)
        if batch_reports:
            first = batch_reports[0]
            report.zone = first.zone or primary_zone
            report.paper_id = first.paper_id
            report.detail_url = first.detail_url
        report.debug_reason = self._pick_first_debug_reason(batch_reports)

    def _pick_first_debug_reason(self, batch_reports: list[RunBatchReport]) -> str:
        for batch in batch_reports:
            reason = self._mask_sensitive_text(str(batch.debug_reason).strip())
            if reason:
                return reason
        return ""

    async def _persist_last_seen_map_if_changed(
        self,
        previous_last_seen: dict[str, str],
        next_last_seen_map: dict[str, str],
    ) -> None:
        if previous_last_seen != next_last_seen_map:
            await self._history_store.set_last_seen_map(next_last_seen_map)

    def _coerce_run_reason(self, value: RunReason | str) -> RunReason:
        if isinstance(value, RunReason):
            return value
        try:
            return RunReason(str(value).strip())
        except ValueError:
            return RunReason.UNKNOWN
