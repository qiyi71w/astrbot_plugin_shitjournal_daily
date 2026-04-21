from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from .models import RunBatchReport, RunReason, RunReport, RunStatus
from .question_selector import QuestionSelectionError


class QuestionRunCycleService:
    def __init__(
        self,
        *,
        cfg_getter: Callable[[str, Any], Any],
        question_selector: Any,
        question_history_store: Any,
        question_batch_sender: Any,
        get_all_target_sessions: Callable[[Any], Awaitable[list[str]] | list[str]],
        get_primary_zone: Callable[[], str],
        get_candidate_zones: Callable[[str], list[str]],
        logger: Any,
        mask_sensitive_text: Callable[[str], str],
        run_fetch_page_size: int,
    ):
        self._cfg_getter = cfg_getter
        self._question_selector = question_selector
        self._question_history_store = question_history_store
        self._question_batch_sender = question_batch_sender
        self._get_all_target_sessions = get_all_target_sessions
        self._get_primary_zone = get_primary_zone
        self._get_candidate_zones = get_candidate_zones
        self._logger = logger
        self._mask_sensitive_text = mask_sensitive_text
        self._run_fetch_page_size = int(run_fetch_page_size)
        self._run_lock = asyncio.Lock()

    async def run_cycle(self, *, force: bool, source: str, latest_only: bool = False) -> RunReport:
        if self._run_lock.locked():
            return RunReport(status=RunStatus.SKIPPED, reason_code=RunReason.RUN_IN_PROGRESS, source=source, latest_only=latest_only)
        async with self._run_lock:
            return await self._run_cycle_locked(force=force, source=source, latest_only=latest_only)

    async def _run_cycle_locked(self, *, force: bool, source: str, latest_only: bool) -> RunReport:
        plan = await self.plan_run_cycle(force=force, source=source, latest_only=latest_only)
        selection = plan.get("selection")
        batch_reports: list[RunBatchReport] = []
        if selection is not None and selection.batches:
            batch_reports = await self._question_batch_sender.send_run_batches(selection.batches)
        return await self.finalize_run_cycle(plan=plan, batch_reports=batch_reports)

    async def plan_run_cycle(self, *, force: bool, source: str, latest_only: bool = False) -> dict[str, Any]:
        primary_zone = self._get_primary_zone()
        report = RunReport(
            status=RunStatus.FAILED,
            source=source,
            zone=primary_zone,
            requested_zone=primary_zone,
            force=force,
            latest_only=latest_only,
        )
        targets = await self._resolve_targets()
        if not targets:
            report.reason_code = RunReason.NO_TARGET_SESSION_CONFIGURED
            return {
                "report": report,
                "primary_zone": primary_zone,
                "previous_last_seen": {},
                "selection": None,
                "batches": [],
            }
        previous_last_seen = await self._question_history_store.get_last_seen_map()
        selection = await self._select_batches(
            report,
            primary_zone,
            targets,
            previous_last_seen,
            force,
            latest_only,
        )
        if selection is not None:
            report.warnings = list(selection.warnings)
        return {
            "report": report,
            "primary_zone": primary_zone,
            "previous_last_seen": previous_last_seen,
            "selection": selection,
            "batches": list(selection.batches) if selection is not None else [],
        }

    async def finalize_run_cycle(
        self,
        *,
        plan: dict[str, Any],
        batch_reports: list[RunBatchReport | dict[str, Any]] | None = None,
    ) -> RunReport:
        report = plan["report"]
        selection = plan.get("selection")
        if selection is None:
            return report
        previous_last_seen = dict(plan.get("previous_last_seen", {}))
        if not selection.batches:
            await self._persist_last_seen_map_if_changed(previous_last_seen, selection.last_seen_map)
            return self._finalize_empty_selection(report, selection)
        if batch_reports is None:
            raise RuntimeError("question run cycle finalize 缺少批次发送报告")
        normalized_reports = [RunBatchReport.from_dict(item) for item in batch_reports]
        self._apply_batch_reports(report, normalized_reports, str(plan.get("primary_zone", "")))
        await self._persist_last_seen_map_if_changed(previous_last_seen, selection.last_seen_map)
        report.status, report.reason_code = self._question_batch_sender.resolve_run_batch_reports(
            batch_reports=normalized_reports,
            sent_ok=report.sent_ok,
            sent_total=report.sent_total,
        )
        return report

    async def _resolve_targets(self) -> list[str]:
        result = self._get_all_target_sessions(self._cfg_getter("target_sessions", []))
        if asyncio.iscoroutine(result):
            return list(await result)
        return list(result)

    async def _select_batches(self, report, primary_zone, targets, previous_last_seen, force, latest_only):
        try:
            return await self._question_selector.select_run_batches(
                zone_order=self._get_candidate_zones(primary_zone),
                targets=targets,
                last_seen_map=previous_last_seen,
                force=force,
                latest_only=latest_only,
                fetch_page_size=self._run_fetch_page_size,
            )
        except QuestionSelectionError as exc:
            return self._apply_selection_error(report, str(exc))
        except Exception as exc:
            return self._apply_selection_error(report, str(exc))

    def _apply_selection_error(self, report: RunReport, message: str):
        masked_message = self._mask_sensitive_text(message)
        self._logger.error("questions 抓取失败：%s", masked_message, exc_info=True)
        report.reason_code = RunReason.FETCH_LATEST_FAILED
        report.debug_reason = masked_message
        return None

    def _finalize_empty_selection(self, report: RunReport, selection: Any) -> RunReport:
        if selection.warnings:
            report.status = RunStatus.FAILED
            report.reason_code = RunReason.FETCH_LATEST_FAILED
            report.debug_reason = "\n".join(selection.warnings)
            return report
        report.status = RunStatus.SKIPPED
        report.reason_code = RunReason.ALREADY_DELIVERED if selection.saw_question else RunReason.LATEST_NOT_FOUND
        return report

    def _apply_batch_reports(self, report: RunReport, batch_reports: list[RunBatchReport], primary_zone: str) -> None:
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
            if batch.status != RunStatus.FAILED:
                continue
            reason = self._mask_sensitive_text(str(batch.debug_reason).strip())
            if reason:
                return reason
        for batch in batch_reports:
            reason = self._mask_sensitive_text(str(batch.debug_reason).strip())
            if reason:
                return reason
        return ""

    async def _persist_last_seen_map_if_changed(self, previous_last_seen: dict[str, str], next_last_seen_map: dict[str, str]) -> None:
        if previous_last_seen != next_last_seen_map:
            await self._question_history_store.set_last_seen_map(next_last_seen_map)
