from __future__ import annotations
import asyncio
import json
from pathlib import Path
from typing import Any, Awaitable, Callable
from .models import RunBatch, RunBatchReport, RunReason, RunStatus
class RunBatchSender:
    def __init__(
        self,
        *,
        context_getter: Callable[[], Any],
        cfg_int_getter: Callable[[str, int, int | None, int | None], int],
        history_store: Any,
        load_submission_payload: Callable[[dict[str, Any], str], Awaitable[dict[str, Any]]],
        prepare_pdf_assets: Callable[[dict[str, Any], str], Awaitable[tuple[Path, Path, str]]],
        build_push_text: Callable[[dict[str, Any], str, str], str],
        build_meta_preview: Callable[[dict[str, Any]], dict[str, Any]],
        send_session_push: Callable[..., Awaitable[bool]],
        release_temp_files: Callable[[Path | None, Path | None], Awaitable[None]],
        maybe_trim_temp_files: Callable[[], Awaitable[None]],
        logger: Any,
        mask_sensitive_text: Callable[[str], str],
        max_send_concurrency: int,
        max_batch_send_concurrency: int,
    ):
        self._context_getter = context_getter
        self._cfg_int_getter = cfg_int_getter
        self._history_store = history_store
        self._load_submission_payload = load_submission_payload
        self._prepare_pdf_assets = prepare_pdf_assets
        self._build_push_text = build_push_text
        self._build_meta_preview = build_meta_preview
        self._send_session_push = send_session_push
        self._release_temp_files = release_temp_files
        self._maybe_trim_temp_files = maybe_trim_temp_files
        self._logger = logger
        self._mask_sensitive_text = mask_sensitive_text
        self._max_send_concurrency = int(max_send_concurrency)
        self._max_batch_send_concurrency = int(max_batch_send_concurrency)
    async def send_run_batches(
        self,
        batches: list[RunBatch],
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> list[RunBatchReport | dict[str, Any]]:
        if not batches:
            return []
        batch_semaphore = asyncio.Semaphore(self.resolve_batch_send_concurrency(len(batches)))
        async def _send_one(batch: RunBatch) -> RunBatchReport:
            async with batch_semaphore:
                return await self.send_run_batch(batch, send_semaphore=send_semaphore)
        return await asyncio.gather(*[_send_one(batch) for batch in batches])
    async def send_run_batch(
        self,
        batch: RunBatch,
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> RunBatchReport:
        return await self.send_run_batch_inner(batch=batch, send_semaphore=send_semaphore)
    async def send_run_batch_inner(
        self,
        *,
        batch: RunBatch,
        send_semaphore: asyncio.Semaphore | None,
    ) -> RunBatchReport:
        zone = str(batch.zone)
        latest = dict(batch.latest)
        paper_id = str(batch.paper_id)
        detail_url = str(batch.detail_url)
        targets = list(batch.targets)
        report = self.build_run_batch_report(zone=zone, paper_id=paper_id, detail_url=detail_url, sent_total=len(targets))
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            _, pdf_file, png_file, pdf_url, text = await self.prepare_run_batch_delivery(
                latest=latest,
                paper_id=paper_id,
                detail_url=detail_url,
                zone=zone,
            )
            sent_ok, success_targets = await self.send_push_to_targets(
                targets=targets,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
                send_semaphore=send_semaphore,
            )
            report.sent_ok = sent_ok
            await self.persist_run_batch_success_targets(
                zone=zone,
                paper_id=paper_id,
                success_targets=success_targets,
            )
            report.status = self.resolve_send_status(sent_ok=sent_ok, sent_total=len(targets))
            report.reason_code = self.resolve_send_reason_code(report.status)
            return report
        except Exception as exc:
            return self.build_run_batch_exception_report(report=report, zone=zone, paper_id=paper_id, error=exc)
        finally:
            await self._release_temp_files(pdf_file, png_file)
            try:
                await self._maybe_trim_temp_files()
            except Exception:
                self._logger.warning("批次推送后清理临时文件失败。", exc_info=True)
    async def prepare_run_batch_delivery(
        self,
        *,
        latest: dict[str, Any],
        paper_id: str,
        detail_url: str,
        zone: str,
    ) -> tuple[dict[str, Any], Path | None, Path | None, str, str]:
        payload = await self._load_submission_payload(latest, paper_id)
        self._logger.info("论文元数据：%s", json.dumps(self._build_meta_preview(payload), ensure_ascii=False))
        pdf_file, png_file, pdf_url = await self._prepare_pdf_assets(payload, paper_id)
        text = self._build_push_text(payload, detail_url, zone)
        return payload, pdf_file, png_file, pdf_url, text
    async def send_push_to_targets(
        self,
        *,
        targets: list[str],
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> tuple[int, list[str]]:
        semaphore = send_semaphore or asyncio.Semaphore(self.get_configured_send_concurrency())
        async def _send_one(session: str) -> tuple[str, bool]:
            async with semaphore:
                try:
                    ok = await self._send_session_push(
                        context=self._context_getter(),
                        session=session,
                        text=text,
                        png_file=png_file,
                        pdf_file=pdf_file,
                        pdf_url=pdf_url,
                    )
                except Exception:
                    self._logger.error("发送消息失败：会话=%s", session, exc_info=True)
                    ok = False
            self._logger.info("发送消息结果：会话=%s 是否成功=%s", session, ok)
            return session, ok
        results = await asyncio.gather(*[_send_one(session) for session in targets], return_exceptions=True)
        pairs = [item for item in results if not isinstance(item, BaseException)]
        for item in results:
            if isinstance(item, BaseException):
                self._logger.error("发送任务出现未预期异常", exc_info=(type(item), item, item.__traceback__))
        success_targets = [session for session, ok in pairs if ok]
        return len(success_targets), success_targets
    async def persist_run_batch_success_targets(
        self,
        *,
        zone: str,
        paper_id: str,
        success_targets: list[str],
    ) -> None:
        if success_targets:
            await self._history_store.mark_run_targets_delivered(
                zone=zone,
                paper_id=paper_id,
                success_targets=success_targets,
            )
    def build_run_batch_exception_report(
        self,
        *,
        report: RunBatchReport,
        zone: str,
        paper_id: str,
        error: Exception,
    ) -> RunBatchReport:
        if report.sent_ok > 0:
            self._logger.error(
                "写入推送去重状态失败：分区=%s 论文ID=%s 错误=%s",
                zone,
                paper_id,
                self._mask_sensitive_text(str(error)),
                exc_info=True,
            )
            return self.build_failed_run_batch_report(
                report=report,
                reason_code=RunReason.DELIVERY_STATE_WRITE_FAILED,
                debug_reason=str(error),
            )
        self._logger.error(
            "批次推送失败：分区=%s 论文ID=%s 错误=%s",
            zone,
            paper_id,
            self._mask_sensitive_text(str(error)),
            exc_info=True,
        )
        return self.build_failed_run_batch_report(
            report=report,
            reason_code=RunReason.ALL_SENDS_FAILED,
            debug_reason=str(error),
        )
    def build_run_batch_report(
        self,
        *,
        zone: str,
        paper_id: str,
        detail_url: str,
        sent_total: int,
    ) -> RunBatchReport:
        return RunBatchReport(
            status=RunStatus.FAILED,
            reason_code=RunReason.UNKNOWN,
            zone=zone,
            paper_id=paper_id,
            detail_url=detail_url,
            sent_ok=0,
            sent_total=sent_total,
            debug_reason="",
        )
    def build_failed_run_batch_report(
        self,
        *,
        report: RunBatchReport,
        reason_code: RunReason | str,
        debug_reason: str,
    ) -> RunBatchReport:
        report.status = RunStatus.FAILED
        report.reason_code = self.coerce_run_reason(reason_code)
        report.debug_reason = self._mask_sensitive_text(debug_reason)
        return report
    def resolve_send_status(self, *, sent_ok: int, sent_total: int) -> RunStatus:
        if sent_total > 0 and sent_ok == sent_total:
            return RunStatus.SUCCESS
        if sent_ok > 0:
            return RunStatus.PARTIAL
        return RunStatus.FAILED
    def resolve_send_reason_code(self, status: RunStatus | str) -> RunReason:
        normalized = self.coerce_run_status(status)
        if normalized == RunStatus.SUCCESS:
            return RunReason.PUSHED_SUCCESSFULLY
        if normalized == RunStatus.PARTIAL:
            return RunReason.PUSHED_PARTIALLY
        return RunReason.ALL_SENDS_FAILED
    def resolve_run_batch_reports(
        self,
        *,
        batch_reports: list[RunBatchReport],
        sent_ok: int,
        sent_total: int,
    ) -> tuple[RunStatus, RunReason]:
        if any(batch.reason_code == RunReason.DELIVERY_STATE_WRITE_FAILED for batch in batch_reports):
            return RunStatus.FAILED, RunReason.DELIVERY_STATE_WRITE_FAILED
        status = self.resolve_send_status(sent_ok=sent_ok, sent_total=sent_total)
        return status, self.resolve_send_reason_code(status)
    def resolve_batch_send_concurrency(self, batch_count: int) -> int:
        return min(self._max_batch_send_concurrency, self.get_configured_send_concurrency(), batch_count)
    def get_configured_send_concurrency(self) -> int:
        configured = self._cfg_int_getter(
            "send_concurrency",
            3,
            min_value=1,
            max_value=self._max_send_concurrency,
        )
        return min(self._max_send_concurrency, max(1, int(configured)))
    def coerce_run_status(self, value: RunStatus | str) -> RunStatus:
        if isinstance(value, RunStatus):
            return value
        try:
            return RunStatus(str(value).strip().lower())
        except ValueError:
            return RunStatus.UNKNOWN
    def coerce_run_reason(self, value: RunReason | str) -> RunReason:
        if isinstance(value, RunReason):
            return value
        try:
            return RunReason(str(value).strip())
        except ValueError:
            return RunReason.UNKNOWN
    def coerce_run_batch_report(self, value: RunBatchReport | dict[str, Any]) -> RunBatchReport:
        if isinstance(value, RunBatchReport):
            return value
        return RunBatchReport.from_dict(value)
