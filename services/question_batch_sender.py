from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from .message_sink import OneBotPlatformResolver
from .models import RunBatch, RunBatchReport, RunReason, RunStatus
from .push_chain_builder import PushChainBuilder
from .question_constants import (
    DEFAULT_QUESTION_DETAIL_URL_BASE,
    QUESTION_DISCIPLINE_LABELS,
    QUESTION_EXCERPT_LIMIT,
    QUESTION_NORMALIZED_FIELDS,
    QUESTION_REQUIRED_TEXT_FIELDS,
    QUESTION_ZONE_LABELS,
    to_text,
)

DEFAULT_MAX_SEND_CONCURRENCY = 30
DEFAULT_MAX_BATCH_SEND_CONCURRENCY = 10


def _default_cfg_int_getter(_key: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    value = int(default)
    if min_value is not None:
        value = max(int(min_value), value)
    if max_value is not None:
        value = min(int(max_value), value)
    return value


class QuestionBatchSender:
    def __init__(
        self,
        *,
        context_getter: Callable[[], Any],
        history_store: Any,
        fetch_question_detail: Callable[[str], Awaitable[dict[str, Any]]],
        send_session_push: Callable[..., Awaitable[bool]],
        logger: Any,
        mask_sensitive_text: Callable[[str], str],
        detail_url_base: str = DEFAULT_QUESTION_DETAIL_URL_BASE,
        detail_hide_domain: Callable[[], bool] | None = None,
        cfg_bool_getter: Callable[[str, bool], bool] | None = None,
        cfg_int_getter: Callable[..., int] | None = None,
        max_send_concurrency: int = DEFAULT_MAX_SEND_CONCURRENCY,
        max_batch_send_concurrency: int = DEFAULT_MAX_BATCH_SEND_CONCURRENCY,
    ):
        self._context_getter = context_getter
        self._history_store = history_store
        self._fetch_question_detail = fetch_question_detail
        self._send_session_push = send_session_push
        self._logger = logger
        self._mask_sensitive_text = mask_sensitive_text
        self._detail_url_base = str(detail_url_base).strip().rstrip("/") or DEFAULT_QUESTION_DETAIL_URL_BASE
        self._detail_hide_domain = detail_hide_domain or (lambda: False)
        self._cfg_bool = cfg_bool_getter or (lambda _key, default: default)
        self._cfg_int_getter = cfg_int_getter or _default_cfg_int_getter
        self._max_send_concurrency = int(max_send_concurrency)
        self._max_batch_send_concurrency = int(max_batch_send_concurrency)
        self._chains = PushChainBuilder(cfg_bool_getter=self._cfg_bool)
        self._platforms = OneBotPlatformResolver(cfg_bool_getter=self._cfg_bool)

    async def send_run_batches(
        self,
        batches: list[RunBatch],
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> list[RunBatchReport]:
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
        question_id = str(batch.paper_id)
        detail_url = str(batch.detail_url)
        targets = list(batch.targets)
        report = self.build_run_batch_report(zone=zone, paper_id=question_id, detail_url=detail_url, sent_total=len(targets))
        try:
            _question, text = await self.prepare_run_batch_delivery(
                latest=dict(batch.latest),
                paper_id=question_id,
                detail_url=detail_url,
                zone=zone,
            )
            sent_ok, success_targets = await self.send_push_to_targets(
                targets=targets,
                text=text,
                send_semaphore=send_semaphore,
            )
            report.sent_ok = sent_ok
            await self.persist_run_batch_success_targets(zone=zone, question_id=question_id, success_targets=success_targets)
            report.status = self.resolve_send_status(sent_ok=sent_ok, sent_total=report.sent_total)
            report.reason_code = self.resolve_send_reason_code(report.status)
            return report
        except Exception as exc:
            return self.build_run_batch_exception_report(report=report, zone=zone, question_id=question_id, error=exc)

    async def prepare_run_batch_delivery(
        self,
        *,
        latest: dict[str, Any],
        paper_id: str,
        detail_url: str,
        zone: str,
    ) -> tuple[dict[str, Any], str]:
        del latest
        question = await self._load_question_payload(paper_id)
        return question, self.build_push_text(question=question, detail_url=detail_url, zone=zone)

    async def send_push_to_targets(
        self,
        *,
        targets: list[str],
        text: str,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> tuple[int, list[str]]:
        semaphore = send_semaphore or asyncio.Semaphore(self.get_configured_send_concurrency())

        async def _send_one(session: str) -> tuple[str, bool]:
            async with semaphore:
                try:
                    ok = await self._send_one_target(session=session, text=text)
                except Exception:
                    self._logger.error("发送课题消息失败：会话=%s", session, exc_info=True)
                    return session, False
            return session, bool(ok)

        results = await asyncio.gather(*[_send_one(session) for session in targets], return_exceptions=True)
        pairs = [item for item in results if not isinstance(item, BaseException)]
        for item in results:
            if isinstance(item, BaseException):
                self._logger.error("发送课题任务出现未预期异常", exc_info=(type(item), item, item.__traceback__))
        success_targets = [session for session, ok in pairs if ok]
        return len(success_targets), success_targets

    async def _send_one_target(self, *, session: str, text: str) -> bool:
        context = self._context_getter()
        standard_chain = self._chains.build_text_only_chain(text=text)
        merge_platform = self._platforms.resolve_merge_forward_platform(context, session)
        if merge_platform is None:
            return bool(await self._send_session_push(context=context, session=session, chain=standard_chain))
        sender_uin = await self._platforms.get_platform_self_id(merge_platform)
        if not sender_uin:
            self._logger.warning("课题主动发送无法获取机器人自身 ID，回退普通消息：会话=%s", session)
            return bool(await self._send_session_push(context=context, session=session, chain=standard_chain))
        merge_chain = self._chains.build_text_only_merge_forward_chain(text=text, sender_uin=sender_uin)
        try:
            merge_ok = await self._send_session_push(context=context, session=session, chain=merge_chain)
            if merge_ok:
                return True
            self._logger.warning("课题合并转发发送返回失败，回退普通消息：会话=%s", session)
        except Exception:
            self._logger.warning("课题合并转发发送异常，回退普通消息：会话=%s", session, exc_info=True)
        return bool(await self._send_session_push(context=context, session=session, chain=standard_chain))

    async def persist_run_batch_success_targets(
        self,
        *,
        zone: str,
        question_id: str,
        success_targets: list[str],
    ) -> None:
        if success_targets:
            await self._history_store.mark_run_targets_delivered(
                zone=zone,
                question_id=question_id,
                success_targets=success_targets,
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

    def build_run_batch_exception_report(
        self,
        *,
        report: RunBatchReport,
        zone: str,
        question_id: str,
        error: Exception,
    ) -> RunBatchReport:
        masked_error = self._mask_sensitive_text(str(error))
        reason = RunReason.DELIVERY_STATE_WRITE_FAILED if report.sent_ok > 0 else RunReason.ALL_SENDS_FAILED
        self._logger.error("课题批次推送失败：分区=%s 课题ID=%s 错误=%s", zone, question_id, masked_error, exc_info=True)
        report.status = RunStatus.FAILED
        report.reason_code = reason
        report.debug_reason = masked_error
        return report

    def build_push_text(self, *, question: dict[str, Any], detail_url: str, zone: str) -> str:
        resolved_zone = to_text(question.get("zone")) or to_text(zone)
        zone_label = QUESTION_ZONE_LABELS.get(resolved_zone, resolved_zone)
        discipline = to_text(question.get("discipline"))
        discipline_label = QUESTION_DISCIPLINE_LABELS.get(discipline, discipline)
        excerpt = to_text(question.get("content"))[:QUESTION_EXCERPT_LIMIT]
        score = " / ".join(
            [
                self._metric_text(question.get("avg_score")),
                self._metric_text(question.get("rating_count")),
                self._metric_text(question.get("comment_count")),
            ],
        )
        lines = [
            "S.H.I.T Journal 课题推送",
            f"分区：{zone_label}",
            f"标题：{to_text(question.get('title'))}",
            f"作者：{to_text(question.get('author_name'))}",
            f"提交时间：{to_text(question.get('created_at'))}",
            f"学科：{discipline_label}",
            f"标签：{to_text(question.get('tag'))}",
            f"评分：{score}",
            f"详情：{self._format_detail_text(detail_url)}",
            "正文摘录：",
            excerpt,
        ]
        return "\n".join(lines)

    def resolve_send_status(self, *, sent_ok: int, sent_total: int) -> RunStatus:
        if sent_total > 0 and sent_ok == sent_total:
            return RunStatus.SUCCESS
        if sent_ok > 0:
            return RunStatus.PARTIAL
        return RunStatus.FAILED

    def resolve_send_reason_code(self, status: RunStatus | str) -> RunReason:
        if str(status) == RunStatus.SUCCESS.value:
            return RunReason.PUSHED_SUCCESSFULLY
        if str(status) == RunStatus.PARTIAL.value:
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

    async def _load_question_payload(self, question_id: str) -> dict[str, Any]:
        payload = await self._fetch_question_detail(question_id)
        missing_fields = [field for field in QUESTION_NORMALIZED_FIELDS if field not in payload]
        empty_required_fields = [
            field for field in QUESTION_REQUIRED_TEXT_FIELDS if not to_text(payload.get(field))
        ]
        if missing_fields or empty_required_fields:
            fields = missing_fields + [field for field in empty_required_fields if field not in missing_fields]
            missing_text = ", ".join(fields)
            raise RuntimeError(f"课题详情缺少或为空字段：id={question_id} fields={missing_text}")
        return payload

    def _metric_text(self, value: Any) -> str:
        text = to_text(value)
        return text if text else "-"

    def _format_detail_text(self, detail_url: str) -> str:
        detail_text = to_text(detail_url)
        if not detail_text:
            return detail_text
        if not self._detail_hide_domain():
            return detail_text
        if detail_text.startswith(self._detail_url_base):
            path = detail_text[len(self._detail_url_base) :].strip()
            return path if path.startswith("/") else f"/{path}"
        return detail_text
