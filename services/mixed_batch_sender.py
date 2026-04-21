from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

from .message_sink import OneBotPlatformResolver
from .onebot_action import resolve_call_action_from_bot
from .napcat_stream_uploader import NapCatStreamUploader, STREAM_ACTION_NAME
from .models import RunBatch, RunBatchReport
from .push_chain_builder import ONEBOT_ADAPTER_NAME, ArticlePushEntry, PushChainBuilder, QuestionPushEntry
from .session_message import is_group_message_session

BUNDLE_MODE_SEPARATE = "separate"
BUNDLE_MODE_BY_SESSION = "bundle_by_session"
_ALLOWED_BUNDLE_MODES = {BUNDLE_MODE_SEPARATE, BUNDLE_MODE_BY_SESSION}


@dataclass(slots=True)
class _PreparedArticleBatch:
    index: int
    batch: RunBatch
    report: RunBatchReport
    entry: ArticlePushEntry


@dataclass(slots=True)
class _PreparedQuestionBatch:
    index: int
    batch: RunBatch
    report: RunBatchReport
    entry: QuestionPushEntry


@dataclass(slots=True)
class _SessionBundle:
    article_batches: list[_PreparedArticleBatch]
    question_batches: list[_PreparedQuestionBatch]


class MixedBatchSender:
    def __init__(
        self,
        *,
        context_getter,
        cfg_bool_getter,
        cfg_int_getter=None,
        run_batch_sender,
        question_batch_sender,
        push_messages=None,
        logger,
    ):
        self._context_getter = context_getter
        self._cfg_bool = cfg_bool_getter
        self._cfg_int_getter = cfg_int_getter
        self._run_batch_sender = run_batch_sender
        self._question_batch_sender = question_batch_sender
        self._push_messages = push_messages
        self._logger = logger
        self._chains = PushChainBuilder(cfg_bool_getter=cfg_bool_getter)
        self._platform_resolver = OneBotPlatformResolver(cfg_bool_getter=cfg_bool_getter)
        self._stream_uploader = NapCatStreamUploader()

    async def send_selected_batches(
        self,
        *,
        article_batches: list[RunBatch],
        question_batches: list[RunBatch],
        bundle_mode: str,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> tuple[list[RunBatchReport], list[RunBatchReport]]:
        if bundle_mode not in _ALLOWED_BUNDLE_MODES:
            raise ValueError(f"bundle_mode 仅支持 separate 或 bundle_by_session: {bundle_mode}")
        if bundle_mode == BUNDLE_MODE_SEPARATE:
            return await self._send_separate_batches(
                article_batches=article_batches,
                question_batches=question_batches,
                send_semaphore=send_semaphore,
            )
        return await self._send_bundle_by_session(
            article_batches=article_batches,
            question_batches=question_batches,
            send_semaphore=send_semaphore,
        )

    async def _send_separate_batches(
        self,
        *,
        article_batches: list[RunBatch],
        question_batches: list[RunBatch],
        send_semaphore: asyncio.Semaphore | None,
    ) -> tuple[list[RunBatchReport], list[RunBatchReport]]:
        article_task = self._run_batch_sender.send_run_batches(article_batches, send_semaphore=send_semaphore)
        question_task = self._question_batch_sender.send_run_batches(question_batches, send_semaphore=send_semaphore)
        article_reports, question_reports = await asyncio.gather(article_task, question_task)
        coerced_articles = [self._run_batch_sender.coerce_run_batch_report(item) for item in article_reports]
        return coerced_articles, list(question_reports)

    async def _send_bundle_by_session(
        self,
        *,
        article_batches: list[RunBatch],
        question_batches: list[RunBatch],
        send_semaphore: asyncio.Semaphore | None,
    ) -> tuple[list[RunBatchReport], list[RunBatchReport]]:
        article_reports: list[RunBatchReport | None] = [None] * len(article_batches)
        question_reports: list[RunBatchReport | None] = [None] * len(question_batches)
        prepared_articles: list[_PreparedArticleBatch] = []
        prepared_questions: list[_PreparedQuestionBatch] = []
        await self._prepare_article_batches(article_batches, article_reports, prepared_articles)
        await self._prepare_question_batches(question_batches, question_reports, prepared_questions)
        try:
            bundles = self._build_session_bundles(prepared_articles, prepared_questions)
            success_map = await self._send_session_bundles(bundles=bundles, send_semaphore=send_semaphore)
            await self._finalize_article_reports(prepared_articles, article_reports, success_map)
            await self._finalize_question_reports(prepared_questions, question_reports, success_map)
        finally:
            await self._cleanup_prepared_articles(prepared_articles)
        return [item for item in article_reports if item is not None], [item for item in question_reports if item is not None]

    async def _prepare_article_batches(
        self,
        article_batches: list[RunBatch],
        article_reports: list[RunBatchReport | None],
        prepared_articles: list[_PreparedArticleBatch],
    ) -> None:
        for index, batch in enumerate(article_batches):
            report = self._run_batch_sender.build_run_batch_report(
                zone=str(batch.zone),
                paper_id=str(batch.paper_id),
                detail_url=str(batch.detail_url),
                sent_total=len(batch.targets),
            )
            try:
                _, pdf_file, png_file, pdf_url, text = await self._run_batch_sender.prepare_run_batch_delivery(
                    latest=dict(batch.latest),
                    paper_id=str(batch.paper_id),
                    detail_url=str(batch.detail_url),
                    zone=str(batch.zone),
                )
            except Exception as exc:
                article_reports[index] = self._run_batch_sender.build_run_batch_exception_report(
                    report=report,
                    zone=str(batch.zone),
                    paper_id=str(batch.paper_id),
                    error=exc,
                )
                continue
            prepared_articles.append(
                _PreparedArticleBatch(
                    index=index,
                    batch=batch,
                    report=report,
                    entry=ArticlePushEntry(
                        text=text,
                        png_file=png_file,
                        pdf_file=pdf_file,
                        pdf_url=pdf_url,
                    ),
                ),
            )

    async def _prepare_question_batches(
        self,
        question_batches: list[RunBatch],
        question_reports: list[RunBatchReport | None],
        prepared_questions: list[_PreparedQuestionBatch],
    ) -> None:
        for index, batch in enumerate(question_batches):
            report = self._question_batch_sender.build_run_batch_report(
                zone=str(batch.zone),
                paper_id=str(batch.paper_id),
                detail_url=str(batch.detail_url),
                sent_total=len(batch.targets),
            )
            try:
                _, text = await self._question_batch_sender.prepare_run_batch_delivery(
                    latest=dict(batch.latest),
                    paper_id=str(batch.paper_id),
                    detail_url=str(batch.detail_url),
                    zone=str(batch.zone),
                )
                entry = QuestionPushEntry(text=text)
            except Exception as exc:
                question_reports[index] = self._question_batch_sender.build_run_batch_exception_report(
                    report=report,
                    zone=str(batch.zone),
                    question_id=str(batch.paper_id),
                    error=exc,
                )
                continue
            prepared_questions.append(_PreparedQuestionBatch(index=index, batch=batch, report=report, entry=entry))

    def _build_session_bundles(
        self,
        prepared_articles: list[_PreparedArticleBatch],
        prepared_questions: list[_PreparedQuestionBatch],
    ) -> dict[str, _SessionBundle]:
        bundles: dict[str, _SessionBundle] = {}
        for prepared in prepared_articles:
            for session in prepared.batch.targets:
                bundle = bundles.setdefault(session, _SessionBundle(article_batches=[], question_batches=[]))
                bundle.article_batches.append(prepared)
        for prepared in prepared_questions:
            for session in prepared.batch.targets:
                bundle = bundles.setdefault(session, _SessionBundle(article_batches=[], question_batches=[]))
                bundle.question_batches.append(prepared)
        return bundles

    async def _send_session_bundles(
        self,
        *,
        bundles: dict[str, _SessionBundle],
        send_semaphore: asyncio.Semaphore | None,
    ) -> dict[str, bool]:
        if not bundles:
            return {}
        semaphore = send_semaphore or asyncio.Semaphore(self._run_batch_sender.get_configured_send_concurrency())

        async def _send_one(session: str, bundle: _SessionBundle) -> tuple[str, bool]:
            async with semaphore:
                ok = await self._send_one_session_bundle(session=session, bundle=bundle)
            return session, ok

        results = await asyncio.gather(*[_send_one(session, bundle) for session, bundle in bundles.items()])
        return {session: ok for session, ok in results}

    async def _send_one_session_bundle(self, *, session: str, bundle: _SessionBundle) -> bool:
        article_entries = await self._resolve_session_article_entries(
            session=session,
            article_entries=[item.entry for item in bundle.article_batches],
        )
        question_entries = [item.entry for item in bundle.question_batches]
        context = self._context_getter()
        adapter_name = self._platform_resolver.resolve_platform_name(context, session)
        merge_platform = self._platform_resolver.resolve_merge_forward_platform(context, session)
        if merge_platform is None:
            return await self._send_standard_bundle(
                context=context,
                session=session,
                adapter_name=adapter_name,
                article_entries=article_entries,
                question_entries=question_entries,
            )
        sender_uin = await self._platform_resolver.get_platform_self_id(merge_platform)
        if not sender_uin:
            self._logger.warning("混合发送无法获取机器人自身 ID，改用普通消息：会话=%s", session)
            return await self._send_standard_bundle(
                context=context,
                session=session,
                adapter_name=adapter_name,
                article_entries=article_entries,
                question_entries=question_entries,
            )
        return await self._send_merge_forward_bundle(
            context=context,
            session=session,
            adapter_name=adapter_name,
            sender_uin=sender_uin,
            article_entries=article_entries,
            question_entries=question_entries,
        )

    async def _send_standard_bundle(
        self,
        *,
        context: Any,
        session: str,
        adapter_name: str,
        article_entries: list[ArticlePushEntry],
        question_entries: list[QuestionPushEntry],
    ) -> bool:
        try:
            main_chain = self._chains.build_mixed_standard_main_chain(
                article_entries=article_entries,
                question_entries=question_entries,
            )
            if not await context.send_message(session, main_chain):
                return False
            for tail in self._chains.build_mixed_pdf_tail_chains(adapter_name=adapter_name, article_entries=article_entries):
                if not await context.send_message(session, tail):
                    return False
            return True
        except Exception:
            self._logger.error("混合普通消息发送失败：会话=%s", session, exc_info=True)
            return False

    async def _send_merge_forward_bundle(
        self,
        *,
        context: Any,
        session: str,
        adapter_name: str,
        sender_uin: str,
        article_entries: list[ArticlePushEntry],
        question_entries: list[QuestionPushEntry],
    ) -> bool:
        include_pdf = is_group_message_session(session)
        try:
            chain = self._chains.build_mixed_merge_forward_chain(
                adapter_name=adapter_name,
                article_entries=article_entries,
                question_entries=question_entries,
                sender_uin=sender_uin,
                include_pdf=include_pdf,
            )
            if not await context.send_message(session, chain):
                self._logger.warning("混合合并转发发送返回失败，回退普通消息：会话=%s", session)
                return await self._send_standard_bundle(
                    context=context,
                    session=session,
                    adapter_name=adapter_name,
                    article_entries=article_entries,
                    question_entries=question_entries,
                )
            if include_pdf:
                return True
            for tail in self._chains.build_mixed_pdf_tail_chains(adapter_name=adapter_name, article_entries=article_entries):
                if not await context.send_message(session, tail):
                    return False
            return True
        except Exception:
            self._logger.warning("混合合并转发发送异常，回退普通消息：会话=%s", session, exc_info=True)
            return await self._send_standard_bundle(
                context=context,
                session=session,
                adapter_name=adapter_name,
                article_entries=article_entries,
                question_entries=question_entries,
            )

    async def _finalize_article_reports(
        self,
        prepared_articles: list[_PreparedArticleBatch],
        article_reports: list[RunBatchReport | None],
        success_map: dict[str, bool],
    ) -> None:
        success_targets_map = self._build_success_targets_map(prepared_articles, success_map)
        for prepared in prepared_articles:
            success_targets = success_targets_map.get(prepared.index, [])
            prepared.report.sent_ok = len(success_targets)
            try:
                await self._run_batch_sender.persist_run_batch_success_targets(
                    zone=str(prepared.batch.zone),
                    paper_id=str(prepared.batch.paper_id),
                    success_targets=success_targets,
                )
                prepared.report.status = self._run_batch_sender.resolve_send_status(
                    sent_ok=prepared.report.sent_ok,
                    sent_total=prepared.report.sent_total,
                )
                prepared.report.reason_code = self._run_batch_sender.resolve_send_reason_code(prepared.report.status)
                article_reports[prepared.index] = prepared.report
            except Exception as exc:
                article_reports[prepared.index] = self._run_batch_sender.build_run_batch_exception_report(
                    report=prepared.report,
                    zone=str(prepared.batch.zone),
                    paper_id=str(prepared.batch.paper_id),
                    error=exc,
                )

    async def _finalize_question_reports(
        self,
        prepared_questions: list[_PreparedQuestionBatch],
        question_reports: list[RunBatchReport | None],
        success_map: dict[str, bool],
    ) -> None:
        success_targets_map = self._build_success_targets_map(prepared_questions, success_map)
        for prepared in prepared_questions:
            success_targets = success_targets_map.get(prepared.index, [])
            prepared.report.sent_ok = len(success_targets)
            try:
                await self._question_batch_sender.persist_run_batch_success_targets(
                    zone=str(prepared.batch.zone),
                    question_id=str(prepared.batch.paper_id),
                    success_targets=success_targets,
                )
                prepared.report.status = self._question_batch_sender.resolve_send_status(
                    sent_ok=prepared.report.sent_ok,
                    sent_total=prepared.report.sent_total,
                )
                prepared.report.reason_code = self._question_batch_sender.resolve_send_reason_code(prepared.report.status)
                question_reports[prepared.index] = prepared.report
            except Exception as exc:
                question_reports[prepared.index] = self._question_batch_sender.build_run_batch_exception_report(
                    report=prepared.report,
                    zone=str(prepared.batch.zone),
                    question_id=str(prepared.batch.paper_id),
                    error=exc,
                )

    def _build_success_targets_map(self, prepared_batches, success_map: dict[str, bool]) -> dict[int, list[str]]:
        mapped: dict[int, list[str]] = {}
        for prepared in prepared_batches:
            for session in prepared.batch.targets:
                if success_map.get(session):
                    mapped.setdefault(prepared.index, []).append(session)
        return mapped

    async def _cleanup_prepared_articles(self, prepared_articles: list[_PreparedArticleBatch]) -> None:
        for prepared in prepared_articles:
            await self._run_batch_sender.release_run_batch_assets(prepared.entry.pdf_file, prepared.entry.png_file)
        await self._run_batch_sender.trim_temp_files_after_send()

    async def _resolve_session_article_entries(
        self,
        *,
        session: str,
        article_entries: list[ArticlePushEntry],
    ) -> list[ArticlePushEntry]:
        if not article_entries:
            return []
        context = self._context_getter()
        adapter_name = self._platform_resolver.resolve_platform_name(context, session)
        if not self._should_try_stream_upload(adapter_name):
            return list(article_entries)
        platform = self._platform_resolver.resolve_platform(context, session)
        call_action = resolve_call_action_from_bot(getattr(platform, "bot", None))
        if not callable(call_action):
            self._logger.warning(
                "NapCat Stream API 调用入口不可用，回退默认 PDF 发送策略：目标=session:%s 动作=%s",
                session,
                STREAM_ACTION_NAME,
            )
            return list(article_entries)
        resolved_entries: list[ArticlePushEntry] = []
        for entry in article_entries:
            resolved_entries.append(
                await self._resolve_onebot_pdf_send_file(
                    session=session,
                    entry=entry,
                    call_action=call_action,
                ),
            )
        return resolved_entries

    async def _resolve_onebot_pdf_send_file(
        self,
        *,
        session: str,
        entry: ArticlePushEntry,
        call_action: Any,
    ) -> ArticlePushEntry:
        try:
            stream_file = await self._stream_uploader.upload_pdf(call_action=call_action, pdf_file=entry.pdf_file)
        except Exception:
            self._logger.warning(
                "NapCat Stream API 上传 PDF 失败，回退默认 PDF 发送策略：目标=session:%s 动作=%s",
                session,
                STREAM_ACTION_NAME,
                exc_info=True,
            )
            return entry
        stream_file = str(stream_file).strip()
        if not stream_file:
            self._logger.warning(
                "NapCat Stream API 返回空 file_path，回退默认 PDF 发送策略：目标=session:%s 动作=%s",
                session,
                STREAM_ACTION_NAME,
            )
            return entry
        return replace(entry, pdf_send_file=stream_file)

    def _should_try_stream_upload(self, adapter_name: str) -> bool:
        return self._cfg_bool("send_pdf", False) and str(adapter_name).strip() == ONEBOT_ADAPTER_NAME
