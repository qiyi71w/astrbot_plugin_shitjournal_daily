from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

if __package__:
    from .services import (
        AssetPipeline,
        HistoryStore,
        PdfService,
        PushMessageService,
        ReportRenderer,
        RunBatch,
        RunBatchReport,
        RunReason,
        RunReport,
        RunSelectionResult,
        RunSelectionError,
        RunSelector,
        RunStatus,
        SupabaseClient,
        TempFileManager,
    )
    from .services.session_message import is_private_message_session
    from .services.sensitive import mask_sensitive_text
else:
    _plugin_dir = Path(__file__).resolve().parent
    if str(_plugin_dir) not in sys.path:
        sys.path.insert(0, str(_plugin_dir))
    from services import (
        AssetPipeline,
        HistoryStore,
        PdfService,
        PushMessageService,
        ReportRenderer,
        RunBatch,
        RunBatchReport,
        RunReason,
        RunReport,
        RunSelectionResult,
        RunSelectionError,
        RunSelector,
        RunStatus,
        SupabaseClient,
        TempFileManager,
    )
    from services.session_message import is_private_message_session
    from services.sensitive import mask_sensitive_text


DEFAULT_SUPABASE_URL = "https://bcgdqepzakcufaadgnda.supabase.co"
DEFAULT_SUPABASE_BUCKET = "manuscripts"
DETAIL_URL_BASE = "https://shitjournal.org"
DEFAULT_ZONE = "stone"
DEFAULT_SCHEDULE_TIMES = ["09:00", "21:00"]
MAX_SEND_CONCURRENCY = 20
MAX_BATCH_SEND_CONCURRENCY = 2
RUN_FETCH_PAGE_SIZE = 20
CHI_SHI_FETCH_PAGE_SIZE = 20
SUPABASE_KEY_ENV_NAME = "SUPABASE_PUBLISHABLE_KEY"
TEMP_TRIM_INTERVAL_SEC = 60
DISCIPLINE_LABELS: dict[str, tuple[str, str]] = {
    "interdisciplinary": ("交叉", "Interdisciplinary"),
    "science": ("理", "Science"),
    "engineering": ("工", "Engineering"),
    "medical": ("医", "Medical"),
    "agriculture": ("农", "Agriculture"),
    "law_social": ("法社", "Law & Social"),
    "humanities": ("文", "Humanities"),
}
VISCOSITY_LABELS: dict[str, tuple[str, str]] = {
    "stringy": ("拉丝型", "Stringy"),
    "semi": ("半固态", "Semi-solid"),
    "high-entropy": ("高熵态", "High-Entropy"),
}
ZONE_LABELS: dict[str, tuple[str, str]] = {
    "latrine": ("旱厕", "The Latrine"),
    "septic": ("化粪池", "Septic Tank"),
    "stone": ("构石", "The Stone"),
    "sediment": ("沉淀区", "Sediment"),
}
REPORT_STATUS_LABELS: dict[str, str] = {
    "success": "成功",
    "partial": "部分成功",
    "failed": "失败",
    "skipped": "已跳过",
    "unknown": "未知",
}
REPORT_REASON_LABELS: dict[str, str] = {
    "RUN_IN_PROGRESS": "已有推送任务正在执行",
    "NO_TARGET_SESSION_CONFIGURED": "未配置推送目标",
    "FETCH_LATEST_FAILED": "获取最新论文失败",
    "ALREADY_DELIVERED": "没有可推送的新论文",
    "LATEST_NOT_FOUND": "未找到最新论文",
    "EMPTY_PAPER_ID": "论文 ID 为空",
    "DELIVERY_STATE_WRITE_FAILED": "消息已发出，但写入去重状态失败",
    "PREPARE_ASSETS_FAILED": "准备 PDF 资源失败",
    "PUSHED_SUCCESSFULLY": "推送成功",
    "PUSHED_PARTIALLY": "部分推送成功",
    "ALL_SENDS_FAILED": "全部推送失败",
    "UNKNOWN": "未知原因",
}


@register(
    "astrbot_plugin_shitjournal_daily",
    "AstrBot",
    "定时推送 shitjournal 最新论文到会话",
    "1.0.0",
)
class ShitJournalDailyPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}
        self._run_lock = asyncio.Lock()
        self._cron_job_ids: list[str] = []
        self._chi_shi_cooldown_lock = asyncio.Lock()
        self._chi_shi_group_cooldown_until_monotonic: dict[str, float] = {}
        self._chi_shi_group_inflight: set[str] = set()
        self._temp_trim_lock = asyncio.Lock()
        self._next_temp_trim_monotonic = 0.0
        self._plugin_data_dir = Path(".")
        self._temp_dir = Path(".")
        self._send_concurrency = 3
        self._supabase = SupabaseClient(
            cfg_getter=self._cfg,
            cfg_int_getter=self._cfg_int,
            key_getter=self._get_supabase_key,
            default_url=DEFAULT_SUPABASE_URL,
            default_bucket=DEFAULT_SUPABASE_BUCKET,
        )
        self._pdf_service = PdfService(cfg_int_getter=self._cfg_int)
        self._push_messages = PushMessageService(cfg_bool_getter=self._cfg_bool)
        self._temp_files = TempFileManager(self._temp_dir, cfg_int_getter=self._cfg_int)
        self._history_store = HistoryStore(
            kv_getter=self.get_kv_data,
            kv_putter=self.put_kv_data,
            cfg_bool_getter=self._cfg_bool,
            cfg_int_getter=self._cfg_int,
            normalize_session_list=self._normalize_session_list,
            normalize_zone_name=self._normalize_zone_name,
        )
        self._run_selector = self._create_run_selector()
        self._asset_pipeline = self._create_asset_pipeline()
        self._report_renderer = self._create_report_renderer()

    def _create_run_selector(self) -> RunSelector:
        return RunSelector(
            fetch_latest_submissions=lambda zone, limit, offset: self._supabase.fetch_latest_submissions(
                zone,
                limit,
                offset,
            ),
            get_run_sent_histories=lambda zone, targets: self._history_store.get_run_sent_histories(
                zone,
                targets,
            ),
            get_chi_shi_sent_history=lambda zone, session_key: self._history_store.get_chi_shi_sent_history(
                zone,
                session_key,
            ),
            logger=logger,
        )

    def _create_asset_pipeline(self) -> AssetPipeline:
        return AssetPipeline(
            fetch_submission_detail=self._supabase.fetch_submission_detail,
            create_signed_pdf_url=self._supabase.create_signed_pdf_url,
            download_pdf_file=self._supabase.download_pdf_file,
            build_output_paths=self._temp_files.build_output_paths,
            mark_in_use=self._temp_files.mark_in_use,
            release_temp_files=self._temp_files.release,
            ensure_pdf_size_limit=self._pdf_service.ensure_pdf_size_limit,
            export_first_page_png=self._pdf_service.export_first_page_png,
            logger=logger,
            mask_sensitive_text=mask_sensitive_text,
            detail_url_base=DETAIL_URL_BASE,
            detail_hide_domain=lambda: self._cfg_bool("detail_hide_domain", False),
            discipline_labels=DISCIPLINE_LABELS,
            viscosity_labels=VISCOSITY_LABELS,
            zone_labels=ZONE_LABELS,
        )

    def _create_report_renderer(self) -> ReportRenderer:
        return ReportRenderer(
            status_labels=REPORT_STATUS_LABELS,
            reason_labels=REPORT_REASON_LABELS,
            mask_sensitive_text=mask_sensitive_text,
            detail_url_base=DETAIL_URL_BASE,
            detail_hide_domain=lambda: self._cfg_bool("detail_hide_domain", False),
        )

    async def initialize(self):
        self._plugin_data_dir = self._resolve_plugin_data_dir()
        self._temp_dir = self._plugin_data_dir / "tmp"
        self._temp_files.set_temp_dir(self._temp_dir)
        self._ensure_supabase_key_or_raise()
        self._send_concurrency = self._cfg_int(
            "send_concurrency",
            3,
            min_value=1,
            max_value=MAX_SEND_CONCURRENCY,
        )
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        await self._history_store.ensure_chi_shi_history_storage_ready()
        await self._clear_cron_jobs()
        await self._register_cron_jobs()
        await self._maybe_trim_temp_files(force=True)
        logger.info("shitjournal_daily 插件初始化完成。")

    async def terminate(self):
        await self._clear_cron_jobs()
        await self._supabase.close()
        logger.info("shitjournal_daily 插件已停止。")

    @filter.command("shitjournal")
    async def shitjournal(
        self,
        event: AstrMessageEvent,
        *args: Any,
        **kwargs: Any,
    ):
        """管理 shitjournal 每日推送：bind/unbind/targets/run/run force"""
        normalized = self._normalize_command_event(
            event=event,
            args=args,
            kwargs=kwargs,
            command_name="shitjournal",
        )
        if not normalized:
            return
        event, args, kwargs = normalized

        if not await self._check_command_permission(event):
            return

        action, arg = self._parse_command_action_arg(
            args=args,
            kwargs=kwargs,
            default_action="help",
        )
        fallback_action, fallback_arg = self._parse_action_arg_from_message(
            event=event,
            command_name="shitjournal",
        )
        if fallback_action:
            action = fallback_action
            arg = fallback_arg

        if action == "bind":
            current = event.unified_msg_origin
            if not await self._history_store.bind_session(current):
                yield event.plain_result(f"当前会话已绑定：{current}")
                return
            yield event.plain_result(f"绑定成功：{current}")
            return

        if action == "unbind":
            current = event.unified_msg_origin
            if not await self._history_store.unbind_session(current):
                yield event.plain_result(f"当前会话未绑定：{current}")
                return
            yield event.plain_result(f"解绑成功：{current}")
            return

        if action == "targets":
            cfg_targets = self._normalize_session_list(self._cfg("target_sessions", []))
            bound = await self._history_store.get_bound_sessions()
            merged = await self._history_store.get_all_target_sessions(cfg_targets)
            if not merged:
                yield event.plain_result("当前没有推送目标。可用 `/shitjournal bind` 绑定当前会话。")
                return
            lines = ["当前推送目标："]
            for idx, target in enumerate(merged, start=1):
                source = []
                if target in cfg_targets:
                    source.append("配置")
                if target in bound:
                    source.append("绑定")
                source_text = ",".join(source)
                lines.append(f"{idx}. {target} [{source_text}]")
            yield event.plain_result("\n".join(lines))
            return

        if action == "run":
            force = bool(arg) and arg.split()[0] == "force"
            report = await self._run_cycle(force=force, source=f"手动:{event.get_sender_id()}")
            yield event.plain_result(self._report_renderer.render_report(report))
            return

        help_text = (
            "用法：\n"
            "/shitjournal bind - 绑定当前会话\n"
            "/shitjournal unbind - 解绑当前会话\n"
            "/shitjournal targets - 查看推送目标\n"
            "/shitjournal run - 手动执行一次\n"
            "/shitjournal run force - 忽略去重强制推送"
        )
        yield event.plain_result(help_text)

    @filter.command("我要赤石")
    async def wo_yao_chi_shi(
        self,
        event: AstrMessageEvent,
        *args: Any,
        **kwargs: Any,
    ):
        """抓取最新论文并推送到当前会话（按会话冷却）"""
        # 兼容 AstrBot 不同注入路径下的命令参数形态。
        normalized = self._normalize_command_event(
            event=event,
            args=args,
            kwargs=kwargs,
            command_name="我要赤石",
        )
        if not normalized:
            return
        event, _, _ = normalized
        scope_label = self._get_chi_shi_session_scope_text(event)
        session_key = event.unified_msg_origin
        ignore_cooldown = self._is_admin_event(event)
        allowed, deny_reason = await self._try_enter_chi_shi_cooldown(
            session_key=session_key,
            scope_label=scope_label,
            ignore_cooldown=ignore_cooldown,
        )
        if not allowed:
            yield event.plain_result(deny_reason)
            return

        outputs: list[str] = []
        apply_success_cooldown = False
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            outputs, apply_success_cooldown, pdf_file, png_file = await self._execute_wo_yao_chi_shi(
                event=event,
                session_key=session_key,
                scope_label=scope_label,
            )
        except Exception as exc:
            logger.error(
                "“我要赤石”指令执行失败：%s",
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            outputs = ["抓取失败，请稍后重试。"]
        finally:
            await self._leave_chi_shi_cooldown(
                session_key=session_key,
                apply_success_cooldown=apply_success_cooldown,
                ignore_cooldown=ignore_cooldown,
            )
            await self._temp_files.release(pdf_file, png_file)
            try:
                await self._maybe_trim_temp_files()
            except Exception:
                logger.warning("“我要赤石”执行后清理临时文件失败。", exc_info=True)
        for output in outputs:
            yield event.plain_result(output)

    async def _execute_wo_yao_chi_shi(
        self,
        *,
        event: AstrMessageEvent,
        session_key: str,
        scope_label: str,
    ) -> tuple[list[str], bool, Path | None, Path | None]:
        primary_zone = self._get_primary_zone()
        zone_order = self._get_candidate_zones(primary_zone)
        selected, saw_candidates, selection_warnings = await self._run_selector.select_chi_shi_candidate_from_zones(
            zone_order=zone_order,
            session_key=session_key,
            fetch_page_size=CHI_SHI_FETCH_PAGE_SIZE,
        )
        if not selected:
            apply_success_cooldown, reply_text = self._build_missing_chi_shi_selection_response(
                zone_order=zone_order,
                scope_label=scope_label,
                saw_candidates=saw_candidates,
                selection_warnings=selection_warnings,
            )
            return [reply_text], apply_success_cooldown, None, None
        apply_success_cooldown, pdf_file, png_file = await self._push_chi_shi_selection(
            event=event,
            session_key=session_key,
            selection=selected,
        )
        if not apply_success_cooldown:
            return ["候选论文缺少 ID。"], False, None, None
        if not selection_warnings:
            return [], True, pdf_file, png_file
        return [
            self._build_chi_shi_success_text(
                requested_zone=primary_zone,
                selected_zone=selected.zone,
                warnings=selection_warnings,
            ),
        ], True, pdf_file, png_file

    def _build_missing_chi_shi_selection_response(
        self,
        *,
        zone_order: list[str],
        scope_label: str,
        saw_candidates: bool,
        selection_warnings: list[str],
    ) -> tuple[bool, str]:
        if selection_warnings:
            return False, self._build_chi_shi_failure_text(
                saw_candidates=saw_candidates,
                warnings=selection_warnings,
                scope_label=scope_label,
            )
        if saw_candidates:
            return True, f"{scope_label}{self._build_zone_scope_text(zone_order)}最近这些论文都推送过了，等下一篇。"
        return False, "未找到最新论文。"

    async def _push_chi_shi_selection(
        self,
        *,
        event: AstrMessageEvent,
        session_key: str,
        selection: Any,
    ) -> tuple[bool, Path | None, Path | None]:
        zone = str(selection.zone).strip()
        paper_id = str(selection.paper_id).strip()
        candidate = dict(selection.candidate or {})
        if not paper_id:
            return False, None, None
        payload = await self._load_submission_payload(candidate, paper_id)
        detail_url = self._build_preprint_detail_url(paper_id)
        pdf_file, png_file, pdf_url = await self._prepare_pdf_assets(payload, paper_id)
        text = self._build_push_text(payload, detail_url, zone=zone)
        await self._push_messages.send_event_push(
            event=event,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        await self._history_store.mark_chi_shi_paper_sent(zone, session_key, paper_id)
        return True, pdf_file, png_file

    async def _check_command_permission(self, event: AstrMessageEvent) -> bool:
        if not self._looks_like_message_event(event):
            logger.error(
                "指令权限检查失败：无效事件类型=%s",
                type(event).__name__,
            )
            return False

        if not self._cfg_bool("command_admin_only", True):
            return True
        if self._is_admin_event(event):
            return True
        if self._cfg_bool("command_no_permission_reply", True):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

    # 命令兼容层：处理 AstrBot 在不同注入路径下 event/action 参数位置不一致的情况。
    def _normalize_command_event(
        self,
        event: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        command_name: str,
    ) -> tuple[AstrMessageEvent, tuple[Any, ...], dict[str, Any]] | None:
        if self._looks_like_message_event(event):
            return event, args, kwargs

        candidate = kwargs.get("event")
        if self._looks_like_message_event(candidate):
            cleaned_kwargs = dict(kwargs)
            cleaned_kwargs.pop("event", None)
            logger.warning(
                "指令 %s 从 kwargs 中修正了偏移的事件对象（原类型=%s）。",
                command_name,
                type(event).__name__,
            )
            return candidate, args, cleaned_kwargs

        for idx, item in enumerate(args):
            if self._looks_like_message_event(item):
                shifted_args = args[:idx] + args[idx + 1 :]
                logger.warning(
                    "指令 %s 从 args[%d] 中修正了偏移的事件对象（原类型=%s）。",
                    command_name,
                    idx,
                    type(event).__name__,
                )
                return item, shifted_args, kwargs

        logger.error(
            "指令 %s 事件归一化失败：event_type=%s args_types=%s kwargs_keys=%s",
            command_name,
            type(event).__name__,
            [type(item).__name__ for item in args[:4]],
            list(kwargs.keys())[:8],
        )
        return None

    def _looks_like_message_event(self, value: Any) -> bool:
        if isinstance(value, AstrMessageEvent):
            return True
        return (
            value is not None
            and hasattr(value, "plain_result")
            and hasattr(value, "send")
            and hasattr(value, "stop_event")
            and hasattr(value, "unified_msg_origin")
            and callable(getattr(value, "get_sender_id", None))
        )

    def _is_admin_event(self, event: AstrMessageEvent) -> bool:
        is_admin = getattr(event, "is_admin", None)
        if callable(is_admin) and is_admin():
            return True
        return self._is_sender_in_admins(event)

    def _is_sender_in_admins(self, event: AstrMessageEvent) -> bool:
        sender_id = str(event.get_sender_id()).strip()
        if not sender_id:
            return False

        config_obj: Any = getattr(self.context, "astrbot_config", None)
        if not isinstance(config_obj, dict):
            return False

        admins_raw = config_obj.get("admins_id", [])
        if isinstance(admins_raw, str):
            normalized_admins = [part.strip() for part in admins_raw.split(",")]
        elif isinstance(admins_raw, (list, tuple, set)):
            normalized_admins = [str(part).strip() for part in admins_raw]
        else:
            normalized_admins = []

        admins = {item for item in normalized_admins if item}
        return sender_id in admins

    def _get_chi_shi_session_scope_text(self, event: AstrMessageEvent) -> str:
        if event.get_group_id():
            return "本群"
        session_key = str(getattr(event, "unified_msg_origin", "")).strip()
        if is_private_message_session(session_key):
            return "当前私聊"
        return "当前会话"

    async def _try_enter_chi_shi_cooldown(
        self,
        session_key: str,
        scope_label: str,
        ignore_cooldown: bool,
    ) -> tuple[bool, str]:
        async with self._chi_shi_cooldown_lock:
            if session_key in self._chi_shi_group_inflight:
                return False, f"{scope_label}已有赤石任务在执行，请稍后再试。"

            if not ignore_cooldown:
                now = time.monotonic()
                cooldown_until = float(self._chi_shi_group_cooldown_until_monotonic.get(session_key, 0.0))
                remaining = cooldown_until - now
                if remaining > 0:
                    return False, f"{scope_label}冷却中，请 {int(remaining + 0.999)} 秒后再试。"

            self._chi_shi_group_inflight.add(session_key)
            return True, ""

    async def _leave_chi_shi_cooldown(
        self,
        session_key: str,
        apply_success_cooldown: bool,
        ignore_cooldown: bool,
    ) -> None:
        async with self._chi_shi_cooldown_lock:
            self._chi_shi_group_inflight.discard(session_key)
            if ignore_cooldown:
                return
            cooldown_sec = self._cfg_int("chi_shi_group_cooldown_sec", 60, min_value=0)
            fail_cooldown_sec = self._cfg_int("chi_shi_group_fail_cooldown_sec", 10, min_value=0)
            effective_cooldown = cooldown_sec if apply_success_cooldown else fail_cooldown_sec
            if effective_cooldown > 0:
                self._chi_shi_group_cooldown_until_monotonic[session_key] = (
                    time.monotonic() + effective_cooldown
                )
            else:
                self._chi_shi_group_cooldown_until_monotonic.pop(session_key, None)

    async def _register_cron_jobs(self) -> None:
        cron_manager = getattr(self.context, "cron_manager", None)
        if cron_manager is None:
            logger.error("cron_manager 不可用，已跳过定时任务注册。")
            return

        timezone = str(self._cfg("timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai"
        times = self._resolve_schedule_times()
        plugin_id = getattr(self, "plugin_id", "shitjournal_daily")
        created_job_ids: list[str] = []
        try:
            for idx, time_text in enumerate(times):
                parsed = self._parse_hhmm(time_text)
                if parsed is None:
                    logger.warning("已跳过无效的定时时间：%s", time_text)
                    continue
                hour, minute = parsed
                cron_expression = f"{minute} {hour} * * *"
                job = await cron_manager.add_basic_job(
                    name=f"{plugin_id}_shitjournal_{idx}",
                    cron_expression=cron_expression,
                    handler=self._scheduled_tick,
                    description=f"shitjournal 定时推送 {time_text}",
                    timezone=timezone,
                    payload={"schedule_time": time_text},
                    enabled=True,
                    persistent=False,
                )
                created_job_ids.append(job.job_id)
                logger.info(
                    "已注册定时任务：任务ID=%s 时间=%s 表达式=%s 时区=%s",
                    job.job_id,
                    time_text,
                    cron_expression,
                    timezone,
                )
        except Exception:
            failed_ids = await self._delete_cron_jobs(cron_manager, created_job_ids)
            self._cron_job_ids = failed_ids
            await self.put_kv_data("cron_job_ids", failed_ids)
            raise

        self._cron_job_ids = created_job_ids
        await self.put_kv_data("cron_job_ids", created_job_ids)

    async def _clear_cron_jobs(self) -> None:
        ids = await self._load_cron_job_ids()
        cron_manager = getattr(self.context, "cron_manager", None)
        if cron_manager is None:
            self._cron_job_ids = ids
            await self.put_kv_data("cron_job_ids", ids)
            return

        failed_ids = await self._delete_cron_jobs(cron_manager, ids)
        self._cron_job_ids = failed_ids
        await self.put_kv_data("cron_job_ids", failed_ids)

    async def _load_cron_job_ids(self) -> list[str]:
        ids = [str(job_id).strip() for job_id in self._cron_job_ids if str(job_id).strip()]
        if ids:
            return ids
        loaded = await self.get_kv_data("cron_job_ids", [])
        if not isinstance(loaded, list):
            return []
        return [str(job_id).strip() for job_id in loaded if str(job_id).strip()]

    async def _delete_cron_jobs(self, cron_manager: Any, ids: list[str]) -> list[str]:
        failed_ids: list[str] = []
        for job_id in ids:
            try:
                await cron_manager.delete_job(job_id)
                logger.info("已删除旧定时任务：%s", job_id)
            except Exception:
                failed_ids.append(job_id)
                logger.warning("删除定时任务失败，已忽略：%s", job_id, exc_info=True)
        return failed_ids

    def _resolve_schedule_times(self) -> list[str]:
        marker = object()
        raw = self._cfg("schedule_times", marker)
        if raw is marker:
            return DEFAULT_SCHEDULE_TIMES.copy()
        return self._normalize_schedule_times(raw)

    async def _scheduled_tick(self, schedule_time: str = "") -> None:
        report = await self._run_cycle(
            force=False,
            source=f"定时:{schedule_time or '未知'}",
            latest_only=self._cfg_bool("schedule_latest_only", False),
        )
        logger.info("定时推送执行完成：%s", self._report_renderer.render_report(report, include_debug=True))

    async def _run_cycle(
        self,
        *,
        force: bool,
        source: str,
        latest_only: bool = False,
    ) -> RunReport:
        if self._run_lock.locked():
            return self._build_run_in_progress_report(source=source, latest_only=latest_only)
        async with self._run_lock:
            return await self._run_cycle_locked(
                force=force,
                source=source,
                latest_only=latest_only,
            )

    async def _run_cycle_locked(
        self,
        *,
        force: bool,
        source: str,
        latest_only: bool,
    ) -> RunReport:
        primary_zone = self._get_primary_zone()
        zone_order = self._get_candidate_zones(primary_zone)
        report = self._build_run_cycle_report(source=source, primary_zone=primary_zone, force=force, latest_only=latest_only)
        logger.info("开始执行推送：来源=%s 主分区=%s 分区顺序=%s 强制=%s 仅最新=%s", source, primary_zone, zone_order, force, latest_only)
        try:
            targets = await self._history_store.get_all_target_sessions(self._cfg("target_sessions", []))
            if not targets:
                report.reason_code = RunReason.NO_TARGET_SESSION_CONFIGURED
                return report
            previous_last_seen, selection = await self._select_run_cycle_batches(report=report, zone_order=zone_order, targets=targets, force=force, latest_only=latest_only)
            if selection is None:
                return report
            if not selection.batches:
                return await self._finalize_empty_run_selection(report=report, selection=selection, previous_last_seen=previous_last_seen)
            batch_reports = await self._deliver_selected_run_batches(batches=selection.batches)
            self._apply_run_batch_reports_to_report(report=report, batch_reports=batch_reports, primary_zone=primary_zone)
            await self._persist_last_seen_map_if_changed(previous_last_seen=previous_last_seen, next_last_seen_map=selection.last_seen_map)
            report.status, report.reason_code = self._resolve_run_batch_reports(batch_reports=batch_reports, sent_ok=report.sent_ok, sent_total=report.sent_total)
            return report
        finally:
            try:
                await self._maybe_trim_temp_files()
            except Exception:
                logger.warning("执行推送后清理临时文件失败。", exc_info=True)

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
                fetch_page_size=RUN_FETCH_PAGE_SIZE,
            )
        except RunSelectionError as exc:
            self._apply_run_selection_error(
                report=report,
                reason_code=self._coerce_run_reason(exc.reason_code),
                message=str(exc).strip(),
            )
            return previous_last_seen, None
        except RuntimeError as exc:
            self._apply_run_selection_error(
                report=report,
                reason_code=RunReason.FETCH_LATEST_FAILED,
                message=str(exc).strip(),
            )
            return previous_last_seen, None
        report.warnings = selection.warnings
        return previous_last_seen, selection

    def _apply_run_selection_error(
        self,
        *,
        report: RunReport,
        reason_code: RunReason,
        message: str,
    ) -> None:
        report.reason_code = reason_code
        logger.error("获取最新论文失败：%s", mask_sensitive_text(message), exc_info=True)
        report.debug_reason = mask_sensitive_text(message)

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
            report.debug_reason = self._join_warning_text(selection.warnings)
        else:
            report.status = RunStatus.SKIPPED
            report.reason_code = (
                RunReason.ALREADY_DELIVERED if selection.saw_submission else RunReason.LATEST_NOT_FOUND
            )
        await self._persist_last_seen_map_if_changed(
            previous_last_seen=previous_last_seen,
            next_last_seen_map=selection.last_seen_map,
        )
        return report

    async def _deliver_selected_run_batches(
        self,
        *,
        batches: list[RunBatch],
    ) -> list[RunBatchReport]:
        raw_batch_reports = await self._send_run_batches(
            batches,
            send_semaphore=asyncio.Semaphore(self._get_configured_send_concurrency()),
        )
        return [self._coerce_run_batch_report(item) for item in raw_batch_reports]

    def _apply_run_batch_reports_to_report(
        self,
        *,
        report: RunReport,
        batch_reports: list[RunBatchReport],
        primary_zone: str,
    ) -> None:
        report.batches = batch_reports
        report.sent_ok = sum(batch.sent_ok for batch in batch_reports)
        report.sent_total = sum(batch.sent_total for batch in batch_reports)
        if batch_reports:
            first_batch = batch_reports[0]
            report.zone = first_batch.zone or primary_zone
            report.paper_id = first_batch.paper_id
            report.detail_url = first_batch.detail_url
        first_debug_reason = self._pick_first_batch_debug_reason(batch_reports)
        if first_debug_reason:
            report.debug_reason = first_debug_reason

    async def _persist_last_seen_map_if_changed(
        self,
        *,
        previous_last_seen: dict[str, str],
        next_last_seen_map: dict[str, str],
    ) -> None:
        if previous_last_seen == next_last_seen_map:
            return
        await self._history_store.set_last_seen_map(next_last_seen_map)

    async def _send_run_batches(
        self,
        batches: list[RunBatch],
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> list[RunBatchReport | dict[str, Any]]:
        if not batches:
            return []
        semaphore = asyncio.Semaphore(self._resolve_batch_send_concurrency(len(batches)))

        async def _send_one(batch: RunBatch) -> RunBatchReport:
            async with semaphore:
                if send_semaphore is None:
                    return await self._send_run_batch(batch)
                return await self._send_run_batch(batch, send_semaphore=send_semaphore)

        return await asyncio.gather(*[_send_one(batch) for batch in batches])

    async def _send_run_batch(
        self,
        batch: RunBatch,
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> RunBatchReport:
        try:
            return await self._send_run_batch_inner(batch=batch, send_semaphore=send_semaphore)
        finally:
            pass

    async def _send_run_batch_inner(
        self,
        *,
        batch: RunBatch,
        send_semaphore: asyncio.Semaphore | None,
    ) -> RunBatchReport:
        zone, latest, paper_id, detail_url, targets = self._unpack_run_batch(batch)
        report = self._build_run_batch_report(
            zone=zone,
            paper_id=paper_id,
            detail_url=detail_url,
            sent_total=len(targets),
        )
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            payload, pdf_file, png_file, pdf_url, text = await self._prepare_run_batch_delivery(
                latest=latest,
                paper_id=paper_id,
                detail_url=detail_url,
                zone=zone,
            )
            sent_ok, sent_success_targets = await self._send_push_to_targets(
                targets=targets,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
                send_semaphore=send_semaphore,
            )
            report.sent_ok = sent_ok
            await self._persist_run_batch_success_targets(
                zone=zone,
                paper_id=paper_id,
                success_targets=sent_success_targets,
            )
            report.status = self._resolve_send_status(sent_ok=sent_ok, sent_total=len(targets))
            report.reason_code = self._resolve_send_reason_code(report.status)
            return report
        except Exception as exc:
            return self._build_run_batch_exception_report(
                report=report,
                zone=zone,
                paper_id=paper_id,
                error=exc,
            )
        finally:
            await self._temp_files.release(pdf_file, png_file)
            try:
                await self._maybe_trim_temp_files()
            except Exception:
                logger.warning("批次推送后清理临时文件失败。", exc_info=True)

    def _unpack_run_batch(
        self,
        batch: RunBatch,
    ) -> tuple[str, dict[str, Any], str, str, list[str]]:
        return (
            str(batch.zone),
            dict(batch.latest),
            str(batch.paper_id),
            str(batch.detail_url),
            list(batch.targets),
        )

    async def _prepare_run_batch_delivery(
        self,
        *,
        latest: dict[str, Any],
        paper_id: str,
        detail_url: str,
        zone: str,
    ) -> tuple[dict[str, Any], Path | None, Path | None, str, str]:
        payload = await self._load_submission_payload(latest, paper_id)
        logger.info(
            "论文元数据：%s",
            json.dumps(self._asset_pipeline.build_meta_preview(payload), ensure_ascii=False),
        )
        pdf_file, png_file, pdf_url = await self._prepare_pdf_assets(payload, paper_id)
        text = self._build_push_text(payload, detail_url, zone=zone)
        return payload, pdf_file, png_file, pdf_url, text

    async def _persist_run_batch_success_targets(
        self,
        *,
        zone: str,
        paper_id: str,
        success_targets: list[str],
    ) -> None:
        if not success_targets:
            return
        await self._history_store.mark_run_targets_delivered(
            zone=zone,
            paper_id=paper_id,
            success_targets=success_targets,
        )

    def _build_run_batch_exception_report(
        self,
        *,
        report: RunBatchReport,
        zone: str,
        paper_id: str,
        error: Exception,
    ) -> RunBatchReport:
        if report.sent_ok > 0:
            logger.error(
                "写入推送去重状态失败：分区=%s 论文ID=%s 错误=%s",
                zone,
                paper_id,
                mask_sensitive_text(str(error)),
                exc_info=True,
            )
            return self._build_failed_run_batch_report(
                report=report,
                reason_code=RunReason.DELIVERY_STATE_WRITE_FAILED,
                debug_reason=str(error),
            )
        logger.error(
            "批次推送失败：分区=%s 论文ID=%s 错误=%s",
            zone,
            paper_id,
            mask_sensitive_text(str(error)),
            exc_info=True,
        )
        return self._build_failed_run_batch_report(
            report=report,
            reason_code=RunReason.ALL_SENDS_FAILED,
            debug_reason=str(error),
        )

    def _build_run_batch_report(
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

    def _build_failed_run_batch_report(
        self,
        *,
        report: RunBatchReport,
        reason_code: RunReason | str,
        debug_reason: str,
    ) -> RunBatchReport:
        report.status = RunStatus.FAILED
        report.reason_code = self._coerce_run_reason(reason_code)
        report.debug_reason = mask_sensitive_text(debug_reason)
        return report

    def _resolve_send_status(self, *, sent_ok: int, sent_total: int) -> RunStatus:
        if sent_total > 0 and sent_ok == sent_total:
            return RunStatus.SUCCESS
        if sent_ok > 0:
            return RunStatus.PARTIAL
        return RunStatus.FAILED

    def _resolve_run_batch_reports(
        self,
        *,
        batch_reports: list[RunBatchReport],
        sent_ok: int,
        sent_total: int,
    ) -> tuple[RunStatus, RunReason]:
        if any(batch.reason_code == RunReason.DELIVERY_STATE_WRITE_FAILED for batch in batch_reports):
            return RunStatus.FAILED, RunReason.DELIVERY_STATE_WRITE_FAILED
        status = self._resolve_send_status(sent_ok=sent_ok, sent_total=sent_total)
        return status, self._resolve_send_reason_code(status)

    def _resolve_send_reason_code(self, status: RunStatus | str) -> RunReason:
        normalized = self._coerce_run_status(status)
        if normalized == RunStatus.SUCCESS:
            return RunReason.PUSHED_SUCCESSFULLY
        if normalized == RunStatus.PARTIAL:
            return RunReason.PUSHED_PARTIALLY
        return RunReason.ALL_SENDS_FAILED

    def _coerce_run_status(self, value: RunStatus | str) -> RunStatus:
        if isinstance(value, RunStatus):
            return value
        text = str(value).strip().lower()
        try:
            return RunStatus(text)
        except ValueError:
            return RunStatus.UNKNOWN

    def _coerce_run_reason(self, value: RunReason | str) -> RunReason:
        if isinstance(value, RunReason):
            return value
        text = str(value).strip()
        try:
            return RunReason(text)
        except ValueError:
            return RunReason.UNKNOWN

    def _coerce_run_batch_report(self, value: RunBatchReport | dict[str, Any]) -> RunBatchReport:
        if isinstance(value, RunBatchReport):
            return value
        return RunBatchReport.from_dict(value)

    def _resolve_batch_send_concurrency(self, batch_count: int) -> int:
        return min(MAX_BATCH_SEND_CONCURRENCY, self._get_configured_send_concurrency(), batch_count)

    def _get_configured_send_concurrency(self) -> int:
        raw_concurrency = getattr(self, "_send_concurrency", 3)
        try:
            configured_concurrency = int(raw_concurrency)
        except (TypeError, ValueError):
            configured_concurrency = 3
        return min(MAX_SEND_CONCURRENCY, max(1, configured_concurrency))

    def _pick_first_batch_debug_reason(self, batch_reports: list[RunBatchReport]) -> str:
        for batch_report in batch_reports:
            debug_reason = mask_sensitive_text(str(batch_report.debug_reason).strip())
            if debug_reason:
                return debug_reason
        return ""

    async def _load_submission_payload(
        self,
        latest: dict[str, Any],
        paper_id: str,
    ) -> dict[str, Any]:
        return await self._asset_pipeline.load_submission_payload(latest, paper_id)

    async def _prepare_pdf_assets(self, payload: dict[str, Any], paper_id: str) -> tuple[Path, Path, str]:
        return await self._asset_pipeline.prepare_pdf_assets(payload, paper_id)

    async def _send_push_to_targets(
        self,
        targets: list[str],
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        *,
        send_semaphore: asyncio.Semaphore | None = None,
    ) -> tuple[int, list[str]]:
        semaphore = send_semaphore or asyncio.Semaphore(self._get_configured_send_concurrency())

        async def _send_one(session: str) -> tuple[str, bool]:
            ok = False
            async with semaphore:
                try:
                    ok = await self._push_messages.send_session_push(
                        context=self.context,
                        session=session,
                        text=text,
                        png_file=png_file,
                        pdf_file=pdf_file,
                        pdf_url=pdf_url,
                    )
                except Exception:
                    logger.error("发送消息失败：会话=%s", session, exc_info=True)
            logger.info("发送消息结果：会话=%s 是否成功=%s", session, ok)
            return session, ok

        results = await asyncio.gather(
            *[_send_one(session) for session in targets],
            return_exceptions=True,
        )
        ok_results: list[tuple[str, bool]] = []
        for result in results:
            if isinstance(result, BaseException):
                logger.error(
                    "发送任务出现未预期异常",
                    exc_info=(type(result), result, result.__traceback__),
                )
                continue
            ok_results.append(result)

        success_targets = [session for session, ok in ok_results if ok]
        sent_ok = len(success_targets)
        return sent_ok, success_targets

    def _build_push_text(
        self,
        payload: dict[str, Any],
        detail_url: str,
        zone: str = "",
    ) -> str:
        return self._asset_pipeline.build_push_text(payload, detail_url, zone=zone)

    def _build_preprint_detail_url(self, paper_id: str) -> str:
        return self._asset_pipeline.build_preprint_detail_url(paper_id)

    def _resolve_plugin_data_dir(self) -> Path:
        plugin_name = str(
            getattr(self, "name", "astrbot_plugin_shitjournal_daily"),
        ).strip() or "astrbot_plugin_shitjournal_daily"
        data_dir = StarTools.get_data_dir(plugin_name)
        return data_dir.resolve()

    def _ensure_supabase_key_or_raise(self) -> None:
        _ = self._get_supabase_key()

    def _get_supabase_key(self) -> str:
        key = str(self._cfg("supabase_publishable_key", "")).strip()
        if not key:
            key = str(os.getenv(SUPABASE_KEY_ENV_NAME, "")).strip()
        if not key:
            raise RuntimeError(
                "缺少 supabase_publishable_key，请在插件配置或环境变量中设置 "
                f"{SUPABASE_KEY_ENV_NAME}",
            )
        return key

    def _parse_command_action_arg(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        default_action: str,
    ) -> tuple[str, str]:
        positional_action: Any = args[0] if len(args) >= 1 else None
        positional_arg: Any = args[1] if len(args) >= 2 else None
        positional_extra: Any = list(args[2:]) if len(args) >= 3 else None
        default_action_normalized = default_action.strip().lower()

        action_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="action",
            alias_key="_action",
            positional_value=positional_action,
        )
        arg_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="arg",
            alias_key="_arg",
            positional_value=positional_arg,
        )
        extra_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="extra_args",
            alias_key="_extra_args",
            positional_value=positional_extra,
        )

        action = action_tokens[0] if action_tokens else default_action_normalized
        action = action or default_action_normalized
        merged_arg_tokens = action_tokens[1:] + arg_tokens + extra_tokens
        if action == default_action_normalized and merged_arg_tokens:
            action = merged_arg_tokens.pop(0)
        arg_text = " ".join(merged_arg_tokens).strip()
        return action, arg_text

    def _parse_action_arg_from_message(
        self,
        event: AstrMessageEvent,
        command_name: str,
    ) -> tuple[str, str]:
        message_text = ""
        getter = getattr(event, "get_message_str", None)
        if callable(getter):
            message_text = str(getter() or "")
        if not message_text:
            message_text = str(getattr(event, "message_str", "") or "")

        text = re.sub(r"\s+", " ", message_text).strip().lower()
        if not text:
            return "", ""

        command = command_name.strip().lower()
        if not command:
            return "", ""

        for candidate in (command, f"/{command}"):
            if text == candidate:
                return "help", ""
            if text.startswith(f"{candidate} "):
                tail = text[len(candidate) :].strip()
                if not tail:
                    return "help", ""
                tokens = [part for part in tail.split(" ") if part]
                action = tokens[0] if tokens else "help"
                arg = " ".join(tokens[1:]).strip() if len(tokens) >= 2 else ""
                return action, arg

        return "", ""

    def _pick_command_tokens(
        self,
        kwargs: dict[str, Any],
        primary_key: str,
        alias_key: str,
        positional_value: Any,
    ) -> list[str]:
        if primary_key in kwargs:
            tokens = self._split_command_tokens(kwargs.get(primary_key))
            if tokens:
                return tokens
        if alias_key in kwargs:
            tokens = self._split_command_tokens(kwargs.get(alias_key))
            if tokens:
                return tokens
        return self._split_command_tokens(positional_value)

    def _split_command_tokens(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            tokens: list[str] = []
            for item in value:
                tokens.extend(self._split_command_tokens(item))
            return tokens

        text = str(value).strip().lower()
        if not text:
            return []
        return [part for part in text.split() if part]

    def _cfg(self, key: str, default: Any) -> Any:
        try:
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
        except Exception:
            pass
        return default

    def _cfg_bool(self, key: str, default: bool) -> bool:
        raw = self._cfg(key, default)
        return self._to_bool(raw, default)

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

    def _cfg_int(
        self,
        key: str,
        default: int,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> int:
        raw = self._cfg(key, default)
        try:
            value = int(raw)
        except Exception:
            value = default

        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    def _get_primary_zone(self) -> str:
        zone = self._normalize_zone_name(self._cfg("zone", DEFAULT_ZONE))
        return zone or DEFAULT_ZONE

    def _normalize_zone_name(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def _normalize_zone_list(self, raw: Any) -> list[str]:
        if isinstance(raw, str):
            values = [self._normalize_zone_name(item) for item in re.split(r"[,\s]+", raw)]
        elif isinstance(raw, list):
            values = [self._normalize_zone_name(item) for item in raw]
        else:
            values = []

        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not value:
                continue
            if value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    def _get_candidate_zones(self, primary_zone: str) -> list[str]:
        zones = [primary_zone]
        if not self._cfg_bool("enable_zone_fallback", False):
            return zones

        seen = {primary_zone}
        for zone in self._normalize_zone_list(self._cfg("fallback_zones", [])):
            if zone in seen:
                continue
            seen.add(zone)
            zones.append(zone)
        return zones

    def _build_zone_scope_text(self, zone_order: list[str]) -> str:
        if len(zone_order) > 1:
            return "目标分区和候补分区"
        return "目标分区"

    def _parse_hhmm(self, value: str) -> tuple[int, int] | None:
        text = str(value).strip()
        match = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", text)
        if not match:
            return None
        hour = int(match.group(1))
        minute = int(match.group(2))
        return hour, minute

    def _normalize_schedule_times(self, raw: Any) -> list[str]:
        values: list[str] = []
        if isinstance(raw, str):
            values = [item.strip() for item in re.split(r"[,\s]+", raw) if item.strip()]
        elif isinstance(raw, list):
            values = [str(item).strip() for item in raw if str(item).strip()]
        else:
            values = []

        normalized: list[str] = []
        seen = set()
        for value in values:
            parsed = self._parse_hhmm(value)
            if parsed is None:
                continue
            formatted = f"{parsed[0]:02d}:{parsed[1]:02d}"
            if formatted in seen:
                continue
            seen.add(formatted)
            normalized.append(formatted)
        return normalized

    def _normalize_session_list(self, raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        normalized: list[str] = []
        for item in raw:
            text = str(item).strip()
            if text:
                normalized.append(text)
        return normalized

    async def _maybe_trim_temp_files(self, *, force: bool = False) -> None:
        async with self._temp_trim_lock:
            now = time.monotonic()
            if (not force) and now < self._next_temp_trim_monotonic:
                return
            self._next_temp_trim_monotonic = now + TEMP_TRIM_INTERVAL_SEC

        try:
            await self._temp_files.trim()
        except Exception:
            async with self._temp_trim_lock:
                self._next_temp_trim_monotonic = 0.0
            raise

    def _build_zone_fetch_warning_text(
        self,
        *,
        zone: str,
        is_primary: bool,
        offset: int,
        error: BaseException,
    ) -> str:
        zone_type = "主分区" if is_primary else "候补分区"
        position = "首页" if offset == 0 else f"偏移={offset}"
        error_text = mask_sensitive_text(str(error).strip()) or type(error).__name__
        return f"{zone_type}抓取失败：分区={zone} {position} 错误={error_text}"

    def _clean_warning_list(self, raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        warnings: list[str] = []
        seen: set[str] = set()
        for item in raw:
            text = mask_sensitive_text(str(item).strip())
            if not text or text in seen:
                continue
            seen.add(text)
            warnings.append(text)
        return warnings

    def _join_warning_text(self, warnings: list[str]) -> str:
        return "；".join(self._clean_warning_list(warnings))

    def _append_warning_lines(self, lines: list[str], warnings: list[str]) -> None:
        for index, warning in enumerate(self._clean_warning_list(warnings), start=1):
            lines.append(f"告警{index}: {warning}")

    def _build_chi_shi_success_text(
        self,
        *,
        requested_zone: str,
        selected_zone: str,
        warnings: list[str],
    ) -> str:
        lines = ["已继续回退并完成推送。"]
        if requested_zone and requested_zone != selected_zone:
            lines.append(f"最终分区: {selected_zone}")
        self._append_warning_lines(lines, warnings)
        return "\n".join(lines)

    def _build_chi_shi_failure_text(
        self,
        *,
        saw_candidates: bool,
        warnings: list[str],
        scope_label: str,
    ) -> str:
        if saw_candidates:
            lines = [f"抓取失败：回退过程中存在分区抓取失败，无法确认{scope_label}是否都已推送。"]
        else:
            lines = ["抓取失败：已尝试所有可回退分区，但未能成功获取可推送论文。"]
        self._append_warning_lines(lines, warnings)
        return "\n".join(lines)
