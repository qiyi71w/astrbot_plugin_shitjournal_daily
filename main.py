from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

if __package__:
    from .services import (
        PdfService,
        PushMessageService,
        RunBatch,
        RunBatchReport,
        RunReason,
        RunReport,
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
        PdfService,
        PushMessageService,
        RunBatch,
        RunBatchReport,
        RunReason,
        RunReport,
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
RUN_SENT_HISTORY_STORAGE_VERSION = "2"
RUN_SENT_HISTORY_STORAGE_VERSION_KEY = "run_sent_history_storage_version"
RUN_SENT_HISTORY_KV_PREFIX = "run_sent_history_v2::"
CHI_SHI_HISTORY_STORAGE_VERSION = "2"
CHI_SHI_HISTORY_STORAGE_VERSION_KEY = "chi_shi_sent_history_storage_version"
CHI_SHI_HISTORY_KV_PREFIX = "chi_shi_sent_history_v2::"
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


class RunSelectionError(RuntimeError):
    def __init__(self, reason_code: str, message: str = ""):
        super().__init__(message or reason_code)
        self.reason_code = reason_code


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
        self._bound_sessions_lock = asyncio.Lock()
        self._cron_job_ids: list[str] = []
        self._run_sent_history_lock = asyncio.Lock()
        self._run_sent_history_storage_ready = False
        self._chi_shi_cooldown_lock = asyncio.Lock()
        self._chi_shi_dedupe_lock = asyncio.Lock()
        self._chi_shi_group_cooldown_until_monotonic: dict[str, float] = {}
        self._chi_shi_group_inflight: set[str] = set()
        self._chi_shi_history_storage_ready = False
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
        await self._ensure_chi_shi_history_storage_ready()
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
            if not await self._bind_session(current):
                yield event.plain_result(f"当前会话已绑定：{current}")
                return
            yield event.plain_result(f"绑定成功：{current}")
            return

        if action == "unbind":
            current = event.unified_msg_origin
            if not await self._unbind_session(current):
                yield event.plain_result(f"当前会话未绑定：{current}")
                return
            yield event.plain_result(f"解绑成功：{current}")
            return

        if action == "targets":
            cfg_targets = self._normalize_session_list(self._cfg("target_sessions", []))
            bound = await self._get_bound_sessions()
            merged = self._merge_sessions(cfg_targets, bound)
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
            yield event.plain_result(self._render_report(report))
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

        apply_success_cooldown = False
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            primary_zone = self._get_primary_zone()
            zone_order = self._get_candidate_zones(primary_zone)
            selected, saw_candidates, selection_warnings = await self._select_chi_shi_candidate_from_zones(
                zone_order=zone_order,
                session_key=session_key,
            )
            if not selected:
                if selection_warnings:
                    yield event.plain_result(
                        self._build_chi_shi_failure_text(
                            saw_candidates=saw_candidates,
                            warnings=selection_warnings,
                            scope_label=scope_label,
                        ),
                    )
                    return
                if saw_candidates:
                    apply_success_cooldown = True
                    yield event.plain_result(
                        f"{scope_label}{self._build_zone_scope_text(zone_order)}最近这些论文都推送过了，等下一篇。",
                    )
                    return
                yield event.plain_result("未找到最新论文。")
                return

            zone, candidate = selected

            paper_id = str(candidate.get("id", "")).strip()
            if not paper_id:
                yield event.plain_result("候选论文缺少 ID。")
                return

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
            await self._mark_chi_shi_paper_sent(zone, session_key, paper_id)
            apply_success_cooldown = True
            if selection_warnings:
                yield event.plain_result(
                    self._build_chi_shi_success_text(
                        requested_zone=primary_zone,
                        selected_zone=zone,
                        warnings=selection_warnings,
                    ),
                )
            return
        except Exception as exc:
            logger.error(
                "“我要赤石”指令执行失败：%s",
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            yield event.plain_result("抓取失败，请稍后重试。")
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
        logger.info("定时推送执行完成：%s", self._render_report(report, include_debug=True))

    async def _run_cycle(
        self,
        *,
        force: bool,
        source: str,
        latest_only: bool = False,
    ) -> RunReport:
        if self._run_lock.locked():
            return RunReport(
                status=RunStatus.SKIPPED,
                reason_code=RunReason.RUN_IN_PROGRESS,
                source=source,
                debug_reason="",
                latest_only=latest_only,
            )

        async with self._run_lock:
            primary_zone = self._get_primary_zone()
            zone_order = self._get_candidate_zones(primary_zone)
            report = RunReport(
                status=RunStatus.FAILED,
                source=source,
                zone=primary_zone,
                requested_zone=primary_zone,
                force=force,
                latest_only=latest_only,
            )
            logger.info(
                "开始执行推送：来源=%s 主分区=%s 分区顺序=%s 强制=%s 仅最新=%s",
                source,
                primary_zone,
                zone_order,
                force,
                latest_only,
            )
            try:
                targets = await self._get_all_target_sessions()
                if not targets:
                    report.reason_code = RunReason.NO_TARGET_SESSION_CONFIGURED
                    return report

                last_seen_map = await self._get_last_seen_map()
                try:
                    batches, saw_submission, last_seen_dirty, selection_warnings = await self._select_run_batches(
                        zone_order=zone_order,
                        targets=targets,
                        last_seen_map=last_seen_map,
                        force=force,
                        latest_only=latest_only,
                    )
                except RunSelectionError as exc:
                    message = str(exc).strip()
                    report.reason_code = self._coerce_run_reason(exc.reason_code)
                    logger.error(
                        "获取最新论文失败：%s",
                        mask_sensitive_text(message),
                        exc_info=True,
                    )
                    report.debug_reason = mask_sensitive_text(message)
                    return report
                except RuntimeError as exc:
                    message = str(exc).strip()
                    logger.error(
                        "获取最新论文失败：%s",
                        mask_sensitive_text(message),
                        exc_info=True,
                    )
                    report.reason_code = RunReason.FETCH_LATEST_FAILED
                    report.debug_reason = mask_sensitive_text(message)
                    return report
                report.warnings = selection_warnings

                if not batches:
                    if selection_warnings:
                        report.status = RunStatus.FAILED
                        report.reason_code = RunReason.FETCH_LATEST_FAILED
                        report.debug_reason = self._join_warning_text(selection_warnings)
                        if last_seen_dirty:
                            await self.put_kv_data("last_seen_by_zone", last_seen_map)
                        return report
                    report.status = RunStatus.SKIPPED
                    report.reason_code = (
                        RunReason.ALREADY_DELIVERED if saw_submission else RunReason.LATEST_NOT_FOUND
                    )
                    if last_seen_dirty:
                        await self.put_kv_data("last_seen_by_zone", last_seen_map)
                    return report

                raw_batch_reports = await self._send_run_batches(
                    batches,
                    send_semaphore=asyncio.Semaphore(self._get_configured_send_concurrency()),
                )
                batch_reports = [self._coerce_run_batch_report(item) for item in raw_batch_reports]
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

                if last_seen_dirty:
                    await self.put_kv_data("last_seen_by_zone", last_seen_map)

                report.status, report.reason_code = self._resolve_run_batch_reports(
                    batch_reports=batch_reports,
                    sent_ok=report.sent_ok,
                    sent_total=report.sent_total,
                )
                return report
            finally:
                try:
                    await self._maybe_trim_temp_files()
                except Exception:
                    logger.warning("执行推送后清理临时文件失败。", exc_info=True)

    async def _select_chi_shi_candidate_from_zones(
        self,
        zone_order: list[str],
        session_key: str,
    ) -> tuple[tuple[str, dict[str, Any]] | None, bool, list[str]]:
        saw_candidates = False
        warnings: list[str] = []
        for index, zone in enumerate(zone_order):
            is_primary = index == 0
            sent_set = set(await self._get_chi_shi_sent_history(zone, session_key))
            offset = 0

            while True:
                try:
                    candidates = await self._supabase.fetch_latest_submissions(
                        zone,
                        CHI_SHI_FETCH_PAGE_SIZE,
                        offset,
                    )
                except Exception as exc:
                    warnings.append(
                        self._build_zone_fetch_warning_text(
                            zone=zone,
                            is_primary=is_primary,
                            offset=offset,
                            error=exc,
                        ),
                    )
                    logger.warning(
                        "获取“我要赤石”分区候选论文失败，已继续回退下一个分区：分区=%s 偏移=%s",
                        zone,
                        offset,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
                    break

                if not candidates:
                    break

                saw_candidates = True
                candidate = self._pick_first_unsent_candidate(candidates, sent_set)
                if candidate:
                    return (zone, candidate), True, warnings

                if len(candidates) < CHI_SHI_FETCH_PAGE_SIZE:
                    break
                offset += len(candidates)

        return None, saw_candidates, warnings

    async def _select_run_batches(
        self,
        *,
        zone_order: list[str],
        targets: list[str],
        last_seen_map: dict[str, str],
        force: bool,
        latest_only: bool,
    ) -> tuple[list[RunBatch], bool, bool, list[str]]:
        if latest_only:
            return await self._select_latest_only_run_batches(
                zone_order=zone_order,
                targets=targets,
                last_seen_map=last_seen_map,
                force=force,
            )

        saw_submission = False
        last_seen_dirty = False
        unresolved_targets = targets.copy()
        batches: list[RunBatch] = []
        warnings: list[str] = []
        for index, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            first_page_candidates, zone_warnings = await self._fetch_run_candidates_page(
                zone=zone,
                is_primary=index == 0,
                offset=0,
            )
            warnings.extend(zone_warnings)
            if first_page_candidates is None:
                continue
            zone_batches, matched_targets, zone_saw_submission, zone_last_seen_dirty, page_warnings = (
                await self._select_zone_run_batches(
                    zone=zone,
                    is_primary=index == 0,
                    unresolved_targets=unresolved_targets,
                    last_seen_map=last_seen_map,
                    force=force,
                    first_page_candidates=first_page_candidates,
                )
            )
            warnings.extend(page_warnings)
            batches.extend(zone_batches)
            saw_submission = saw_submission or zone_saw_submission
            last_seen_dirty = last_seen_dirty or zone_last_seen_dirty
            unresolved_targets = self._subtract_targets(unresolved_targets, matched_targets)
        return batches, saw_submission, last_seen_dirty, warnings

    async def _select_latest_only_run_batches(
        self,
        *,
        zone_order: list[str],
        targets: list[str],
        last_seen_map: dict[str, str],
        force: bool,
    ) -> tuple[list[RunBatch], bool, bool, list[str]]:
        saw_submission = False
        last_seen_dirty = False
        unresolved_targets = targets.copy()
        batches: list[RunBatch] = []
        warnings: list[str] = []
        for index, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            first_page_candidates, zone_warnings = await self._fetch_run_candidates_page(
                zone=zone,
                is_primary=index == 0,
                offset=0,
            )
            warnings.extend(zone_warnings)
            if first_page_candidates is None:
                continue
            batch, matched_targets, zone_saw_submission, zone_last_seen_dirty = (
                await self._select_latest_only_zone_batch(
                    zone=zone,
                    is_primary=index == 0,
                    unresolved_targets=unresolved_targets,
                    last_seen_map=last_seen_map,
                    force=force,
                    first_page_candidates=first_page_candidates,
                )
            )
            if batch:
                batches.append(batch)
            saw_submission = saw_submission or zone_saw_submission
            last_seen_dirty = last_seen_dirty or zone_last_seen_dirty
            unresolved_targets = self._subtract_targets(unresolved_targets, matched_targets)
        return batches, saw_submission, last_seen_dirty, warnings

    async def _select_zone_run_batches(
        self,
        *,
        zone: str,
        is_primary: bool,
        unresolved_targets: list[str],
        last_seen_map: dict[str, str],
        force: bool,
        first_page_candidates: list[dict[str, Any]] | None,
    ) -> tuple[list[RunBatch], set[str], bool, bool, list[str]]:
        if not first_page_candidates:
            return [], set(), False, False, []
        sent_history_by_target = await self._get_run_sent_histories(zone, unresolved_targets)
        sent_history_lookup_by_target = self._build_history_lookup(sent_history_by_target)
        batches: list[RunBatch] = []
        matched_targets: set[str] = set()
        saw_submission = False
        last_seen_dirty = False
        zone_latest_recorded = False
        offset = 0
        warnings: list[str] = []
        while True:
            candidates, page_warnings = await self._load_run_candidates_page(
                zone=zone,
                is_primary=is_primary,
                first_page_candidates=first_page_candidates,
                offset=offset,
            )
            warnings.extend(page_warnings)
            if candidates is None:
                break
            if not candidates:
                break
            saw_submission = True
            page_batches, skip_zone, zone_latest_recorded, zone_last_seen_dirty, page_targets = (
                self._build_zone_run_page_batches(
                    zone=zone,
                    candidates=candidates,
                    offset=offset,
                    unresolved_targets=unresolved_targets,
                    matched_targets=matched_targets,
                    sent_history_by_target=sent_history_by_target,
                    sent_history_lookup_by_target=sent_history_lookup_by_target,
                    force=force,
                    is_primary=is_primary,
                    zone_latest_recorded=zone_latest_recorded,
                    last_seen_map=last_seen_map,
                )
            )
            last_seen_dirty = last_seen_dirty or zone_last_seen_dirty
            batches.extend(page_batches)
            matched_targets.update(page_targets)
            if skip_zone or len(candidates) < RUN_FETCH_PAGE_SIZE or len(matched_targets) == len(unresolved_targets):
                break
            offset += len(candidates)
        return batches, matched_targets, saw_submission, last_seen_dirty, warnings

    async def _select_latest_only_zone_batch(
        self,
        *,
        zone: str,
        is_primary: bool,
        unresolved_targets: list[str],
        last_seen_map: dict[str, str],
        force: bool,
        first_page_candidates: list[dict[str, Any]] | None,
    ) -> tuple[RunBatch | None, set[str], bool, bool]:
        candidates = first_page_candidates
        if not candidates:
            return None, set(), False, False
        candidate = candidates[0]
        paper_id = self._extract_run_candidate_paper_id(
            zone=zone,
            candidate=candidate,
            is_primary=is_primary,
        )
        if paper_id is None:
            return None, set(), True, False
        last_seen_dirty = False
        if last_seen_map.get(zone) != paper_id:
            last_seen_map[zone] = paper_id
            last_seen_dirty = True
        sent_history_by_target = await self._get_run_sent_histories(zone, unresolved_targets)
        sent_history_lookup_by_target = self._build_history_lookup(sent_history_by_target)
        matched_targets = self._match_run_batch_targets(
            paper_id=paper_id,
            unresolved_targets=unresolved_targets,
            matched_targets=set(),
            sent_history_lookup_by_target=sent_history_lookup_by_target,
            force=force,
        )
        if not matched_targets:
            return None, set(), True, last_seen_dirty
        batch = self._build_run_batch(
            zone=zone,
            candidate=candidate,
            paper_id=paper_id,
            targets=matched_targets,
            sent_history_by_target=sent_history_by_target,
            sent_history_lookup_by_target=sent_history_lookup_by_target,
        )
        return batch, set(matched_targets), True, last_seen_dirty

    async def _load_run_candidates_page(
        self,
        *,
        zone: str,
        is_primary: bool,
        first_page_candidates: list[dict[str, Any]] | None,
        offset: int,
    ) -> tuple[list[dict[str, Any]] | None, list[str]]:
        if offset == 0:
            return first_page_candidates, []
        return await self._fetch_run_candidates_page(
            zone=zone,
            is_primary=is_primary,
            offset=offset,
        )

    def _build_zone_run_page_batches(
        self,
        *,
        zone: str,
        candidates: list[dict[str, Any]],
        offset: int,
        unresolved_targets: list[str],
        matched_targets: set[str],
        sent_history_by_target: dict[str, list[str]],
        sent_history_lookup_by_target: dict[str, set[str]],
        force: bool,
        is_primary: bool,
        zone_latest_recorded: bool,
        last_seen_map: dict[str, str],
    ) -> tuple[list[RunBatch], bool, bool, bool, set[str]]:
        page_batches: list[RunBatch] = []
        page_targets: set[str] = set()
        occupied_targets = set(matched_targets)
        last_seen_dirty = False
        for candidate_index, candidate in enumerate(candidates):
            paper_id = str(candidate.get("id", "")).strip()
            if not paper_id:
                if offset == 0 and candidate_index == 0:
                    paper_id = self._extract_run_candidate_paper_id(
                        zone=zone,
                        is_primary=is_primary,
                        candidate=candidate,
                    )
                    if paper_id is None:
                        return [], True, zone_latest_recorded, last_seen_dirty, page_targets
                else:
                    logger.warning(
                        "候选论文缺少 ID，已跳过：分区=%s 载荷=%s",
                        zone,
                        json.dumps(self._build_meta_preview(candidate), ensure_ascii=False),
                    )
                    continue
            if not zone_latest_recorded:
                logger.info(
                    "已获取最新论文：分区=%s 论文ID=%s 详情=%s",
                    zone,
                    paper_id,
                    self._build_preprint_detail_url(paper_id),
                )
                zone_latest_recorded = True
                if last_seen_map.get(zone) != paper_id:
                    last_seen_map[zone] = paper_id
                    last_seen_dirty = True
            batch_targets = self._match_run_batch_targets(
                paper_id=paper_id,
                unresolved_targets=unresolved_targets,
                matched_targets=occupied_targets,
                sent_history_lookup_by_target=sent_history_lookup_by_target,
                force=force,
            )
            if not batch_targets:
                continue
            page_batches.append(
                self._build_run_batch(
                    zone=zone,
                    candidate=candidate,
                    paper_id=paper_id,
                    targets=batch_targets,
                    sent_history_by_target=sent_history_by_target,
                    sent_history_lookup_by_target=sent_history_lookup_by_target,
                )
            )
            page_targets.update(batch_targets)
            occupied_targets.update(batch_targets)
        return page_batches, False, zone_latest_recorded, last_seen_dirty, page_targets

    def _subtract_targets(self, targets: list[str], removed_targets: set[str]) -> list[str]:
        if not removed_targets:
            return targets
        return [target for target in targets if target not in removed_targets]

    def _match_run_batch_targets(
        self,
        *,
        paper_id: str,
        unresolved_targets: list[str],
        matched_targets: set[str],
        sent_history_lookup_by_target: dict[str, set[str]],
        force: bool,
    ) -> list[str]:
        pending_targets: list[str] = []
        for session in unresolved_targets:
            if session in matched_targets:
                continue
            if not force:
                history_lookup = sent_history_lookup_by_target.get(session)
                if history_lookup is not None and paper_id in history_lookup:
                    continue
            pending_targets.append(session)
        return pending_targets

    def _build_run_batch(
        self,
        *,
        zone: str,
        candidate: dict[str, Any],
        paper_id: str,
        targets: list[str],
        sent_history_by_target: dict[str, list[str]],
        sent_history_lookup_by_target: dict[str, set[str]],
    ) -> RunBatch:
        return RunBatch(
            zone=zone,
            latest=candidate,
            paper_id=paper_id,
            detail_url=self._build_preprint_detail_url(paper_id),
            targets=targets,
            sent_history_by_target=sent_history_by_target,
            sent_history_lookup_by_target=sent_history_lookup_by_target,
        )

    async def _fetch_run_candidates_page(
        self,
        *,
        zone: str,
        is_primary: bool,
        offset: int,
    ) -> tuple[list[dict[str, Any]] | None, list[str]]:
        try:
            return await self._supabase.fetch_latest_submissions(zone, RUN_FETCH_PAGE_SIZE, offset), []
        except Exception as exc:
            warning = self._build_zone_fetch_warning_text(
                zone=zone,
                is_primary=is_primary,
                offset=offset,
                error=exc,
            )
            self._log_run_candidate_fetch_warning(
                zone=zone,
                is_primary=is_primary,
                offset=offset,
                error=exc,
            )
            return None, [warning]

    def _log_run_candidate_fetch_warning(
        self,
        *,
        zone: str,
        is_primary: bool,
        offset: int,
        error: BaseException,
    ) -> None:
        if offset == 0:
            zone_type = "主分区" if is_primary else "候补分区"
            logger.warning(
                "获取%s最新论文失败，已继续回退后续分区：分区=%s",
                zone_type,
                zone,
                exc_info=(type(error), error, error.__traceback__),
            )
            return
        zone_type = "主分区" if is_primary else "候补分区"
        logger.warning(
            "获取%s候选页失败，已停止继续检查该分区并回退后续分区：分区=%s 偏移=%s",
            zone_type,
            zone,
            offset,
            exc_info=(type(error), error, error.__traceback__),
        )

    def _raise_run_candidate_id_error(
        self,
        *,
        zone: str,
        is_primary: bool,
        candidate: dict[str, Any],
    ) -> None:
        zone_type = "主分区最新论文" if is_primary else "候补分区最新论文"
        meta_preview = json.dumps(self._build_meta_preview(candidate), ensure_ascii=False)
        raise RunSelectionError(
            "EMPTY_PAPER_ID",
            f"{zone_type}缺少 ID：分区={zone} 载荷={meta_preview}",
        )

    def _log_run_candidate_id_warning(
        self,
        *,
        zone: str,
        candidate: dict[str, Any],
    ) -> None:
        logger.warning(
            "候补分区最新论文缺少 ID，已跳过该分区：分区=%s 载荷=%s",
            zone,
            json.dumps(self._build_meta_preview(candidate), ensure_ascii=False),
        )

    def _extract_run_candidate_paper_id(
        self,
        *,
        zone: str,
        candidate: dict[str, Any],
        is_primary: bool,
    ) -> str | None:
        paper_id = str(candidate.get("id", "")).strip()
        if paper_id:
            return paper_id
        if is_primary:
            self._raise_run_candidate_id_error(
                zone=zone,
                is_primary=is_primary,
                candidate=candidate,
            )
        self._log_run_candidate_id_warning(zone=zone, candidate=candidate)
        return None

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
        zone = str(batch.zone)
        latest = dict(batch.latest)
        paper_id = str(batch.paper_id)
        detail_url = str(batch.detail_url)
        targets = list(batch.targets)
        sent_history_by_target = dict(batch.sent_history_by_target)
        sent_history_lookup_by_target = dict(batch.sent_history_lookup_by_target)
        report = self._build_run_batch_report(
            zone=zone,
            paper_id=paper_id,
            detail_url=detail_url,
            sent_total=len(targets),
        )
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            payload = await self._load_submission_payload(latest, paper_id)
            logger.info(
                "论文元数据：%s",
                json.dumps(self._build_meta_preview(payload), ensure_ascii=False),
            )
            pdf_file, png_file, pdf_url = await self._prepare_pdf_assets(payload, paper_id)
            text = self._build_push_text(
                payload,
                detail_url,
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
            if sent_success_targets:
                await self._mark_targets_delivered(
                    zone=zone,
                    paper_id=paper_id,
                    success_targets=sent_success_targets,
                    sent_history_by_target=sent_history_by_target,
                    sent_history_lookup_by_target=sent_history_lookup_by_target,
                )
            report.status = self._resolve_send_status(sent_ok=sent_ok, sent_total=len(targets))
            report.reason_code = self._resolve_send_reason_code(report.status)
            return report
        except Exception as exc:
            if report.sent_ok > 0:
                logger.error(
                    "写入推送去重状态失败：分区=%s 论文ID=%s 错误=%s",
                    zone,
                    paper_id,
                    mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                return self._build_failed_run_batch_report(
                    report=report,
                    reason_code=RunReason.DELIVERY_STATE_WRITE_FAILED,
                    debug_reason=str(exc),
                )
            logger.error(
                "批次推送失败：分区=%s 论文ID=%s 错误=%s",
                zone,
                paper_id,
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            return self._build_failed_run_batch_report(
                report=report,
                reason_code=RunReason.ALL_SENDS_FAILED,
                debug_reason=str(exc),
            )
        finally:
            await self._temp_files.release(pdf_file, png_file)
            try:
                await self._maybe_trim_temp_files()
            except Exception:
                logger.warning("批次推送后清理临时文件失败。", exc_info=True)

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

    def _to_run_report_dict(self, report: RunReport | dict[str, Any]) -> dict[str, Any]:
        if isinstance(report, RunReport):
            return report.to_dict()
        return dict(report)

    def _resolve_batch_send_concurrency(self, batch_count: int) -> int:
        return min(MAX_BATCH_SEND_CONCURRENCY, self._get_configured_send_concurrency(), batch_count)

    def _get_configured_send_concurrency(self) -> int:
        raw_concurrency = getattr(self, "_send_concurrency", 3)
        try:
            configured_concurrency = int(raw_concurrency)
        except (TypeError, ValueError):
            configured_concurrency = 3
        return min(MAX_SEND_CONCURRENCY, max(1, configured_concurrency))

    def _render_status_text(self, status: str) -> str:
        return REPORT_STATUS_LABELS.get(status, status)

    def _render_reason_text(self, reason_code: str) -> str:
        return REPORT_REASON_LABELS.get(reason_code, reason_code)

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
        payload = dict(latest)
        if str(payload.get("pdf_path") or payload.get("file_path") or "").strip():
            return payload

        try:
            detail = await self._supabase.fetch_submission_detail(paper_id)
        except Exception:
            logger.warning("获取论文详情失败，将回退为列表页载荷。", exc_info=True)
            detail = {}
        payload = {**payload, **(detail or {})}
        return payload

    async def _prepare_pdf_assets(self, payload: dict[str, Any], paper_id: str) -> tuple[Path, Path, str]:
        pdf_key = str(payload.get("pdf_path") or payload.get("file_path") or "").strip()
        if not pdf_key:
            raise RuntimeError("PDF 路径缺失")

        try:
            signed_url = await self._supabase.create_signed_pdf_url(pdf_key)
        except Exception as exc:
            logger.error(
                "生成签名 URL 失败：%s",
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            raise RuntimeError("生成签名 URL 失败") from exc

        if not signed_url:
            raise RuntimeError("签名 URL 为空")

        logger.info("已取得签名 PDF 地址：%s", self._mask_token(signed_url))
        pdf_file, png_file = self._temp_files.build_output_paths(paper_id)
        await self._temp_files.mark_in_use(pdf_file, png_file)

        try:
            try:
                pdf_status, pdf_type = await self._supabase.download_pdf_file(
                    signed_url,
                    pdf_file,
                )
                logger.info("PDF 下载完成：状态码=%s 内容类型=%s", pdf_status, pdf_type)
            except Exception as exc:
                logger.error(
                    "下载 PDF 失败：%s",
                    mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                raise RuntimeError("下载 PDF 失败") from exc

            self._pdf_service.ensure_pdf_size_limit(pdf_file)
            try:
                await asyncio.to_thread(self._pdf_service.export_first_page_png, pdf_file, png_file)
            except Exception as exc:
                logger.error(
                    "导出 PDF 首页预览图失败：%s",
                    mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                raise RuntimeError("导出 PDF 首页预览图失败") from exc

            png_size = png_file.stat().st_size if png_file.exists() else 0
            logger.info("PDF 首页预览图导出完成：文件=%s 字节数=%s", str(png_file), png_size)
            return pdf_file, png_file, signed_url
        except Exception:
            await self._temp_files.release(pdf_file, png_file)
            raise

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
        title = self._fallback_text(payload.get("manuscript_title"))
        author = self._fallback_text(payload.get("author_name"))
        institution = self._fallback_text(payload.get("institution"))
        zone_text = self._format_bilingual_label(zone or payload.get("zone"), ZONE_LABELS)
        submitted = self._format_datetime(payload.get("created_at"))
        discipline = self._format_bilingual_label(payload.get("discipline"), DISCIPLINE_LABELS)
        viscosity = self._format_bilingual_label(payload.get("viscosity"), VISCOSITY_LABELS)
        avg_score = self._format_number(payload.get("avg_score"))
        weighted_score = self._format_number(payload.get("weighted_score"))
        rating_count = self._fallback_text(payload.get("rating_count"))
        detail_text = self._format_detail_text(detail_url)
        lines = [
            "S.H.I.T Journal 论文推送",
            f"分区: {zone_text}",
            f"标题: {title}",
            f"作者: {author}",
            f"单位: {institution}",
            f"提交时间: {submitted}",
            f"学科: {discipline}",
            f"粘度: {viscosity}",
            f"评分: 平均={avg_score}, 加权={weighted_score}, 票数={rating_count}",
            f"详情: {detail_text}",
        ]
        return "\n".join(lines)

    def _build_preprint_detail_url(self, paper_id: str) -> str:
        normalized_paper_id = str(paper_id).strip()
        return f"{DETAIL_URL_BASE}/preprints/{normalized_paper_id}"

    def _format_detail_text(self, detail_url: str) -> str:
        detail_text = str(detail_url or "").strip()
        if not detail_text:
            return detail_text
        if not self._cfg_bool("detail_hide_domain", False):
            return detail_text
        if detail_text.startswith(DETAIL_URL_BASE):
            path = detail_text[len(DETAIL_URL_BASE) :].strip()
            return path if path.startswith("/") else f"/{path}"
        return detail_text

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

    def _merge_sessions(self, first: list[str], second: list[str]) -> list[str]:
        merged: list[str] = []
        seen = set()
        for item in first + second:
            if item in seen:
                continue
            seen.add(item)
            merged.append(item)
        return merged

    async def _get_bound_sessions(self) -> list[str]:
        raw = await self.get_kv_data("bound_sessions", [])
        return self._normalize_session_list(raw)

    async def _set_bound_sessions(self, sessions: list[str]) -> None:
        normalized = self._normalize_session_list(sessions)
        await self.put_kv_data("bound_sessions", normalized)

    async def _bind_session(self, session: str) -> bool:
        async with self._bound_sessions_lock:
            bound = await self._get_bound_sessions()
            if session in bound:
                return False
            bound.append(session)
            await self._set_bound_sessions(bound)
            return True

    async def _unbind_session(self, session: str) -> bool:
        async with self._bound_sessions_lock:
            bound = await self._get_bound_sessions()
            if session not in bound:
                return False
            updated = [item for item in bound if item != session]
            await self._set_bound_sessions(updated)
            return True

    async def _get_all_target_sessions(self) -> list[str]:
        cfg_targets = self._normalize_session_list(self._cfg("target_sessions", []))
        bound = await self._get_bound_sessions()
        return self._merge_sessions(cfg_targets, bound)

    async def _get_last_seen_map(self) -> dict[str, str]:
        raw = await self.get_kv_data("last_seen_by_zone", {})
        return self._clean_kv_dict(raw)

    def _run_history_store_key(self, zone: str, session: str) -> str:
        return f"{RUN_SENT_HISTORY_KV_PREFIX}{self._target_zone_key(zone, session)}"

    def _normalize_target_zone_key(self, raw_key: Any) -> str:
        text = str(raw_key or "").strip()
        if not text:
            return ""
        zone, sep, session = text.partition("::")
        if not sep:
            return ""
        session = session.strip()
        if not session:
            return ""
        return self._target_zone_key(zone, session)

    async def _ensure_run_sent_history_storage_ready(self) -> None:
        if self._run_sent_history_storage_ready:
            return
        async with self._run_sent_history_lock:
            if self._run_sent_history_storage_ready:
                return

            version = str(await self.get_kv_data(RUN_SENT_HISTORY_STORAGE_VERSION_KEY, "") or "").strip()
            if version == RUN_SENT_HISTORY_STORAGE_VERSION:
                self._run_sent_history_storage_ready = True
                return

            legacy_raw = await self.get_kv_data("last_sent_by_target_zone", {})
            merged = self._build_legacy_run_sent_histories(legacy_raw)
            for target_zone_key, history in merged.items():
                zone, _, session = target_zone_key.partition("::")
                if not session:
                    continue
                await self.put_kv_data(self._run_history_store_key(zone, session), history)
            if legacy_raw:
                await self.put_kv_data("last_sent_by_target_zone", {})
            await self.put_kv_data(RUN_SENT_HISTORY_STORAGE_VERSION_KEY, RUN_SENT_HISTORY_STORAGE_VERSION)
            self._run_sent_history_storage_ready = True

    def _build_legacy_run_sent_histories(self, legacy_raw: Any) -> dict[str, list[str]]:
        merged: dict[str, list[str]] = {}
        for raw_key, paper_id in self._clean_kv_dict(legacy_raw).items():
            key = self._normalize_target_zone_key(raw_key)
            if not key:
                continue
            merged[key] = [paper_id]

        return merged

    async def _get_run_sent_history(self, zone: str, session: str) -> list[str]:
        await self._ensure_run_sent_history_storage_ready()
        return await self._get_run_sent_history_unlocked(zone, session)

    async def _get_run_sent_history_unlocked(self, zone: str, session: str) -> list[str]:
        raw = await self.get_kv_data(self._run_history_store_key(zone, session), [])
        return self._clean_kv_list(raw)

    async def _get_run_sent_histories(self, zone: str, targets: list[str]) -> dict[str, list[str]]:
        await self._ensure_run_sent_history_storage_ready()
        raw_histories = await asyncio.gather(
            *[self.get_kv_data(self._run_history_store_key(zone, session), []) for session in targets],
        )
        return {
            session: self._clean_kv_list(raw_history)
            for session, raw_history in zip(targets, raw_histories)
        }

    def _chi_shi_group_zone_key(self, zone: str, session: str) -> str:
        return f"{self._normalize_zone_name(zone)}::{session}"

    def _chi_shi_history_store_key(self, zone: str, session: str) -> str:
        return f"{CHI_SHI_HISTORY_KV_PREFIX}{self._chi_shi_group_zone_key(zone, session)}"

    def _normalize_group_zone_key(self, raw_key: Any) -> str:
        text = str(raw_key or "").strip()
        if not text:
            return ""
        zone, sep, session = text.partition("::")
        if not sep:
            return ""
        session = session.strip()
        if not session:
            return ""
        return self._chi_shi_group_zone_key(zone, session)

    async def _ensure_chi_shi_history_storage_ready(self) -> None:
        if self._chi_shi_history_storage_ready:
            return
        async with self._chi_shi_dedupe_lock:
            if self._chi_shi_history_storage_ready:
                return

            version = str(await self.get_kv_data(CHI_SHI_HISTORY_STORAGE_VERSION_KEY, "") or "").strip()
            if version == CHI_SHI_HISTORY_STORAGE_VERSION:
                self._chi_shi_history_storage_ready = True
                return

            merged = await self._load_legacy_chi_shi_histories()
            for group_zone_key, history in merged.items():
                zone, _, session = group_zone_key.partition("::")
                if not session:
                    continue
                await self.put_kv_data(self._chi_shi_history_store_key(zone, session), history)
            await self.put_kv_data(CHI_SHI_HISTORY_STORAGE_VERSION_KEY, CHI_SHI_HISTORY_STORAGE_VERSION)
            self._chi_shi_history_storage_ready = True

    async def _load_legacy_chi_shi_histories(self) -> dict[str, list[str]]:
        history_raw = await self.get_kv_data("chi_shi_sent_history_by_group_zone", {})
        legacy_raw = await self.get_kv_data("chi_shi_last_sent_by_group_zone", {})

        merged: dict[str, list[str]] = {}
        for raw_key, history in self._clean_kv_list_dict(history_raw).items():
            key = self._normalize_group_zone_key(raw_key)
            if not key:
                continue
            merged[key] = self._apply_chi_shi_history_policy(history)

        for raw_key, paper_id in self._clean_kv_dict(legacy_raw).items():
            key = self._normalize_group_zone_key(raw_key)
            if not key:
                continue
            history = merged.get(key, [])
            if paper_id not in history:
                history = history + [paper_id]
            merged[key] = self._apply_chi_shi_history_policy(history)

        return {key: history for key, history in merged.items() if history}

    async def _get_chi_shi_sent_history(self, zone: str, session: str) -> list[str]:
        await self._ensure_chi_shi_history_storage_ready()
        async with self._chi_shi_dedupe_lock:
            return await self._get_chi_shi_sent_history_unlocked(zone, session)

    async def _get_chi_shi_sent_history_unlocked(self, zone: str, session: str) -> list[str]:
        raw = await self.get_kv_data(self._chi_shi_history_store_key(zone, session), [])
        return self._apply_chi_shi_history_policy(self._clean_kv_list(raw))

    def _pick_first_unsent_candidate(
        self,
        candidates: list[dict[str, Any]],
        sent_set: set[str],
    ) -> dict[str, Any] | None:
        for candidate in candidates:
            paper_id = str(candidate.get("id", "")).strip()
            if paper_id and paper_id not in sent_set:
                return candidate
        return None

    async def _mark_chi_shi_paper_sent(
        self,
        zone: str,
        session: str,
        paper_id: str,
    ) -> None:
        await self._ensure_chi_shi_history_storage_ready()
        async with self._chi_shi_dedupe_lock:
            history = await self._get_chi_shi_sent_history_unlocked(zone, session)
            await self.put_kv_data(
                self._chi_shi_history_store_key(zone, session),
                self._apply_chi_shi_history_policy(self._prepend_history_item(history, paper_id)),
            )

    def _apply_chi_shi_history_policy(self, history: list[str]) -> list[str]:
        if self._cfg_bool("chi_shi_keep_full_history", True):
            return history

        limit = self._cfg_int("chi_shi_history_limit", 30, min_value=1)
        return history[:limit]

    def _clean_kv_dict(self, raw: Any) -> dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        result: dict[str, str] = {}
        for key, value in raw.items():
            key_s = str(key).strip()
            val_s = str(value).strip()
            if key_s and val_s:
                result[key_s] = val_s
        return result

    def _clean_kv_list(self, raw: Any) -> list[str]:
        values = raw if isinstance(raw, list) else [raw]
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in values:
            item_s = str(item).strip()
            if not item_s or item_s in seen:
                continue
            seen.add(item_s)
            cleaned.append(item_s)
        return cleaned

    def _prepend_history_item(self, history: list[str], item: str) -> list[str]:
        value = str(item).strip()
        if not value:
            return history
        updated = [entry for entry in history if entry != value]
        updated.insert(0, value)
        return updated

    def _build_history_lookup(self, history_by_target: dict[str, list[str]]) -> dict[str, set[str]]:
        return {session: set(history) for session, history in history_by_target.items()}

    def _clean_kv_list_dict(self, raw: Any) -> dict[str, list[str]]:
        if not isinstance(raw, dict):
            return {}
        result: dict[str, list[str]] = {}
        for key, value in raw.items():
            key_s = str(key).strip()
            if not key_s:
                continue
            if isinstance(value, list):
                values = value
            else:
                values = [value]
            cleaned: list[str] = []
            seen: set[str] = set()
            for item in values:
                item_s = str(item).strip()
                if not item_s or item_s in seen:
                    continue
                seen.add(item_s)
                cleaned.append(item_s)
            if cleaned:
                result[key_s] = cleaned
        return result

    def _target_zone_key(self, zone: str, session: str) -> str:
        return f"{self._normalize_zone_name(zone)}::{session}"

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

    async def _mark_targets_delivered(
        self,
        zone: str,
        paper_id: str,
        success_targets: list[str],
        sent_history_by_target: dict[str, list[str]],
        sent_history_lookup_by_target: dict[str, set[str]],
    ) -> None:
        await self._ensure_run_sent_history_storage_ready()
        pending_writes: list[tuple[str, list[str]]] = []
        async with self._run_sent_history_lock:
            for session in success_targets:
                history = sent_history_by_target.get(session)
                if history is None:
                    history = await self._get_run_sent_history_unlocked(zone, session)
                else:
                    history = list(history)
                history = self._prepend_history_item(history, paper_id)
                sent_history_by_target[session] = history
                history_lookup = sent_history_lookup_by_target.get(session)
                if history_lookup is None:
                    history_lookup = set(history)
                    sent_history_lookup_by_target[session] = history_lookup
                else:
                    history_lookup.add(paper_id)
                pending_writes.append((self._run_history_store_key(zone, session), history))

        await asyncio.gather(
            *[self.put_kv_data(store_key, history) for store_key, history in pending_writes],
        )

    def _format_datetime(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return "无"
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return text

    def _format_number(self, value: Any) -> str:
        if value is None or value == "":
            return "无"
        try:
            return f"{float(value):.4f}"
        except Exception:
            return str(value)

    def _fallback_text(self, value: Any) -> str:
        text = str(value or "").strip()
        return text if text else "无"

    def _format_bilingual_label(
        self,
        raw_value: Any,
        mapping: dict[str, tuple[str, str]],
    ) -> str:
        text = str(raw_value or "").strip()
        if not text:
            return "无"
        key = text.lower()
        if key in mapping:
            cn, en = mapping[key]
            return f"{cn} / {en}"
        return text

    def _mask_token(self, url: str) -> str:
        if "token=" not in url:
            return url
        head, _, _ = url.partition("token=")
        return head + "token=<已隐藏>"

    def _build_meta_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        keys = [
            "id",
            "manuscript_title",
            "author_name",
            "institution",
            "discipline",
            "viscosity",
            "created_at",
            "avg_score",
            "rating_count",
            "weighted_score",
            "pdf_path",
            "zone",
        ]
        return {k: payload.get(k) for k in keys}

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

    def _render_report(self, report: RunReport | dict[str, Any], include_debug: bool = False) -> str:
        report_dict = self._to_run_report_dict(report)
        status = str(report_dict.get("status", "unknown") or "unknown")
        reason_code = str(report_dict.get("reason_code") or report_dict.get("reason") or "UNKNOWN")
        debug_reason = mask_sensitive_text(str(report_dict.get("debug_reason", "")).strip())
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
        empty_batch = {
            "status": str(report_dict.get("status", "unknown") or "unknown"),
            "zone": str(report_dict.get("zone", "")).strip(),
            "paper_id": str(report_dict.get("paper_id", "")).strip(),
            "detail_url": str(report_dict.get("detail_url", "")).strip(),
            "sent_ok": report_dict.get("sent_ok", 0),
            "sent_total": report_dict.get("sent_total", 0),
        }
        return self._render_single_batch_report(
            report=report_dict,
            batch=empty_batch,
            status_text=status_text,
            reason_text=reason_text,
            latest_only=latest_only,
            requested_zone=requested_zone,
            warnings=warnings,
            debug_reason=debug_reason,
            include_debug=include_debug,
        )
