from __future__ import annotations

import asyncio
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
        ChiShiService,
        CommandGate,
        CronScheduler,
        HistoryStore,
        PdfService,
        PushMessageService,
        ReportRenderer,
        RunBatchSender,
        RunCycleService,
        RunSelector,
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
        ChiShiService,
        CommandGate,
        CronScheduler,
        HistoryStore,
        PdfService,
        PushMessageService,
        ReportRenderer,
        RunBatchSender,
        RunCycleService,
        RunSelector,
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
        self._cron_job_ids: list[str] = []
        self._temp_trim_lock = asyncio.Lock()
        self._next_temp_trim_monotonic = 0.0
        self._plugin_data_dir = Path(".")
        self._temp_dir = Path(".")
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
        self._command_gate = self._create_command_gate()
        self._run_batch_sender = self._create_run_batch_sender()
        self._run_cycle_service = self._create_run_cycle_service()
        self._cron_scheduler = self._create_cron_scheduler()
        self._chi_shi_service = self._create_chi_shi_service()

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

    def _create_command_gate(self) -> CommandGate:
        return CommandGate(
            context_getter=lambda: self.context,
            cfg_bool_getter=self._cfg_bool,
            logger=logger,
        )

    def _create_run_batch_sender(self) -> RunBatchSender:
        return RunBatchSender(
            context_getter=lambda: self.context,
            cfg_int_getter=self._cfg_int,
            history_store=self._history_store,
            load_submission_payload=lambda latest, paper_id: self._asset_pipeline.load_submission_payload(
                latest,
                paper_id,
            ),
            prepare_pdf_assets=lambda payload, paper_id: self._asset_pipeline.prepare_pdf_assets(payload, paper_id),
            build_push_text=lambda payload, detail_url, zone: self._asset_pipeline.build_push_text(
                payload,
                detail_url,
                zone=zone,
            ),
            build_meta_preview=lambda payload: self._asset_pipeline.build_meta_preview(payload),
            send_session_push=lambda **kwargs: self._push_messages.send_session_push(**kwargs),
            release_temp_files=lambda pdf_file, png_file: self._temp_files.release(pdf_file, png_file),
            maybe_trim_temp_files=lambda: self._maybe_trim_temp_files(),
            logger=logger,
            mask_sensitive_text=mask_sensitive_text,
            max_send_concurrency=MAX_SEND_CONCURRENCY,
            max_batch_send_concurrency=MAX_BATCH_SEND_CONCURRENCY,
        )

    def _create_run_cycle_service(self) -> RunCycleService:
        return RunCycleService(
            cfg_getter=self._cfg,
            run_selector=self._run_selector,
            history_store=self._history_store,
            send_batches=lambda batches, send_semaphore=None: self._run_batch_sender.send_run_batches(
                batches,
                send_semaphore=send_semaphore,
            ),
            coerce_run_batch_report=lambda value: self._run_batch_sender.coerce_run_batch_report(value),
            resolve_run_batch_reports=lambda batch_reports, sent_ok, sent_total: self._run_batch_sender.resolve_run_batch_reports(
                batch_reports=batch_reports,
                sent_ok=sent_ok,
                sent_total=sent_total,
            ),
            get_primary_zone=self._get_primary_zone,
            get_candidate_zones=self._get_candidate_zones,
            get_configured_send_concurrency=self._run_batch_sender.get_configured_send_concurrency,
            maybe_trim_temp_files=lambda: self._maybe_trim_temp_files(),
            logger=logger,
            mask_sensitive_text=mask_sensitive_text,
            run_fetch_page_size=RUN_FETCH_PAGE_SIZE,
        )

    def _create_cron_scheduler(self) -> CronScheduler:
        def _update_cron_job_ids(ids: list[str]) -> None:
            self._cron_job_ids = [str(job_id).strip() for job_id in ids if str(job_id).strip()]

        return CronScheduler(
            context_getter=lambda: self.context,
            plugin_id_getter=lambda: getattr(self, "plugin_id", "shitjournal_daily"),
            cfg_getter=self._cfg,
            cfg_bool_getter=self._cfg_bool,
            kv_getter=self.get_kv_data,
            kv_putter=self.put_kv_data,
            get_cron_job_ids=lambda: list(self._cron_job_ids),
            set_cron_job_ids=_update_cron_job_ids,
            run_cycle=lambda **kwargs: self._run_cycle_service.run_cycle(**kwargs),
            render_report=lambda report, include_debug=False: self._report_renderer.render_report(
                report,
                include_debug=include_debug,
            ),
            logger=logger,
            default_schedule_times=DEFAULT_SCHEDULE_TIMES,
        )

    def _create_chi_shi_service(self) -> ChiShiService:
        return ChiShiService(
            select_candidate_from_zones=lambda **kwargs: self._run_selector.select_chi_shi_candidate_from_zones(
                **kwargs,
            ),
            load_submission_payload=lambda candidate, paper_id: self._asset_pipeline.load_submission_payload(
                candidate,
                paper_id,
            ),
            prepare_pdf_assets=lambda payload, paper_id: self._asset_pipeline.prepare_pdf_assets(payload, paper_id),
            build_push_text=lambda payload, detail_url, zone: self._asset_pipeline.build_push_text(
                payload,
                detail_url,
                zone=zone,
            ),
            build_preprint_detail_url=lambda paper_id: self._asset_pipeline.build_preprint_detail_url(paper_id),
            send_event_push=lambda **kwargs: self._push_messages.send_event_push(**kwargs),
            mark_chi_shi_paper_sent=lambda zone, session_key, paper_id: self._history_store.mark_chi_shi_paper_sent(
                zone,
                session_key,
                paper_id,
            ),
            get_primary_zone=self._get_primary_zone,
            get_candidate_zones=self._get_candidate_zones,
            build_zone_scope_text=self._build_zone_scope_text,
            cfg_int_getter=self._cfg_int,
            mask_sensitive_text=mask_sensitive_text,
            fetch_page_size=CHI_SHI_FETCH_PAGE_SIZE,
        )

    async def initialize(self):
        self._plugin_data_dir = self._resolve_plugin_data_dir()
        self._temp_dir = self._plugin_data_dir / "tmp"
        self._temp_files.set_temp_dir(self._temp_dir)
        self._ensure_supabase_key_or_raise()
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        await self._history_store.ensure_chi_shi_history_storage_ready()
        await self._cron_scheduler.clear_cron_jobs()
        await self._cron_scheduler.register_cron_jobs()
        await self._maybe_trim_temp_files(force=True)
        logger.info("shitjournal_daily 插件初始化完成。")

    async def terminate(self):
        await self._cron_scheduler.clear_cron_jobs()
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
        normalized = self._command_gate.normalize_command_event(
            event=event,
            args=args,
            kwargs=kwargs,
            command_name="shitjournal",
        )
        if not normalized:
            return
        event, args, kwargs = normalized

        if not await self._command_gate.check_command_permission(event):
            return

        action, arg = self._command_gate.parse_command_action_arg(
            args=args,
            kwargs=kwargs,
            default_action="help",
        )
        fallback_action, fallback_arg = self._command_gate.parse_action_arg_from_message(
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
            report = await self._run_cycle_service.run_cycle(force=force, source=f"手动:{event.get_sender_id()}")
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
        normalized = self._command_gate.normalize_command_event(
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
        ignore_cooldown = self._command_gate.is_admin_event(event)
        allowed, deny_reason = await self._chi_shi_service.try_enter_chi_shi_cooldown(
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
            outputs, apply_success_cooldown, pdf_file, png_file = await self._chi_shi_service.execute_wo_yao_chi_shi(
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
            await self._chi_shi_service.leave_chi_shi_cooldown(
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

    def _get_chi_shi_session_scope_text(self, event: AstrMessageEvent) -> str:
        if event.get_group_id():
            return "本群"
        session_key = str(getattr(event, "unified_msg_origin", "")).strip()
        if is_private_message_session(session_key):
            return "当前私聊"
        return "当前会话"

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
