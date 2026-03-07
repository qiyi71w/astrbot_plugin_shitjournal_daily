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
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.message_components import File, Image, Plain
from astrbot.api.star import Context, Star, StarTools, register

try:
    from .services import PdfService, SupabaseClient, TempFileManager
    from .services.sensitive import mask_sensitive_text
except ImportError:
    _plugin_dir = Path(__file__).resolve().parent
    if str(_plugin_dir) not in sys.path:
        sys.path.insert(0, str(_plugin_dir))
    from services import PdfService, SupabaseClient, TempFileManager
    from services.sensitive import mask_sensitive_text


DEFAULT_SUPABASE_URL = "https://bcgdqepzakcufaadgnda.supabase.co"
DEFAULT_SUPABASE_BUCKET = "manuscripts"
DEFAULT_ZONE = "septic"
DEFAULT_SCHEDULE_TIMES = ["09:00", "21:00"]
MAX_SEND_CONCURRENCY = 20
CHI_SHI_FETCH_CANDIDATE_LIMIT = 5
CHI_SHI_SENT_HISTORY_KEEP = 30
SUPABASE_KEY_ENV_NAME = "SUPABASE_PUBLISHABLE_KEY"
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


@register(
    "astrbot_plugin_shitjournal_daily",
    "AstrBot",
    "定时推送 shitjournal 最新论文到群聊",
    "1.0.0",
)
class ShitJournalDailyPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}
        self._run_lock = asyncio.Lock()
        self._cron_job_ids: list[str] = []
        self._chi_shi_cooldown_lock = asyncio.Lock()
        self._chi_shi_dedupe_lock = asyncio.Lock()
        self._chi_shi_group_cooldown_until_monotonic: dict[str, float] = {}
        self._chi_shi_group_inflight: set[str] = set()
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
        await self._clear_cron_jobs()
        await self._register_cron_jobs()
        await self._temp_files.trim()
        logger.info("shitjournal_daily initialized.")

    async def terminate(self):
        await self._clear_cron_jobs()
        self._supabase.close()
        logger.info("shitjournal_daily terminated.")

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
            bound = await self._get_bound_sessions()
            current = event.unified_msg_origin
            if current in bound:
                yield event.plain_result(f"当前会话已绑定：{current}")
                return
            bound.append(current)
            await self._set_bound_sessions(bound)
            yield event.plain_result(f"绑定成功：{current}")
            return

        if action == "unbind":
            bound = await self._get_bound_sessions()
            current = event.unified_msg_origin
            if current not in bound:
                yield event.plain_result(f"当前会话未绑定：{current}")
                return
            bound = [s for s in bound if s != current]
            await self._set_bound_sessions(bound)
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
                    source.append("config")
                if target in bound:
                    source.append("bind")
                source_text = ",".join(source)
                lines.append(f"{idx}. {target} [{source_text}]")
            yield event.plain_result("\n".join(lines))
            return

        if action == "run":
            force = bool(arg) and arg.split()[0] == "force"
            report = await self._run_cycle(force=force, source=f"manual:{event.get_sender_id()}")
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
        """抓取最新论文并推送到当前群聊（按群冷却）"""
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

        if not event.get_group_id():
            yield event.plain_result("该指令仅支持群聊。")
            return

        session_key = event.unified_msg_origin
        allowed, deny_reason = await self._try_enter_chi_shi_cooldown(session_key=session_key)
        if not allowed:
            yield event.plain_result(deny_reason)
            return

        success = False
        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            primary_zone = self._get_primary_zone()
            zone_order = self._get_candidate_zones(primary_zone)
            selected, saw_candidates = await self._select_chi_shi_candidate_from_zones(
                zone_order=zone_order,
                session_key=session_key,
            )
            if not selected:
                if saw_candidates:
                    success = True
                    yield event.plain_result(
                        f"本群{self._build_zone_scope_text(zone_order)}最近这些论文都推送过了，等下一篇。",
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
            detail_url = f"https://shitjournal.org/preprints/{paper_id}"
            pdf_file, png_file = await self._prepare_pdf_assets(payload, paper_id)
            text = self._build_push_text(payload, detail_url)
            chain = self._build_push_chain(text, png_file, pdf_file)
            await self._mark_chi_shi_paper_sent(zone, session_key, paper_id)
            yield chain
            success = True
        except Exception as exc:
            logger.error(
                "chi_shi command failed: %s",
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            yield event.plain_result("抓取失败，请稍后重试。")
        finally:
            await self._leave_chi_shi_cooldown(session_key=session_key, success=success)
            await self._temp_files.release(pdf_file, png_file)
            try:
                await self._temp_files.trim()
            except Exception:
                logger.warning("trim temp files failed after chi_shi command", exc_info=True)

    async def _check_command_permission(self, event: AstrMessageEvent) -> bool:
        if not self._looks_like_message_event(event):
            logger.error(
                "command permission check failed: invalid event type=%s",
                type(event).__name__,
            )
            return False

        if not self._cfg_bool("command_admin_only", True):
            return True
        is_admin = getattr(event, "is_admin", None)
        if callable(is_admin) and is_admin():
            return True
        if self._is_sender_in_admins(event):
            return True
        if self._cfg_bool("command_no_permission_reply", True):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

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
                "command %s normalized shifted event from kwargs (type=%s).",
                command_name,
                type(event).__name__,
            )
            return candidate, args, cleaned_kwargs

        for idx, item in enumerate(args):
            if self._looks_like_message_event(item):
                shifted_args = args[:idx] + args[idx + 1 :]
                logger.warning(
                    "command %s normalized shifted event from args[%d] (type=%s).",
                    command_name,
                    idx,
                    type(event).__name__,
                )
                return item, shifted_args, kwargs

        logger.error(
            "command %s event normalization failed: event_type=%s args_types=%s kwargs_keys=%s",
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

    async def _try_enter_chi_shi_cooldown(
        self,
        session_key: str,
    ) -> tuple[bool, str]:
        async with self._chi_shi_cooldown_lock:
            if session_key in self._chi_shi_group_inflight:
                return False, "本群已有赤石任务在执行，请稍后再试。"

            now = time.monotonic()
            cooldown_until = float(self._chi_shi_group_cooldown_until_monotonic.get(session_key, 0.0))
            remaining = cooldown_until - now
            if remaining > 0:
                return False, f"本群冷却中，请 {int(remaining + 0.999)} 秒后再试。"

            self._chi_shi_group_inflight.add(session_key)
            return True, ""

    async def _leave_chi_shi_cooldown(self, session_key: str, success: bool) -> None:
        async with self._chi_shi_cooldown_lock:
            self._chi_shi_group_inflight.discard(session_key)
            cooldown_sec = self._cfg_int("chi_shi_group_cooldown_sec", 60, min_value=0)
            fail_cooldown_sec = self._cfg_int("chi_shi_group_fail_cooldown_sec", 10, min_value=0)
            effective_cooldown = cooldown_sec if success else fail_cooldown_sec
            if effective_cooldown > 0:
                self._chi_shi_group_cooldown_until_monotonic[session_key] = (
                    time.monotonic() + effective_cooldown
                )
            else:
                self._chi_shi_group_cooldown_until_monotonic.pop(session_key, None)

    async def _register_cron_jobs(self) -> None:
        cron_manager = getattr(self.context, "cron_manager", None)
        if cron_manager is None:
            logger.error("cron_manager unavailable, schedule registration skipped.")
            return

        timezone = str(self._cfg("timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai"
        times = self._normalize_schedule_times(self._cfg("schedule_times", DEFAULT_SCHEDULE_TIMES))
        plugin_id = getattr(self, "plugin_id", "shitjournal_daily")
        created_job_ids: list[str] = []

        for idx, time_text in enumerate(times):
            parsed = self._parse_hhmm(time_text)
            if parsed is None:
                logger.warning("invalid schedule time skipped: %s", time_text)
                continue
            hour, minute = parsed
            cron_expression = f"{minute} {hour} * * *"
            job = await cron_manager.add_basic_job(
                name=f"{plugin_id}_shitjournal_{idx}",
                cron_expression=cron_expression,
                handler=self._scheduled_tick,
                description=f"shitjournal daily push at {time_text}",
                timezone=timezone,
                payload={"schedule_time": time_text},
                enabled=True,
                persistent=False,
            )
            created_job_ids.append(job.job_id)
            logger.info(
                "registered cron job: id=%s time=%s expr=%s tz=%s",
                job.job_id,
                time_text,
                cron_expression,
                timezone,
            )

        self._cron_job_ids = created_job_ids
        await self.put_kv_data("cron_job_ids", created_job_ids)

    async def _clear_cron_jobs(self) -> None:
        cron_manager = getattr(self.context, "cron_manager", None)
        if cron_manager is None:
            self._cron_job_ids = []
            await self.put_kv_data("cron_job_ids", [])
            return

        ids = self._cron_job_ids
        if not ids:
            loaded = await self.get_kv_data("cron_job_ids", [])
            if isinstance(loaded, list):
                ids = [str(i) for i in loaded if str(i).strip()]

        for job_id in ids:
            try:
                await cron_manager.delete_job(job_id)
                logger.info("deleted old cron job: %s", job_id)
            except Exception:
                logger.warning("delete cron job failed (ignored): %s", job_id, exc_info=True)

        self._cron_job_ids = []
        await self.put_kv_data("cron_job_ids", [])

    async def _scheduled_tick(self, schedule_time: str = "") -> None:
        report = await self._run_cycle(force=False, source=f"cron:{schedule_time or 'unknown'}")
        logger.info("scheduled run finished: %s", self._render_report(report, include_debug=True))

    async def _run_cycle(self, force: bool, source: str) -> dict[str, Any]:
        if self._run_lock.locked():
            return {
                "status": "skipped",
                "reason_code": "RUN_IN_PROGRESS",
                "source": source,
                "debug_reason": "",
            }

        async with self._run_lock:
            primary_zone = self._get_primary_zone()
            zone_order = self._get_candidate_zones(primary_zone)
            pdf_file: Path | None = None
            png_file: Path | None = None
            report: dict[str, Any] = {
                "status": "failed",
                "source": source,
                "zone": primary_zone,
                "requested_zone": primary_zone,
                "force": force,
                "paper_id": "",
                "detail_url": "",
                "sent_ok": 0,
                "sent_total": 0,
                "reason_code": "",
                "debug_reason": "",
            }
            logger.info(
                "run start: source=%s primary_zone=%s zone_order=%s force=%s",
                source,
                primary_zone,
                zone_order,
                force,
            )
            try:
                targets = await self._get_all_target_sessions()
                if not targets:
                    report["reason_code"] = "NO_TARGET_SESSION_CONFIGURED"
                    return report

                last_seen_map = await self._get_last_seen_map()
                last_sent_target_map = await self._get_last_sent_target_map()
                try:
                    selected, saw_submission, last_seen_dirty = await self._select_run_candidate(
                        zone_order=zone_order,
                        targets=targets,
                        last_seen_map=last_seen_map,
                        last_sent_target_map=last_sent_target_map,
                        force=force,
                    )
                except RuntimeError as exc:
                    message = str(exc).strip()
                    if message == "EMPTY_PAPER_ID":
                        report["reason_code"] = "EMPTY_PAPER_ID"
                        return report
                    logger.error(
                        "fetch latest failed: %s",
                        mask_sensitive_text(message),
                        exc_info=True,
                    )
                    report["reason_code"] = "FETCH_LATEST_FAILED"
                    report["debug_reason"] = mask_sensitive_text(message)
                    return report

                if not selected:
                    report["status"] = "skipped"
                    report["reason_code"] = "ALREADY_DELIVERED" if saw_submission else "LATEST_NOT_FOUND"
                    if last_seen_dirty:
                        await self.put_kv_data("last_seen_by_zone", last_seen_map)
                    return report

                zone = str(selected["zone"])
                latest = dict(selected["latest"])
                paper_id = str(selected["paper_id"])
                detail_url = str(selected["detail_url"])
                pending_targets = list(selected["pending_targets"])
                report["zone"] = zone
                report["paper_id"] = paper_id
                report["detail_url"] = detail_url
                report["sent_total"] = len(pending_targets)

                payload = await self._load_submission_payload(latest, paper_id)
                logger.info(
                    "submission metadata: %s",
                    json.dumps(self._build_meta_preview(payload), ensure_ascii=False),
                )

                try:
                    pdf_file, png_file = await self._prepare_pdf_assets(payload, paper_id)
                except Exception as exc:
                    logger.error(
                        "prepare pdf assets failed: %s",
                        mask_sensitive_text(str(exc)),
                        exc_info=True,
                    )
                    report["reason_code"] = "PREPARE_ASSETS_FAILED"
                    report["debug_reason"] = mask_sensitive_text(str(exc))
                    return report

                text = self._build_push_text(payload, detail_url)
                sent_ok, sent_success_targets = await self._send_push_to_targets(
                    targets=pending_targets,
                    text=text,
                    png_file=png_file,
                    pdf_file=pdf_file,
                )
                report["sent_ok"] = sent_ok
                if sent_success_targets:
                    self._mark_targets_delivered(
                        zone=zone,
                        paper_id=paper_id,
                        success_targets=sent_success_targets,
                        last_sent_target_map=last_sent_target_map,
                    )
                    await self.put_kv_data("last_sent_by_target_zone", last_sent_target_map)

                all_delivered = self._all_targets_delivered(
                    zone=zone,
                    paper_id=paper_id,
                    targets=targets,
                    last_sent_target_map=last_sent_target_map,
                )
                if all_delivered:
                    last_seen_map[zone] = paper_id
                if all_delivered or last_seen_dirty:
                    await self.put_kv_data("last_seen_by_zone", last_seen_map)

                if sent_ok == len(pending_targets):
                    report["status"] = "success"
                    report["reason_code"] = "PUSHED_SUCCESSFULLY"
                elif sent_ok > 0:
                    report["status"] = "partial"
                    report["reason_code"] = "PUSHED_PARTIALLY"
                else:
                    report["status"] = "failed"
                    report["reason_code"] = "ALL_SENDS_FAILED"

                return report
            finally:
                await self._temp_files.release(pdf_file, png_file)
                try:
                    await self._temp_files.trim()
                except Exception:
                    logger.warning("trim temp files failed after run_cycle", exc_info=True)

    async def _select_chi_shi_candidate_from_zones(
        self,
        zone_order: list[str],
        session_key: str,
    ) -> tuple[tuple[str, dict[str, Any]] | None, bool]:
        saw_candidates = False
        for index, zone in enumerate(zone_order):
            is_primary = index == 0
            try:
                candidates = await asyncio.to_thread(
                    self._supabase.fetch_latest_submissions,
                    zone,
                    CHI_SHI_FETCH_CANDIDATE_LIMIT,
                )
            except Exception:
                if is_primary:
                    raise
                logger.warning(
                    "fetch chi_shi fallback candidates failed: zone=%s",
                    zone,
                    exc_info=True,
                )
                continue

            if not candidates:
                continue

            saw_candidates = True
            candidate = await self._pick_chi_shi_candidate(
                zone=zone,
                session_key=session_key,
                candidates=candidates,
            )
            if candidate:
                return (zone, candidate), True

        return None, saw_candidates

    async def _select_run_candidate(
        self,
        zone_order: list[str],
        targets: list[str],
        last_seen_map: dict[str, str],
        last_sent_target_map: dict[str, str],
        force: bool,
    ) -> tuple[dict[str, Any] | None, bool, bool]:
        saw_submission = False
        last_seen_dirty = False

        for index, zone in enumerate(zone_order):
            is_primary = index == 0
            try:
                latest = await asyncio.to_thread(self._supabase.fetch_latest_submission, zone)
            except Exception as exc:
                if is_primary:
                    raise RuntimeError(str(exc)) from exc
                logger.warning("fetch latest fallback failed: zone=%s", zone, exc_info=True)
                continue

            if not latest:
                continue

            saw_submission = True
            paper_id = str(latest.get("id", "")).strip()
            detail_url = f"https://shitjournal.org/preprints/{paper_id}" if paper_id else ""
            logger.info("latest paper fetched: zone=%s id=%s detail_url=%s", zone, paper_id, detail_url)

            if not paper_id:
                if is_primary:
                    raise RuntimeError("EMPTY_PAPER_ID")
                logger.warning(
                    "skip fallback zone with empty paper id: zone=%s payload=%s",
                    zone,
                    json.dumps(self._build_meta_preview(latest), ensure_ascii=False),
                )
                continue

            pending_targets = self._filter_pending_targets(
                zone=zone,
                paper_id=paper_id,
                targets=targets,
                last_sent_target_map=last_sent_target_map,
                force=force,
            )
            if pending_targets:
                return {
                    "zone": zone,
                    "latest": latest,
                    "paper_id": paper_id,
                    "detail_url": detail_url,
                    "pending_targets": pending_targets,
                }, saw_submission, last_seen_dirty

            if last_seen_map.get(zone) != paper_id:
                last_seen_map[zone] = paper_id
                last_seen_dirty = True

        return None, saw_submission, last_seen_dirty

    async def _load_submission_payload(
        self,
        latest: dict[str, Any],
        paper_id: str,
    ) -> dict[str, Any]:
        try:
            detail = await asyncio.to_thread(self._supabase.fetch_submission_detail, paper_id)
        except Exception:
            logger.warning("fetch detail failed, fallback to latest payload", exc_info=True)
            detail = {}
        payload = {**latest, **(detail or {})}
        return payload

    async def _prepare_pdf_assets(self, payload: dict[str, Any], paper_id: str) -> tuple[Path, Path]:
        pdf_key = str(payload.get("pdf_path") or payload.get("file_path") or "").strip()
        if not pdf_key:
            raise RuntimeError("pdf path missing")

        try:
            signed_url = await asyncio.to_thread(self._supabase.create_signed_pdf_url, pdf_key)
        except Exception as exc:
            logger.error(
                "create signed url failed: %s",
                mask_sensitive_text(str(exc)),
                exc_info=True,
            )
            raise RuntimeError("create signed url failed") from exc

        if not signed_url:
            raise RuntimeError("empty signed url")

        logger.info("signed pdf url selected: %s", self._mask_token(signed_url))
        pdf_file, png_file = self._temp_files.build_output_paths(paper_id)
        await self._temp_files.mark_in_use(pdf_file, png_file)

        try:
            try:
                pdf_status, pdf_type = await asyncio.to_thread(
                    self._supabase.download_pdf_file,
                    signed_url,
                    pdf_file,
                )
                logger.info("pdf downloaded: status=%s content-type=%s", pdf_status, pdf_type)
            except Exception as exc:
                logger.error(
                    "download pdf failed: %s",
                    mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                raise RuntimeError("download pdf failed") from exc

            self._pdf_service.ensure_pdf_size_limit(pdf_file)
            try:
                await asyncio.to_thread(self._pdf_service.export_first_page_png, pdf_file, png_file)
            except Exception as exc:
                logger.error(
                    "export page1 failed: %s",
                    mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                raise RuntimeError("export page1 failed") from exc

            png_size = png_file.stat().st_size if png_file.exists() else 0
            logger.info("page1 png exported: file=%s bytes=%s", str(png_file), png_size)
            return pdf_file, png_file
        except Exception:
            await self._temp_files.release(pdf_file, png_file)
            raise

    async def _send_push_to_targets(
        self,
        targets: list[str],
        text: str,
        png_file: Path,
        pdf_file: Path,
    ) -> tuple[int, list[str]]:
        raw_concurrency = getattr(self, "_send_concurrency", 3)
        try:
            configured_concurrency = int(raw_concurrency)
        except (TypeError, ValueError):
            configured_concurrency = 3
        concurrency = min(MAX_SEND_CONCURRENCY, max(1, configured_concurrency))
        semaphore = asyncio.Semaphore(concurrency)

        async def _send_one(session: str) -> tuple[str, bool]:
            ok = False
            async with semaphore:
                try:
                    chain = self._build_push_chain(text, png_file, pdf_file)
                    ok = await self.context.send_message(session, chain)
                except Exception:
                    logger.error("send message failed: session=%s", session, exc_info=True)
            logger.info("send message result: session=%s success=%s", session, ok)
            return session, ok

        results = await asyncio.gather(
            *[_send_one(session) for session in targets],
            return_exceptions=True,
        )
        ok_results: list[tuple[str, bool]] = []
        for result in results:
            if isinstance(result, BaseException):
                logger.error(
                    "unexpected send task error",
                    exc_info=(type(result), result, result.__traceback__),
                )
                continue
            ok_results.append(result)

        success_targets = [session for session, ok in ok_results if ok]
        sent_ok = len(success_targets)
        return sent_ok, success_targets

    def _build_push_chain(self, text: str, png_file: Path, pdf_file: Path) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.append(Plain(text))
        chain.chain.append(Image.fromFileSystem(str(png_file)))
        if self._cfg_bool("send_pdf", False):
            chain.chain.append(File(name=pdf_file.name, file=str(pdf_file)))
        return chain

    def _build_push_text(self, payload: dict[str, Any], detail_url: str) -> str:
        title = self._fallback_text(payload.get("manuscript_title"))
        author = self._fallback_text(payload.get("author_name"))
        institution = self._fallback_text(payload.get("institution"))
        submitted = self._format_datetime(payload.get("created_at"))
        discipline = self._format_bilingual_label(payload.get("discipline"), DISCIPLINE_LABELS)
        viscosity = self._format_bilingual_label(payload.get("viscosity"), VISCOSITY_LABELS)
        avg_score = self._format_number(payload.get("avg_score"))
        weighted_score = self._format_number(payload.get("weighted_score"))
        rating_count = self._fallback_text(payload.get("rating_count"))

        return (
            "S.H.I.T Journal 最新论文推送\n"
            f"标题: {title}\n"
            f"作者: {author}\n"
            f"单位: {institution}\n"
            f"提交时间: {submitted}\n"
            f"学科: {discipline}\n"
            f"粘度: {viscosity}\n"
            f"评分: avg={avg_score}, weighted={weighted_score}, votes={rating_count}\n"
            f"详情: {detail_url}"
        )

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
                "supabase_publishable_key is required; set plugin config or env "
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
        zone = str(self._cfg("zone", DEFAULT_ZONE)).strip()
        return zone or DEFAULT_ZONE

    def _normalize_zone_list(self, raw: Any) -> list[str]:
        if isinstance(raw, str):
            values = [item.strip() for item in re.split(r"[,\s]+", raw) if item.strip()]
        elif isinstance(raw, list):
            values = [str(item).strip() for item in raw if str(item).strip()]
        else:
            values = []

        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
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

        if normalized:
            return normalized
        return DEFAULT_SCHEDULE_TIMES.copy()

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

    async def _get_all_target_sessions(self) -> list[str]:
        cfg_targets = self._normalize_session_list(self._cfg("target_sessions", []))
        bound = await self._get_bound_sessions()
        return self._merge_sessions(cfg_targets, bound)

    async def _get_last_seen_map(self) -> dict[str, str]:
        raw = await self.get_kv_data("last_seen_by_zone", {})
        return self._clean_kv_dict(raw)

    async def _get_last_sent_target_map(self) -> dict[str, str]:
        raw = await self.get_kv_data("last_sent_by_target_zone", {})
        return self._clean_kv_dict(raw)

    def _chi_shi_group_zone_key(self, zone: str, session: str) -> str:
        return f"{zone}::{session}"

    async def _get_chi_shi_sent_history_map(self) -> dict[str, list[str]]:
        raw = await self.get_kv_data("chi_shi_sent_history_by_group_zone", {})
        history_map = self._clean_kv_list_dict(raw)
        if history_map:
            return history_map

        # 向后兼容：首次读取时把旧版单值结构迁移为列表结构。
        legacy_raw = await self.get_kv_data("chi_shi_last_sent_by_group_zone", {})
        legacy_map = self._clean_kv_dict(legacy_raw)
        return {key: [paper_id] for key, paper_id in legacy_map.items()}

    async def _pick_chi_shi_candidate(
        self,
        zone: str,
        session_key: str,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        key = self._chi_shi_group_zone_key(zone, session_key)
        async with self._chi_shi_dedupe_lock:
            history_map = await self._get_chi_shi_sent_history_map()
            sent_set = set(history_map.get(key, []))

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
        key = self._chi_shi_group_zone_key(zone, session)
        async with self._chi_shi_dedupe_lock:
            history_map = await self._get_chi_shi_sent_history_map()
            history = history_map.get(key, [])
            history = [item for item in history if item != paper_id]
            history.insert(0, paper_id)
            history_map[key] = history[:CHI_SHI_SENT_HISTORY_KEEP]
            await self.put_kv_data("chi_shi_sent_history_by_group_zone", history_map)

            # 保留旧键，兼容仍在读取旧结构的数据。
            legacy_raw = await self.get_kv_data("chi_shi_last_sent_by_group_zone", {})
            legacy_map = self._clean_kv_dict(legacy_raw)
            legacy_map[key] = paper_id
            await self.put_kv_data("chi_shi_last_sent_by_group_zone", legacy_map)

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
        return f"{zone}::{session}"

    def _filter_pending_targets(
        self,
        zone: str,
        paper_id: str,
        targets: list[str],
        last_sent_target_map: dict[str, str],
        force: bool,
    ) -> list[str]:
        if force:
            return targets.copy()
        pending_targets: list[str] = []
        for session in targets:
            key = self._target_zone_key(zone, session)
            if last_sent_target_map.get(key) != paper_id:
                pending_targets.append(session)
        return pending_targets

    def _mark_targets_delivered(
        self,
        zone: str,
        paper_id: str,
        success_targets: list[str],
        last_sent_target_map: dict[str, str],
    ) -> None:
        for session in success_targets:
            key = self._target_zone_key(zone, session)
            last_sent_target_map[key] = paper_id

    def _all_targets_delivered(
        self,
        zone: str,
        paper_id: str,
        targets: list[str],
        last_sent_target_map: dict[str, str],
    ) -> bool:
        if not targets:
            return False
        for session in targets:
            key = self._target_zone_key(zone, session)
            if last_sent_target_map.get(key) != paper_id:
                return False
        return True

    def _format_datetime(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return "N/A"
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return text

    def _format_number(self, value: Any) -> str:
        if value is None or value == "":
            return "N/A"
        try:
            return f"{float(value):.4f}"
        except Exception:
            return str(value)

    def _fallback_text(self, value: Any) -> str:
        text = str(value or "").strip()
        return text if text else "N/A"

    def _format_bilingual_label(
        self,
        raw_value: Any,
        mapping: dict[str, tuple[str, str]],
    ) -> str:
        text = str(raw_value or "").strip()
        if not text:
            return "N/A"
        key = text.lower()
        if key in mapping:
            cn, en = mapping[key]
            return f"{cn} / {en}"
        return text

    def _mask_token(self, url: str) -> str:
        if "token=" not in url:
            return url
        head, _, _ = url.partition("token=")
        return head + "token=<hidden>"

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

    def _render_report(self, report: dict[str, Any], include_debug: bool = False) -> str:
        status = report.get("status", "unknown")
        reason_code = str(report.get("reason_code") or report.get("reason") or "UNKNOWN")
        debug_reason = mask_sensitive_text(str(report.get("debug_reason", "")).strip())
        paper_id = report.get("paper_id", "")
        zone = str(report.get("zone", "")).strip()
        requested_zone = str(report.get("requested_zone", "")).strip()
        sent_ok = report.get("sent_ok", 0)
        sent_total = report.get("sent_total", 0)
        detail_url = report.get("detail_url", "")
        text = (
            f"[shitjournal run] status={status}\n"
            f"reason={reason_code}\n"
            f"zone={zone}\n"
            f"paper_id={paper_id}\n"
            f"send={sent_ok}/{sent_total}\n"
            f"url={detail_url}"
        )
        if requested_zone and requested_zone != zone:
            text += f"\nrequested_zone={requested_zone}"
        if include_debug and debug_reason:
            text += f"\ndebug_reason={debug_reason}"
        return text
