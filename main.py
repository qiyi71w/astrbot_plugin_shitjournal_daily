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
except Exception:
    _plugin_dir = Path(__file__).resolve().parent
    if str(_plugin_dir) not in sys.path:
        sys.path.insert(0, str(_plugin_dir))
    from services import PdfService, SupabaseClient, TempFileManager


DEFAULT_SUPABASE_URL = "https://bcgdqepzakcufaadgnda.supabase.co"
DEFAULT_SUPABASE_BUCKET = "manuscripts"
DEFAULT_ZONE = "septic"
DEFAULT_SCHEDULE_TIMES = ["09:00", "21:00"]
MAX_SEND_CONCURRENCY = 20
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
        action: str = "help",
        arg: str = "",
        *extra_args: str,
    ):
        """管理 shitjournal 每日推送：bind/unbind/targets/run/run force"""
        if not await self._check_command_permission(event):
            return

        action = (action or "help").strip().lower()
        arg_parts = [str(arg or "").strip().lower()]
        arg_parts.extend(str(part).strip().lower() for part in extra_args if str(part).strip())
        arg = " ".join(part for part in arg_parts if part).strip()

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
    async def wo_yao_chi_shi(self, event: AstrMessageEvent):
        """抓取最新论文并推送到当前群聊（按群冷却）"""
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
            zone = str(self._cfg("zone", DEFAULT_ZONE)).strip() or DEFAULT_ZONE
            latest = await asyncio.to_thread(self._supabase.fetch_latest_submission, zone)
            if not latest:
                yield event.plain_result("未找到最新论文。")
                return

            paper_id = str(latest.get("id", "")).strip()
            if not paper_id:
                yield event.plain_result("最新论文缺少 ID。")
                return

            payload = await self._load_submission_payload(latest, paper_id)
            detail_url = f"https://shitjournal.org/preprints/{paper_id}"
            pdf_file, png_file = await self._prepare_pdf_assets(payload, paper_id)
            text = self._build_push_text(payload, detail_url)
            yield self._build_push_chain(text, png_file, pdf_file)
            success = True
        except Exception as exc:
            logger.error(
                "chi_shi command failed: %s",
                self._mask_sensitive_text(str(exc)),
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
        if not self._cfg_bool("command_admin_only", True):
            return True
        if event.is_admin():
            return True
        if self._cfg_bool("command_no_permission_reply", True):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

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
            zone = str(self._cfg("zone", DEFAULT_ZONE)).strip() or DEFAULT_ZONE
            pdf_file: Path | None = None
            png_file: Path | None = None
            report: dict[str, Any] = {
                "status": "failed",
                "source": source,
                "zone": zone,
                "force": force,
                "paper_id": "",
                "detail_url": "",
                "sent_ok": 0,
                "sent_total": 0,
                "reason_code": "",
                "debug_reason": "",
            }
            logger.info("run start: source=%s zone=%s force=%s", source, zone, force)
            try:
                try:
                    latest = await asyncio.to_thread(self._supabase.fetch_latest_submission, zone)
                except Exception as exc:
                    logger.error(
                        "fetch latest failed: %s",
                        self._mask_sensitive_text(str(exc)),
                        exc_info=True,
                    )
                    report["reason_code"] = "FETCH_LATEST_FAILED"
                    report["debug_reason"] = self._mask_sensitive_text(str(exc))
                    return report

                if not latest:
                    report["reason_code"] = "LATEST_NOT_FOUND"
                    return report

                paper_id = str(latest.get("id", "")).strip()
                detail_url = f"https://shitjournal.org/preprints/{paper_id}" if paper_id else ""
                report["paper_id"] = paper_id
                report["detail_url"] = detail_url
                logger.info("latest paper fetched: id=%s detail_url=%s", paper_id, detail_url)

                if not paper_id:
                    report["reason_code"] = "EMPTY_PAPER_ID"
                    return report

                targets = await self._get_all_target_sessions()
                if not targets:
                    report["reason_code"] = "NO_TARGET_SESSION_CONFIGURED"
                    return report

                payload = await self._load_submission_payload(latest, paper_id)
                logger.info(
                    "submission metadata: %s",
                    json.dumps(self._build_meta_preview(payload), ensure_ascii=False),
                )

                last_seen_map = await self._get_last_seen_map()
                last_sent_target_map = await self._get_last_sent_target_map()
                pending_targets = self._filter_pending_targets(
                    zone=zone,
                    paper_id=paper_id,
                    targets=targets,
                    last_sent_target_map=last_sent_target_map,
                    force=force,
                )
                report["sent_total"] = len(pending_targets)
                if not pending_targets:
                    report["status"] = "skipped"
                    report["reason_code"] = "ALREADY_DELIVERED"
                    if str(last_seen_map.get(zone, "")).strip() != paper_id:
                        last_seen_map[zone] = paper_id
                        await self.put_kv_data("last_seen_by_zone", last_seen_map)
                    return report

                try:
                    pdf_file, png_file = await self._prepare_pdf_assets(payload, paper_id)
                except Exception as exc:
                    logger.error(
                        "prepare pdf assets failed: %s",
                        self._mask_sensitive_text(str(exc)),
                        exc_info=True,
                    )
                    report["reason_code"] = "PREPARE_ASSETS_FAILED"
                    report["debug_reason"] = self._mask_sensitive_text(str(exc))
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
                self._mask_sensitive_text(str(exc)),
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
                    self._mask_sensitive_text(str(exc)),
                    exc_info=True,
                )
                raise RuntimeError("download pdf failed") from exc

            self._pdf_service.ensure_pdf_size_limit(pdf_file)
            try:
                await asyncio.to_thread(self._pdf_service.export_first_page_png, pdf_file, png_file)
            except Exception as exc:
                logger.error(
                    "export page1 failed: %s",
                    self._mask_sensitive_text(str(exc)),
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
        return [str(item).strip() for item in raw if str(item).strip()]

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
            if str(last_sent_target_map.get(key, "")).strip() != paper_id:
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
            if str(last_sent_target_map.get(key, "")).strip() != paper_id:
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

    def _mask_sensitive_text(self, text: str) -> str:
        masked = str(text)
        masked = re.sub(r"(token=)[^&\s]+", r"\1<hidden>", masked, flags=re.IGNORECASE)
        masked = re.sub(r"(apikey=)[^&\s]+", r"\1<hidden>", masked, flags=re.IGNORECASE)
        masked = re.sub(r"(authorization:\s*bearer\s+)[^\s]+", r"\1<hidden>", masked, flags=re.I)
        return masked

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
        debug_reason = self._mask_sensitive_text(str(report.get("debug_reason", "")).strip())
        paper_id = report.get("paper_id", "")
        sent_ok = report.get("sent_ok", 0)
        sent_total = report.get("sent_total", 0)
        detail_url = report.get("detail_url", "")
        text = (
            f"[shitjournal run] status={status}\n"
            f"reason={reason_code}\n"
            f"paper_id={paper_id}\n"
            f"send={sent_ok}/{sent_total}\n"
            f"url={detail_url}"
        )
        if include_debug and debug_reason:
            text += f"\ndebug_reason={debug_reason}"
        return text
