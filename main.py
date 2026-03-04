from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import fitz
import requests
from requests.adapters import HTTPAdapter
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.message_components import File, Image, Plain
from astrbot.api.star import Context, Star, StarTools, register


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
        self._chi_shi_group_last_ts_monotonic: dict[str, float] = {}
        self._chi_shi_group_inflight: set[str] = set()
        self._plugin_data_dir = Path(".")
        self._temp_dir = Path(".")
        self._thread_local = threading.local()
        self._session_registry_lock = threading.Lock()
        self._session_registry: list[requests.Session] = []
        self._send_concurrency = 3

    async def initialize(self):
        self._plugin_data_dir = self._resolve_plugin_data_dir()
        self._temp_dir = self._plugin_data_dir / "tmp"
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
        await self._trim_temp_files()
        logger.info("shitjournal_daily initialized.")

    async def terminate(self):
        await self._clear_cron_jobs()
        self._close_all_http_sessions()
        logger.info("shitjournal_daily terminated.")

    @filter.command("shitjournal")
    async def shitjournal(self, event: AstrMessageEvent, action: str = "help", arg: str = ""):
        """管理 shitjournal 每日推送：bind/unbind/targets/run/run force"""
        if not await self._check_command_permission(event):
            return

        action = (action or "help").strip().lower()
        arg = (arg or "").strip().lower()

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
            force = arg == "force"
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

        cooldown_sec = self._cfg_int("chi_shi_group_cooldown_sec", 60, min_value=0)
        session_key = event.unified_msg_origin
        allowed, deny_reason = await self._try_enter_chi_shi_cooldown(
            session_key=session_key,
            cooldown_sec=cooldown_sec,
        )
        if not allowed:
            yield event.plain_result(deny_reason)
            return

        success = False
        try:
            zone = str(self._cfg("zone", DEFAULT_ZONE)).strip() or DEFAULT_ZONE
            latest = await asyncio.to_thread(self._fetch_latest_submission, zone)
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
            logger.error("chi_shi command failed: %s", exc, exc_info=True)
            yield event.plain_result(f"抓取失败：{exc}")
        finally:
            await self._leave_chi_shi_cooldown(session_key=session_key, success=success)
            try:
                await self._trim_temp_files()
            except Exception:
                logger.warning("trim temp files failed after chi_shi command", exc_info=True)

    async def _check_command_permission(self, event: AstrMessageEvent) -> bool:
        if not bool(self._cfg("command_admin_only", True)):
            return True
        if event.is_admin():
            return True
        if bool(self._cfg("command_no_permission_reply", True)):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

    async def _try_enter_chi_shi_cooldown(
        self,
        session_key: str,
        cooldown_sec: int,
    ) -> tuple[bool, str]:
        async with self._chi_shi_cooldown_lock:
            if session_key in self._chi_shi_group_inflight:
                return False, "本群已有赤石任务在执行，请稍后再试。"

            now = time.monotonic()
            last_ts = float(self._chi_shi_group_last_ts_monotonic.get(session_key, 0.0))
            remaining = cooldown_sec - (now - last_ts)
            if remaining > 0:
                return False, f"本群冷却中，请 {int(remaining + 0.999)} 秒后再试。"

            self._chi_shi_group_inflight.add(session_key)
            return True, ""

    async def _leave_chi_shi_cooldown(self, session_key: str, success: bool) -> None:
        async with self._chi_shi_cooldown_lock:
            self._chi_shi_group_inflight.discard(session_key)
            if success:
                self._chi_shi_group_last_ts_monotonic[session_key] = time.monotonic()

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
        logger.info("scheduled run finished: %s", self._render_report(report))

    async def _run_cycle(self, force: bool, source: str) -> dict[str, Any]:
        if self._run_lock.locked():
            return {
                "status": "skipped",
                "reason": "another run is in progress",
                "source": source,
            }

        async with self._run_lock:
            zone = str(self._cfg("zone", DEFAULT_ZONE)).strip() or DEFAULT_ZONE
            report: dict[str, Any] = {
                "status": "failed",
                "source": source,
                "zone": zone,
                "force": force,
                "paper_id": "",
                "detail_url": "",
                "sent_ok": 0,
                "sent_total": 0,
                "reason": "",
            }
            logger.info("run start: source=%s zone=%s force=%s", source, zone, force)
            try:
                try:
                    latest = await asyncio.to_thread(self._fetch_latest_submission, zone)
                except Exception as exc:
                    logger.error("fetch latest failed: %s", exc, exc_info=True)
                    report["reason"] = f"fetch latest failed: {exc}"
                    return report

                if not latest:
                    report["reason"] = "latest submission not found"
                    return report

                paper_id = str(latest.get("id", "")).strip()
                detail_url = f"https://shitjournal.org/preprints/{paper_id}" if paper_id else ""
                report["paper_id"] = paper_id
                report["detail_url"] = detail_url
                logger.info("latest paper fetched: id=%s detail_url=%s", paper_id, detail_url)

                if not paper_id:
                    report["reason"] = "empty paper id in latest submission"
                    return report

                targets = await self._get_all_target_sessions()
                if not targets:
                    report["reason"] = "no target session configured"
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
                    report["reason"] = "already delivered to all targets"
                    if str(last_seen_map.get(zone, "")).strip() != paper_id:
                        last_seen_map[zone] = paper_id
                        await self.put_kv_data("last_seen_by_zone", last_seen_map)
                    return report

                try:
                    pdf_file, png_file = await self._prepare_pdf_assets(payload, paper_id)
                except Exception as exc:
                    report["reason"] = str(exc)
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
                    report["reason"] = "pushed successfully"
                elif sent_ok > 0:
                    report["status"] = "partial"
                    report["reason"] = "partially sent"
                else:
                    report["status"] = "failed"
                    report["reason"] = "all sends failed"

                return report
            finally:
                try:
                    await self._trim_temp_files()
                except Exception:
                    logger.warning("trim temp files failed after run_cycle", exc_info=True)

    async def _load_submission_payload(
        self,
        latest: dict[str, Any],
        paper_id: str,
    ) -> dict[str, Any]:
        try:
            detail = await asyncio.to_thread(self._fetch_submission_detail, paper_id)
        except Exception:
            logger.warning("fetch detail failed, fallback to latest payload", exc_info=True)
            detail = {}
        payload = {**latest, **(detail or {})}
        return payload

    async def _prepare_pdf_assets(self, payload: dict[str, Any], paper_id: str) -> tuple[Path, Path]:
        pdf_key = str(payload.get("pdf_path") or payload.get("file_path") or "").strip()
        if not pdf_key:
            raise RuntimeError("pdf path missing in payload")

        try:
            signed_url = await asyncio.to_thread(self._create_signed_pdf_url, pdf_key)
        except Exception as exc:
            logger.error("create signed url failed: %s", exc, exc_info=True)
            raise RuntimeError(f"create signed url failed: {exc}") from exc

        if not signed_url:
            raise RuntimeError("empty signed url")

        logger.info("signed pdf url selected: %s", self._mask_token(signed_url))
        pdf_file, png_file = self._build_output_paths(paper_id)

        try:
            pdf_status, pdf_type = await asyncio.to_thread(
                self._download_pdf_file,
                signed_url,
                pdf_file,
            )
            logger.info("pdf downloaded: status=%s content-type=%s", pdf_status, pdf_type)
        except Exception as exc:
            logger.error("download pdf failed: %s", exc, exc_info=True)
            raise RuntimeError(f"download pdf failed: {exc}") from exc

        dpi = self._cfg_int("pdf_dpi", 170, min_value=72, max_value=300)
        try:
            await asyncio.to_thread(self._export_first_page_png, pdf_file, png_file, dpi)
        except Exception as exc:
            logger.error("export page1 failed: %s", exc, exc_info=True)
            raise RuntimeError(f"export page1 failed: {exc}") from exc

        png_size = png_file.stat().st_size if png_file.exists() else 0
        logger.info("page1 png exported: file=%s bytes=%s", str(png_file), png_size)
        return pdf_file, png_file

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
        if bool(self._cfg("send_pdf", False)):
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

    def _fetch_latest_submission(self, zone: str) -> dict[str, Any] | None:
        supabase_url = str(self._cfg("supabase_url", DEFAULT_SUPABASE_URL)).strip()
        select_fields = (
            "id,manuscript_title,author_name,institution,viscosity,discipline,"
            "created_at,avg_score,rating_count,weighted_score,co_authors,"
            "solicited_topic,comment_count,unique_commenters,promoted_to_septic_at,pdf_path,zone"
        )
        params = {
            "select": select_fields,
            "zone": f"eq.{zone}",
            "limit": "1",
        }
        if zone == "septic":
            params["order"] = "promoted_to_septic_at.desc.nullslast"
        else:
            params["order"] = "created_at.desc"

        url = f"{supabase_url}/rest/v1/preprints_with_ratings_mat"
        data = self._request_json("GET", url, params=params)
        if not isinstance(data, list) or not data:
            return None
        return data[0]

    def _fetch_submission_detail(self, paper_id: str) -> dict[str, Any]:
        supabase_url = str(self._cfg("supabase_url", DEFAULT_SUPABASE_URL)).strip()
        url = f"{supabase_url}/rest/v1/preprints_with_ratings_mat"
        params = {
            "select": "*",
            "id": f"eq.{paper_id}",
            "limit": "1",
        }
        data = self._request_json("GET", url, params=params)
        if not isinstance(data, list) or not data:
            return {}
        return data[0]

    def _create_signed_pdf_url(self, pdf_path: str) -> str:
        supabase_url = str(self._cfg("supabase_url", DEFAULT_SUPABASE_URL)).strip()
        bucket = str(self._cfg("supabase_bucket", DEFAULT_SUPABASE_BUCKET)).strip()
        escaped = quote(pdf_path, safe="/")
        url = f"{supabase_url}/storage/v1/object/sign/{bucket}/{escaped}"
        data = self._request_json("POST", url, json_body={"expiresIn": 3600})
        signed = str((data or {}).get("signedURL", "")).strip()
        if not signed:
            raise RuntimeError(f"signedURL missing in response: {data}")
        return f"{supabase_url}/storage/v1{signed}"

    def _download_pdf_file(self, signed_url: str, target_path: Path) -> tuple[int, str]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        timeout, retry, headers = self._http_request_options()
        last_error: Exception | None = None
        status_code = 0
        content_type = ""

        for attempt in range(1, retry + 1):
            try:
                with self._get_http_session().request(
                    method="GET",
                    url=signed_url,
                    headers=headers,
                    timeout=timeout,
                    stream=True,
                ) as response:
                    status_code = response.status_code
                    content_type = str(response.headers.get("content-type", ""))
                    if status_code >= 500 and attempt < retry:
                        logger.warning(
                            "pdf download retry due to status=%s attempt=%s/%s url=%s",
                            status_code,
                            attempt,
                            retry,
                            signed_url,
                        )
                        self._backoff_sleep(attempt)
                        continue
                    if status_code >= 400:
                        body_preview = response.text[:300]
                        raise RuntimeError(
                            f"http error {status_code} for {signed_url}; body={body_preview}",
                        )
                    if "application/pdf" not in content_type.lower():
                        logger.warning(
                            "pdf content-type is not application/pdf: %s (will validate header)",
                            content_type,
                        )

                    with target_path.open("wb") as fp:
                        for chunk in response.iter_content(chunk_size=1024 * 64):
                            if chunk:
                                fp.write(chunk)
                break
            except Exception as exc:
                last_error = exc
                if attempt < retry:
                    logger.warning(
                        "pdf download retry due to exception attempt=%s/%s url=%s err=%s",
                        attempt,
                        retry,
                        signed_url,
                        exc,
                    )
                    self._backoff_sleep(attempt)
                    continue
                break

        if (not target_path.exists()) or target_path.stat().st_size <= 0:
            if last_error is not None:
                raise last_error
            raise RuntimeError("downloaded pdf file missing")

        with target_path.open("rb") as fp:
            header = fp.read(5)
        if header != b"%PDF-":
            raise RuntimeError("downloaded file is not a valid PDF")

        return status_code, content_type

    def _export_first_page_png(self, pdf_file: Path, png_file: Path, dpi: int) -> None:
        dpi = max(72, min(dpi, 300))
        pdf = fitz.open(str(pdf_file))
        try:
            if pdf.page_count < 1:
                raise RuntimeError("pdf has no pages")
            page = pdf.load_page(0)
            scale = dpi / 72.0
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            pix.save(str(png_file))
        finally:
            pdf.close()

    def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        timeout, retry, headers = self._http_request_options()
        last_error: Exception | None = None
        for attempt in range(1, retry + 1):
            try:
                with self._get_http_session().request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=timeout,
                    stream=False,
                ) as response:
                    if response.status_code >= 500 and attempt < retry:
                        logger.warning(
                            "http retrying due to status=%s attempt=%s/%s url=%s",
                            response.status_code,
                            attempt,
                            retry,
                            url,
                        )
                        self._backoff_sleep(attempt)
                        continue
                    if response.status_code >= 400:
                        body_preview = response.text[:300]
                        raise RuntimeError(
                            f"http error {response.status_code} for {url}; body={body_preview}",
                        )
                    try:
                        return response.json()
                    except Exception as exc:
                        raise RuntimeError(
                            f"json decode failed: {exc}; body={response.text[:300]}",
                        ) from exc
            except Exception as exc:
                last_error = exc
                if attempt < retry:
                    logger.warning(
                        "http retrying due to exception attempt=%s/%s url=%s err=%s",
                        attempt,
                        retry,
                        url,
                        exc,
                    )
                    self._backoff_sleep(attempt)
                    continue
                break

        assert last_error is not None
        raise last_error

    def _http_request_options(self) -> tuple[int, int, dict[str, str]]:
        timeout = self._cfg_int("http_timeout_sec", 20, min_value=5)
        retry = self._cfg_int("http_retry", 3, min_value=1)
        headers = self._build_supabase_headers()
        return timeout, retry, headers

    def _backoff_sleep(self, attempt: int) -> None:
        delay = min(2 ** (attempt - 1), 8)
        time.sleep(delay)

    def _get_http_session(self) -> requests.Session:
        session = getattr(self._thread_local, "http_session", None)
        if session is not None:
            return session

        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self._thread_local.http_session = session
        with self._session_registry_lock:
            self._session_registry.append(session)
        return session

    def _close_all_http_sessions(self) -> None:
        with self._session_registry_lock:
            sessions = self._session_registry[:]
            self._session_registry.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                logger.warning("close http session failed", exc_info=True)

    def _build_supabase_headers(self) -> dict[str, str]:
        key = self._get_supabase_key()
        return {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

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
        if not isinstance(raw, dict):
            return {}
        result: dict[str, str] = {}
        for key, value in raw.items():
            key_s = str(key).strip()
            val_s = str(value).strip()
            if key_s and val_s:
                result[key_s] = val_s
        return result

    async def _get_last_sent_target_map(self) -> dict[str, str]:
        raw = await self.get_kv_data("last_sent_by_target_zone", {})
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

    def _build_output_paths(self, paper_id: str) -> tuple[Path, Path]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        nonce = uuid.uuid4().hex[:8]
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", paper_id)
        pdf_file = self._temp_dir / f"{ts}_{safe_id}_{nonce}.pdf"
        png_file = self._temp_dir / f"{ts}_{safe_id}_{nonce}_p1.png"
        return pdf_file, png_file

    async def _trim_temp_files(self) -> None:
        keep = self._cfg_int("temp_keep_files", 30, min_value=0)
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        files = [p for p in self._temp_dir.iterdir() if p.is_file()]
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for file_path in files[keep:]:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                logger.warning("failed to delete temp file: %s", str(file_path), exc_info=True)

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

    def _render_report(self, report: dict[str, Any]) -> str:
        status = report.get("status", "unknown")
        reason = report.get("reason", "")
        paper_id = report.get("paper_id", "")
        sent_ok = report.get("sent_ok", 0)
        sent_total = report.get("sent_total", 0)
        detail_url = report.get("detail_url", "")
        return (
            f"[shitjournal run] status={status}\n"
            f"reason={reason}\n"
            f"paper_id={paper_id}\n"
            f"send={sent_ok}/{sent_total}\n"
            f"url={detail_url}"
        )
