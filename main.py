from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import fitz
import requests
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.message_components import File, Image, Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path


DEFAULT_SUPABASE_URL = "https://bcgdqepzakcufaadgnda.supabase.co"
DEFAULT_SUPABASE_PUBLISHABLE_KEY = "sb_publishable_wHqWLjQwO2lMwkGLeBktng_Mk_xf5xd"
DEFAULT_SUPABASE_BUCKET = "manuscripts"
DEFAULT_ZONE = "septic"
DEFAULT_SCHEDULE_TIMES = ["09:00", "21:00"]
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
        plugin_dir = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
        self._plugin_data_dir = Path(get_astrbot_plugin_data_path()) / plugin_dir
        self._temp_dir = self._plugin_data_dir / "tmp"

    async def initialize(self):
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        await self._clear_cron_jobs()
        await self._register_cron_jobs()
        await self._trim_temp_files()
        logger.info("shitjournal_daily initialized.")

    async def terminate(self):
        await self._clear_cron_jobs()
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

    async def _check_command_permission(self, event: AstrMessageEvent) -> bool:
        if not bool(self._cfg("command_admin_only", True)):
            return True
        if event.is_admin():
            return True
        if bool(self._cfg("command_no_permission_reply", True)):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

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
            await self._trim_temp_files()

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

            last_seen_map = await self._get_last_seen_map()
            last_seen = str(last_seen_map.get(zone, "")).strip()
            if not force and paper_id == last_seen:
                report["status"] = "skipped"
                report["reason"] = "no new paper"
                return report

            try:
                detail = await asyncio.to_thread(self._fetch_submission_detail, paper_id)
            except Exception:
                logger.warning("fetch detail failed, fallback to latest payload", exc_info=True)
                detail = {}
            payload = {**latest, **(detail or {})}
            logger.info(
                "submission metadata: %s",
                json.dumps(self._build_meta_preview(payload), ensure_ascii=False),
            )

            pdf_key = str(payload.get("pdf_path") or payload.get("file_path") or "").strip()
            if not pdf_key:
                report["reason"] = "pdf path missing in payload"
                return report

            try:
                signed_url = await asyncio.to_thread(self._create_signed_pdf_url, pdf_key)
            except Exception as exc:
                logger.error("create signed url failed: %s", exc, exc_info=True)
                report["reason"] = f"create signed url failed: {exc}"
                return report

            if not signed_url:
                report["reason"] = "empty signed url"
                return report

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
                report["reason"] = f"download pdf failed: {exc}"
                return report

            dpi = int(self._cfg("pdf_dpi", 170))
            try:
                await asyncio.to_thread(self._export_first_page_png, pdf_file, png_file, dpi)
            except Exception as exc:
                logger.error("export page1 failed: %s", exc, exc_info=True)
                report["reason"] = f"export page1 failed: {exc}"
                return report

            png_size = png_file.stat().st_size if png_file.exists() else 0
            logger.info("page1 png exported: file=%s bytes=%s", str(png_file), png_size)

            targets = await self._get_all_target_sessions()
            report["sent_total"] = len(targets)
            if not targets:
                report["reason"] = "no target session configured"
                return report

            text = self._build_push_text(payload, detail_url)
            sent_ok = 0
            for session in targets:
                chain = self._build_push_chain(text, png_file, pdf_file)
                ok = False
                try:
                    ok = await self.context.send_message(session, chain)
                except Exception:
                    logger.error("send message failed: session=%s", session, exc_info=True)
                logger.info("send message result: session=%s success=%s", session, ok)
                if ok:
                    sent_ok += 1

            report["sent_ok"] = sent_ok
            if sent_ok == len(targets):
                last_seen_map[zone] = paper_id
                await self.put_kv_data("last_seen_by_zone", last_seen_map)
                report["status"] = "success"
                report["reason"] = "pushed successfully"
            elif sent_ok > 0:
                report["status"] = "partial"
                report["reason"] = "partially sent"
            else:
                report["status"] = "failed"
                report["reason"] = "all sends failed"

            await self._trim_temp_files()
            return report

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
        response = self._request_raw("GET", signed_url, stream=True)
        content_type = str(response.headers.get("content-type", ""))
        if "application/pdf" not in content_type.lower():
            raise RuntimeError(f"unexpected content-type: {content_type}")

        with target_path.open("wb") as fp:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    fp.write(chunk)

        with target_path.open("rb") as fp:
            header = fp.read(5)
        if header != b"%PDF-":
            raise RuntimeError("downloaded file is not a valid PDF")

        return response.status_code, content_type

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
        response = self._request_raw(method, url, params=params, json_body=json_body, stream=False)
        try:
            return response.json()
        except Exception as exc:
            raise RuntimeError(f"json decode failed: {exc}; body={response.text[:300]}") from exc

    def _request_raw(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        timeout = max(5, int(self._cfg("http_timeout_sec", 20)))
        retry = max(1, int(self._cfg("http_retry", 3)))
        headers = self._build_supabase_headers()

        last_error: Exception | None = None
        for attempt in range(1, retry + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=timeout,
                    stream=stream,
                )
                if response.status_code >= 500 and attempt < retry:
                    logger.warning(
                        "http retrying due to status=%s attempt=%s/%s url=%s",
                        response.status_code,
                        attempt,
                        retry,
                        url,
                    )
                    time.sleep(2 ** (attempt - 1))
                    continue
                if response.status_code >= 400:
                    body_preview = response.text[:300]
                    raise RuntimeError(
                        f"http error {response.status_code} for {url}; body={body_preview}",
                    )
                return response
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
                    time.sleep(2 ** (attempt - 1))
                    continue
                break

        assert last_error is not None
        raise last_error

    def _build_supabase_headers(self) -> dict[str, str]:
        key = str(self._cfg("supabase_publishable_key", DEFAULT_SUPABASE_PUBLISHABLE_KEY)).strip()
        if not key:
            raise RuntimeError("supabase_publishable_key is empty")
        return {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _cfg(self, key: str, default: Any) -> Any:
        try:
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
        except Exception:
            pass
        return default

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

    def _build_output_paths(self, paper_id: str) -> tuple[Path, Path]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", paper_id)
        pdf_file = self._temp_dir / f"{ts}_{safe_id}.pdf"
        png_file = self._temp_dir / f"{ts}_{safe_id}_p1.png"
        return pdf_file, png_file

    async def _trim_temp_files(self) -> None:
        keep = int(self._cfg("temp_keep_files", 30))
        keep = max(0, keep)
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
