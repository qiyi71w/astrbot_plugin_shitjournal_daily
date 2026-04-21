from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from .warning_text import append_warning_lines


class ChiShiService:
    def __init__(
        self,
        *,
        select_candidate_from_zones: Callable[..., Awaitable[tuple[Any, bool, list[str]]]],
        load_submission_payload: Callable[[dict[str, Any], str], Awaitable[dict[str, Any]]],
        prepare_pdf_assets: Callable[[dict[str, Any], str], Awaitable[tuple[Path, Path, str]]],
        build_push_text: Callable[[dict[str, Any], str, str], str],
        build_preprint_detail_url: Callable[[str], str],
        send_event_push: Callable[..., Awaitable[None]],
        mark_chi_shi_paper_sent: Callable[[str, str, str], Awaitable[None]],
        release_temp_files: Callable[[Path | None, Path | None], Awaitable[None]],
        get_primary_zone: Callable[[], str],
        get_candidate_zones: Callable[[str], list[str]],
        build_zone_scope_text: Callable[[list[str]], str],
        cfg_int_getter: Callable[[str, int, int | None, int | None], int],
        mask_sensitive_text: Callable[[str], str],
        fetch_page_size: int,
    ):
        self._select_candidate_from_zones = select_candidate_from_zones
        self._load_submission_payload = load_submission_payload
        self._prepare_pdf_assets = prepare_pdf_assets
        self._build_push_text = build_push_text
        self._build_preprint_detail_url = build_preprint_detail_url
        self._send_event_push = send_event_push
        self._mark_chi_shi_paper_sent = mark_chi_shi_paper_sent
        self._release_temp_files = release_temp_files
        self._get_primary_zone = get_primary_zone
        self._get_candidate_zones = get_candidate_zones
        self._build_zone_scope_text = build_zone_scope_text
        self._cfg_int_getter = cfg_int_getter
        self._mask_sensitive_text = mask_sensitive_text
        self._fetch_page_size = int(fetch_page_size)
        self._cooldown_lock = asyncio.Lock()
        self._group_cooldown_until_monotonic: dict[str, float] = {}
        self._group_inflight: set[str] = set()

    async def execute_wo_yao_chi_shi(
        self,
        *,
        event: Any,
        session_key: str,
        scope_label: str,
    ) -> tuple[list[str], bool, Path | None, Path | None]:
        primary_zone = self._get_primary_zone()
        zone_order = self._get_candidate_zones(primary_zone)
        selected, saw_candidates, warnings = await self._select_candidate_from_zones(
            zone_order=zone_order,
            session_key=session_key,
            fetch_page_size=self._fetch_page_size,
        )
        if not selected:
            apply_ok, reply = self.build_missing_chi_shi_selection_response(
                zone_order=zone_order,
                scope_label=scope_label,
                saw_candidates=saw_candidates,
                selection_warnings=warnings,
            )
            return [reply], apply_ok, None, None

        apply_ok, pdf_file, png_file = await self.push_chi_shi_selection(
            event=event,
            session_key=session_key,
            selection=selected,
        )
        if not apply_ok:
            return ["候选论文缺少 ID。"], False, None, None
        if not warnings:
            return [], True, pdf_file, png_file
        return [
            self.build_chi_shi_success_text(
                requested_zone=primary_zone,
                selected_zone=str(selected.zone),
                warnings=warnings,
            ),
        ], True, pdf_file, png_file

    def build_missing_chi_shi_selection_response(
        self,
        *,
        zone_order: list[str],
        scope_label: str,
        saw_candidates: bool,
        selection_warnings: list[str],
    ) -> tuple[bool, str]:
        if selection_warnings:
            return False, self.build_chi_shi_failure_text(
                saw_candidates=saw_candidates,
                warnings=selection_warnings,
                scope_label=scope_label,
            )
        if saw_candidates:
            zone_scope = self._build_zone_scope_text(zone_order)
            return True, f"{scope_label}{zone_scope}最近这些论文都推送过了，等下一篇。"
        return False, "未找到最新论文。"

    async def push_chi_shi_selection(
        self,
        *,
        event: Any,
        session_key: str,
        selection: Any,
    ) -> tuple[bool, Path | None, Path | None]:
        zone = str(selection.zone).strip()
        paper_id = str(selection.paper_id).strip()
        candidate = dict(selection.candidate or {})
        if not paper_id:
            return False, None, None

        pdf_file: Path | None = None
        png_file: Path | None = None
        try:
            payload = await self._load_submission_payload(candidate, paper_id)
            detail_url = self._build_preprint_detail_url(paper_id)
            pdf_file, png_file, pdf_url = await self._prepare_pdf_assets(payload, paper_id)
            text = self._build_push_text(payload, detail_url, zone)
            await self._send_event_push(
                event=event,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
            await self._mark_chi_shi_paper_sent(zone, session_key, paper_id)
            return True, pdf_file, png_file
        except Exception as exc:
            if pdf_file is None and png_file is None:
                raise
            try:
                await self._release_temp_files(pdf_file, png_file)
            except Exception as release_exc:
                raise release_exc from exc
            raise

    async def try_enter_chi_shi_cooldown(
        self,
        *,
        session_key: str,
        scope_label: str,
        ignore_cooldown: bool,
    ) -> tuple[bool, str]:
        async with self._cooldown_lock:
            if session_key in self._group_inflight:
                return False, f"{scope_label}已有赤石任务在执行，请稍后再试。"
            if ignore_cooldown:
                self._group_inflight.add(session_key)
                return True, ""

            now = time.monotonic()
            cooldown_until = float(self._group_cooldown_until_monotonic.get(session_key, 0.0))
            remaining = cooldown_until - now
            if remaining > 0:
                return False, f"{scope_label}冷却中，请 {int(remaining + 0.999)} 秒后再试。"

            self._group_inflight.add(session_key)
            return True, ""

    async def leave_chi_shi_cooldown(
        self,
        *,
        session_key: str,
        apply_success_cooldown: bool,
        ignore_cooldown: bool,
    ) -> None:
        async with self._cooldown_lock:
            self._group_inflight.discard(session_key)
            if ignore_cooldown:
                return
            cooldown_sec = self._cfg_int_getter("chi_shi_group_cooldown_sec", 60, min_value=0)
            fail_cooldown_sec = self._cfg_int_getter("chi_shi_group_fail_cooldown_sec", 10, min_value=0)
            effective = cooldown_sec if apply_success_cooldown else fail_cooldown_sec
            if effective > 0:
                self._group_cooldown_until_monotonic[session_key] = time.monotonic() + effective
                return
            self._group_cooldown_until_monotonic.pop(session_key, None)

    def build_chi_shi_success_text(
        self,
        *,
        requested_zone: str,
        selected_zone: str,
        warnings: list[str],
    ) -> str:
        lines = ["已继续回退并完成推送。"]
        if requested_zone and requested_zone != selected_zone:
            lines.append(f"最终分区: {selected_zone}")
        append_warning_lines(lines, warnings, self._mask_sensitive_text)
        return "\n".join(lines)

    def build_chi_shi_failure_text(
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
        append_warning_lines(lines, warnings, self._mask_sensitive_text)
        return "\n".join(lines)
