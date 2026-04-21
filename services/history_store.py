from __future__ import annotations
import asyncio
from typing import Any, Awaitable, Callable
RUN_SENT_HISTORY_STORAGE_VERSION = "2"
RUN_SENT_HISTORY_STORAGE_VERSION_KEY = "run_sent_history_storage_version"
RUN_SENT_HISTORY_KV_PREFIX = "run_sent_history_v2::"
CHI_SHI_HISTORY_STORAGE_VERSION = "2"
CHI_SHI_HISTORY_STORAGE_VERSION_KEY = "chi_shi_sent_history_storage_version"
CHI_SHI_HISTORY_KV_PREFIX = "chi_shi_sent_history_v2::"
class HistoryStore:
    def __init__(
        self,
        *,
        kv_getter: Callable[[str, Any], Awaitable[Any]],
        kv_putter: Callable[[str, Any], Awaitable[None]],
        cfg_bool_getter: Callable[[str, bool], bool],
        cfg_int_getter: Callable[[str, int, int | None, int | None], int],
        normalize_session_list: Callable[[Any], list[str]],
        normalize_zone_name: Callable[[Any], str],
    ):
        self._kv_getter = kv_getter
        self._kv_putter = kv_putter
        self._cfg_bool_getter = cfg_bool_getter
        self._cfg_int_getter = cfg_int_getter
        self._normalize_session_list = normalize_session_list
        self._normalize_zone_name = normalize_zone_name
        self._bound_sessions_lock = asyncio.Lock()
        self._run_sent_history_lock = asyncio.Lock()
        self._run_sent_history_storage_ready = False
        self._chi_shi_dedupe_lock = asyncio.Lock()
        self._chi_shi_history_storage_ready = False
    async def get_bound_sessions(self) -> list[str]:
        raw = await self._kv_getter("bound_sessions", [])
        return self._normalize_sessions(raw)
    async def set_bound_sessions(self, sessions: list[str]) -> None:
        normalized = self._normalize_sessions(sessions)
        await self._kv_putter("bound_sessions", normalized)
    async def bind_session(self, session: str) -> bool:
        normalized_session = self._normalize_session(session)
        if not normalized_session:
            return False
        async with self._bound_sessions_lock:
            bound = await self.get_bound_sessions()
            if normalized_session in bound:
                return False
            bound.append(normalized_session)
            await self.set_bound_sessions(bound)
            return True
    async def unbind_session(self, session: str) -> bool:
        normalized_session = self._normalize_session(session)
        if not normalized_session:
            return False
        async with self._bound_sessions_lock:
            bound = await self.get_bound_sessions()
            if normalized_session not in bound:
                return False
            updated = [item for item in bound if item != normalized_session]
            await self.set_bound_sessions(updated)
            return True
    async def get_all_target_sessions(self, cfg_targets_raw: Any) -> list[str]:
        cfg_targets = self._normalize_sessions(cfg_targets_raw)
        bound = await self.get_bound_sessions()
        merged: list[str] = []
        seen: set[str] = set()
        for item in cfg_targets + bound:
            if item in seen:
                continue
            seen.add(item)
            merged.append(item)
        return merged
    async def get_last_seen_map(self) -> dict[str, str]:
        raw = await self._kv_getter("last_seen_by_zone", {})
        return self._clean_kv_dict(raw)
    async def set_last_seen_map(self, last_seen_map: dict[str, str]) -> None:
        await self._kv_putter("last_seen_by_zone", self._clean_kv_dict(last_seen_map))
    async def ensure_run_sent_history_storage_ready(self) -> None:
        if self._run_sent_history_storage_ready:
            return
        async with self._run_sent_history_lock:
            if self._run_sent_history_storage_ready:
                return
            version = str(await self._kv_getter(RUN_SENT_HISTORY_STORAGE_VERSION_KEY, "") or "").strip()
            if version == RUN_SENT_HISTORY_STORAGE_VERSION:
                self._run_sent_history_storage_ready = True
                return
            legacy_raw = await self._kv_getter("last_sent_by_target_zone", {})
            merged = self._build_legacy_run_sent_histories(legacy_raw)
            for target_zone_key, history in merged.items():
                zone, _, session = target_zone_key.partition("::")
                if not session:
                    continue
                await self._kv_putter(self._run_history_store_key(zone, session), history)
            if legacy_raw:
                await self._kv_putter("last_sent_by_target_zone", {})
            await self._kv_putter(RUN_SENT_HISTORY_STORAGE_VERSION_KEY, RUN_SENT_HISTORY_STORAGE_VERSION)
            self._run_sent_history_storage_ready = True
    async def get_run_sent_history(self, zone: str, session: str) -> list[str]:
        normalized_session = self._normalize_session(session)
        if not normalized_session:
            return []
        await self.ensure_run_sent_history_storage_ready()
        return await self._get_run_sent_history_unlocked(zone, normalized_session)
    async def get_run_sent_histories(self, zone: str, targets: list[str]) -> dict[str, list[str]]:
        normalized_targets = self._normalize_sessions(targets)
        if not normalized_targets:
            return {}
        await self.ensure_run_sent_history_storage_ready()
        raw_histories = await asyncio.gather(
            *[self._kv_getter(self._run_history_store_key(zone, session), []) for session in normalized_targets],
        )
        return {
            session: self._clean_kv_list(raw_history)
            for session, raw_history in zip(normalized_targets, raw_histories)
        }
    async def mark_run_targets_delivered(
        self,
        *,
        zone: str,
        paper_id: str,
        success_targets: list[str],
    ) -> None:
        normalized_targets = self._normalize_sessions(success_targets)
        if not normalized_targets:
            return
        await self.ensure_run_sent_history_storage_ready()
        async with self._run_sent_history_lock:
            for session in normalized_targets:
                current_history = await self._get_run_sent_history_unlocked(zone, session)
                updated_history = self._prepend_history_item(current_history, paper_id)
                await self._kv_putter(self._run_history_store_key(zone, session), updated_history)
    async def ensure_chi_shi_history_storage_ready(self) -> None:
        if self._chi_shi_history_storage_ready:
            return
        async with self._chi_shi_dedupe_lock:
            if self._chi_shi_history_storage_ready:
                return
            version = str(await self._kv_getter(CHI_SHI_HISTORY_STORAGE_VERSION_KEY, "") or "").strip()
            if version == CHI_SHI_HISTORY_STORAGE_VERSION:
                self._chi_shi_history_storage_ready = True
                return
            merged = await self._load_legacy_chi_shi_histories()
            for group_zone_key, history in merged.items():
                zone, _, session = group_zone_key.partition("::")
                if not session:
                    continue
                await self._kv_putter(self._chi_shi_history_store_key(zone, session), history)
            await self._kv_putter(CHI_SHI_HISTORY_STORAGE_VERSION_KEY, CHI_SHI_HISTORY_STORAGE_VERSION)
            self._chi_shi_history_storage_ready = True
    async def get_chi_shi_sent_history(self, zone: str, session: str) -> list[str]:
        normalized_session = self._normalize_session(session)
        if not normalized_session:
            return []
        await self.ensure_chi_shi_history_storage_ready()
        async with self._chi_shi_dedupe_lock:
            return await self._get_chi_shi_sent_history_unlocked(zone, normalized_session)
    async def mark_chi_shi_paper_sent(
        self,
        zone: str,
        session: str,
        paper_id: str,
    ) -> None:
        normalized_session = self._normalize_session(session)
        if not normalized_session:
            return
        await self.ensure_chi_shi_history_storage_ready()
        async with self._chi_shi_dedupe_lock:
            history = await self._get_chi_shi_sent_history_unlocked(zone, normalized_session)
            await self._kv_putter(
                self._chi_shi_history_store_key(zone, normalized_session),
                self._apply_chi_shi_history_policy(self._prepend_history_item(history, paper_id)),
            )
    def _run_history_store_key(self, zone: str, session: str) -> str:
        return f"{RUN_SENT_HISTORY_KV_PREFIX}{self._target_zone_key(zone, session)}"
    def _target_zone_key(self, zone: str, session: str) -> str:
        return f"{self._normalize_zone_name(zone)}::{session}"
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
    def _build_legacy_run_sent_histories(self, legacy_raw: Any) -> dict[str, list[str]]:
        merged: dict[str, list[str]] = {}
        for raw_key, paper_id in self._clean_kv_dict(legacy_raw).items():
            key = self._normalize_target_zone_key(raw_key)
            if not key:
                continue
            merged[key] = [paper_id]
        return merged
    async def _get_run_sent_history_unlocked(self, zone: str, session: str) -> list[str]:
        raw = await self._kv_getter(self._run_history_store_key(zone, session), [])
        return self._clean_kv_list(raw)
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
    async def _load_legacy_chi_shi_histories(self) -> dict[str, list[str]]:
        history_raw = await self._kv_getter("chi_shi_sent_history_by_group_zone", {})
        legacy_raw = await self._kv_getter("chi_shi_last_sent_by_group_zone", {})
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
    async def _get_chi_shi_sent_history_unlocked(self, zone: str, session: str) -> list[str]:
        raw = await self._kv_getter(self._chi_shi_history_store_key(zone, session), [])
        return self._apply_chi_shi_history_policy(self._clean_kv_list(raw))
    def _apply_chi_shi_history_policy(self, history: list[str]) -> list[str]:
        if self._cfg_bool_getter("chi_shi_keep_full_history", True):
            return history
        limit = self._cfg_int_getter("chi_shi_history_limit", 30, min_value=1)
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
    def _clean_kv_list_dict(self, raw: Any) -> dict[str, list[str]]:
        if not isinstance(raw, dict):
            return {}
        result: dict[str, list[str]] = {}
        for key, value in raw.items():
            key_s = str(key).strip()
            if not key_s:
                continue
            values = value if isinstance(value, list) else [value]
            cleaned = self._clean_kv_list(values)
            if cleaned:
                result[key_s] = cleaned
        return result
    def _prepend_history_item(self, history: list[str], item: str) -> list[str]:
        value = str(item).strip()
        if not value:
            return history
        updated = [entry for entry in history if entry != value]
        updated.insert(0, value)
        return updated
    def _normalize_sessions(self, raw: Any) -> list[str]:
        values = raw if isinstance(raw, list) else [raw]
        normalized = self._normalize_session_list(values)
        deduped: list[str] = []
        seen: set[str] = set()
        for session in normalized:
            if session in seen:
                continue
            seen.add(session)
            deduped.append(session)
        return deduped
    def _normalize_session(self, raw: Any) -> str:
        normalized = self._normalize_sessions([raw])
        if not normalized:
            return ""
        return normalized[0]
