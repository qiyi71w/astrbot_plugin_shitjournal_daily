from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

QUESTION_LAST_SEEN_KV_KEY = "questions_last_seen_by_zone"
QUESTION_RUN_SENT_HISTORY_KV_PREFIX = "question_run_sent_history_v1::"


class QuestionHistoryStore:
    def __init__(
        self,
        *,
        kv_getter: Callable[[str, Any], Awaitable[Any]],
        kv_putter: Callable[[str, Any], Awaitable[None]],
        normalize_session_list: Callable[[Any], list[str]],
        normalize_zone_name: Callable[[Any], str],
    ):
        self._kv_getter = kv_getter
        self._kv_putter = kv_putter
        self._normalize_sessions = normalize_session_list
        self._normalize_zone_name = normalize_zone_name
        self._run_history_lock = asyncio.Lock()

    async def get_last_seen_map(self) -> dict[str, str]:
        raw = await self._kv_getter(QUESTION_LAST_SEEN_KV_KEY, {})
        return self._clean_dict(raw)

    async def set_last_seen_map(self, last_seen_map: dict[str, str]) -> None:
        await self._kv_putter(QUESTION_LAST_SEEN_KV_KEY, self._clean_dict(last_seen_map))

    async def get_run_sent_histories(self, zone: str, targets: list[str]) -> dict[str, list[str]]:
        normalized_targets = self._normalize_sessions(targets)
        if not normalized_targets:
            return {}
        raw_histories = await asyncio.gather(
            *[self._kv_getter(self._run_history_key(zone, session), []) for session in normalized_targets],
        )
        return {
            session: self._clean_list(raw_history)
            for session, raw_history in zip(normalized_targets, raw_histories)
        }

    async def mark_run_targets_delivered(
        self,
        *,
        zone: str,
        question_id: str,
        success_targets: list[str],
    ) -> None:
        normalized_targets = self._normalize_sessions(success_targets)
        if not normalized_targets:
            return
        async with self._run_history_lock:
            for session in normalized_targets:
                store_key = self._run_history_key(zone, session)
                history = self._clean_list(await self._kv_getter(store_key, []))
                await self._kv_putter(store_key, self._prepend_history_item(history, question_id))

    def _run_history_key(self, zone: str, session: str) -> str:
        return f"{QUESTION_RUN_SENT_HISTORY_KV_PREFIX}{self._normalize_zone_name(zone)}::{session}"

    def _prepend_history_item(self, history: list[str], question_id: str) -> list[str]:
        item = str(question_id).strip()
        if not item:
            return list(history)
        filtered = [existing for existing in history if existing != item]
        return [item, *filtered]

    def _clean_dict(self, raw: Any) -> dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        result: dict[str, str] = {}
        for key, value in raw.items():
            key_text = str(key).strip()
            value_text = str(value).strip()
            if key_text and value_text:
                result[key_text] = value_text
        return result

    def _clean_list(self, raw: Any) -> list[str]:
        values = raw if isinstance(raw, list) else [raw]
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in values:
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text)
        return cleaned

