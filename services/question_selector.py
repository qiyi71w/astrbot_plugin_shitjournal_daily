from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .models import RunBatch
from .question_api_client import QuestionConfigError
from .question_constants import DEFAULT_QUESTION_DETAIL_URL_BASE, build_question_detail_url


@dataclass(slots=True)
class QuestionRunSelectionResult:
    batches: list[RunBatch] = field(default_factory=list)
    saw_question: bool = False
    warnings: list[str] = field(default_factory=list)
    last_seen_map: dict[str, str] = field(default_factory=dict)


class QuestionSelectionError(RuntimeError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


class QuestionSelector:
    def __init__(
        self,
        *,
        fetch_latest_questions: Callable[[str, int, int], Awaitable[list[dict[str, Any]]]],
        get_run_sent_histories: Callable[[str, list[str]], Awaitable[dict[str, list[str]]]],
        logger: Any,
        detail_url_base: str = DEFAULT_QUESTION_DETAIL_URL_BASE,
    ):
        self._fetch = fetch_latest_questions
        self._get_run_histories = get_run_sent_histories
        self._logger = logger
        self._detail_url_base = str(detail_url_base).strip() or DEFAULT_QUESTION_DETAIL_URL_BASE

    async def select_run_batches(
        self,
        *,
        zone_order: list[str],
        targets: list[str],
        last_seen_map: dict[str, str],
        force: bool,
        latest_only: bool,
        fetch_page_size: int,
    ) -> QuestionRunSelectionResult:
        next_last_seen_map = dict(last_seen_map)
        if latest_only:
            batches, saw_question, warnings = await self._select_latest_only(zone_order, targets, next_last_seen_map, force, fetch_page_size)
        else:
            batches, saw_question, warnings = await self._select_all(zone_order, targets, next_last_seen_map, force, fetch_page_size)
        return QuestionRunSelectionResult(
            batches=batches,
            saw_question=saw_question,
            warnings=warnings,
            last_seen_map=next_last_seen_map,
        )

    async def _select_all(self, zone_order, targets, last_seen_map, force, fetch_page_size):
        batches: list[RunBatch] = []
        warnings: list[str] = []
        saw_question = False
        unresolved_targets = list(targets)
        for index, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            zone_batches, zone_saw, zone_warnings, matched = await self._select_zone_batches(
                zone=zone,
                targets=unresolved_targets,
                last_seen_map=last_seen_map,
                force=force,
                fetch_page_size=fetch_page_size,
                is_primary=index == 0,
            )
            batches.extend(zone_batches)
            warnings.extend(zone_warnings)
            saw_question = saw_question or zone_saw
            unresolved_targets = [target for target in unresolved_targets if target not in matched]
        return batches, saw_question, warnings

    async def _select_latest_only(self, zone_order, targets, last_seen_map, force, fetch_page_size):
        batches: list[RunBatch] = []
        warnings: list[str] = []
        saw_question = False
        unresolved_targets = list(targets)
        for index, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            candidates, zone_warnings = await self._fetch_candidates(zone, 0, fetch_page_size, is_primary=index == 0)
            warnings.extend(zone_warnings)
            if candidates is None or not candidates:
                continue
            saw_question = True
            question_id = self._extract_latest_question_id(zone, candidates[0], is_primary=index == 0)
            if question_id is None:
                continue
            last_seen_map[zone] = question_id
            histories = await self._get_run_histories(zone, unresolved_targets)
            matched = self._match_targets(question_id, unresolved_targets, set(), histories, force)
            if matched:
                batches.append(self._build_batch(zone, candidates[0], question_id, matched))
                unresolved_targets = [target for target in unresolved_targets if target not in matched]
        return batches, saw_question, warnings

    async def _select_zone_batches(self, *, zone, targets, last_seen_map, force, fetch_page_size, is_primary):
        histories = await self._get_run_histories(zone, targets)
        matched: set[str] = set()
        warnings: list[str] = []
        batches: list[RunBatch] = []
        saw_question = False
        offset = 0
        zone_latest_recorded = False
        while len(matched) < len(targets):
            candidates, page_warnings = await self._fetch_candidates(zone, offset, fetch_page_size, is_primary=is_primary)
            warnings.extend(page_warnings)
            if candidates is None or not candidates:
                return batches, saw_question, warnings, matched
            saw_question = True
            if not zone_latest_recorded:
                question_id = self._extract_latest_question_id(zone, candidates[0], is_primary=is_primary)
                if question_id is None:
                    return batches, saw_question, warnings, matched
                last_seen_map[zone] = question_id
                zone_latest_recorded = True
            page_batches, page_matches = self._build_page_batches(zone, candidates, targets, matched, histories, force)
            batches.extend(page_batches)
            matched.update(page_matches)
            if len(candidates) < fetch_page_size:
                return batches, saw_question, warnings, matched
            offset += len(candidates)
        return batches, saw_question, warnings, matched

    async def _fetch_candidates(self, zone, offset, fetch_page_size, *, is_primary):
        try:
            return await self._fetch(zone, fetch_page_size, offset), []
        except QuestionConfigError as exc:
            raise QuestionSelectionError("QUESTION_CONFIG_ERROR", str(exc)) from exc
        except Exception as exc:
            zone_type = "主分区" if is_primary else "候补分区"
            warning = f"{zone_type}课题抓取失败：分区={zone} 偏移={offset} 错误={type(exc).__name__}"
            self._logger.warning("获取课题候选失败：分区=%s 偏移=%s", zone, offset, exc_info=True)
            return None, [warning]

    def _build_page_batches(self, zone, candidates, targets, matched, histories, force):
        page_batches: list[RunBatch] = []
        page_matches: set[str] = set()
        for candidate in candidates:
            question_id = str(candidate.get("id", "")).strip()
            if not question_id:
                self._logger.warning("课题候选缺少 ID，已跳过：分区=%s", zone)
                continue
            batch_targets = self._match_targets(question_id, targets, matched | page_matches, histories, force)
            if batch_targets:
                page_batches.append(self._build_batch(zone, candidate, question_id, batch_targets))
                page_matches.update(batch_targets)
        return page_batches, page_matches

    def _extract_latest_question_id(self, zone, candidate, *, is_primary):
        question_id = str(candidate.get("id", "")).strip()
        if question_id:
            return question_id
        message = f"课题最新记录缺少 ID：分区={zone}"
        if is_primary:
            raise QuestionSelectionError("EMPTY_QUESTION_ID", message)
        self._logger.warning(message)
        return None

    def _match_targets(self, question_id, targets, matched, histories, force):
        pending: list[str] = []
        for session in targets:
            if session in matched:
                continue
            sent_history = set(histories.get(session, []))
            if not force and question_id in sent_history:
                continue
            pending.append(session)
        return pending

    def _build_batch(self, zone, candidate, question_id, targets):
        return RunBatch(
            zone=zone,
            latest=dict(candidate),
            paper_id=question_id,
            detail_url=build_question_detail_url(self._detail_url_base, question_id),
            targets=list(targets),
        )
