from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from .models import RunBatch
from .sensitive import mask_sensitive_text

DETAIL_URL_BASE = "https://shitjournal.org"
META_PREVIEW_KEYS = (
    "id", "manuscript_title", "author_name", "institution", "discipline", "viscosity",
    "created_at", "avg_score", "rating_count", "weighted_score", "pdf_path", "zone",
)

@dataclass(slots=True)
class ChiShiSelection:
    zone: str
    paper_id: str
    candidate: dict[str, Any]

@dataclass(slots=True)
class RunSelectionResult:
    batches: list[RunBatch] = field(default_factory=list)
    saw_submission: bool = False
    warnings: list[str] = field(default_factory=list)
    last_seen_map: dict[str, str] = field(default_factory=dict)

class RunSelectionError(RuntimeError):
    def __init__(self, reason_code: str, message: str = ""):
        super().__init__(message or reason_code)
        self.reason_code = reason_code

class RunSelector:
    def __init__(self, *, fetch_latest_submissions: Callable[[str, int, int], Awaitable[list[dict[str, Any]]]], get_run_sent_histories: Callable[[str, list[str]], Awaitable[dict[str, list[str]]]], get_chi_shi_sent_history: Callable[[str, str], Awaitable[list[str]]], logger: Any, detail_url_base: str = DETAIL_URL_BASE):
        self._fetch = fetch_latest_submissions
        self._get_run_histories = get_run_sent_histories
        self._get_chi_shi_history = get_chi_shi_sent_history
        self._logger = logger
        self._detail_url_base = str(detail_url_base).strip() or DETAIL_URL_BASE

    async def select_chi_shi_candidate_from_zones(self, *, zone_order: list[str], session_key: str, fetch_page_size: int) -> tuple[ChiShiSelection | None, bool, list[str]]:
        return await self._select_chi_shi_candidate_from_zones(zone_order, session_key, fetch_page_size)

    async def select_run_batches(self, *, zone_order: list[str], targets: list[str], last_seen_map: dict[str, str], force: bool, latest_only: bool, fetch_page_size: int) -> RunSelectionResult:
        next_last_seen_map = dict(last_seen_map)
        batches, saw_submission, warnings = await self._select_run_batches(
            zone_order,
            targets,
            next_last_seen_map,
            force,
            latest_only,
            fetch_page_size,
        )
        return RunSelectionResult(
            batches=batches,
            saw_submission=saw_submission,
            warnings=warnings,
            last_seen_map=next_last_seen_map,
        )

    async def _select_chi_shi_candidate_from_zones(self, zone_order, session_key, fetch_page_size):
        saw_candidates = False
        warnings = []
        for idx, zone in enumerate(zone_order):
            selected, zone_saw, zone_warnings = await self._select_chi_shi_zone_candidate(zone=zone, is_primary=idx == 0, session_key=session_key, fetch_page_size=fetch_page_size)
            saw_candidates = saw_candidates or zone_saw
            warnings.extend(zone_warnings)
            if selected is not None:
                return selected, True, warnings
        return None, saw_candidates, warnings

    async def _select_chi_shi_zone_candidate(self, *, zone, is_primary, session_key, fetch_page_size):
        sent_set = set(await self._get_chi_shi_history(zone, session_key))
        warnings, saw_candidates, offset = [], False, 0
        while True:
            try:
                candidates = await self._fetch(zone, fetch_page_size, offset)
            except Exception as exc:
                warnings.append(self._build_zone_fetch_warning_text(zone=zone, is_primary=is_primary, offset=offset, error=exc))
                self._logger.warning("获取“我要赤石”分区候选论文失败，已继续回退下一个分区：分区=%s 偏移=%s", zone, offset, exc_info=(type(exc), exc, exc.__traceback__))
                return None, saw_candidates, warnings
            if not candidates:
                return None, saw_candidates, warnings
            saw_candidates = True
            picked = self._pick_first_unsent_candidate(candidates, sent_set)
            if picked is not None:
                paper_id, candidate = picked
                return ChiShiSelection(zone=zone, paper_id=paper_id, candidate=candidate), True, warnings
            if len(candidates) < fetch_page_size:
                return None, saw_candidates, warnings
            offset += len(candidates)

    async def _select_run_batches(self, zone_order, targets, last_seen_map, force, latest_only, fetch_page_size):
        if latest_only:
            return await self._select_latest_only_run_batches(zone_order, targets, last_seen_map, force, fetch_page_size)
        batches, warnings = [], []
        saw_submission = False
        unresolved_targets = targets.copy()
        for idx, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            first_page, zone_warnings = await self._fetch_run_candidates_page(zone, idx == 0, 0, fetch_page_size)
            warnings.extend(zone_warnings)
            if first_page is None:
                continue
            zone_batches, matched, zone_saw, page_warnings = await self._select_zone_run_batches(zone=zone, is_primary=idx == 0, unresolved_targets=unresolved_targets, last_seen_map=last_seen_map, force=force, first_page_candidates=first_page, fetch_page_size=fetch_page_size)
            warnings.extend(page_warnings)
            batches.extend(zone_batches)
            saw_submission = saw_submission or zone_saw
            unresolved_targets = self._subtract_targets(unresolved_targets, matched)
        return batches, saw_submission, warnings

    async def _select_latest_only_run_batches(self, zone_order, targets, last_seen_map, force, fetch_page_size):
        batches, warnings = [], []
        saw_submission = False
        unresolved_targets = targets.copy()
        for idx, zone in enumerate(zone_order):
            if not unresolved_targets:
                break
            first_page, zone_warnings = await self._fetch_run_candidates_page(zone, idx == 0, 0, fetch_page_size)
            warnings.extend(zone_warnings)
            if first_page is None:
                continue
            batch, matched, zone_saw = await self._select_latest_only_zone_batch(zone=zone, is_primary=idx == 0, unresolved_targets=unresolved_targets, last_seen_map=last_seen_map, force=force, first_page_candidates=first_page)
            if batch is not None:
                batches.append(batch)
            saw_submission = saw_submission or zone_saw
            unresolved_targets = self._subtract_targets(unresolved_targets, matched)
        return batches, saw_submission, warnings

    async def _select_zone_run_batches(self, *, zone, is_primary, unresolved_targets, last_seen_map, force, first_page_candidates, fetch_page_size):
        if not first_page_candidates:
            return [], set(), False, []
        run_history_by_target = await self._get_run_histories(zone, unresolved_targets)
        sent_lookup = self._build_history_lookup(run_history_by_target)
        batches, matched, warnings = [], set(), []
        saw_submission, zone_latest_recorded, offset = False, False, 0
        while True:
            candidates, page_warnings = await self._load_run_candidates_page(zone=zone, is_primary=is_primary, first_page_candidates=first_page_candidates, offset=offset, fetch_page_size=fetch_page_size)
            warnings.extend(page_warnings)
            if not candidates:
                break
            saw_submission = True
            page_batches, skip_zone, zone_latest_recorded, page_targets = self._build_zone_run_page_batches(zone, candidates, offset, unresolved_targets, matched, sent_lookup, force, is_primary, zone_latest_recorded, last_seen_map)
            batches.extend(page_batches)
            matched.update(page_targets)
            if skip_zone or len(candidates) < fetch_page_size or len(matched) == len(unresolved_targets):
                break
            offset += len(candidates)
        return batches, matched, saw_submission, warnings

    async def _select_latest_only_zone_batch(self, *, zone, is_primary, unresolved_targets, last_seen_map, force, first_page_candidates):
        if not first_page_candidates:
            return None, set(), False
        candidate = first_page_candidates[0]
        paper_id = self._extract_run_candidate_paper_id(zone, candidate, is_primary)
        if paper_id is None:
            return None, set(), True
        last_seen_map[zone] = paper_id
        run_history_by_target = await self._get_run_histories(zone, unresolved_targets)
        sent_lookup = self._build_history_lookup(run_history_by_target)
        matched = self._match_run_batch_targets(paper_id, unresolved_targets, set(), sent_lookup, force)
        if not matched:
            return None, set(), True
        batch = self._build_run_batch(zone, candidate, paper_id, matched)
        return batch, set(matched), True

    async def _load_run_candidates_page(self, *, zone, is_primary, first_page_candidates, offset, fetch_page_size):
        if offset == 0:
            return first_page_candidates, []
        return await self._fetch_run_candidates_page(zone, is_primary, offset, fetch_page_size)

    def _build_zone_run_page_batches(self, zone, candidates, offset, unresolved, matched, lookup, force, is_primary, zone_latest_recorded, last_seen_map):
        page_batches, page_targets, occupied = [], set(), set(matched)
        for idx, candidate in enumerate(candidates):
            paper_id, skip_zone = self._resolve_page_candidate_id(zone, candidate, offset, idx, is_primary)
            if skip_zone:
                return [], True, zone_latest_recorded, page_targets
            if not paper_id:
                continue
            zone_latest_recorded = self._record_zone_latest(zone, paper_id, zone_latest_recorded, last_seen_map)
            batch_targets = self._match_run_batch_targets(paper_id, unresolved, occupied, lookup, force)
            if not batch_targets:
                continue
            page_batches.append(self._build_run_batch(zone, candidate, paper_id, batch_targets))
            page_targets.update(batch_targets)
            occupied.update(batch_targets)
        return page_batches, False, zone_latest_recorded, page_targets

    def _resolve_page_candidate_id(self, zone, candidate, offset, candidate_index, is_primary):
        paper_id = str(candidate.get("id", "")).strip()
        if paper_id:
            return paper_id, False
        if offset == 0 and candidate_index == 0:
            paper_id = self._extract_run_candidate_paper_id(zone, candidate, is_primary)
            return paper_id, paper_id is None
        self._logger.warning("候选论文缺少 ID，已跳过：分区=%s 载荷=%s", zone, json.dumps(self._build_meta_preview(candidate), ensure_ascii=False))
        return None, False

    def _record_zone_latest(self, zone, paper_id, zone_latest_recorded, last_seen_map):
        if zone_latest_recorded:
            return True
        self._logger.info("已获取最新论文：分区=%s 论文ID=%s 详情=%s", zone, paper_id, self._build_preprint_detail_url(paper_id))
        last_seen_map[zone] = paper_id
        return True

    def _subtract_targets(self, targets, removed_targets):
        return targets if not removed_targets else [target for target in targets if target not in removed_targets]

    def _match_run_batch_targets(self, paper_id, unresolved_targets, matched_targets, sent_lookup, force):
        pending = []
        for session in unresolved_targets:
            if session in matched_targets:
                continue
            if not force and paper_id in sent_lookup.get(session, set()):
                continue
            pending.append(session)
        return pending

    def _build_run_batch(self, zone, candidate, paper_id, targets):
        return RunBatch(zone=zone, latest=candidate, paper_id=paper_id, detail_url=self._build_preprint_detail_url(paper_id), targets=targets)

    async def _fetch_run_candidates_page(self, zone, is_primary, offset, fetch_page_size):
        try:
            return await self._fetch(zone, fetch_page_size, offset), []
        except Exception as exc:
            warning = self._build_zone_fetch_warning_text(zone=zone, is_primary=is_primary, offset=offset, error=exc)
            self._log_run_candidate_fetch_warning(zone, is_primary, offset, exc)
            return None, [warning]

    def _log_run_candidate_fetch_warning(self, zone, is_primary, offset, error):
        zone_type = "主分区" if is_primary else "候补分区"
        if offset == 0:
            self._logger.warning("获取%s最新论文失败，已继续回退后续分区：分区=%s", zone_type, zone, exc_info=(type(error), error, error.__traceback__))
            return
        self._logger.warning("获取%s候选页失败，已停止继续检查该分区并回退后续分区：分区=%s 偏移=%s", zone_type, zone, offset, exc_info=(type(error), error, error.__traceback__))

    def _raise_run_candidate_id_error(self, zone, is_primary, candidate):
        zone_type = "主分区最新论文" if is_primary else "候补分区最新论文"
        meta_preview = json.dumps(self._build_meta_preview(candidate), ensure_ascii=False)
        raise RunSelectionError("EMPTY_PAPER_ID", f"{zone_type}缺少 ID：分区={zone} 载荷={meta_preview}")

    def _log_run_candidate_id_warning(self, zone, candidate):
        self._logger.warning("候补分区最新论文缺少 ID，已跳过该分区：分区=%s 载荷=%s", zone, json.dumps(self._build_meta_preview(candidate), ensure_ascii=False))

    def _extract_run_candidate_paper_id(self, zone, candidate, is_primary):
        paper_id = str(candidate.get("id", "")).strip()
        if paper_id:
            return paper_id
        if is_primary:
            self._raise_run_candidate_id_error(zone, is_primary, candidate)
        self._log_run_candidate_id_warning(zone, candidate)
        return None

    def _pick_first_unsent_candidate(self, candidates, sent_set):
        for candidate in candidates:
            paper_id = str(candidate.get("id", "")).strip()
            if paper_id and paper_id not in sent_set:
                return paper_id, candidate
        return None

    def _build_history_lookup(self, history_by_target):
        return {session: set(history) for session, history in history_by_target.items()}

    def _build_preprint_detail_url(self, paper_id):
        return f"{self._detail_url_base}/preprints/{str(paper_id).strip()}"

    def _build_zone_fetch_warning_text(self, *, zone, is_primary, offset, error):
        zone_type = "主分区" if is_primary else "候补分区"
        position = "首页" if offset == 0 else f"偏移={offset}"
        error_text = mask_sensitive_text(str(error).strip()) or type(error).__name__
        return f"{zone_type}抓取失败：分区={zone} {position} 错误={error_text}"

    def _build_meta_preview(self, payload):
        return {key: payload.get(key) for key in META_PREVIEW_KEYS}
