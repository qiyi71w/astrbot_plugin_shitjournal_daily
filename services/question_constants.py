from __future__ import annotations

from typing import Any
from urllib.parse import quote

DEFAULT_QUESTION_API_BASE_URL = "https://shitspace.xyz"
DEFAULT_QUESTION_DETAIL_URL_BASE = "https://shitspace.xyz"
QUESTION_EXCERPT_LIMIT = 600
QUESTION_NORMALIZED_FIELDS = (
    "id",
    "title",
    "content",
    "author_name",
    "created_at",
    "discipline",
    "avg_score",
    "rating_count",
    "comment_count",
    "tag",
    "zone",
)
QUESTION_ALLOWED_SORTS = {
    "latrine": ("newest", "hottest", "random"),
    "septic": ("hottest", "highest_rated", "random"),
    "sediment": ("newest",),
    "stone": ("highest_rated", "random"),
}
QUESTION_DEFAULT_SORTS = {
    "latrine": "newest",
    "septic": "hottest",
    "sediment": "newest",
    "stone": "highest_rated",
}
QUESTION_ZONE_LABELS = {
    "latrine": "啥课题",
    "septic": "是个课题",
    "sediment": "不是课题",
    "stone": "好课题",
}
QUESTION_DISCIPLINE_LABELS = {
    "science": "科学",
    "engineering": "工程",
    "agriculture": "农业",
    "medicine": "医学",
    "economics": "经济学",
    "management": "管理学",
    "law": "法学",
    "social": "社会学",
    "literature": "文学",
    "history": "历史学",
    "philosophy": "哲学",
    "art": "艺术学",
    "business": "商科",
    "mathematics": "数学",
    "interdisciplinary": "交叉学科",
}


def to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value).strip()
    return ""


def resolve_question_zone(payload: dict[str, Any], default_zone: str = "") -> str:
    zone = to_text(payload.get("zone"))
    if zone:
        return zone
    zones = payload.get("zones")
    if isinstance(zones, list):
        for item in zones:
            candidate = to_text(item)
            if candidate:
                return candidate
        return to_text(default_zone)
    candidate = to_text(zones)
    return candidate or to_text(default_zone)


def normalize_question_payload(payload: dict[str, Any], default_zone: str = "") -> dict[str, Any]:
    author = payload.get("author")
    author_name = to_text(payload.get("author_name"))
    if not author_name and isinstance(author, dict):
        author_name = to_text(author.get("display_name"))
    normalized = {
        "id": to_text(payload.get("id")),
        "title": to_text(payload.get("title")),
        "content": to_text(payload.get("content")),
        "author_name": author_name,
        "created_at": to_text(payload.get("created_at")),
        "discipline": to_text(payload.get("discipline")),
        "avg_score": payload.get("avg_score"),
        "rating_count": payload.get("rating_count"),
        "comment_count": payload.get("comment_count"),
        "tag": to_text(payload.get("tag")),
        "zone": resolve_question_zone(payload, default_zone),
    }
    return normalized


def build_question_detail_url(detail_url_base: str, question_id: str) -> str:
    safe_base = str(detail_url_base).strip().rstrip("/") or DEFAULT_QUESTION_DETAIL_URL_BASE
    safe_id = quote(str(question_id).strip(), safe="")
    return f"{safe_base}/question/{safe_id}"


def truncate_question_excerpt(content: str, limit: int = QUESTION_EXCERPT_LIMIT) -> str:
    text = str(content or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "..."

