from __future__ import annotations

from typing import Any, Callable


def clean_warning_list(raw: Any, mask_sensitive_text: Callable[[str], str]) -> list[str]:
    if not isinstance(raw, list):
        return []
    warnings: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = mask_sensitive_text(str(item).strip())
        if not text or text in seen:
            continue
        seen.add(text)
        warnings.append(text)
    return warnings


def join_warning_text(warnings: list[str], mask_sensitive_text: Callable[[str], str]) -> str:
    return "；".join(clean_warning_list(warnings, mask_sensitive_text))


def append_warning_lines(
    lines: list[str],
    warnings: list[str],
    mask_sensitive_text: Callable[[str], str],
) -> None:
    for index, warning in enumerate(
        clean_warning_list(warnings, mask_sensitive_text),
        start=1,
    ):
        lines.append(f"告警{index}: {warning}")
