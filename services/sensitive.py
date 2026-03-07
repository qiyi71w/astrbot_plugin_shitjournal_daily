from __future__ import annotations

import re


_GENERIC_SECRET_KEY_PATTERN = (
    r"(?:x[-_]?api[-_]?key|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"id[_-]?token|token)"
)
_MASK_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"(authorization\s*:\s*(?:[A-Za-z][A-Za-z0-9_-]*\s+)?)"
            r"[^\s,;\"'}]+",
            flags=re.IGNORECASE,
        ),
        r"\1<hidden>",
    ),
    (
        re.compile(
            r"([\"']authorization[\"']\s*:\s*[\"']"
            r"(?:[A-Za-z][A-Za-z0-9_-]*\s+)?)"
            r"[^\"']+",
            flags=re.IGNORECASE,
        ),
        r"\1<hidden>",
    ),
    (
        re.compile(
            rf"(({_GENERIC_SECRET_KEY_PATTERN})\s*=\s*)[^&\s,;\"']+",
            flags=re.IGNORECASE,
        ),
        r"\1<hidden>",
    ),
    (
        re.compile(
            rf"(({_GENERIC_SECRET_KEY_PATTERN})\s*:\s*)[^\s,;\"'}}]+",
            flags=re.IGNORECASE,
        ),
        r"\1<hidden>",
    ),
    (
        re.compile(
            rf"([\"']{_GENERIC_SECRET_KEY_PATTERN}[\"']\s*:\s*[\"'])[^\"']+",
            flags=re.IGNORECASE,
        ),
        r"\1<hidden>",
    ),
)


def mask_sensitive_text(text: str) -> str:
    masked = str(text)
    for pattern, replacement in _MASK_PATTERNS:
        masked = pattern.sub(replacement, masked)
    return masked
