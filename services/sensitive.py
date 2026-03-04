from __future__ import annotations

import re


def mask_sensitive_text(text: str) -> str:
    masked = str(text)
    masked = re.sub(r"(token=)[^&\s]+", r"\1<hidden>", masked, flags=re.IGNORECASE)
    masked = re.sub(r"(apikey=)[^&\s]+", r"\1<hidden>", masked, flags=re.IGNORECASE)
    masked = re.sub(r"(authorization:\s*bearer\s+)[^\s]+", r"\1<hidden>", masked, flags=re.IGNORECASE)
    return masked
