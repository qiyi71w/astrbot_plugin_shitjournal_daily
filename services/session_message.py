from __future__ import annotations

from typing import Any

from astrbot.core.platform.message_session import MessageSesion
from astrbot.core.platform.message_type import MessageType


def get_session_message_type(raw_session: Any) -> MessageType | None:
    session = parse_message_session(raw_session)
    if session is None:
        return None
    return session.message_type


def is_group_message_session(raw_session: Any) -> bool:
    return get_session_message_type(raw_session) == MessageType.GROUP_MESSAGE


def is_private_message_session(raw_session: Any) -> bool:
    return get_session_message_type(raw_session) == MessageType.FRIEND_MESSAGE


def parse_message_session(raw_session: Any) -> MessageSesion | None:
    session_text = str(raw_session).strip()
    if not session_text:
        return None
    try:
        return MessageSesion.from_str(session_text)
    except Exception:
        return None
