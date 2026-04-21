from __future__ import annotations

from typing import Any, Protocol


class OneBotCallAction(Protocol):
    def __call__(self, action: str, **params: Any) -> Any: ...


def resolve_call_action_from_bot(bot: Any) -> OneBotCallAction | None:
    direct_call_action = getattr(bot, "call_action", None)
    if callable(direct_call_action):
        return direct_call_action
    api = getattr(bot, "api", None)
    api_call_action = getattr(api, "call_action", None)
    if callable(api_call_action):
        return api_call_action
    return None
