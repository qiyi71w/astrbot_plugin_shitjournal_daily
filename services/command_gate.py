from __future__ import annotations

import re
from typing import Any, Callable

from astrbot.api.event import AstrMessageEvent


class CommandGate:
    def __init__(
        self,
        *,
        context_getter: Callable[[], Any],
        cfg_bool_getter: Callable[[str, bool], bool],
        logger: Any,
    ):
        self._context_getter = context_getter
        self._cfg_bool_getter = cfg_bool_getter
        self._logger = logger

    async def check_command_permission(self, event: AstrMessageEvent) -> bool:
        if not self.looks_like_message_event(event):
            self._logger.error("指令权限检查失败：无效事件类型=%s", type(event).__name__)
            return False
        if not self._cfg_bool_getter("command_admin_only", True):
            return True
        if self.is_admin_event(event):
            return True
        if self._cfg_bool_getter("command_no_permission_reply", True):
            await event.send(event.plain_result("权限不足：仅管理员可使用该指令。"))
        event.stop_event()
        return False

    def normalize_command_event(
        self,
        event: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        command_name: str,
    ) -> tuple[AstrMessageEvent, tuple[Any, ...], dict[str, Any]] | None:
        if self.looks_like_message_event(event):
            return event, args, kwargs

        candidate = kwargs.get("event")
        if self.looks_like_message_event(candidate):
            cleaned_kwargs = dict(kwargs)
            cleaned_kwargs.pop("event", None)
            self._logger.warning(
                "指令 %s 从 kwargs 中修正了偏移的事件对象（原类型=%s）。",
                command_name,
                type(event).__name__,
            )
            return candidate, args, cleaned_kwargs

        for idx, item in enumerate(args):
            if not self.looks_like_message_event(item):
                continue
            shifted_args = args[:idx] + args[idx + 1 :]
            self._logger.warning(
                "指令 %s 从 args[%d] 中修正了偏移的事件对象（原类型=%s）。",
                command_name,
                idx,
                type(event).__name__,
            )
            return item, shifted_args, kwargs

        self._logger.error(
            "指令 %s 事件归一化失败：event_type=%s args_types=%s kwargs_keys=%s",
            command_name,
            type(event).__name__,
            [type(item).__name__ for item in args[:4]],
            list(kwargs.keys())[:8],
        )
        return None

    def looks_like_message_event(self, value: Any) -> bool:
        if isinstance(value, AstrMessageEvent):
            return True
        required = (
            "plain_result",
            "send",
            "stop_event",
            "unified_msg_origin",
        )
        if value is None or not all(hasattr(value, attr) for attr in required):
            return False
        return callable(getattr(value, "get_sender_id", None))

    def is_admin_event(self, event: AstrMessageEvent) -> bool:
        is_admin = getattr(event, "is_admin", None)
        if callable(is_admin) and is_admin():
            return True
        return self.is_sender_in_admins(event)

    def is_sender_in_admins(self, event: AstrMessageEvent) -> bool:
        sender_id = str(event.get_sender_id()).strip()
        if not sender_id:
            return False

        config_obj: Any = getattr(self._context_getter(), "astrbot_config", None)
        if not isinstance(config_obj, dict):
            return False

        admins_raw = config_obj.get("admins_id", [])
        if isinstance(admins_raw, str):
            normalized_admins = [part.strip() for part in admins_raw.split(",")]
        elif isinstance(admins_raw, (list, tuple, set)):
            normalized_admins = [str(part).strip() for part in admins_raw]
        else:
            normalized_admins = []
        admins = {item for item in normalized_admins if item}
        return sender_id in admins

    def parse_command_action_arg(
        self,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        default_action: str,
    ) -> tuple[str, str]:
        positional_action: Any = args[0] if len(args) >= 1 else None
        positional_arg: Any = args[1] if len(args) >= 2 else None
        positional_extra: Any = list(args[2:]) if len(args) >= 3 else None
        default_action_normalized = default_action.strip().lower()

        action_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="action",
            alias_key="_action",
            positional_value=positional_action,
        )
        arg_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="arg",
            alias_key="_arg",
            positional_value=positional_arg,
        )
        extra_tokens = self._pick_command_tokens(
            kwargs=kwargs,
            primary_key="extra_args",
            alias_key="_extra_args",
            positional_value=positional_extra,
        )

        action = action_tokens[0] if action_tokens else default_action_normalized
        action = action or default_action_normalized
        merged_arg_tokens = action_tokens[1:] + arg_tokens + extra_tokens
        if action == default_action_normalized and merged_arg_tokens:
            action = merged_arg_tokens.pop(0)
        return action, " ".join(merged_arg_tokens).strip()

    def parse_action_arg_from_message(
        self,
        *,
        event: AstrMessageEvent,
        command_name: str,
    ) -> tuple[str, str]:
        message_text = ""
        getter = getattr(event, "get_message_str", None)
        if callable(getter):
            message_text = str(getter() or "")
        if not message_text:
            message_text = str(getattr(event, "message_str", "") or "")

        text = re.sub(r"\s+", " ", message_text).strip().lower()
        command = command_name.strip().lower()
        if not text or not command:
            return "", ""

        for candidate in (command, f"/{command}"):
            if text == candidate:
                return "help", ""
            if not text.startswith(f"{candidate} "):
                continue
            tail = text[len(candidate) :].strip()
            if not tail:
                return "help", ""
            tokens = [part for part in tail.split(" ") if part]
            action = tokens[0] if tokens else "help"
            arg = " ".join(tokens[1:]).strip() if len(tokens) >= 2 else ""
            return action, arg

        return "", ""

    def _pick_command_tokens(
        self,
        *,
        kwargs: dict[str, Any],
        primary_key: str,
        alias_key: str,
        positional_value: Any,
    ) -> list[str]:
        if primary_key in kwargs:
            tokens = self._split_command_tokens(kwargs.get(primary_key))
            if tokens:
                return tokens
        if alias_key in kwargs:
            tokens = self._split_command_tokens(kwargs.get(alias_key))
            if tokens:
                return tokens
        return self._split_command_tokens(positional_value)

    def _split_command_tokens(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            tokens: list[str] = []
            for item in value:
                tokens.extend(self._split_command_tokens(item))
            return tokens

        text = str(value).strip().lower()
        if not text:
            return []
        return [part for part in text.split() if part]
