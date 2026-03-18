from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult
from astrbot.core.platform.message_session import MessageSesion

from .push_chain_builder import ONEBOT_ADAPTER_NAME, ONEBOT_MERGE_FORWARD_MESSAGE_TYPES


class MessageSink(Protocol):
    async def send(self, chain: MessageEventResult) -> bool:
        ...


@dataclass(frozen=True)
class EventMessageSink:
    event: AstrMessageEvent

    async def send(self, chain: MessageEventResult) -> bool:
        await self.event.send(chain)
        return True


@dataclass(frozen=True)
class SessionMessageSink:
    context: Any
    session: str

    async def send(self, chain: MessageEventResult) -> bool:
        return bool(await self.context.send_message(self.session, chain))


class OneBotPlatformResolver:
    def __init__(self, cfg_bool_getter):
        self._cfg_bool = cfg_bool_getter
        self._self_id_cache: dict[str, str] = {}

    def resolve_merge_forward_platform(self, context: Any, session: str) -> Any | None:
        if not self._cfg_bool("send_merge_forward", False):
            return None
        try:
            message_session = MessageSesion.from_str(session)
        except Exception:
            logger.warning("无法解析主动消息会话，回退普通消息：%s", session, exc_info=True)
            return None
        if message_session.message_type not in ONEBOT_MERGE_FORWARD_MESSAGE_TYPES:
            return None
        platform = self._find_platform_by_id(context, message_session.platform_name)
        if platform is None:
            logger.warning("未找到目标平台实例，回退普通消息：%s", session)
            return None
        if str(platform.meta().name) != ONEBOT_ADAPTER_NAME:
            return None
        return platform

    def resolve_platform_name(self, context: Any, session: str) -> str:
        try:
            message_session = MessageSesion.from_str(session)
        except Exception:
            return ""
        platform = self._find_platform_by_id(context, message_session.platform_name)
        if platform is None:
            return ""
        return str(platform.meta().name)

    async def get_platform_self_id(self, platform: Any) -> str:
        platform_meta = platform.meta()
        platform_id = str(getattr(platform_meta, "id", "")).strip()
        cached_self_id = self._self_id_cache.get(platform_id, "")
        if cached_self_id:
            return cached_self_id
        bot = getattr(platform, "bot", None)
        call_action = getattr(bot, "call_action", None)
        if not callable(call_action):
            return ""
        try:
            login_info = await call_action("get_login_info")
        except Exception:
            logger.warning("获取 OneBot 登录信息失败：平台=%s", platform_id, exc_info=True)
            return ""
        user_id = self._extract_user_id(login_info)
        if user_id:
            self._self_id_cache[platform_id] = user_id
        return user_id

    def _find_platform_by_id(self, context: Any, platform_id: str) -> Any | None:
        platform_manager = getattr(context, "platform_manager", None)
        if platform_manager is None:
            return None
        get_insts = getattr(platform_manager, "get_insts", None)
        platforms = list(get_insts()) if callable(get_insts) else list(getattr(platform_manager, "platform_insts", []))
        for platform in platforms:
            meta = getattr(platform, "meta", None)
            if not callable(meta):
                continue
            try:
                platform_meta = meta()
            except Exception:
                continue
            if str(getattr(platform_meta, "id", "")) == platform_id:
                return platform
        return None

    def _extract_user_id(self, login_info: Any) -> str:
        if not isinstance(login_info, dict):
            return ""
        direct_user_id = str(login_info.get("user_id", "")).strip()
        if direct_user_id:
            return direct_user_id
        data = login_info.get("data")
        if not isinstance(data, dict):
            return ""
        return str(data.get("user_id", "")).strip()
