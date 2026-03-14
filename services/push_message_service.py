from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult
from astrbot.api.message_components import File, Image, Node, Nodes, Plain
from astrbot.core.platform.message_session import MessageSesion
from astrbot.core.platform.message_type import MessageType


FORWARD_SENDER_NAME = "S.H.I.T Journal"
ONEBOT_ADAPTER_NAME = "aiocqhttp"


class PushMessageService:
    def __init__(self, cfg_bool_getter: Callable[[str, bool], bool]):
        self._cfg_bool = cfg_bool_getter
        self._self_id_cache: dict[str, str] = {}

    def build_standard_chain(
        self,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.extend(self._build_push_components(text, png_file, pdf_file, pdf_url))
        return chain

    def build_merge_forward_chain(
        self,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        sender_uin: str,
    ) -> MessageEventResult:
        chain = MessageEventResult()
        nodes = [
            Node(
                name=FORWARD_SENDER_NAME,
                uin=str(sender_uin),
                content=[
                    Plain(text),
                    Image.fromFileSystem(str(png_file)),
                ],
            ),
        ]
        file_component = self._build_pdf_component(pdf_file, pdf_url)
        if file_component is not None:
            nodes.append(
                Node(
                    name=FORWARD_SENDER_NAME,
                    uin=str(sender_uin),
                    content=[file_component],
                ),
            )
        chain.chain.append(Nodes(nodes=nodes))
        return chain

    async def send_event_push(
        self,
        event: AstrMessageEvent,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> None:
        normal_chain = self.build_standard_chain(text, png_file, pdf_file, pdf_url)
        if not self._should_try_merge_forward_for_event(event):
            await event.send(normal_chain)
            return

        sender_uin = str(event.get_self_id()).strip()
        if not sender_uin:
            logger.warning("事件发送无法获取机器人自身 ID，回退普通消息。")
            await event.send(normal_chain)
            return

        merge_chain = self.build_merge_forward_chain(text, png_file, pdf_file, pdf_url, sender_uin)
        await self._send_event_with_fallback(event, merge_chain, normal_chain)

    async def send_session_push(
        self,
        context: Any,
        session: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> bool:
        normal_chain = self.build_standard_chain(text, png_file, pdf_file, pdf_url)
        platform = self._resolve_merge_forward_platform(context, session)
        if platform is None:
            return await context.send_message(session, normal_chain)

        sender_uin = await self._get_platform_self_id(platform)
        if not sender_uin:
            logger.warning("主动发送无法获取机器人自身 ID，回退普通消息：会话=%s", session)
            return await context.send_message(session, normal_chain)

        merge_chain = self.build_merge_forward_chain(text, png_file, pdf_file, pdf_url, sender_uin)
        return await self._send_session_with_fallback(
            context=context,
            session=session,
            merge_chain=merge_chain,
            normal_chain=normal_chain,
        )

    def _build_push_components(
        self,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> list[Any]:
        components: list[Any] = [
            Plain(text),
            Image.fromFileSystem(str(png_file)),
        ]
        file_component = self._build_pdf_component(pdf_file, pdf_url)
        if file_component is not None:
            components.append(file_component)
        return components

    def _build_pdf_component(self, pdf_file: Path, pdf_url: str) -> File | None:
        if not self._cfg_bool("send_pdf", False):
            return None
        if pdf_url:
            return File(name=pdf_file.name, url=pdf_url)
        return File(name=pdf_file.name, file=str(pdf_file))

    def _should_try_merge_forward_for_event(self, event: AstrMessageEvent) -> bool:
        if not self._cfg_bool("send_merge_forward", False):
            return False
        if not event.get_group_id():
            return False
        return event.get_platform_name() == ONEBOT_ADAPTER_NAME

    def _resolve_merge_forward_platform(self, context: Any, session: str) -> Any | None:
        if not self._cfg_bool("send_merge_forward", False):
            return None

        try:
            message_session = MessageSesion.from_str(session)
        except Exception:
            logger.warning("无法解析主动消息会话，回退普通消息：%s", session, exc_info=True)
            return None

        if message_session.message_type != MessageType.GROUP_MESSAGE:
            return None

        platform = self._find_platform_by_id(context, message_session.platform_name)
        if platform is None:
            logger.warning("未找到目标平台实例，回退普通消息：%s", session)
            return None
        if str(platform.meta().name) != ONEBOT_ADAPTER_NAME:
            return None
        return platform

    def _find_platform_by_id(self, context: Any, platform_id: str) -> Any | None:
        platform_manager = getattr(context, "platform_manager", None)
        if platform_manager is None:
            return None

        get_insts = getattr(platform_manager, "get_insts", None)
        if callable(get_insts):
            platforms = list(get_insts())
        else:
            platforms = list(getattr(platform_manager, "platform_insts", []))

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

    async def _send_event_with_fallback(
        self,
        event: AstrMessageEvent,
        merge_chain: MessageEventResult,
        normal_chain: MessageEventResult,
    ) -> None:
        try:
            await event.send(merge_chain)
            return
        except Exception:
            logger.warning("合并转发发送失败，回退普通消息。", exc_info=True)
        await event.send(normal_chain)

    async def _send_session_with_fallback(
        self,
        *,
        context: Any,
        session: str,
        merge_chain: MessageEventResult,
        normal_chain: MessageEventResult,
    ) -> bool:
        try:
            merge_ok = await context.send_message(session, merge_chain)
            if merge_ok:
                return True
            logger.warning("合并转发发送返回失败，回退普通消息：会话=%s", session)
        except Exception:
            logger.warning("合并转发发送异常，回退普通消息：会话=%s", session, exc_info=True)
        return await context.send_message(session, normal_chain)

    async def _get_platform_self_id(self, platform: Any) -> str:
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
