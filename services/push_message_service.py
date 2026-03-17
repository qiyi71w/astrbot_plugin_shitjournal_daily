from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult
from astrbot.api.message_components import File, Image, Node, Nodes, Plain
from astrbot.core.platform.message_session import MessageSesion
from astrbot.core.platform.message_type import MessageType

from .session_message import is_group_message_session, is_private_message_session


FORWARD_SENDER_NAME = "S.H.I.T Journal"
ONEBOT_ADAPTER_NAME = "aiocqhttp"
SATORI_ADAPTER_NAME = "satori"
LARK_ADAPTER_NAME = "lark"
LOCAL_PDF_FILE_ADAPTERS = frozenset({
    SATORI_ADAPTER_NAME,
    LARK_ADAPTER_NAME,
})
ONEBOT_MERGE_FORWARD_MESSAGE_TYPES = frozenset({
    MessageType.GROUP_MESSAGE,
    MessageType.FRIEND_MESSAGE,
})


class PushMessageService:
    def __init__(self, cfg_bool_getter: Callable[[str, bool], bool]):
        self._cfg_bool = cfg_bool_getter
        self._self_id_cache: dict[str, str] = {}

    def build_standard_chain(
        self,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.extend(
            self._build_push_components(
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            ),
        )
        return chain

    def build_merge_forward_chain(
        self,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        sender_uin: str,
        *,
        include_pdf: bool = False,
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
        if include_pdf:
            file_component = self._build_pdf_component(
                adapter_name=adapter_name,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
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
        adapter_name = str(event.get_platform_name()).strip()
        if not self._should_try_merge_forward_for_event(event):
            await self._send_standard_event_push(
                event=event,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
            return

        sender_uin = str(event.get_self_id()).strip()
        if not sender_uin:
            logger.warning("事件发送无法获取机器人自身 ID，回退普通消息。")
            await self._send_standard_event_push(
                event=event,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
            return

        embed_pdf_in_merge = self._should_embed_pdf_in_event_forward(event)
        merge_chain = self.build_merge_forward_chain(
            adapter_name=adapter_name,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            sender_uin=sender_uin,
            include_pdf=embed_pdf_in_merge,
        )
        await self._send_event_with_fallback(
            event=event,
            adapter_name=adapter_name,
            merge_chain=merge_chain,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            send_pdf_tail=not embed_pdf_in_merge,
        )

    async def send_session_push(
        self,
        context: Any,
        session: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> bool:
        adapter_name = self._resolve_platform_name(context, session)
        platform = self._resolve_merge_forward_platform(context, session)
        if platform is None:
            return await self._send_standard_session_push(
                context=context,
                session=session,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )

        sender_uin = await self._get_platform_self_id(platform)
        if not sender_uin:
            logger.warning("主动发送无法获取机器人自身 ID，回退普通消息：会话=%s", session)
            return await self._send_standard_session_push(
                context=context,
                session=session,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )

        embed_pdf_in_merge = self._should_embed_pdf_in_session_forward(session)
        merge_chain = self.build_merge_forward_chain(
            adapter_name=adapter_name,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            sender_uin=sender_uin,
            include_pdf=embed_pdf_in_merge,
        )
        return await self._send_session_with_fallback(
            context=context,
            session=session,
            adapter_name=adapter_name,
            merge_chain=merge_chain,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            send_pdf_tail=not embed_pdf_in_merge,
        )

    def _build_push_components(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> list[Any]:
        components: list[Any] = [
            Plain(text),
            Image.fromFileSystem(str(png_file)),
        ]
        file_component = self._build_pdf_component(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        if file_component is not None:
            components.append(file_component)
        return components

    def _build_pdf_component(
        self,
        *,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
    ) -> File | None:
        if not self._cfg_bool("send_pdf", False):
            return None
        if self._should_use_pdf_url(adapter_name) and pdf_url:
            return File(name=pdf_file.name, url=pdf_url)
        return File(name=pdf_file.name, file=str(pdf_file))

    def _should_use_pdf_url(self, adapter_name: str) -> bool:
        normalized_adapter_name = str(adapter_name).strip()
        return normalized_adapter_name not in LOCAL_PDF_FILE_ADAPTERS

    def _should_try_merge_forward_for_event(self, event: AstrMessageEvent) -> bool:
        if not self._cfg_bool("send_merge_forward", False):
            return False
        if str(event.get_platform_name()).strip() != ONEBOT_ADAPTER_NAME:
            return False
        return bool(event.get_group_id()) or self._is_onebot_private_event(event)

    def _should_embed_pdf_in_event_forward(self, event: AstrMessageEvent) -> bool:
        return bool(event.get_group_id())

    def _should_embed_pdf_in_session_forward(self, session: str) -> bool:
        return is_group_message_session(session)

    def _is_onebot_private_event(self, event: AstrMessageEvent) -> bool:
        session = str(getattr(event, "unified_msg_origin", "")).strip()
        return is_private_message_session(session)

    def _resolve_merge_forward_platform(self, context: Any, session: str) -> Any | None:
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

    def _resolve_platform_name(self, context: Any, session: str) -> str:
        try:
            message_session = MessageSesion.from_str(session)
        except Exception:
            return ""
        platform = self._find_platform_by_id(context, message_session.platform_name)
        if platform is None:
            return ""
        return str(platform.meta().name)

    async def _send_event_with_fallback(
        self,
        event: AstrMessageEvent,
        adapter_name: str,
        merge_chain: MessageEventResult,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        send_pdf_tail: bool,
    ) -> None:
        try:
            await event.send(merge_chain)
        except Exception:
            logger.warning("合并转发发送失败，回退普通消息。", exc_info=True)
            await self._send_standard_event_push(
                event=event,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
            return
        if send_pdf_tail:
            await self._send_merge_pdf_tail_for_event(
                event=event,
                adapter_name=adapter_name,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )

    async def _send_session_with_fallback(
        self,
        *,
        context: Any,
        session: str,
        adapter_name: str,
        merge_chain: MessageEventResult,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        send_pdf_tail: bool,
    ) -> bool:
        try:
            merge_ok = await context.send_message(session, merge_chain)
            if not merge_ok:
                logger.warning("合并转发发送返回失败，回退普通消息：会话=%s", session)
                return await self._send_standard_session_push(
                    context=context,
                    session=session,
                    adapter_name=adapter_name,
                    text=text,
                    png_file=png_file,
                    pdf_file=pdf_file,
                    pdf_url=pdf_url,
                )
        except Exception:
            logger.warning("合并转发发送异常，回退普通消息：会话=%s", session, exc_info=True)
            return await self._send_standard_session_push(
                context=context,
                session=session,
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
        if not send_pdf_tail:
            return True
        await self._send_merge_pdf_tail_for_session(
            context=context,
            session=session,
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        return True

    async def _send_merge_pdf_tail_for_event(
        self,
        *,
        event: AstrMessageEvent,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
    ) -> bool:
        file_chain = self._build_pdf_only_chain(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        if file_chain is None:
            return True
        try:
            await event.send(file_chain)
            return True
        except Exception:
            logger.warning("合并转发尾部 PDF 发送异常，但主体消息已发送成功。", exc_info=True)
            return False

    async def _send_merge_pdf_tail_for_session(
        self,
        *,
        context: Any,
        session: str,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
    ) -> bool:
        file_chain = self._build_pdf_only_chain(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        if file_chain is None:
            return True
        try:
            tail_ok = await context.send_message(session, file_chain)
        except Exception:
            logger.warning("合并转发尾部 PDF 发送异常，但主体消息已发送成功：会话=%s", session, exc_info=True)
            return False
        if not tail_ok:
            logger.warning("合并转发尾部 PDF 发送失败，但主体消息已发送成功：会话=%s", session)
            return False
        return True

    async def _send_standard_event_push(
        self,
        *,
        event: AstrMessageEvent,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> None:
        for chain in self._build_standard_chains(
            adapter_name=adapter_name,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        ):
            await event.send(chain)

    async def _send_standard_session_push(
        self,
        *,
        context: Any,
        session: str,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> bool:
        sent_ok = True
        for chain in self._build_standard_chains(
            adapter_name=adapter_name,
            text=text,
            png_file=png_file,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        ):
            if not await context.send_message(session, chain):
                sent_ok = False
        return sent_ok

    def _build_standard_chains(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> list[MessageEventResult]:
        if self._should_split_standard_file_send(adapter_name):
            chains = [self._build_text_image_chain(text, png_file)]
            file_chain = self._build_pdf_only_chain(
                adapter_name=adapter_name,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            )
            if file_chain is not None:
                chains.append(file_chain)
            return chains
        return [
            self.build_standard_chain(
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
            ),
        ]

    def _build_text_image_chain(self, text: str, png_file: Path) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.extend([
            Plain(text),
            Image.fromFileSystem(str(png_file)),
        ])
        return chain

    def _build_pdf_only_chain(
        self,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
    ) -> MessageEventResult | None:
        file_component = self._build_pdf_component(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
        )
        if file_component is None:
            return None
        chain = MessageEventResult()
        chain.chain.append(file_component)
        return chain

    def _should_split_standard_file_send(self, adapter_name: str) -> bool:
        return adapter_name == ONEBOT_ADAPTER_NAME and self._cfg_bool("send_pdf", False)

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
