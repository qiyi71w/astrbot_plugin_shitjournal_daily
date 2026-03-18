from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult

from .message_sink import EventMessageSink, MessageSink, OneBotPlatformResolver, SessionMessageSink
from .push_chain_builder import ONEBOT_ADAPTER_NAME, PushChainBuilder
from .session_message import is_group_message_session, is_private_message_session


@dataclass(frozen=True)
class PushPayload:
    text: str
    png_file: Path
    pdf_file: Path
    pdf_url: str


class PushMessageService:
    def __init__(self, cfg_bool_getter: Callable[[str, bool], bool]):
        self._cfg_bool = cfg_bool_getter
        self._chains = PushChainBuilder(cfg_bool_getter=cfg_bool_getter)
        self._platforms = OneBotPlatformResolver(cfg_bool_getter=cfg_bool_getter)

    def build_standard_chain(
        self,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> MessageEventResult:
        payload = PushPayload(text=text, png_file=png_file, pdf_file=pdf_file, pdf_url=pdf_url)
        return self._chains.build_standard_chain(
            adapter_name=adapter_name,
            text=payload.text,
            png_file=payload.png_file,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
        )

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
        payload = PushPayload(text=text, png_file=png_file, pdf_file=pdf_file, pdf_url=pdf_url)
        return self._chains.build_merge_forward_chain(
            adapter_name=adapter_name,
            text=payload.text,
            png_file=payload.png_file,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
            sender_uin=sender_uin,
            include_pdf=include_pdf,
        )

    async def send_event_push(
        self,
        event: AstrMessageEvent,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
    ) -> None:
        adapter_name = str(event.get_platform_name()).strip()
        payload = PushPayload(text=text, png_file=png_file, pdf_file=pdf_file, pdf_url=pdf_url)
        sink = EventMessageSink(event=event)
        if not self._should_try_merge_forward_for_event(event):
            await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
            return
        sender_uin = str(event.get_self_id()).strip()
        if not sender_uin:
            logger.warning("事件发送无法获取机器人自身 ID，回退普通消息。")
            await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
            return
        embed_pdf_in_merge = self._should_embed_pdf_in_event_forward(event)
        merge_chain = self._chains.build_merge_forward_chain(
            adapter_name=adapter_name,
            text=payload.text,
            png_file=payload.png_file,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
            sender_uin=sender_uin,
            include_pdf=embed_pdf_in_merge,
        )
        await self._send_event_with_fallback(
            sink=sink,
            adapter_name=adapter_name,
            payload=payload,
            merge_chain=merge_chain,
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
        adapter_name = self._platforms.resolve_platform_name(context, session)
        payload = PushPayload(text=text, png_file=png_file, pdf_file=pdf_file, pdf_url=pdf_url)
        sink = SessionMessageSink(context=context, session=session)
        platform = self._platforms.resolve_merge_forward_platform(context, session)
        if platform is None:
            return await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
        sender_uin = await self._platforms.get_platform_self_id(platform)
        if not sender_uin:
            logger.warning("主动发送无法获取机器人自身 ID，回退普通消息：会话=%s", session)
            return await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
        embed_pdf_in_merge = self._should_embed_pdf_in_session_forward(session)
        merge_chain = self._chains.build_merge_forward_chain(
            adapter_name=adapter_name,
            text=payload.text,
            png_file=payload.png_file,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
            sender_uin=sender_uin,
            include_pdf=embed_pdf_in_merge,
        )
        return await self._send_session_with_fallback(
            sink=sink,
            session=session,
            adapter_name=adapter_name,
            payload=payload,
            merge_chain=merge_chain,
            send_pdf_tail=not embed_pdf_in_merge,
        )

    async def _send_event_with_fallback(
        self,
        *,
        sink: MessageSink,
        adapter_name: str,
        payload: PushPayload,
        merge_chain: MessageEventResult,
        send_pdf_tail: bool,
    ) -> None:
        try:
            await sink.send(merge_chain)
        except Exception:
            logger.warning("合并转发发送失败，回退普通消息。", exc_info=True)
            await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
            return
        if send_pdf_tail:
            await self._send_pdf_tail(
                sink=sink,
                adapter_name=adapter_name,
                payload=payload,
                exception_message="合并转发尾部 PDF 发送异常，但主体消息已发送成功。",
                failed_message=None,
            )

    async def _send_session_with_fallback(
        self,
        *,
        sink: MessageSink,
        session: str,
        adapter_name: str,
        payload: PushPayload,
        merge_chain: MessageEventResult,
        send_pdf_tail: bool,
    ) -> bool:
        try:
            merge_ok = await sink.send(merge_chain)
            if not merge_ok:
                logger.warning("合并转发发送返回失败，回退普通消息：会话=%s", session)
                return await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
        except Exception:
            logger.warning("合并转发发送异常，回退普通消息：会话=%s", session, exc_info=True)
            return await self._send_standard(sink=sink, adapter_name=adapter_name, payload=payload)
        if send_pdf_tail:
            return await self._send_pdf_tail(
                sink=sink,
                adapter_name=adapter_name,
                payload=payload,
                exception_message=f"合并转发尾部 PDF 发送异常，但主体消息已发送成功：会话={session}",
                failed_message=f"合并转发尾部 PDF 发送失败，但主体消息已发送成功：会话={session}",
            )
        return True

    async def _send_standard(
        self,
        *,
        sink: MessageSink,
        adapter_name: str,
        payload: PushPayload,
    ) -> bool:
        sent_ok = True
        for chain in self._chains.build_standard_chains(
            adapter_name=adapter_name,
            text=payload.text,
            png_file=payload.png_file,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
        ):
            if not await sink.send(chain):
                sent_ok = False
        return sent_ok

    async def _send_pdf_tail(
        self,
        *,
        sink: MessageSink,
        adapter_name: str,
        payload: PushPayload,
        exception_message: str,
        failed_message: str | None,
    ) -> bool:
        chain = self._chains.build_pdf_only_chain(
            adapter_name=adapter_name,
            pdf_file=payload.pdf_file,
            pdf_url=payload.pdf_url,
        )
        if chain is None:
            return True
        try:
            tail_ok = await sink.send(chain)
        except Exception:
            logger.warning(exception_message, exc_info=True)
            return False
        if not tail_ok:
            if failed_message:
                logger.warning(failed_message)
            return False
        return True

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
