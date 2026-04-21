from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from astrbot.api.event import MessageEventResult
from astrbot.api.message_components import File, Image, Node, Nodes, Plain
from astrbot.core.platform.message_type import MessageType


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


@dataclass(frozen=True)
class ArticlePushEntry:
    text: str
    png_file: Path
    pdf_file: Path
    pdf_url: str
    pdf_send_file: str = ""


@dataclass(frozen=True)
class QuestionPushEntry:
    text: str


class PushChainBuilder:
    def __init__(self, cfg_bool_getter: Callable[[str, bool], bool]):
        self._cfg_bool = cfg_bool_getter

    def build_standard_chain(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
    ) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.extend(
            self._build_standard_components(
                adapter_name=adapter_name,
                text=text,
                png_file=png_file,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
                pdf_send_file=pdf_send_file,
            ),
        )
        return chain

    def build_standard_chains(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
    ) -> list[MessageEventResult]:
        if not self._should_split_standard_file_send(adapter_name):
            return [
                self.build_standard_chain(
                    adapter_name=adapter_name,
                    text=text,
                    png_file=png_file,
                    pdf_file=pdf_file,
                    pdf_url=pdf_url,
                    pdf_send_file=pdf_send_file,
                ),
            ]
        chains = [self._build_text_image_chain(text=text, png_file=png_file)]
        pdf_chain = self.build_pdf_only_chain(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            pdf_send_file=pdf_send_file,
        )
        if pdf_chain is not None:
            chains.append(pdf_chain)
        return chains

    def build_merge_forward_chain(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
        sender_uin: str,
        include_pdf: bool,
    ) -> MessageEventResult:
        nodes = [
            Node(
                name=FORWARD_SENDER_NAME,
                uin=str(sender_uin),
                content=[Plain(text), Image.fromFileSystem(str(png_file))],
            ),
        ]
        if include_pdf:
            pdf_component = self._build_pdf_component(
                adapter_name=adapter_name,
                pdf_file=pdf_file,
                pdf_url=pdf_url,
                pdf_send_file=pdf_send_file,
            )
            if pdf_component is not None:
                nodes.append(
                    Node(
                        name=FORWARD_SENDER_NAME,
                        uin=str(sender_uin),
                        content=[pdf_component],
                    ),
                )
        chain = MessageEventResult()
        chain.chain.append(Nodes(nodes=nodes))
        return chain

    def build_text_only_chain(self, *, text: str) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.append(Plain(text))
        return chain

    def build_text_only_merge_forward_chain(
        self,
        *,
        text: str,
        sender_uin: str,
    ) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.append(
            Nodes(
                nodes=[
                    Node(
                        name=FORWARD_SENDER_NAME,
                        uin=str(sender_uin),
                        content=[Plain(text)],
                    ),
                ],
            ),
        )
        return chain

    def build_mixed_standard_main_chain(
        self,
        *,
        article_entries: list[ArticlePushEntry],
        question_entries: list[QuestionPushEntry],
    ) -> MessageEventResult:
        chain = MessageEventResult()
        for entry in article_entries:
            chain.chain.extend([Plain(entry.text), Image.fromFileSystem(str(entry.png_file))])
        for entry in question_entries:
            chain.chain.append(Plain(entry.text))
        return chain

    def build_mixed_pdf_tail_chains(
        self,
        *,
        adapter_name: str,
        article_entries: list[ArticlePushEntry],
    ) -> list[MessageEventResult]:
        tails: list[MessageEventResult] = []
        for entry in article_entries:
            chain = self.build_pdf_only_chain(
                adapter_name=adapter_name,
                pdf_file=entry.pdf_file,
                pdf_url=entry.pdf_url,
                pdf_send_file=entry.pdf_send_file,
            )
            if chain is not None:
                tails.append(chain)
        return tails

    def build_mixed_merge_forward_chain(
        self,
        *,
        adapter_name: str,
        article_entries: list[ArticlePushEntry],
        question_entries: list[QuestionPushEntry],
        sender_uin: str,
        include_pdf: bool,
    ) -> MessageEventResult:
        nodes: list[Node] = []
        for entry in article_entries:
            nodes.append(
                Node(
                    name=FORWARD_SENDER_NAME,
                    uin=str(sender_uin),
                    content=[Plain(entry.text), Image.fromFileSystem(str(entry.png_file))],
                ),
            )
            if include_pdf:
                pdf_component = self._build_pdf_component(
                    adapter_name=adapter_name,
                    pdf_file=entry.pdf_file,
                    pdf_url=entry.pdf_url,
                    pdf_send_file=entry.pdf_send_file,
                )
                if pdf_component is not None:
                    nodes.append(
                        Node(
                            name=FORWARD_SENDER_NAME,
                            uin=str(sender_uin),
                            content=[pdf_component],
                        ),
                    )
        for entry in question_entries:
            nodes.append(
                Node(
                    name=FORWARD_SENDER_NAME,
                    uin=str(sender_uin),
                    content=[Plain(entry.text)],
                ),
            )
        chain = MessageEventResult()
        chain.chain.append(Nodes(nodes=nodes))
        return chain

    def build_pdf_only_chain(
        self,
        *,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
    ) -> MessageEventResult | None:
        pdf_component = self._build_pdf_component(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            pdf_send_file=pdf_send_file,
        )
        if pdf_component is None:
            return None
        chain = MessageEventResult()
        chain.chain.append(pdf_component)
        return chain

    def _build_standard_components(
        self,
        *,
        adapter_name: str,
        text: str,
        png_file: Path,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
    ) -> list[Any]:
        components: list[Any] = [Plain(text), Image.fromFileSystem(str(png_file))]
        pdf_component = self._build_pdf_component(
            adapter_name=adapter_name,
            pdf_file=pdf_file,
            pdf_url=pdf_url,
            pdf_send_file=pdf_send_file,
        )
        if pdf_component is not None:
            components.append(pdf_component)
        return components

    def _build_pdf_component(
        self,
        *,
        adapter_name: str,
        pdf_file: Path,
        pdf_url: str,
        pdf_send_file: str = "",
    ) -> File | None:
        if not self._cfg_bool("send_pdf", False):
            return None
        if pdf_send_file:
            return File(name=pdf_file.name, file=pdf_send_file)
        if self._should_use_pdf_url(adapter_name) and pdf_url:
            return File(name=pdf_file.name, url=pdf_url)
        return File(name=pdf_file.name, file=str(pdf_file))

    def _build_text_image_chain(self, *, text: str, png_file: Path) -> MessageEventResult:
        chain = MessageEventResult()
        chain.chain.extend([Plain(text), Image.fromFileSystem(str(png_file))])
        return chain

    def _should_use_pdf_url(self, adapter_name: str) -> bool:
        return str(adapter_name).strip() not in LOCAL_PDF_FILE_ADAPTERS

    def _should_split_standard_file_send(self, adapter_name: str) -> bool:
        return adapter_name == ONEBOT_ADAPTER_NAME and self._cfg_bool("send_pdf", False)
