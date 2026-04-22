from __future__ import annotations

from typing import Literal, TypedDict


ButtonType = Literal["node", "url", "reply"]
MediaType = Literal["photo", "video", "document", "animation", "audio", "voice"]


class NodeSettings(TypedDict, total=False):
    show_back: bool
    show_main_menu: bool
    main_menu_target: str


class Button(TypedDict):
    id: str
    text: str
    type: ButtonType
    target: str
    row: int
    sort: int


class NodeMedia(TypedDict):
    type: MediaType
    file_id: str


class Node(TypedDict):
    id: str
    text: str
    buttons: list[Button]
    settings: NodeSettings
    media: NodeMedia | None
