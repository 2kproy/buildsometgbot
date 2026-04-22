from __future__ import annotations

import html
from typing import Any

from aiogram.types import InlineKeyboardMarkup, Message

from bot.keyboards.user import build_user_keyboard
from bot.services.node_resolver import resolve_node_id


async def render_node(payload: dict[str, Any], node_id: str) -> tuple[str, InlineKeyboardMarkup, str]:
    metadata = payload["metadata"]
    nodes = payload["nodes"]
    resolved_id = resolve_node_id(node_id, metadata, nodes)
    node = nodes[resolved_id]
    settings = node.get("settings", {})
    text = node.get("text", "")
    keyboard = build_user_keyboard(
        node.get("buttons", []),
        show_back=bool(settings.get("show_back", True)),
        show_main=bool(settings.get("show_main_menu", True)),
    )
    return text, keyboard, resolved_id


async def send_rendered_node(message: Message, node: dict[str, Any], text: str, keyboard: InlineKeyboardMarkup) -> Message:
    media = node.get("media")
    if not isinstance(media, dict) or not media.get("file_id") or not media.get("type"):
        return await message.answer(text, reply_markup=keyboard)

    media_type = media["type"]
    file_id = media["file_id"]
    if media_type == "photo":
        return await message.answer_photo(file_id, caption=text, reply_markup=keyboard)
    if media_type == "video":
        return await message.answer_video(file_id, caption=text, reply_markup=keyboard)
    if media_type == "document":
        return await message.answer_document(file_id, caption=text, reply_markup=keyboard)
    if media_type == "animation":
        return await message.answer_animation(file_id, caption=text, reply_markup=keyboard)
    if media_type == "audio":
        return await message.answer_audio(file_id, caption=text, reply_markup=keyboard)
    if media_type == "voice":
        await message.answer_voice(file_id)
        return await message.answer(text, reply_markup=keyboard)
    return await message.answer(text, reply_markup=keyboard)


def render_admin_node(node_id: str, node: dict[str, Any]) -> str:
    lines = [f"Текущая нода: <code>{html.escape(str(node_id))}</code>", "", "Text:", html.escape(str(node.get("text", ""))), "", "Buttons:"]
    buttons = node.get("buttons", [])
    if not buttons:
        lines.append("— нет кнопок")
    else:
        for idx, btn in enumerate(buttons, start=1):
            btn_id = f"<code>{html.escape(str(btn.get('id')))}</code>"
            btn_text = html.escape(str(btn.get("text", "")))
            btn_type = html.escape(str(btn.get("type", "")))
            row = html.escape(str(btn.get("row", "")))
            sort = html.escape(str(btn.get("sort", "")))
            target_raw = str(btn.get("target", ""))
            if btn.get("type") == "node":
                target = f"<code>{html.escape(target_raw)}</code>"
            else:
                target = html.escape(target_raw)
            lines.append(
                f"{idx}. id={btn_id} | {btn_text} -> {target} [type={btn_type}, row={row}, sort={sort}]"
            )
    settings = node.get("settings", {})
    menu_target = settings.get("main_menu_target", "start")
    menu_target_fmt = f"<code>{html.escape(str(menu_target))}</code>"
    media = node.get("media")
    if isinstance(media, dict) and media.get("file_id"):
        media_line = f"{html.escape(str(media.get('type')))} | <code>{html.escape(str(media.get('file_id')))}</code>"
    else:
        media_line = "нет"
    lines.extend(
        [
            "",
            f"Media: {media_line}",
            "",
            "Settings:",
            f"- Back: {'ON' if settings.get('show_back', True) else 'OFF'}",
            f"- Main Menu: {'ON' if settings.get('show_main_menu', True) else 'OFF'}",
            f"- Main Menu Target: {menu_target_fmt}",
        ]
    )
    return "\n".join(lines)
