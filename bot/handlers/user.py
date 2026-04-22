from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, Message

from bot.runtime import get_app
from bot.services.navigation import go_back, go_main_menu, move_to_node
from bot.services.renderer import render_node, send_rendered_node

router = Router()


def is_admin(message: Message, admin_ids: set[int]) -> bool:
    return bool(message.from_user and message.from_user.id in admin_ids)


async def _sync_admin_edit_node_if_admin(user_id: int, node_id: str) -> None:
    app = get_app()
    if user_id not in app.config.admin_ids:
        return
    admin_state = await app.storage.load_admin_state(user_id)
    admin_state["current_edit_node"] = node_id
    await app.storage.save_admin_state(user_id, admin_state)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    app = get_app()
    await app.storage.ensure_files()
    payload = await app.storage.load_nodes_payload()
    user_id = message.from_user.id
    state = {"current_node": "start", "history": []}
    text, keyboard, resolved = await render_node(payload, "start")
    state["current_node"] = resolved
    await app.storage.save_user_state(user_id, state)
    await _sync_admin_edit_node_if_admin(user_id, resolved)
    await send_rendered_node(message, payload["nodes"][resolved], text, keyboard)


@router.callback_query(F.data == "s:back")
async def cb_back(callback: CallbackQuery) -> None:
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    user_id = callback.from_user.id
    user_state = await app.storage.load_user_state(user_id)
    user_state = go_back(user_state, metadata=payload["metadata"], nodes=payload["nodes"])
    text, keyboard, resolved = await render_node(payload, user_state["current_node"])
    user_state["current_node"] = resolved
    await app.storage.save_user_state(user_id, user_state)
    await _sync_admin_edit_node_if_admin(user_id, resolved)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_rendered_node(callback.message, payload["nodes"][resolved], text, keyboard)
    await callback.answer()


@router.callback_query(F.data == "s:menu")
async def cb_menu(callback: CallbackQuery) -> None:
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    user_id = callback.from_user.id
    user_state = await app.storage.load_user_state(user_id)
    current = payload["nodes"].get(user_state.get("current_node")) or {}
    user_state = go_main_menu(user_state, node=current, metadata=payload["metadata"], nodes=payload["nodes"])
    text, keyboard, resolved = await render_node(payload, user_state["current_node"])
    user_state["current_node"] = resolved
    await app.storage.save_user_state(user_id, user_state)
    await _sync_admin_edit_node_if_admin(user_id, resolved)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_rendered_node(callback.message, payload["nodes"][resolved], text, keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("b:"))
async def cb_button(callback: CallbackQuery) -> None:
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    user_id = callback.from_user.id
    user_state = await app.storage.load_user_state(user_id)
    current_id = user_state.get("current_node", "start")
    current_node = payload["nodes"].get(current_id) or payload["nodes"]["__error__"]
    btn_id = callback.data.split(":", 1)[1]
    button = next((b for b in current_node.get("buttons", []) if b.get("id") == btn_id), None)
    if not button:
        text, keyboard, resolved = await render_node(payload, "__error__")
        user_state["current_node"] = resolved
        await app.storage.save_user_state(user_id, user_state)
        await _sync_admin_edit_node_if_admin(user_id, resolved)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await send_rendered_node(callback.message, payload["nodes"][resolved], text, keyboard)
        await callback.answer()
        return
    b_type = button.get("type")
    if b_type == "reply":
        await callback.message.answer(str(button.get("target", "")), parse_mode="HTML")
        await callback.answer()
        return
    if b_type == "url":
        await callback.answer("Откройте ссылку через кнопку URL")
        return
    target = str(button.get("target", "__error__"))
    user_state = move_to_node(
        user_state,
        target,
        metadata=payload["metadata"],
        nodes=payload["nodes"],
        max_history=app.config.max_history,
    )
    text, keyboard, resolved = await render_node(payload, user_state["current_node"])
    user_state["current_node"] = resolved
    await app.storage.save_user_state(user_id, user_state)
    await _sync_admin_edit_node_if_admin(user_id, resolved)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_rendered_node(callback.message, payload["nodes"][resolved], text, keyboard)
    await callback.answer()
