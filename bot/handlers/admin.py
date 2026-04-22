from __future__ import annotations

import html
import json
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.keyboards.admin import admin_node_actions_keyboard, admin_panel_keyboard, confirm_delete_keyboard
from bot.runtime import get_app
from bot.services.fixer import auto_fix_broken_links, ensure_error_node
from bot.services.graph_refs import find_incoming_refs
from bot.services.id_compactor import compact_payload_ids, remap_admin_state, remap_user_state
from bot.services.importer import import_crawler_graph
from bot.services.renderer import render_admin_node, render_node, send_rendered_node
from bot.services.tree_builder import build_tree_view
from bot.services.validator import validate_graph
from bot.states.admin import AdminStates
from bot.utils.formatting import format_validation
from bot.utils.ids import short_button_id
from bot.utils.mermaid import build_mermaid_subtree

router = Router()


def _is_admin(message: Message) -> bool:
    app = get_app()
    return bool(message.from_user and message.from_user.id in app.config.admin_ids)


def _code(value: str) -> str:
    return f"<code>{html.escape(str(value))}</code>"


async def _get_admin_state(user_id: int) -> dict:
    app = get_app()
    state = await app.storage.load_admin_state(user_id)
    state.setdefault("current_edit_node", "start")
    state.setdefault("mode", "idle")
    return state


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.clear()
    admin_state = await _get_admin_state(message.from_user.id)
    admin_state["mode"] = "idle"
    await get_app().storage.save_admin_state(message.from_user.id, admin_state)
    await message.answer("Операция отменена.")


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.clear()
    await message.answer("Админ-панель", reply_markup=admin_panel_keyboard())


@router.message(Command("open"))
async def cmd_open(message: Message, command: CommandObject, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.clear()
    node_id = (command.args or "").strip()
    if not node_id:
        await message.answer("Usage: /open <node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    if node_id == "start":
        node_id = payload["metadata"].get("real_root_id", "start")
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        return
    admin_state = await _get_admin_state(message.from_user.id)
    admin_state["current_edit_node"] = node_id
    await app.storage.save_admin_state(message.from_user.id, admin_state)
    await message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())


@router.message(Command("current"))
async def cmd_current(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state.get("current_edit_node", "start")
    if node_id == "start":
        user_state = await app.storage.load_user_state(message.from_user.id)
        node_id = user_state.get("current_node", "start")
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Текущая нода отсутствует")
        return
    await message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())


@router.message(Command("list"))
async def cmd_list(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    node_ids = sorted(payload["nodes"].keys())
    lines = ["Ноды:"]
    lines.extend(f"- {nid}" for nid in node_ids[:80])
    await message.answer("\n".join(lines))


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    term = (command.args or "").lower().strip()
    if not term:
        await message.answer("Usage: /search <text>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    matched = []
    for node_id, node in payload["nodes"].items():
        if term in node_id.lower() or term in str(node.get("text", "")).lower():
            matched.append(node_id)
            continue
        for btn in node.get("buttons", []):
            if term in str(btn.get("text", "")).lower():
                matched.append(node_id)
                break
    await message.answer("Результаты:\n" + ("\n".join(f"- {x}" for x in matched[:80]) if matched else "ничего не найдено"))


@router.message(Command("create"))
async def cmd_create(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.set_state(AdminStates.creating_node_id)
    await message.answer("Введите ID новой ноды:")


@router.message(AdminStates.creating_node_id)
async def fsm_create_id(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    node_id = message.text.strip()
    await state.update_data(new_node_id=node_id)
    await state.set_state(AdminStates.creating_node_text)
    await message.answer("Введите текст ноды:")


@router.message(AdminStates.creating_node_text)
async def fsm_create_text(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    data = await state.get_data()
    node_id = data["new_node_id"]
    text = message.text or ""
    payload = await app.storage.load_nodes_payload()
    if node_id in payload["nodes"]:
        await message.answer("Нода уже существует.")
        await state.clear()
        return
    payload["nodes"][node_id] = {
        "id": node_id,
        "text": text,
        "buttons": [],
        "media": None,
        "settings": {"show_back": True, "show_main_menu": True, "main_menu_target": "start"},
    }
    ensure_error_node(payload["nodes"])
    await app.storage.save_nodes_payload(payload)
    admin_state = await _get_admin_state(message.from_user.id)
    admin_state["current_edit_node"] = node_id
    await app.storage.save_admin_state(message.from_user.id, admin_state)
    await state.clear()
    await message.answer(f"Нода {_code(node_id)} создана.\nТекущая нода: {_code(node_id)}")


@router.message(Command("new"))
async def cmd_new_alias(message: Message, state: FSMContext) -> None:
    await cmd_create(message, state)


@router.message(Command("edit"))
async def cmd_edit(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.set_state(AdminStates.editing_text)
    await message.answer("Введите новый текст для текущей ноды:")


@router.message(AdminStates.editing_text)
async def fsm_edit_text(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state["current_edit_node"]
    payload = await app.storage.load_nodes_payload()
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена.")
        await state.clear()
        return
    node["text"] = message.text or ""
    await app.storage.save_nodes_payload(payload)
    await state.clear()
    await message.answer(f"Сохранено.\nТекущая нода: {_code(node_id)}")


@router.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.set_state(AdminStates.adding_type)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="node", callback_data="addt:node"),
                InlineKeyboardButton(text="url", callback_data="addt:url"),
                InlineKeyboardButton(text="reply", callback_data="addt:reply"),
            ]
        ]
    )
    await message.answer("Выберите тип кнопки:", reply_markup=kb)


@router.callback_query(F.data.startswith("addt:"))
async def cb_add_type(callback, state: FSMContext) -> None:
    app = get_app()
    if callback.from_user.id not in app.config.admin_ids:
        return
    b_type = callback.data.split(":")[1]
    await state.update_data(add_type=b_type)
    await state.set_state(AdminStates.adding_text)
    await callback.message.answer("Введите текст кнопки:")
    await callback.answer()


@router.message(AdminStates.adding_text)
async def fsm_add_text(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    node_ids = sorted(payload["nodes"].keys())
    await state.update_data(add_text=message.text or "", picker_nodes=node_ids[:40])
    kb_rows = []
    for idx, node_id in enumerate(node_ids[:20]):
        kb_rows.append([InlineKeyboardButton(text=node_id[:24], callback_data=f"pick:{idx}")])
    kb_rows.append([InlineKeyboardButton(text="/new", callback_data="pick:new")])
    await state.set_state(AdminStates.adding_target)
    await message.answer("Куда ведёт кнопка? Можно выбрать или ввести вручную.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("pick:"))
async def cb_pick_target(callback, state: FSMContext) -> None:
    app = get_app()
    if callback.from_user.id not in app.config.admin_ids:
        return
    key = callback.data.split(":")[1]
    if key == "new":
        await state.clear()
        await callback.message.answer("Запущено создание ноды. спользуйте /new.")
        await callback.answer()
        return
    data = await state.get_data()
    nodes = data.get("picker_nodes", [])
    idx = int(key)
    if idx < 0 or idx >= len(nodes):
        await callback.answer("Invalid")
        return
    await state.update_data(add_target=nodes[idx])
    await state.set_state(AdminStates.adding_row)
    await callback.message.answer("Введите row (число, default 0):")
    await callback.answer()


@router.message(AdminStates.adding_target)
async def fsm_add_target_manual(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.update_data(add_target=(message.text or "").strip())
    await state.set_state(AdminStates.adding_row)
    await message.answer("Введите row (число, default 0):")


@router.message(AdminStates.adding_row)
async def fsm_add_row(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    data = await state.get_data()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state["current_edit_node"]
    payload = await app.storage.load_nodes_payload()
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        await state.clear()
        return
    row = int((message.text or "0").strip() or "0")
    b_type = data["add_type"]
    target = data["add_target"]
    if b_type == "node" and target not in payload["nodes"]:
        await message.answer("Target нода не существует. спользуйте /new или введите существующий id.")
        await state.clear()
        return
    button = {
        "id": short_button_id(),
        "text": data["add_text"],
        "type": b_type,
        "target": target,
        "row": row,
        "sort": len(node.get("buttons", [])),
    }
    node.setdefault("buttons", []).append(button)
    await app.storage.save_nodes_payload(payload)
    await state.clear()
    await message.answer(f"Кнопка добавлена.\nТекущая нода: {_code(node_id)}")


@router.message(Command("del"))
async def cmd_del_button(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state["current_edit_node"]
    payload = await app.storage.load_nodes_payload()
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        return
    arg = (command.args or "").strip()
    if not arg:
        lines = ["Usage: /del <button_id>", "Buttons:"]
        lines.extend(f"- {b['id']}: {b['text']}" for b in node.get("buttons", []))
        await message.answer("\n".join(lines))
        return
    node["buttons"] = [b for b in node.get("buttons", []) if b.get("id") != arg]
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Удалено.\nТекущая нода: {_code(node_id)}")


@router.message(Command("link"))
async def cmd_link(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    args = (command.args or "").split()
    if len(args) != 2:
        await message.answer("Usage: /link <button_id> <target_node_id>")
        return
    btn_id, target = args
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    if target not in payload["nodes"]:
        await message.answer("Target нода не существует")
        return
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    if not node:
        await message.answer("Нода не найдена")
        return
    for btn in node.get("buttons", []):
        if btn.get("id") == btn_id and btn.get("type") == "node":
            btn["target"] = target
            await app.storage.save_nodes_payload(payload)
            await message.answer(f"Связь обновлена.\nТекущая нода: {_code(admin_state['current_edit_node'])}")
            return
    await message.answer("Button не найден или не типа node")


@router.message(Command("buttonedit"))
async def cmd_buttonedit(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    args = (command.args or "").split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Usage: /buttonedit <button_id> <field:text|type|target|row|sort> <value>")
        return
    button_id, field, value = args
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    if not node:
        await message.answer("Нода не найдена")
        return
    button = next((b for b in node.get("buttons", []) if b.get("id") == button_id), None)
    if not button:
        await message.answer("Button not found")
        return
    if field not in {"text", "type", "target", "row", "sort"}:
        await message.answer("Invalid field")
        return
    if field in {"row", "sort"}:
        button[field] = int(value)
    else:
        button[field] = value
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Обновлено.\nТекущая нода: {_code(admin_state['current_edit_node'])}")


@router.message(Command("rows"))
async def cmd_rows(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    args = (command.args or "").split()
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    if not node:
        await message.answer("Нода не найдена")
        return
    if len(args) == 2:
        btn_id, row_value = args
        for btn in node.get("buttons", []):
            if btn.get("id") == btn_id:
                btn["row"] = int(row_value)
                await app.storage.save_nodes_payload(payload)
                await message.answer("Row updated")
                return
        await message.answer("Button not found")
        return
    lines = ["Usage: /rows <button_id> <row>", "Current rows:"]
    lines.extend(f"- {b['id']} row={b.get('row', 0)} sort={b.get('sort', 0)} text={b.get('text')}" for b in node.get("buttons", []))
    await message.answer("\n".join(lines))


@router.message(Command("backbtn"))
async def cmd_backbtn(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    arg = (command.args or "").strip().lower()
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    if not node:
        await message.answer("Нода не найдена")
        return
    current = bool(node["settings"].get("show_back", True))
    if arg == "on":
        node["settings"]["show_back"] = True
    elif arg == "off":
        node["settings"]["show_back"] = False
    else:
        node["settings"]["show_back"] = not current
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Back: {'ON' if node['settings']['show_back'] else 'OFF'}\nТекущая нода: {_code(admin_state['current_edit_node'])}")


@router.message(Command("menubtn"))
async def cmd_menubtn(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    arg = (command.args or "").strip().lower()
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    if not node:
        await message.answer("Нода не найдена")
        return
    current = bool(node["settings"].get("show_main_menu", True))
    if arg == "on":
        node["settings"]["show_main_menu"] = True
    elif arg == "off":
        node["settings"]["show_main_menu"] = False
    else:
        node["settings"]["show_main_menu"] = not current
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Main menu: {'ON' if node['settings']['show_main_menu'] else 'OFF'}\nТекущая нода: {_code(admin_state['current_edit_node'])}")


@router.message(Command("menutarget"))
async def cmd_menutarget(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    target = (command.args or "").strip()
    if not target:
        await message.answer("Usage: /menutarget <node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    if target != "start" and target not in payload["nodes"]:
        await message.answer("Node not found")
        return
    admin_state = await _get_admin_state(message.from_user.id)
    node = payload["nodes"].get(admin_state["current_edit_node"])
    node["settings"]["main_menu_target"] = target
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Main menu target set: {_code(target)}\nТекущая нода: {_code(admin_state['current_edit_node'])}")


@router.message(Command("rename"))
async def cmd_rename(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    new_id = (command.args or "").strip()
    if not new_id:
        await message.answer("Usage: /rename <new_node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    old_id = admin_state["current_edit_node"]
    if new_id in payload["nodes"]:
        await message.answer("ID already exists")
        return
    node = payload["nodes"].pop(old_id, None)
    if not node:
        await message.answer("Нода не найдена")
        return
    node["id"] = new_id
    payload["nodes"][new_id] = node
    for n in payload["nodes"].values():
        for btn in n.get("buttons", []):
            if btn.get("type") == "node" and btn.get("target") == old_id:
                btn["target"] = new_id
    if payload["metadata"].get("real_root_id") == old_id:
        payload["metadata"]["real_root_id"] = new_id
    admin_state["current_edit_node"] = new_id
    await app.storage.save_nodes_payload(payload)
    await app.storage.save_admin_state(message.from_user.id, admin_state)
    await message.answer(f"Renamed {old_id} -> {new_id}")


@router.message(Command("clone"))
async def cmd_clone(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    new_id = (command.args or "").strip()
    if not new_id:
        await message.answer("Usage: /clone <new_node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    src_id = admin_state["current_edit_node"]
    src = payload["nodes"].get(src_id)
    if not src:
        await message.answer("Source node not found")
        return
    if new_id in payload["nodes"]:
        await message.answer("Target ID already exists")
        return
    clone = json.loads(json.dumps(src, ensure_ascii=False))
    clone["id"] = new_id
    payload["nodes"][new_id] = clone
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Cloned {src_id} -> {new_id}")


@router.message(Command("delete_node"))
async def cmd_delete_node(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state["current_edit_node"]
    if node_id in {"__error__", payload["metadata"].get("real_root_id")}:
        await message.answer("Системную/корневую ноду удалить нельзя")
        return
    refs = find_incoming_refs(payload["nodes"], node_id)
    lines = [f"Node is referenced by {len(refs)} nodes:"]
    lines.extend(f"- {x['from_node']} / {x['button_text']}" for x in refs[:30])
    await state.set_state(AdminStates.delete_confirm)
    await state.update_data(delete_node_id=node_id)
    await message.answer("\n".join(lines), reply_markup=confirm_delete_keyboard())


@router.message(Command("preview"))
async def cmd_preview(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    node_id = (command.args or "").strip()
    if not node_id:
        await message.answer("Usage: /preview <node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    text, keyboard, resolved = await render_node(payload, node_id)
    await send_rendered_node(message, payload["nodes"][resolved], text, keyboard)


@router.message(Command("goto"))
async def cmd_goto(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    node_id = (command.args or "").strip()
    if not node_id:
        await message.answer("Usage: /goto <node_id>")
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    text, keyboard, resolved = await render_node(payload, node_id)
    await app.storage.save_user_state(message.from_user.id, {"current_node": resolved, "history": [resolved]})
    admin_state = await _get_admin_state(message.from_user.id)
    admin_state["current_edit_node"] = resolved
    await app.storage.save_admin_state(message.from_user.id, admin_state)
    await message.answer(f"Goto complete.\nТекущая нода: {_code(resolved)}")
    await send_rendered_node(message, payload["nodes"][resolved], text, keyboard)


@router.message(Command("media"))
async def cmd_media(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state.get("current_edit_node", "start")
    await state.set_state(AdminStates.attaching_media)
    await message.answer(
        f"Текущая нода: {_code(node_id)}\n"
        "Пришлите медиа сообщением: photo/video/document/animation/audio/voice.\n"
        "Или /cancel."
    )


@router.message(Command("media_clear"))
async def cmd_media_clear(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state.get("current_edit_node", "start")
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        return
    node["media"] = None
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Медиа удалено.\nТекущая нода: {_code(node_id)}")


@router.message(AdminStates.attaching_media)
async def fsm_attach_media(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    media_type = None
    file_id = None
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    elif message.video:
        media_type = "video"
        file_id = message.video.file_id
    elif message.document:
        media_type = "document"
        file_id = message.document.file_id
    elif message.animation:
        media_type = "animation"
        file_id = message.animation.file_id
    elif message.audio:
        media_type = "audio"
        file_id = message.audio.file_id
    elif message.voice:
        media_type = "voice"
        file_id = message.voice.file_id

    if not media_type or not file_id:
        await message.answer("Это не поддерживаемое медиа. Пришлите photo/video/document/animation/audio/voice или /cancel.")
        return

    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state.get("current_edit_node", "start")
    node = payload["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        await state.clear()
        return
    node["media"] = {"type": media_type, "file_id": file_id}
    await app.storage.save_nodes_payload(payload)
    await state.clear()
    await message.answer(f"Медиа прикреплено: {_code(media_type)}\nТекущая нода: {_code(node_id)}")


@router.message(Command("tree"))
async def cmd_tree(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = (command.args or "").strip() or admin_state["current_edit_node"]
    depth = int(admin_state.get("tree_depth", 2))
    await message.answer(build_tree_view(payload["nodes"], node_id, depth=depth))


@router.message(Command("tree_depth"))
async def cmd_tree_depth(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    val = int((command.args or "2").strip())
    admin_state = await _get_admin_state(message.from_user.id)
    admin_state["tree_depth"] = max(1, min(val, 8))
    await get_app().storage.save_admin_state(message.from_user.id, admin_state)
    await message.answer(f"tree depth = {admin_state['tree_depth']}")


@router.message(Command("mermaid"))
async def cmd_mermaid(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = (command.args or "").strip() or admin_state["current_edit_node"]
    depth = int(admin_state.get("tree_depth", 2))
    text = build_mermaid_subtree(payload["nodes"], node_id, depth)
    await message.answer(f"```mermaid\n{text}\n```", parse_mode="Markdown")


@router.message(Command("validate"))
async def cmd_validate(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    report = validate_graph(payload["nodes"], payload["metadata"].get("real_root_id", "start"))
    await message.answer(format_validation(report))


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    report = validate_graph(payload["nodes"], payload["metadata"].get("real_root_id", "start"))
    s = report["summary"]
    await message.answer(
        f"Stats:\n- total nodes: {s['total_nodes']}\n- total buttons: {s['total_buttons']}\n- broken links: {s['broken_links']}\n- orphan nodes: {s['orphans']}"
    )


@router.message(Command("fix"))
async def cmd_fix(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    create_placeholder = (command.args or "").strip().lower() == "placeholder"
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    result = auto_fix_broken_links(payload["nodes"], create_placeholder=create_placeholder)
    await app.storage.save_nodes_payload(payload)
    await message.answer(f"Fix done: {json.dumps(result, ensure_ascii=False)}")


@router.message(Command("compact_ids"))
async def cmd_compact_ids(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    compacted = compact_payload_ids(payload)
    node_map = compacted.get("id_compact_map", {})
    await app.storage.save_nodes_payload({"metadata": compacted["metadata"], "nodes": compacted["nodes"]})

    user_state = await app.storage.load_user_state_all()
    admin_state = await app.storage.load_admin_state_all()
    user_state = remap_user_state(user_state, node_map)
    admin_state = remap_admin_state(admin_state, node_map)
    await app.storage.save_user_state_all(user_state)
    await app.storage.save_admin_state_all(admin_state)
    await message.answer(f"ID compact complete. Nodes remapped: {len(node_map)}")


@router.message(Command("broadcast_new"))
async def cmd_broadcast_new(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    name = (command.args or "").strip() or f"broadcast_{int(datetime.now(tz=timezone.utc).timestamp())}"
    admin_state = await _get_admin_state(message.from_user.id)
    node_id = admin_state.get("current_edit_node", "start")
    payload_nodes = await app.storage.load_nodes_payload()
    node = payload_nodes["nodes"].get(node_id)
    if not node:
        await message.answer("Нода не найдена")
        return
    payload = {
        "text": node.get("text", ""),
        "media": node.get("media"),
        "buttons": [b for b in node.get("buttons", []) if b.get("type") == "url"],
        "source_node_id": node_id,
    }
    broadcast_id = await app.storage.create_broadcast(name, payload, message.from_user.id)
    await message.answer(f"Создана рассылка id={_code(str(broadcast_id))} из ноды {_code(node_id)}")


@router.message(Command("broadcast_list"))
async def cmd_broadcast_list(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    rows = await app.storage.list_broadcasts(limit=30)
    if not rows:
        await message.answer("Рассылок нет.")
        return
    lines = ["Рассылки:"]
    for r in rows:
        lines.append(f"- id={_code(str(r['id']))} | {r['name']} | {r['status']} | {r.get('scheduled_at')}")
    await message.answer("\n".join(lines))


@router.message(Command("broadcast_status"))
async def cmd_broadcast_status(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit():
        await message.answer("Usage: /broadcast_status <id>")
        return
    app = get_app()
    row = await app.storage.get_broadcast(int(arg))
    if not row:
        await message.answer("Рассылка не найдена.")
        return
    report = row.get("report") or {}
    await message.answer(
        f"id={_code(str(row['id']))}\nname={html.escape(str(row['name']))}\nstatus={row['status']}\n"
        f"scheduled_at={row.get('scheduled_at')}\nstarted_at={row.get('started_at')}\nfinished_at={row.get('finished_at')}\n"
        f"report={html.escape(json.dumps(report, ensure_ascii=False))}"
    )


@router.message(Command("broadcast_send"))
async def cmd_broadcast_send(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit():
        await message.answer("Usage: /broadcast_send <id>")
        return
    app = get_app()
    await app.storage.schedule_broadcast(int(arg), datetime.now(timezone.utc))
    await message.answer(f"Рассылка {_code(arg)} поставлена на немедленный запуск.")


@router.message(Command("broadcast_schedule"))
async def cmd_broadcast_schedule(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    args = (command.args or "").split(maxsplit=1)
    if len(args) != 2 or not args[0].isdigit():
        await message.answer("Usage: /broadcast_schedule <id> <YYYY-MM-DDTHH:MM[:SS][+TZ]>")
        return
    bid = int(args[0])
    dt_raw = args[1].strip()
    try:
        dt = datetime.fromisoformat(dt_raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(get_app().config.tz))
    except Exception:
        await message.answer("Неверный формат даты.")
        return
    await get_app().storage.schedule_broadcast(bid, dt.astimezone(timezone.utc))
    await message.answer(f"Рассылка {_code(str(bid))} запланирована на {html.escape(dt.isoformat())}")


@router.message(Command("broadcast_cancel"))
async def cmd_broadcast_cancel(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit():
        await message.answer("Usage: /broadcast_cancel <id>")
        return
    await get_app().storage.cancel_broadcast(int(arg))
    await message.answer(f"Рассылка {_code(arg)} отменена.")


@router.message(Command("import"))
async def cmd_import(message: Message, command: CommandObject) -> None:
    if not _is_admin(message):
        return
    args = (command.args or "").strip()
    path = Path(args) if args else Path(r"D:\crawler\output\bot_graph.json")
    if not path.exists():
        await message.answer(f"File not found: {path}")
        return
    payload = import_crawler_graph(path)
    ensure_error_node(payload["nodes"])
    await get_app().storage.save_nodes_payload({"metadata": payload["metadata"], "nodes": payload["nodes"]})
    await message.answer(f"Import complete: nodes={len(payload['nodes'])}, root={payload['metadata'].get('real_root_id')}")


@router.message(F.document)
async def cmd_import_document(message: Message) -> None:
    if not _is_admin(message):
        return
    caption = (message.caption or "").strip()
    if not caption.startswith("/import"):
        return
    app = get_app()
    file = await message.bot.get_file(message.document.file_id)
    if not file.file_path:
        await message.answer("Cannot fetch uploaded file")
        return
    local_path = app.config.data_dir / "uploaded_import.json"
    await message.bot.download_file(file.file_path, destination=local_path)
    payload = import_crawler_graph(local_path)
    ensure_error_node(payload["nodes"])
    await app.storage.save_nodes_payload({"metadata": payload["metadata"], "nodes": payload["nodes"]})
    await message.answer(f"Import complete from upload: nodes={len(payload['nodes'])}, root={payload['metadata'].get('real_root_id')}")


@router.message(Command("export"))
async def cmd_export(message: Message) -> None:
    if not _is_admin(message):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    dump_path = app.config.data_dir / "nodes.export.json"
    dump_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    await message.answer_document(FSInputFile(str(dump_path)))


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if not _is_admin(message):
        return
    await message.answer(
        "/open start\n/edit\n/add\n/media\n/media_clear\n/backbtn off\n/menubtn on\n/menutarget start\n/tree\n/tree_depth 3\n/search адвокат\n/validate\n/preview <id>\n/goto <id>\n/stats\n/fix\n/compact_ids\n/broadcast_new [name]\n/broadcast_list\n/broadcast_status <id>\n/broadcast_send <id>\n/broadcast_schedule <id> <iso_datetime>\n/broadcast_cancel <id>"
    )

