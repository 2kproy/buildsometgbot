from __future__ import annotations

import html
import json

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from datetime import datetime, timezone

from bot.keyboards.admin import admin_node_actions_keyboard, admin_panel_keyboard, broadcast_menu_keyboard
from bot.runtime import get_app
from bot.services.fixer import auto_fix_broken_links
from bot.services.graph_refs import find_incoming_refs
from bot.services.renderer import render_admin_node, render_node, send_rendered_node
from bot.services.tree_builder import build_tree_view
from bot.services.validator import validate_graph
from bot.states.admin import AdminStates

router = Router()


def _is_admin(callback: CallbackQuery) -> bool:
    return callback.from_user.id in get_app().config.admin_ids


def _code(value: str) -> str:
    return f"<code>{html.escape(str(value))}</code>"


async def _current_node_ctx(user_id: int) -> tuple[dict, str, dict]:
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    admin_state = await app.storage.load_admin_state(user_id)
    node_id = admin_state.get("current_edit_node", "start")
    if node_id == "start":
        node_id = payload["metadata"].get("real_root_id", "start")
    return payload, node_id, payload["nodes"].get(node_id)


def _broadcast_actions_keyboard(rows: list[dict]) -> InlineKeyboardMarkup | None:
    if not rows:
        return None
    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for row in rows[:10]:
        bid = int(row["id"])
        keyboard_rows.append(
            [
                InlineKeyboardButton(text=f"Send {bid}", callback_data=f"bcs:{bid}"),
                InlineKeyboardButton(text=f"Status {bid}", callback_data=f"bci:{bid}"),
                InlineKeyboardButton(text=f"Cancel {bid}", callback_data=f"bcc:{bid}"),
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


@router.callback_query(F.data == "adm:list")
async def cb_adm_list(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    payload = await get_app().storage.load_nodes_payload()
    text = "Ноды:\n" + "\n".join(f"- {nid}" for nid in sorted(payload["nodes"].keys())[:80])
    await callback.message.answer(text)
    await callback.answer()


@router.callback_query(F.data == "adm:menu")
async def cb_adm_menu(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    await callback.message.answer("Admin panel", reply_markup=admin_panel_keyboard())
    await callback.answer()


@router.callback_query(F.data == "adm:broadcast")
async def cb_adm_broadcast(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    await callback.message.answer("Broadcast menu", reply_markup=broadcast_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "bc:new")
async def cb_bc_new(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    app = get_app()
    payload, node_id, node = await _current_node_ctx(callback.from_user.id)
    if not node:
        await callback.message.answer("Current node not found.")
        await callback.answer()
        return
    name = f"broadcast_{int(datetime.now(tz=timezone.utc).timestamp())}"
    bc_payload = {
        "text": node.get("text", ""),
        "media": node.get("media"),
        "buttons": [b for b in node.get("buttons", []) if b.get("type") == "url"],
        "source_node_id": node_id,
    }
    bc_id = await app.storage.create_broadcast(name, bc_payload, callback.from_user.id)
    await callback.message.answer(f"Broadcast created: {_code(str(bc_id))} from node {_code(node_id)}", reply_markup=broadcast_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "bc:list")
async def cb_bc_list(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    app = get_app()
    rows = await app.storage.list_broadcasts(limit=20)
    if not rows:
        await callback.message.answer("No broadcasts yet.", reply_markup=broadcast_menu_keyboard())
        await callback.answer()
        return
    lines = ["Broadcasts:"]
    for row in rows[:10]:
        lines.append(f"- id={_code(str(row['id']))} | {html.escape(str(row['name']))} | {row['status']} | {row.get('scheduled_at')}")
    await callback.message.answer("\n".join(lines), reply_markup=_broadcast_actions_keyboard(rows))
    await callback.answer()


@router.callback_query(F.data == "bc:send_latest")
async def cb_bc_send_latest(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    rows = await get_app().storage.list_broadcasts(limit=1)
    if not rows:
        await callback.message.answer("No broadcasts yet.")
        await callback.answer()
        return
    bid = int(rows[0]["id"])
    await _send_broadcast_now(callback.message, bid)
    await callback.answer()


@router.callback_query(F.data == "bc:status_latest")
async def cb_bc_status_latest(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    rows = await get_app().storage.list_broadcasts(limit=1)
    if not rows:
        await callback.message.answer("No broadcasts yet.")
        await callback.answer()
        return
    await _show_broadcast_status(callback.message, int(rows[0]["id"]))
    await callback.answer()


@router.callback_query(F.data.startswith("bcs:"))
async def cb_bc_send(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    bid = int(callback.data.split(":", 1)[1])
    await _send_broadcast_now(callback.message, bid)
    await callback.answer()


@router.callback_query(F.data.startswith("bci:"))
async def cb_bc_status(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    bid = int(callback.data.split(":", 1)[1])
    await _show_broadcast_status(callback.message, bid)
    await callback.answer()


@router.callback_query(F.data.startswith("bcc:"))
async def cb_bc_cancel(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    bid = int(callback.data.split(":", 1)[1])
    app = get_app()
    row = await app.storage.get_broadcast(bid)
    if not row:
        await callback.message.answer("Broadcast not found.")
        await callback.answer()
        return
    await app.storage.cancel_broadcast(bid)
    await callback.message.answer(f"Broadcast {_code(str(bid))} canceled.", reply_markup=broadcast_menu_keyboard())
    await callback.answer()


async def _send_broadcast_now(message: Message, bid: int) -> None:
    app = get_app()
    row = await app.storage.get_broadcast(bid)
    if not row:
        await message.answer("Broadcast not found.")
        return
    recipients = await app.storage.list_recipients()
    if not recipients:
        await message.answer("No recipients yet. Ask users to open bot first.")
        return
    await app.storage.schedule_broadcast(bid, datetime.now(timezone.utc))
    await message.answer(
        f"Broadcast {_code(str(bid))} scheduled now. Recipients: {_code(str(len(recipients)))}",
        reply_markup=broadcast_menu_keyboard(),
    )


async def _show_broadcast_status(message: Message, bid: int) -> None:
    app = get_app()
    row = await app.storage.get_broadcast(bid)
    if not row:
        await message.answer("Broadcast not found.")
        return
    report = row.get("report") or {}
    await message.answer(
        f"id={_code(str(row['id']))}\nname={html.escape(str(row['name']))}\nstatus={row['status']}\n"
        f"scheduled_at={row.get('scheduled_at')}\nstarted_at={row.get('started_at')}\nfinished_at={row.get('finished_at')}\n"
        f"report={html.escape(json.dumps(report, ensure_ascii=False))}",
        reply_markup=broadcast_menu_keyboard(),
    )


@router.callback_query(F.data == "adm:validate")
async def cb_adm_validate(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    await callback.message.answer("Используйте /validate")
    await callback.answer()


@router.callback_query(F.data == "adm:open")
async def cb_adm_open(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    payload = await get_app().storage.load_nodes_payload()
    node_ids = sorted(payload["nodes"].keys())
    rows = [[InlineKeyboardButton(text=nid, callback_data=f"aopen:{nid}")] for nid in node_ids[:40]]
    await callback.message.answer("Выберите ноду для открытия:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()


@router.callback_query(F.data.startswith("aopen:"))
async def cb_adm_open_pick(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    node_id = callback.data.split(":", 1)[1]
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    node = payload["nodes"].get(node_id)
    if not node:
        await callback.message.answer("Нода не найдена.")
        await callback.answer()
        return
    admin_state = await app.storage.load_admin_state(callback.from_user.id)
    admin_state["current_edit_node"] = node_id
    await app.storage.save_admin_state(callback.from_user.id, admin_state)
    await callback.message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())
    await callback.answer()


@router.callback_query(F.data == "adm:tree")
async def cb_adm_tree(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    payload, node_id, node = await _current_node_ctx(callback.from_user.id)
    if not node:
        await callback.message.answer("Нода не найдена.")
        await callback.answer()
        return
    admin_state = await get_app().storage.load_admin_state(callback.from_user.id)
    depth = int(admin_state.get("tree_depth", 2))
    await callback.message.answer(build_tree_view(payload["nodes"], node_id, depth=depth))
    await callback.answer()


@router.callback_query(F.data == "adm:stats")
async def cb_adm_stats(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    payload = await get_app().storage.load_nodes_payload()
    report = validate_graph(payload["nodes"], payload["metadata"].get("real_root_id", "start"))
    s = report["summary"]
    await callback.message.answer(
        f"Stats:\n- total nodes: {s['total_nodes']}\n- total buttons: {s['total_buttons']}\n- broken links: {s['broken_links']}\n- orphan nodes: {s['orphans']}"
    )
    await callback.answer()


@router.callback_query(F.data == "adm:fix")
async def cb_adm_fix(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    result = auto_fix_broken_links(payload["nodes"], create_placeholder=False)
    await app.storage.save_nodes_payload(payload)
    await callback.message.answer(
        f"Fix done:\n- removed_broken_buttons: {result['removed_broken_buttons']}\n- created_placeholders: {result['created_placeholders']}"
    )
    await callback.answer()


@router.callback_query(F.data == "adm:export")
async def cb_adm_export(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    app = get_app()
    payload = await app.storage.load_nodes_payload()
    dump_path = app.config.data_dir / "nodes.export.json"
    dump_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    await callback.message.answer_document(FSInputFile(str(dump_path)))
    await callback.answer()


@router.callback_query(F.data.startswith("delnode:"))
async def cb_delete_node(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback):
        return
    _, action = callback.data.split(":", 1)
    data = await state.get_data()
    node_id = data.get("delete_node_id")
    if not node_id:
        await callback.message.answer("Сессия удаления устарела.")
        await callback.answer()
        return
    if action == "cancel":
        await state.clear()
        await callback.message.answer("Удаление отменено.")
        await callback.answer()
        return
    payload = await get_app().storage.load_nodes_payload()
    refs = find_incoming_refs(payload["nodes"], node_id)
    for ref in refs:
        from_node = payload["nodes"].get(ref["from_node"])
        if from_node:
            from_node["buttons"] = [b for b in from_node.get("buttons", []) if not (b.get("type") == "node" and b.get("target") == node_id)]
    payload["nodes"].pop(node_id, None)
    await get_app().storage.save_nodes_payload(payload)
    await state.clear()
    await callback.message.answer(f"Нода {node_id} удалена, входящие ссылки очищены.")
    await callback.answer()


@router.callback_query(F.data.startswith("anei:"))
async def cb_admin_node_action(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback):
        return
    action = callback.data.split(":", 1)[1]
    payload, node_id, node = await _current_node_ctx(callback.from_user.id)
    if not node:
        await callback.message.answer("Нода не найдена.")
        await callback.answer()
        return

    if action == "edit":
        await state.set_state(AdminStates.editing_text)
        await callback.message.answer(f"Текущая нода: {_code(node_id)}\nВведите новый текст:")
    elif action == "add":
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
        await callback.message.answer(f"Текущая нода: {_code(node_id)}\nВыберите тип кнопки:", reply_markup=kb)
    elif action == "del":
        buttons = node.get("buttons", [])
        if not buttons:
            await callback.message.answer("В ноде нет кнопок.")
        else:
            rows = [[InlineKeyboardButton(text=f"{b.get('id')} | {str(b.get('text', ''))[:28]}", callback_data=f"adel:{b.get('id')}")] for b in buttons[:40]]
            await callback.message.answer(
                f"Текущая нода: {_code(node_id)}\nВыберите кнопку для удаления:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            )
    elif action == "link":
        node_buttons = [b for b in node.get("buttons", []) if b.get("type") == "node"]
        if not node_buttons:
            await callback.message.answer("Нет node-кнопок для изменения ссылки.")
        else:
            rows = [[InlineKeyboardButton(text=f"{b.get('id')} | {str(b.get('text', ''))[:28]}", callback_data=f"alink:{b.get('id')}")] for b in node_buttons[:40]]
            await callback.message.answer(
                f"Текущая нода: {_code(node_id)}\nВыберите кнопку для изменения target:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            )
    elif action == "preview":
        text, keyboard, _ = await render_node(payload, node_id)
        await callback.message.answer(f"Preview: {_code(node_id)}")
        await send_rendered_node(callback.message, payload["nodes"][node_id], text, keyboard)
    elif action == "tree":
        admin_state = await get_app().storage.load_admin_state(callback.from_user.id)
        depth = int(admin_state.get("tree_depth", 2))
        await callback.message.answer(build_tree_view(payload["nodes"], node_id, depth=depth))
    elif action == "delete_node":
        refs = find_incoming_refs(payload["nodes"], node_id)
        lines = [f"Node is referenced by {len(refs)} nodes:"]
        lines.extend(f"- {x['from_node']} / {x['button_text']}" for x in refs[:30])
        await state.set_state(AdminStates.delete_confirm)
        await state.update_data(delete_node_id=node_id)
        await callback.message.answer("\n".join(lines))
        await callback.message.answer(
            "Подтвердите удаление:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Cancel", callback_data="delnode:cancel")],
                    [InlineKeyboardButton(text="Force Delete", callback_data="delnode:force")],
                ]
            ),
        )
    elif action == "clone":
        await callback.message.answer("Используйте /clone <new_node_id>")
    elif action == "settings":
        await callback.message.answer(
            f"Текущая нода: {_code(node_id)}\n/backbtn on|off\n/menubtn on|off\n/menutarget &lt;node_id&gt;\n/media\n/media_clear"
        )
    else:
        await callback.message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("adel:"))
async def cb_admin_delete_button(callback: CallbackQuery) -> None:
    if not _is_admin(callback):
        return
    button_id = callback.data.split(":", 1)[1]
    payload, node_id, node = await _current_node_ctx(callback.from_user.id)
    if not node:
        await callback.answer("Node missing")
        return
    before = len(node.get("buttons", []))
    node["buttons"] = [b for b in node.get("buttons", []) if b.get("id") != button_id]
    after = len(node["buttons"])
    await get_app().storage.save_nodes_payload(payload)
    if before == after:
        await callback.message.answer("Кнопка не найдена.")
    else:
        await callback.message.answer(f"Удалено: {_code(button_id)}\nТекущая нода: {_code(node_id)}")
        await callback.message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("alink:"))
async def cb_admin_link_pick(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback):
        return
    button_id = callback.data.split(":", 1)[1]
    payload, node_id, _ = await _current_node_ctx(callback.from_user.id)
    node_ids = sorted(payload["nodes"].keys())
    rows = [[InlineKeyboardButton(text=x, callback_data=f"alinkto:{x}")] for x in node_ids[:40]]
    await state.set_state(AdminStates.linking_target)
    await state.update_data(link_button_id=button_id)
    await callback.message.answer(
        f"Текущая нода: {_code(node_id)}\nВыберите target ноду или введите id сообщением:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("alinkto:"))
async def cb_admin_link_target(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback):
        return
    target = callback.data.split(":", 1)[1]
    await _apply_link_target(callback.message, callback.from_user.id, target, state)
    await callback.answer()


@router.message(AdminStates.linking_target)
async def msg_admin_link_target(message: Message, state: FSMContext) -> None:
    app = get_app()
    if message.from_user.id not in app.config.admin_ids:
        return
    target = (message.text or "").strip()
    await _apply_link_target(message, message.from_user.id, target, state)


async def _apply_link_target(message: Message, user_id: int, target: str, state: FSMContext) -> None:
    app = get_app()
    payload, node_id, node = await _current_node_ctx(user_id)
    if not node:
        await message.answer("Нода не найдена.")
        await state.clear()
        return
    if target not in payload["nodes"]:
        await message.answer("Target нода не существует.")
        return
    data = await state.get_data()
    button_id = data.get("link_button_id")
    button = next((b for b in node.get("buttons", []) if b.get("id") == button_id and b.get("type") == "node"), None)
    if not button:
        await message.answer("Кнопка не найдена или не типа node.")
        await state.clear()
        return
    button["target"] = target
    await app.storage.save_nodes_payload(payload)
    await state.clear()
    await message.answer(f"Ссылка обновлена: {_code(button_id)} -> {_code(target)}\nТекущая нода: {_code(node_id)}")
    await message.answer(render_admin_node(node_id, node), reply_markup=admin_node_actions_keyboard())
