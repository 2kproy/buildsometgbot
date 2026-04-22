from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.utils.keyboard_normalizer import normalize_rows


def build_user_keyboard(buttons: list[dict], show_back: bool, show_main: bool) -> InlineKeyboardMarkup:
    rows = []
    for grouped in normalize_rows(buttons):
        line = []
        for btn in grouped:
            if btn.get("type") == "url":
                line.append(InlineKeyboardButton(text=btn["text"], url=btn["target"]))
            else:
                line.append(InlineKeyboardButton(text=btn["text"], callback_data=f"b:{btn['id']}"))
        rows.append(line)
    if show_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="s:back")])
    if show_main:
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="s:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

