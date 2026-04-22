from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Open", callback_data="adm:open")],
            [InlineKeyboardButton(text="List", callback_data="adm:list"), InlineKeyboardButton(text="Tree", callback_data="adm:tree")],
            [InlineKeyboardButton(text="Validate", callback_data="adm:validate"), InlineKeyboardButton(text="Stats", callback_data="adm:stats")],
            [InlineKeyboardButton(text="Broadcast", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="Fix", callback_data="adm:fix"), InlineKeyboardButton(text="Export", callback_data="adm:export")],
        ]
    )


def admin_node_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Edit text", callback_data="anei:edit"), InlineKeyboardButton(text="Add button", callback_data="anei:add")],
            [InlineKeyboardButton(text="Delete button", callback_data="anei:del"), InlineKeyboardButton(text="Change link", callback_data="anei:link")],
            [InlineKeyboardButton(text="Preview", callback_data="anei:preview"), InlineKeyboardButton(text="Tree", callback_data="anei:tree")],
            [InlineKeyboardButton(text="Settings", callback_data="anei:settings"), InlineKeyboardButton(text="Clone", callback_data="anei:clone")],
            [InlineKeyboardButton(text="Delete node", callback_data="anei:delete_node")],
        ]
    )


def confirm_delete_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Cancel", callback_data="delnode:cancel")],
            [InlineKeyboardButton(text="Force Delete", callback_data="delnode:force")],
        ]
    )


def broadcast_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Create from current node", callback_data="bc:new")],
            [InlineKeyboardButton(text="List recent", callback_data="bc:list")],
            [InlineKeyboardButton(text="Send latest", callback_data="bc:send_latest"), InlineKeyboardButton(text="Latest status", callback_data="bc:status_latest")],
            [InlineKeyboardButton(text="Back to admin", callback_data="adm:menu")],
        ]
    )
