from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть ноду", callback_data="adm:open")],
            [InlineKeyboardButton(text="Список нод", callback_data="adm:list"), InlineKeyboardButton(text="Дерево", callback_data="adm:tree")],
            [InlineKeyboardButton(text="Валидация", callback_data="adm:validate"), InlineKeyboardButton(text="Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton(text="Рассылки", callback_data="adm:broadcast")],
            [InlineKeyboardButton(text="Автофикс", callback_data="adm:fix"), InlineKeyboardButton(text="Экспорт", callback_data="adm:export")],
        ]
    )


def admin_node_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Редактировать текст", callback_data="anei:edit"), InlineKeyboardButton(text="Добавить кнопку", callback_data="anei:add")],
            [InlineKeyboardButton(text="Удалить кнопку", callback_data="anei:del"), InlineKeyboardButton(text="Изменить ссылку", callback_data="anei:link")],
            [InlineKeyboardButton(text="Предпросмотр", callback_data="anei:preview"), InlineKeyboardButton(text="Дерево", callback_data="anei:tree")],
            [InlineKeyboardButton(text="Настройки", callback_data="anei:settings"), InlineKeyboardButton(text="Клонировать", callback_data="anei:clone")],
            [InlineKeyboardButton(text="Удалить ноду", callback_data="anei:delete_node")],
        ]
    )


def confirm_delete_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="delnode:cancel")],
            [InlineKeyboardButton(text="Удалить принудительно", callback_data="delnode:force")],
        ]
    )


def broadcast_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать из текущей ноды", callback_data="bc:new")],
            [InlineKeyboardButton(text="Список рассылок", callback_data="bc:list")],
            [InlineKeyboardButton(text="Отправить последнюю", callback_data="bc:send_latest"), InlineKeyboardButton(text="Статус последней", callback_data="bc:status_latest")],
            [InlineKeyboardButton(text="Назад в админку", callback_data="adm:menu")],
        ]
    )
