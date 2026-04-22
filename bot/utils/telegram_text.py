from __future__ import annotations

from aiogram.types import Message


def message_text_as_html(message: Message) -> str:
    """Return message text converted to HTML using Telegram entities."""
    html_text = getattr(message, "html_text", None)
    if isinstance(html_text, str):
        return html_text
    text = message.text
    if isinstance(text, str):
        return text
    return ""

