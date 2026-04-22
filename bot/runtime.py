from __future__ import annotations

from bot.app import AppContext

_app: AppContext | None = None


def set_app(app: AppContext) -> None:
    global _app
    _app = app


def get_app() -> AppContext:
    if _app is None:
        raise RuntimeError("App context is not initialized")
    return _app

