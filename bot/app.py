from __future__ import annotations

from dataclasses import dataclass

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from bot.config import Config
from bot.services.broadcast_service import BroadcastService
from bot.services.hybrid_storage import HybridStorage


@dataclass(slots=True)
class AppContext:
    config: Config
    storage: HybridStorage
    broadcast_service: BroadcastService | None = None
    tree_depth_default: int = 2


def build_dispatcher() -> Dispatcher:
    return Dispatcher(storage=MemoryStorage())


def build_bot(token: str) -> Bot:
    return Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
