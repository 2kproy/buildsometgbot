from __future__ import annotations

from typing import Any

from bot.services.runtime_storage import RuntimeStorage
from bot.services.storage import JsonStorage


class HybridStorage:
    def __init__(self, node_storage: JsonStorage, runtime_storage: RuntimeStorage) -> None:
        self.node_storage = node_storage
        self.runtime_storage = runtime_storage

    async def ensure_files(self) -> None:
        await self.node_storage.ensure_files()

    async def ensure_initialized(self) -> None:
        await self.node_storage.ensure_files()
        await self.runtime_storage.connect()

    async def close(self) -> None:
        await self.runtime_storage.close()

    async def track_user(self, user: Any) -> None:
        await self.runtime_storage.track_user(user)

    # Node content (JSON)
    async def load_nodes_payload(self) -> dict[str, Any]:
        return await self.node_storage.load_nodes_payload()

    async def save_nodes_payload(self, payload: dict[str, Any]) -> None:
        await self.node_storage.save_nodes_payload(payload)

    async def load_nodes(self) -> dict[str, Any]:
        return await self.node_storage.load_nodes()

    # Runtime user/admin state (Postgres + Redis cache)
    async def load_user_state(self, user_id: int) -> dict[str, Any]:
        return await self.runtime_storage.load_user_state(user_id)

    async def save_user_state(self, user_id: int, state: dict[str, Any]) -> None:
        await self.runtime_storage.save_user_state(user_id, state)

    async def load_admin_state(self, user_id: int) -> dict[str, Any]:
        return await self.runtime_storage.load_admin_state(user_id)

    async def save_admin_state(self, user_id: int, state: dict[str, Any]) -> None:
        await self.runtime_storage.save_admin_state(user_id, state)

    async def load_user_state_all(self) -> dict[str, Any]:
        return await self.runtime_storage.load_user_state_all()

    async def save_user_state_all(self, payload: dict[str, Any]) -> None:
        await self.runtime_storage.save_user_state_all(payload)

    async def load_admin_state_all(self) -> dict[str, Any]:
        return await self.runtime_storage.load_admin_state_all()

    async def save_admin_state_all(self, payload: dict[str, Any]) -> None:
        await self.runtime_storage.save_admin_state_all(payload)

    # Broadcast
    async def create_broadcast(self, name: str, payload: dict[str, Any], created_by: int) -> int:
        return await self.runtime_storage.create_broadcast(name, payload, created_by)

    async def list_broadcasts(self, limit: int = 20) -> list[dict[str, Any]]:
        return await self.runtime_storage.list_broadcasts(limit)

    async def get_broadcast(self, broadcast_id: int) -> dict[str, Any] | None:
        return await self.runtime_storage.get_broadcast(broadcast_id)

    async def schedule_broadcast(self, broadcast_id: int, when) -> None:
        await self.runtime_storage.schedule_broadcast(broadcast_id, when)

    async def cancel_broadcast(self, broadcast_id: int) -> None:
        await self.runtime_storage.cancel_broadcast(broadcast_id)

    async def claim_due_broadcasts(self, limit: int = 10) -> list[dict[str, Any]]:
        return await self.runtime_storage.claim_due_broadcasts(limit)

    async def mark_broadcast_running(self, broadcast_id: int) -> None:
        await self.runtime_storage.mark_broadcast_running(broadcast_id)

    async def mark_broadcast_done(self, broadcast_id: int, job_id: int, status: str, report: dict[str, Any]) -> None:
        await self.runtime_storage.mark_broadcast_done(broadcast_id, job_id, status, report)

    async def add_broadcast_event(self, broadcast_id: int, telegram_id: int, status: str, error: str | None = None) -> None:
        await self.runtime_storage.add_broadcast_event(broadcast_id, telegram_id, status, error)

    async def list_recipients(self) -> list[int]:
        return await self.runtime_storage.list_recipients()

    async def dedup_broadcast_recipient(self, broadcast_id: int, telegram_id: int, ttl_sec: int = 86400 * 7) -> bool:
        return await self.runtime_storage.dedup_broadcast_recipient(broadcast_id, telegram_id, ttl_sec)

