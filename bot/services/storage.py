from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from bot.services.types import Button, Node

logger = logging.getLogger(__name__)

ERROR_NODE_ID = "__error__"
ERROR_NODE: Node = {
    "id": ERROR_NODE_ID,
    "text": "⚠️ Раздел временно недоступен",
    "buttons": [],
    "media": None,
    "settings": {"show_back": False, "show_main_menu": True, "main_menu_target": "start"},
}


def _default_settings() -> dict[str, Any]:
    return {"show_back": True, "show_main_menu": True, "main_menu_target": "start"}


class JsonStorage:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.nodes_path = data_dir / "nodes.json"
        self.user_state_path = data_dir / "user_state.json"
        self.admin_state_path = data_dir / "admin_state.json"
        self._file_locks: dict[str, asyncio.Lock] = {
            "nodes": asyncio.Lock(),
            "user_state": asyncio.Lock(),
            "admin_state": asyncio.Lock(),
        }
        self._user_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def ensure_files(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.nodes_path.exists():
            await self._atomic_write(self.nodes_path, {"metadata": {"real_root_id": "start"}, "nodes": {ERROR_NODE_ID: ERROR_NODE}})
        if not self.user_state_path.exists():
            await self._atomic_write(self.user_state_path, {})
        if not self.admin_state_path.exists():
            await self._atomic_write(self.admin_state_path, {})

    async def _atomic_write(self, path: Path, payload: Any) -> None:
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            tmp.write(text)
            tmp.flush()
            tmp_path = Path(tmp.name)
        tmp_path.replace(path)

    async def _read_json(self, path: Path) -> Any:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _normalize_nodes_payload(self, payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict) and "nodes" in payload:
            metadata = payload.get("metadata") or {}
            nodes = payload.get("nodes") or {}
        else:
            metadata = {}
            nodes = payload if isinstance(payload, dict) else {}
        if "real_root_id" not in metadata:
            if "start" in nodes:
                # Backward compatibility for legacy physical start node.
                metadata["real_root_id"] = "start"
            else:
                metadata["real_root_id"] = next(iter(nodes.keys()), "start")
        for node_id, node in list(nodes.items()):
            if not isinstance(node, dict):
                continue
            node.setdefault("id", node_id)
            node.setdefault("text", "")
            node.setdefault("buttons", [])
            node.setdefault("media", None)
            node.setdefault("settings", _default_settings())
            settings = node["settings"]
            settings.setdefault("show_back", True)
            settings.setdefault("show_main_menu", True)
            settings.setdefault("main_menu_target", "start")
            for idx, btn in enumerate(node["buttons"]):
                if not isinstance(btn, dict):
                    continue
                btn.setdefault("id", f"b{idx:07d}")
                btn.setdefault("text", "Button")
                btn.setdefault("type", "node")
                btn.setdefault("target", "")
                btn.setdefault("row", 0)
                btn.setdefault("sort", idx)
        nodes.setdefault(ERROR_NODE_ID, ERROR_NODE)
        return {"metadata": metadata, "nodes": nodes}

    async def load_nodes_payload(self) -> dict[str, Any]:
        async with self._file_locks["nodes"]:
            raw = await self._read_json(self.nodes_path)
            payload = self._normalize_nodes_payload(raw)
            # migrate old shape if needed
            if raw != payload:
                await self._atomic_write(self.nodes_path, payload)
            return payload

    async def save_nodes_payload(self, payload: dict[str, Any]) -> None:
        async with self._file_locks["nodes"]:
            normalized = self._normalize_nodes_payload(payload)
            await self._atomic_write(self.nodes_path, normalized)

    async def load_nodes(self) -> dict[str, Node]:
        return (await self.load_nodes_payload())["nodes"]

    async def save_nodes(self, nodes: dict[str, Node], metadata: dict[str, Any] | None = None) -> None:
        existing = await self.load_nodes_payload()
        payload = {"metadata": metadata or existing["metadata"], "nodes": nodes}
        await self.save_nodes_payload(payload)

    async def update_nodes(self, updater) -> dict[str, Any]:
        async with self._file_locks["nodes"]:
            payload = self._normalize_nodes_payload(await self._read_json(self.nodes_path))
            result = updater(payload)
            await self._atomic_write(self.nodes_path, payload)
            return result if result is not None else payload

    async def _load_state(self, key: str, path: Path) -> dict[str, Any]:
        async with self._file_locks[key]:
            data = await self._read_json(path)
            return data if isinstance(data, dict) else {}

    async def _save_state(self, key: str, path: Path, data: dict[str, Any]) -> None:
        async with self._file_locks[key]:
            await self._atomic_write(path, data)

    async def load_user_state_all(self) -> dict[str, Any]:
        return await self._load_state("user_state", self.user_state_path)

    async def load_user_state(self, user_id: int) -> dict[str, Any]:
        lock = self._user_locks[str(user_id)]
        async with lock:
            async with self._file_locks["user_state"]:
                data = await self._read_json(self.user_state_path)
                if not isinstance(data, dict):
                    data = {}
                return data.get(str(user_id), {"current_node": "start", "history": []})

    async def save_user_state(self, user_id: int, state: dict[str, Any]) -> None:
        lock = self._user_locks[str(user_id)]
        async with lock:
            async with self._file_locks["user_state"]:
                data = await self._read_json(self.user_state_path)
                if not isinstance(data, dict):
                    data = {}
                data[str(user_id)] = state
                await self._atomic_write(self.user_state_path, data)

    async def load_admin_state_all(self) -> dict[str, Any]:
        return await self._load_state("admin_state", self.admin_state_path)

    async def load_admin_state(self, user_id: int) -> dict[str, Any]:
        async with self._file_locks["admin_state"]:
            data = await self._read_json(self.admin_state_path)
            if not isinstance(data, dict):
                data = {}
            return data.get(str(user_id), {"current_edit_node": "start", "mode": "idle"})

    async def save_admin_state(self, user_id: int, state: dict[str, Any]) -> None:
        async with self._file_locks["admin_state"]:
            data = await self._read_json(self.admin_state_path)
            if not isinstance(data, dict):
                data = {}
            data[str(user_id)] = state
            await self._atomic_write(self.admin_state_path, data)


NodeContentStorage = JsonStorage
