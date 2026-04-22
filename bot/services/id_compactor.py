from __future__ import annotations

from typing import Any

from bot.services.storage import ERROR_NODE_ID
from bot.utils.ids import short_button_id, short_node_id


def _build_node_id_map(nodes: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    counter = 1
    for old_id in sorted(nodes.keys()):
        if old_id == ERROR_NODE_ID:
            mapping[old_id] = ERROR_NODE_ID
            continue
        if len(old_id) <= 8 and old_id not in mapping.values():
            mapping[old_id] = old_id
            continue
        while True:
            candidate = short_node_id(counter)
            counter += 1
            if candidate not in mapping.values():
                mapping[old_id] = candidate
                break
    return mapping


def compact_payload_ids(payload: dict[str, Any]) -> dict[str, Any]:
    nodes = payload.get("nodes", {})
    metadata = payload.get("metadata", {})
    node_map = _build_node_id_map(nodes)
    new_nodes: dict[str, Any] = {}

    for old_id, node in nodes.items():
        new_id = node_map[old_id]
        copied = {
            "id": new_id,
            "text": node.get("text", ""),
            "buttons": [],
            "media": node.get("media"),
            "settings": dict(node.get("settings", {})),
        }
        main_target = copied["settings"].get("main_menu_target")
        if isinstance(main_target, str) and main_target in node_map:
            copied["settings"]["main_menu_target"] = node_map[main_target]
        for btn in node.get("buttons", []):
            target = btn.get("target", "")
            if btn.get("type") == "node" and isinstance(target, str) and target in node_map:
                target = node_map[target]
            copied["buttons"].append(
                {
                    "id": short_button_id(),
                    "text": btn.get("text", ""),
                    "type": btn.get("type", "node"),
                    "target": target,
                    "row": int(btn.get("row", 0)),
                    "sort": int(btn.get("sort", 0)),
                }
            )
        new_nodes[new_id] = copied

    real_root_id = metadata.get("real_root_id")
    if isinstance(real_root_id, str) and real_root_id in node_map:
        metadata["real_root_id"] = node_map[real_root_id]
    payload["nodes"] = new_nodes
    payload["metadata"] = metadata
    payload["id_compact_map"] = node_map
    return payload


def remap_user_state(state: dict[str, Any], node_map: dict[str, str]) -> dict[str, Any]:
    out = {}
    for user_id, entry in state.items():
        cur = entry.get("current_node")
        hist = entry.get("history", [])
        out[user_id] = {
            "current_node": node_map.get(cur, cur),
            "history": [node_map.get(x, x) for x in hist],
        }
    return out


def remap_admin_state(state: dict[str, Any], node_map: dict[str, str]) -> dict[str, Any]:
    out = {}
    for user_id, entry in state.items():
        cur = entry.get("current_edit_node")
        copied = dict(entry)
        copied["current_edit_node"] = node_map.get(cur, cur)
        out[user_id] = copied
    return out
