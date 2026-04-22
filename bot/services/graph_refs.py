from __future__ import annotations

from collections import deque
from typing import Any


def find_incoming_refs(nodes: dict[str, Any], target_id: str) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for node_id, node in nodes.items():
        for btn in node.get("buttons", []):
            if btn.get("type") == "node" and btn.get("target") == target_id:
                refs.append({"from_node": node_id, "button_id": btn.get("id", ""), "button_text": btn.get("text", "")})
    return refs


def reachable_from(nodes: dict[str, Any], start_id: str) -> set[str]:
    if start_id not in nodes:
        return set()
    visited: set[str] = set()
    queue = deque([start_id])
    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        node = nodes.get(node_id, {})
        for btn in node.get("buttons", []):
            if btn.get("type") == "node":
                target = btn.get("target")
                if isinstance(target, str) and target in nodes and target not in visited:
                    queue.append(target)
    return visited


def find_unreachable(nodes: dict[str, Any], start_id: str) -> set[str]:
    reachable = reachable_from(nodes, start_id)
    return {node_id for node_id in nodes if node_id not in reachable}

