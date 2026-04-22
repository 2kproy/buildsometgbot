from __future__ import annotations

from typing import Any


def _escape_label(text: str, limit: int = 40) -> str:
    safe = text.replace('"', "'").replace("\n", " ").strip()
    if len(safe) > limit:
        safe = safe[: limit - 1] + "…"
    return safe


def build_mermaid_subtree(nodes: dict[str, Any], root_id: str, depth: int = 2) -> str:
    lines = ["flowchart TD"]
    visited: set[tuple[str, int]] = set()

    def walk(node_id: str, current_depth: int) -> None:
        if current_depth > depth:
            return
        key = (node_id, current_depth)
        if key in visited:
            return
        visited.add(key)
        node = nodes.get(node_id)
        if not node:
            return
        src_label = _escape_label(node.get("text", node_id))
        lines.append(f'  {node_id}["{src_label}"]')
        for btn in node.get("buttons", []):
            if btn.get("type") != "node":
                continue
            target = btn.get("target")
            if not isinstance(target, str):
                continue
            edge_label = _escape_label(btn.get("text", ""))
            lines.append(f'  {node_id} -->|"{edge_label}"| {target}')
            walk(target, current_depth + 1)

    walk(root_id, 0)
    return "\n".join(lines)

