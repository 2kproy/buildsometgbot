from __future__ import annotations

import html
from typing import Any

from bot.services.graph_refs import find_incoming_refs


def build_tree_view(nodes: dict[str, Any], node_id: str, depth: int = 2) -> str:
    lines: list[str] = []
    visited: set[str] = set()

    def walk(current_id: str, current_depth: int, indent: int) -> None:
        prefix = "  " * indent
        current_id_safe = html.escape(str(current_id))
        if current_id in visited:
            lines.append(f"{prefix}- <code>{current_id_safe}</code> (cycle)")
            return
        visited.add(current_id)
        node = nodes.get(current_id)
        if not node:
            lines.append(f"{prefix}- <code>{current_id_safe}</code> (missing)")
            return
        lines.append(f"{prefix}- <code>{current_id_safe}</code>")
        if current_depth >= depth:
            return
        for btn in node.get("buttons", []):
            if btn.get("type") == "node":
                lines.append(f"{prefix}  -> {html.escape(str(btn.get('text', '')))}")
                walk(str(btn.get("target")), current_depth + 1, indent + 2)
            elif btn.get("type") == "url":
                lines.append(
                    f"{prefix}  [url] {html.escape(str(btn.get('text', '')))} => {html.escape(str(btn.get('target', '')))}"
                )
            elif btn.get("type") == "reply":
                lines.append(f"{prefix}  [reply] {html.escape(str(btn.get('text', '')))}")

    walk(node_id, 0, 0)
    incoming = find_incoming_refs(nodes, node_id)
    lines.append("")
    lines.append("Incoming references:")
    if not incoming:
        lines.append("- none")
    else:
        for ref in incoming:
            lines.append(
                f"- <code>{html.escape(str(ref['from_node']))}</code> via {html.escape(str(ref['button_text']))} "
                f"(<code>{html.escape(str(ref['button_id']))}</code>)"
            )
    return "\n".join(lines)
