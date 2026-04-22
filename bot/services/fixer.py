from __future__ import annotations

from typing import Any

from bot.services.storage import ERROR_NODE, ERROR_NODE_ID


def ensure_error_node(nodes: dict[str, Any]) -> None:
    if ERROR_NODE_ID not in nodes:
        nodes[ERROR_NODE_ID] = ERROR_NODE.copy()


def auto_fix_broken_links(nodes: dict[str, Any], create_placeholder: bool = False) -> dict[str, int]:
    removed = 0
    created = 0
    for node in nodes.values():
        valid_buttons = []
        for btn in node.get("buttons", []):
            if btn.get("type") != "node":
                valid_buttons.append(btn)
                continue
            target = btn.get("target")
            if target in nodes:
                valid_buttons.append(btn)
                continue
            if create_placeholder and isinstance(target, str) and target:
                nodes[target] = {
                    "id": target,
                    "text": f"Placeholder for {target}",
                    "buttons": [],
                    "media": None,
                    "settings": {"show_back": True, "show_main_menu": True, "main_menu_target": "start"},
                }
                created += 1
                valid_buttons.append(btn)
            else:
                removed += 1
        node["buttons"] = valid_buttons
    ensure_error_node(nodes)
    return {"removed_broken_buttons": removed, "created_placeholders": created}
