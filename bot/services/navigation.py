from __future__ import annotations

from typing import Any

from bot.services.node_resolver import resolve_node_id


def trim_history(history: list[str], max_history: int) -> list[str]:
    if len(history) <= max_history:
        return history
    return history[-max_history:]


def move_to_node(
    user_state: dict[str, Any],
    target_node_id: str,
    *,
    metadata: dict[str, Any],
    nodes: dict[str, Any],
    max_history: int,
) -> dict[str, Any]:
    current = user_state.get("current_node", "start")
    history = list(user_state.get("history", []))
    history.append(current)
    user_state["history"] = trim_history(history, max_history)
    user_state["current_node"] = resolve_node_id(target_node_id, metadata, nodes)
    return user_state


def go_back(user_state: dict[str, Any], *, metadata: dict[str, Any], nodes: dict[str, Any]) -> dict[str, Any]:
    history = list(user_state.get("history", []))
    if history:
        previous = history.pop()
        user_state["current_node"] = resolve_node_id(previous, metadata, nodes)
    else:
        user_state["current_node"] = resolve_node_id("start", metadata, nodes)
    user_state["history"] = history
    return user_state


def go_main_menu(user_state: dict[str, Any], *, node: dict[str, Any], metadata: dict[str, Any], nodes: dict[str, Any]) -> dict[str, Any]:
    menu_target = node.get("settings", {}).get("main_menu_target", "start")
    resolved = resolve_node_id(menu_target, metadata, nodes)
    user_state["current_node"] = resolved
    user_state["history"] = [resolved]
    return user_state

