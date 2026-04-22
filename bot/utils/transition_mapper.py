from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _extract_target(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        maybe = value.get("next_node_id") or value.get("target")
        return maybe if isinstance(maybe, str) else None
    return None


def map_button_to_transition(
    *,
    node_id: str,
    button_text: str,
    row: int,
    col: int,
    button_meta_by_pos: dict[tuple[int, int], dict[str, Any]],
    transitions: dict[str, Any],
    positional_keys: list[str],
    used_transition_keys: set[str],
) -> tuple[str | None, str]:
    meta = button_meta_by_pos.get((row, col), {})
    transition_key = meta.get("transition_key")
    if isinstance(transition_key, str) and transition_key in transitions:
        used_transition_keys.add(transition_key)
        return _extract_target(transitions[transition_key]), "meta.transition_key"

    if button_text in transitions:
        used_transition_keys.add(button_text)
        return _extract_target(transitions[button_text]), "exact_text"

    for key in positional_keys:
        if key in used_transition_keys:
            continue
        used_transition_keys.add(key)
        logger.warning(
            "Transition fallback by position",
            extra={"node_id": node_id, "button_text": button_text, "row": row, "col": col, "transition_key": key},
        )
        return _extract_target(transitions[key]), "positional_fallback"

    logger.warning(
        "Missing transition mapping",
        extra={"node_id": node_id, "button_text": button_text, "row": row, "col": col},
    )
    return None, "missing"

