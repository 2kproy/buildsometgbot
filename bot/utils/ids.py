from __future__ import annotations

import uuid


def short_id(prefix: str = "id") -> str:
    # Kept for backward compatibility; prefer helpers below for strict 8 chars.
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def short_button_id() -> str:
    # Strictly 8 chars.
    return uuid.uuid4().hex[:8]


def short_node_id(index: int) -> str:
    # Strictly 8 chars: n + 7 digits.
    return f"n{index:07d}"
