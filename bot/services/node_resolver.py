from __future__ import annotations

import logging
from typing import Any

from bot.services.storage import ERROR_NODE_ID

logger = logging.getLogger(__name__)


def resolve_node_id(requested_id: str, metadata: dict[str, Any], nodes: dict[str, Any]) -> str:
    if requested_id == "start":
        real_root = metadata.get("real_root_id") or "start"
        if real_root in nodes:
            return real_root
    if requested_id in nodes:
        return requested_id
    logger.warning("Missing node at runtime", extra={"requested_id": requested_id})
    return ERROR_NODE_ID

