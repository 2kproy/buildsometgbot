from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from bot.services.graph_refs import find_unreachable


def _is_valid_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def validate_graph(nodes: dict[str, Any], real_root_id: str) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    total_buttons = 0
    incoming_count: dict[str, int] = {node_id: 0 for node_id in nodes}

    for node_id, node in nodes.items():
        if not str(node.get("text", "")).strip():
            errors.append(f"Empty text: {node_id}")
        media = node.get("media")
        if media is not None:
            if not isinstance(media, dict):
                errors.append(f"Invalid media shape: {node_id}")
            else:
                m_type = media.get("type")
                file_id = media.get("file_id")
                if m_type not in {"photo", "video", "document", "animation", "audio", "voice"}:
                    errors.append(f"Invalid media type: {node_id} [{m_type}]")
                if not isinstance(file_id, str) or not file_id.strip():
                    errors.append(f"Invalid media file_id: {node_id}")
        for btn in node.get("buttons", []):
            total_buttons += 1
            b_type = btn.get("type")
            target = btn.get("target", "")
            if b_type == "node":
                if target not in nodes:
                    errors.append(f"Broken transition: {node_id} -> {target}")
                else:
                    incoming_count[target] = incoming_count.get(target, 0) + 1
            elif b_type == "url":
                if not isinstance(target, str) or not _is_valid_url(target):
                    errors.append(f"Invalid URL: {node_id} [{btn.get('id')}] {target}")
            elif b_type == "reply":
                if not str(target).strip():
                    warnings.append(f"Empty reply payload: {node_id} [{btn.get('id')}]")
            else:
                errors.append(f"Invalid button type: {node_id} [{btn.get('id')}] {b_type}")

    unreachable = sorted([nid for nid in find_unreachable(nodes, real_root_id) if nid != "__error__"])
    orphans = sorted([nid for nid, cnt in incoming_count.items() if cnt == 0 and nid != real_root_id and nid != "__error__"])
    report = {
        "summary": {
            "total_nodes": len(nodes),
            "total_buttons": total_buttons,
            "broken_links": len([e for e in errors if e.startswith("Broken transition")]),
            "orphans": len(orphans),
            "unreachable": len(unreachable),
        },
        "errors": errors,
        "warnings": warnings,
        "orphans": orphans,
        "unreachable": unreachable,
    }
    return report
