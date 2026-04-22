from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from bot.utils.transition_mapper import map_button_to_transition
from bot.utils.ids import short_button_id, short_node_id

logger = logging.getLogger(__name__)


def _default_settings() -> dict[str, Any]:
    return {"show_back": True, "show_main_menu": True, "main_menu_target": "start"}


def _pick_node_text(source: dict[str, Any]) -> str:
    """Prefer crawler HTML text when available, fallback to plain text."""
    text_html = source.get("text_html")
    if isinstance(text_html, str) and text_html.strip():
        return text_html
    return str(source.get("text", ""))


def import_crawler_graph(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    source_nodes = data.get("nodes", {})
    root_id = data.get("root")
    report = {
        "warnings": [],
        "mapped_by": {"meta.transition_key": 0, "exact_text": 0, "positional_fallback": 0, "missing": 0},
        "text_mode": {"text_html": 0, "text": 0},
    }
    normalized_nodes: dict[str, Any] = {}
    source_ids = list(source_nodes.keys())
    id_map: dict[str, str] = {}
    for idx, src_id in enumerate(source_ids, start=1):
        id_map[src_id] = short_node_id(idx)

    for source_node_id, source in source_nodes.items():
        node_id = id_map[source_node_id]
        text_html = source.get("text_html")
        text = _pick_node_text(source)
        if isinstance(text_html, str) and text_html.strip():
            report["text_mode"]["text_html"] += 1
        else:
            report["text_mode"]["text"] += 1
        buttons_grid = source.get("buttons") or []
        transitions = source.get("transitions") or {}
        button_meta = source.get("button_meta") or []
        meta_by_pos: dict[tuple[int, int], dict[str, Any]] = {}
        for item in button_meta:
            if isinstance(item, dict):
                meta_by_pos[(int(item.get("row", 0)), int(item.get("col", 0)))] = item
        positional_keys = list(transitions.keys())
        used_transition_keys: set[str] = set()
        buttons: list[dict[str, Any]] = []

        for row_index, row in enumerate(buttons_grid):
            if not isinstance(row, list):
                continue
            for col_index, btn_text_raw in enumerate(row):
                btn_text = str(btn_text_raw)
                target, mapped_by = map_button_to_transition(
                    node_id=node_id,
                    button_text=btn_text,
                    row=row_index,
                    col=col_index,
                    button_meta_by_pos=meta_by_pos,
                    transitions=transitions,
                    positional_keys=positional_keys,
                    used_transition_keys=used_transition_keys,
                )
                report["mapped_by"][mapped_by] += 1
                if mapped_by in {"positional_fallback", "missing"}:
                    report["warnings"].append(f"{node_id}:{row_index},{col_index}:{mapped_by}")
                if not target:
                    continue
                target_value = str(target)
                button_type = "url" if target_value.startswith(("http://", "https://")) else "node"
                if button_type == "node":
                    target_value = id_map.get(target_value, target_value)
                buttons.append(
                    {
                        "id": short_button_id(),
                        "text": btn_text,
                        "type": button_type,
                        "target": target_value,
                        "row": row_index,
                        "sort": col_index,
                    }
                )

        normalized_nodes[node_id] = {"id": node_id, "text": text, "buttons": buttons, "media": None, "settings": _default_settings()}

    if isinstance(root_id, str) and root_id in id_map:
        root_id = id_map[root_id]
    if not isinstance(root_id, str) or root_id not in normalized_nodes:
        root_id = next(iter(normalized_nodes.keys()), "__error__")
        report["warnings"].append("Root missing in source; fallback applied")

    payload = {
        "metadata": {"real_root_id": root_id, "source_file": str(path)},
        "nodes": normalized_nodes,
        "import_report": report,
    }
    logger.info("Crawler graph imported", extra={"nodes_count": len(normalized_nodes), "root_id": root_id, "warnings": len(report["warnings"])})
    return payload
