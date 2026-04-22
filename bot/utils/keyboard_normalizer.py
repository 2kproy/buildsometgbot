from __future__ import annotations

from collections import defaultdict
from typing import Any


def normalize_rows(buttons: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    def _as_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    rows = sorted({_as_int(btn.get("row", 0)) for btn in buttons})
    row_map = {row: index for index, row in enumerate(rows)}
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for btn in buttons:
        normalized_row = row_map[_as_int(btn.get("row", 0))]
        grouped[normalized_row].append(btn)
    return [
        sorted(grouped[row], key=lambda item: (int(item.get("sort", 0)), str(item.get("id", ""))))
        for row in sorted(grouped)
    ]
