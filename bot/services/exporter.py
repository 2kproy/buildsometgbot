from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def export_graph(payload: dict[str, Any], out_path: Path) -> Path:
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

