from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.services.fixer import ensure_error_node
from bot.services.importer import import_crawler_graph


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="bot/data/nodes.json")
    args = parser.parse_args()
    payload = import_crawler_graph(Path(args.input))
    ensure_error_node(payload["nodes"])
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"metadata": payload["metadata"], "nodes": payload["nodes"]}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Imported {len(payload['nodes'])} nodes -> {out_path}")


if __name__ == "__main__":
    main()
