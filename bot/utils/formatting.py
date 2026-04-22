from __future__ import annotations

from typing import Any


def format_validation(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Validation report:",
        f"- nodes: {summary['total_nodes']}",
        f"- buttons: {summary['total_buttons']}",
        f"- broken links: {summary['broken_links']}",
        f"- orphans: {summary['orphans']}",
        f"- unreachable: {summary['unreachable']}",
    ]
    if report["errors"]:
        lines.append("Errors:")
        lines.extend(f"- {err}" for err in report["errors"][:30])
    if report["warnings"]:
        lines.append("Warnings:")
        lines.extend(f"- {warn}" for warn in report["warnings"][:30])
    return "\n".join(lines)

