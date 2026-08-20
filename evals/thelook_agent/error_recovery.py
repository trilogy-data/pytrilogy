#!/usr/bin/env python
"""Add partial-bridge recovery metrics to a completed thelook eval report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ERROR_MARKERS = (
    "UnconstrainedPartialBridgeException",
    "every datasource relating them",
)
SECTION_MARKER = "<!-- partial-bridge-recovery -->"


def report_mtimes(results_dir: Path) -> dict[Path, int]:
    return {
        path.resolve(): path.stat().st_mtime_ns
        for path in results_dir.glob("*/report.json")
    }


def _events(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _error_index(events: list[dict[str, Any]]) -> int | None:
    for index, event in enumerate(events):
        if event.get("type") != "tool_result":
            continue
        result = str(event.get("result", ""))
        if (
            all(marker in result for marker in ERROR_MARKERS[1:])
            or ERROR_MARKERS[0] in result
        ):
            return index
    return None


def _attempted_pin(events: list[dict[str, Any]], after: int) -> bool:
    for event in events[after + 1 :]:
        if event.get("type") != "tool_call":
            continue
        text = json.dumps(event.get("arguments", {})).lower()
        if "is not null" not in text:
            continue
        if "order_item" in text or ("user" in text and "product" in text):
            return True
    return False


def _prompts(eval_dir: Path) -> dict[int, dict[str, Any]]:
    entries = json.loads((eval_dir / "query_prompts.json").read_text(encoding="utf-8"))
    return {int(entry["id"]): entry for entry in entries}


def build_metrics(run_dir: Path) -> dict[str, Any]:
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    prompts = _prompts(run_dir.parents[1])
    statuses = {int(query["id"]): query["status"] for query in report["queries"]}
    per_question: list[dict[str, Any]] = []
    for query_id, status in sorted(statuses.items()):
        events = _events(run_dir / f"agent_log.q{query_id:02d}.jsonl")
        error_index = _error_index(events)
        encountered = error_index is not None
        attempted_pin = (
            _attempted_pin(events, error_index) if error_index is not None else False
        )
        converged = status == "pass"
        kind = prompts.get(query_id, {}).get("kind", "unspecified")
        per_question.append(
            {
                "id": query_id,
                "kind": kind,
                "encountered_partial_bridge_error": encountered,
                "converged": converged,
                "recovered_after_error": encountered and converged,
                "attempted_population_pin_after_error": attempted_pin,
                "misread_message": encountered and not converged and not attempted_pin,
                "status": status,
            }
        )

    encountered_rows = [
        row for row in per_question if row["encountered_partial_bridge_error"]
    ]
    recovered = sum(row["recovered_after_error"] for row in encountered_rows)
    expected = [row for row in per_question if row["kind"] == "error_triggering"]
    return {
        "encountered_count": len(encountered_rows),
        "recovered_count": recovered,
        "recovery_ratio": (
            round(recovered / len(encountered_rows), 3) if encountered_rows else None
        ),
        "expected_trigger_count": len(expected),
        "expected_trigger_encounter_count": sum(
            row["encountered_partial_bridge_error"] for row in expected
        ),
        "per_question": per_question,
    }


def _markdown(metrics: dict[str, Any]) -> str:
    ratio = metrics["recovery_ratio"]
    ratio_text = "n/a" if ratio is None else f"{ratio * 100:.0f}%"
    lines = [
        SECTION_MARKER,
        "## Partial-bridge error recovery",
        "",
        (
            f"- Recovered after seeing the error: {metrics['recovered_count']}/"
            f"{metrics['encountered_count']} ({ratio_text})"
        ),
        (
            "- Expected trigger questions that exposed the error: "
            f"{metrics['expected_trigger_encounter_count']}/"
            f"{metrics['expected_trigger_count']}"
        ),
        "",
        "| Question | Kind | Saw error | Correct | Pin attempted | Misread message |",
        "|---:|---|:---:|:---:|:---:|:---:|",
    ]
    for row in metrics["per_question"]:
        mark = lambda value: "yes" if value else "no"
        lines.append(
            f"| {row['id']} | {row['kind']} | "
            f"{mark(row['encountered_partial_bridge_error'])} | "
            f"{mark(row['converged'])} | "
            f"{mark(row['attempted_population_pin_after_error'])} | "
            f"{mark(row['misread_message'])} |"
        )
    return "\n".join(lines) + "\n"


def enrich_report(run_dir: Path) -> None:
    report_path = run_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    metrics = build_metrics(run_dir)
    report["partial_bridge_recovery"] = metrics
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    markdown_path = run_dir / "report.md"
    current = (
        markdown_path.read_text(encoding="utf-8") if markdown_path.exists() else ""
    )
    current = current.split(SECTION_MARKER, 1)[0].rstrip()
    markdown_path.write_text(f"{current}\n\n{_markdown(metrics)}", encoding="utf-8")
    print(
        "  partial bridge recovery: "
        f"{metrics['recovered_count']}/{metrics['encountered_count']} -> {run_dir.name}"
    )


def enrich_changed_reports(results_dir: Path, before: dict[Path, int]) -> None:
    for path in sorted(results_dir.glob("*/report.json")):
        resolved = path.resolve()
        if before.get(resolved) == path.stat().st_mtime_ns:
            continue
        enrich_report(path.parent)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path, help="completed run directory")
    args = parser.parse_args()
    enrich_report(args.run.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
