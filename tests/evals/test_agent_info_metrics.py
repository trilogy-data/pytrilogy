from __future__ import annotations

import json
from pathlib import Path

from evals.common import scoring


def write_events(path: Path, events: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8"
    )


def tool_call(*args: str) -> dict:
    return {
        "type": "tool_call",
        "name": "trilogy",
        "arguments": {"args": list(args)},
    }


def test_parse_agent_info_consecutive_drilldown_trajectory(tmp_path: Path) -> None:
    log = tmp_path / "agent.jsonl"
    write_events(
        log,
        [
            tool_call("agent-info"),
            tool_call("agent-info", "authoring"),
            tool_call("agent-info", "syntax", "example", "python-datasource"),
            tool_call("file", "write", "answer.py"),
            tool_call("agent-info"),
            tool_call("run", "answer.preql"),
        ],
    )

    metrics = scoring.parse_agent_log(log)

    assert metrics.agent_info_sequences == [
        ["index", "authoring", "syntax example python-datasource"],
        ["index"],
    ]
    assert metrics.agent_info_transitions == {
        "index -> authoring": 1,
        "authoring -> syntax example python-datasource": 1,
    }
    assert metrics.agent_info_directory_calls == 2
    assert metrics.agent_info_directory_followups == 1


def test_agent_info_metrics_round_trip_and_aggregate() -> None:
    metrics = scoring.AgentMetrics(
        agent_info_sequences=[["index", "query"]],
        agent_info_transitions={"index -> query": 1},
        agent_info_directory_calls=1,
        agent_info_directory_followups=1,
    )

    restored = scoring.metrics_from_dict(scoring.metrics_to_dict(metrics))
    aggregate = scoring.aggregate_metrics([restored, restored])

    assert aggregate.agent_info_sequences == [
        ["index", "query"],
        ["index", "query"],
    ]
    assert aggregate.agent_info_transitions == {"index -> query": 2}
    assert aggregate.agent_info_directory_calls == 2
    assert aggregate.agent_info_directory_followups == 2
