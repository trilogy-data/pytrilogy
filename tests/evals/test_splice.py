"""Splicing-equivalence tests.

The core invariant: re-running a subset of queries and splicing the rest from a
prior run must reconstruct the SAME full-benchmark report — pass count AND the
aggregate `agent` harness metrics (tokens/iterations/tool stats that feed the
charts) — as a single run of all queries. A regression here silently truncates
the dashboard aggregates to the freshly-run subset (the bug this guards).
"""

from __future__ import annotations

from evals.common import scoring
from evals.common.main import _splice_prior_results


def _metrics(qid: int) -> dict:
    """Deterministic per-query metrics so a merged report is exactly checkable."""
    return {
        "id": qid,
        **scoring.metrics_to_dict(
            scoring.AgentMetrics(
                iterations=qid,
                tool_calls_total=2 * qid,
                tool_results_total=2 * qid,
                tool_errors=0,
                prompt_tokens=100 * qid,
                completion_tokens=qid,
                total_tokens=101 * qid,
                tool_calls_by_name={"trilogy": 2 * qid},
                trilogy_subcommands={"run": qid, "explore": qid},
                tool_output_stats={
                    "trilogy": scoring.ToolOutputStats(
                        count=2 * qid, truncated=0, total_chars=10 * qid, max_chars=qid
                    )
                },
            )
        ),
    }


def _full_report(ids: list[int]) -> dict:
    """A synthetic full-benchmark report (all queries pass) for ``ids``."""
    pqm = [_metrics(i) for i in ids]
    agg = scoring.aggregate_metrics([scoring.metrics_from_dict(m) for m in pqm])
    return {
        "meta": {"num_queries": len(ids)},
        "agent": {
            "exit_code": 0,
            "timed_out": False,
            "duration_seconds": float(sum(ids)),
            "wall_duration_seconds": float(sum(ids)),
            "avg_query_seconds": 0.0,
            "iterations": agg.iterations,
            "tokens": {
                "prompt": agg.prompt_tokens,
                "completion": agg.completion_tokens,
                "total": agg.total_tokens,
            },
        },
        "queries": [{"id": i, "status": "pass"} for i in ids],
        "per_query": [
            {"id": i, "exit_code": 0, "duration_seconds": float(i)} for i in ids
        ],
        "per_query_metrics": pqm,
        "summary": {"pass_count": len(ids), "pass_rate": 1.0},
    }


def _fresh_subset(full: dict, fresh_ids: set[int]) -> dict:
    """The report a run that freshly executed only ``fresh_ids`` would produce."""
    return {
        "meta": dict(full["meta"]),
        "agent": dict(full["agent"]),
        "queries": [q for q in full["queries"] if q["id"] in fresh_ids],
        "per_query": [r for r in full["per_query"] if r["id"] in fresh_ids],
        "per_query_metrics": [
            m for m in full["per_query_metrics"] if m["id"] in fresh_ids
        ],
        "summary": {},
    }


def test_splice_reconstructs_full_aggregate(tmp_path):
    ids = list(range(1, 11))
    full = _full_report(ids)
    fresh_ids = {2, 5, 8}

    fresh = _fresh_subset(full, fresh_ids)
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    merged = _splice_prior_results(fresh, full, fresh_ids, prior_dir)

    assert merged["meta"]["num_queries"] == len(ids)
    assert merged["summary"]["pass_count"] == len(ids)
    # Aggregate agent metrics must match the single-run full report exactly.
    assert merged["agent"]["tokens"]["total"] == full["agent"]["tokens"]["total"]
    assert merged["agent"]["iterations"] == full["agent"]["iterations"]
    assert len(merged["per_query_metrics"]) == len(ids)
    # Durations sum over the full set, not the fresh subset.
    assert merged["agent"]["duration_seconds"] == float(sum(ids))


def test_splice_metrics_independent_of_fresh_subset(tmp_path):
    """The merged aggregate is invariant to WHICH ids were rerun (given identical
    per-query results) — it always equals the full single-run aggregate."""
    ids = list(range(1, 11))
    full = _full_report(ids)
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()

    totals = set()
    for fresh_ids in ({1}, {1, 2, 3}, set(ids), {4, 9}):
        fresh = _fresh_subset(full, fresh_ids)
        merged = _splice_prior_results(fresh, full, set(fresh_ids), prior_dir)
        totals.add(merged["agent"]["tokens"]["total"])
        assert merged["agent"]["iterations"] == full["agent"]["iterations"]
    assert totals == {full["agent"]["tokens"]["total"]}


def test_splice_falls_back_to_reparsing_logs(tmp_path):
    """A prior report predating per_query_metrics still yields the full aggregate
    by re-parsing the prior run's agent_log JSONL for the spliced ids."""
    ids = [1, 2, 3]
    full = _full_report(ids)
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    # Emit a minimal JSONL log per query whose parsed metrics are non-trivial,
    # then strip per_query_metrics so splice must re-parse.
    for i in ids:
        lines = [
            '{"type": "session_start"}',
            '{"type": "tool_call", "name": "trilogy", "arguments": {"args": ["run"]}}',
            '{"type": "tool_result", "name": "trilogy", "result": "exit_code: 0"}',
            f'{{"type": "llm_response", "usage": {{"total_tokens": {1000 * i}}}}}',
        ]
        (prior_dir / f"agent_log.q{i:02d}.jsonl").write_text(
            "\n".join(lines), encoding="utf-8"
        )
    prior_no_pqm = dict(full)
    prior_no_pqm.pop("per_query_metrics")

    fresh_ids = {2}
    fresh = _fresh_subset(full, fresh_ids)
    merged = _splice_prior_results(fresh, prior_no_pqm, fresh_ids, prior_dir)

    assert merged["meta"]["num_queries"] == len(ids)
    assert len(merged["per_query_metrics"]) == len(ids)
    # Spliced ids (1, 3) contribute the re-parsed 1000*i tokens; fresh id 2 keeps
    # its own metrics (202). Assert the spliced contributions are present.
    by_id = {m["id"]: m for m in merged["per_query_metrics"]}
    assert by_id[1]["total_tokens"] == 1000
    assert by_id[3]["total_tokens"] == 3000
    assert by_id[2]["total_tokens"] == 101 * 2
