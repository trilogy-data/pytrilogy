"""Unit tests for the viewer's Launch screen command builder.

The UI's only job is to produce a ``run_eval.py`` command line, so these tests
pin the flag mapping (categories, question scope, knobs) and the validation
that stops a bad form before it spends a database build. Suite discovery is
tested too: a spec.py that imports a sibling module used to be dropped from the
picker silently, which took TPC-DS - the main benchmark - offline."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from viewer.launch import _parse_ids, build_command, launch_options
from viewer.suites import discover_suites


@pytest.fixture(scope="module")
def suites():
    return discover_suites()


@pytest.fixture(scope="module")
def tpcds(suites):
    return suites["tpcds"]


def test_tpcds_is_discoverable(suites):
    assert "tpcds" in suites
    assert suites["tpcds"].spec.default_num_queries == 99


def test_full_funnel_over_every_question(tpcds):
    argv, label = build_command(
        tpcds,
        {
            "categories": ["sql_bare", "sql_schema", "ingest", "enriched"],
            "num_queries": 99,
            "scale_factor": 1,
            "provider": "deepseek",
            "model": "deepseek-v4-flash",
            "concurrency": 2,
        },
    )
    tail = argv[2:]
    assert tail[:4] == [
        "--categories",
        "sql_bare,sql_schema,ingest,enriched",
        "--num-queries",
        "99",
    ]
    assert "--scale-factor" in tail and tail[tail.index("--scale-factor") + 1] == "1"
    assert tail[tail.index("--concurrency") + 1] == "2"
    assert argv[1].endswith("run_eval.py")
    assert "first 99" in label


def test_single_category_uses_the_singular_flag(tpcds):
    argv, _ = build_command(tpcds, {"categories": ["enriched"]})
    assert "--category" in argv and "--categories" not in argv


def test_query_ids_override_the_count_and_default_to_no_splice(tpcds):
    argv, label = build_command(
        tpcds, {"categories": ["enriched"], "query_ids": "5,13,18, 20-22"}
    )
    assert argv[argv.index("--query-ids") + 1] == "5,13,18,20,21,22"
    assert argv[argv.index("--splice-from") + 1] == "none"
    assert "--num-queries" not in argv
    assert "6 questions" in label


def test_splice_opt_in_keeps_the_default_behaviour(tpcds):
    argv, _ = build_command(
        tpcds, {"categories": ["enriched"], "query_ids": "5", "splice": True}
    )
    assert "--splice-from" not in argv


def test_reasoning_effort_and_todo_are_opt_in(tpcds):
    plain, _ = build_command(tpcds, {"categories": ["enriched"]})
    assert "--reasoning-effort" not in plain and "--enable-todo" not in plain
    argv, _ = build_command(
        tpcds,
        {"categories": ["enriched"], "reasoning_effort": "max", "enable_todo": True},
    )
    assert argv[argv.index("--reasoning-effort") + 1] == "max"
    assert "--enable-todo" in argv


def test_warehouse_variants_are_selectable(tpcds):
    argv, _ = build_command(
        tpcds, {"categories": ["sql_schema_noise_x4", "enriched_noise"]}
    )
    assert argv[argv.index("--categories") + 1] == "sql_schema_noise_x4,enriched_noise"


@pytest.mark.parametrize(
    "form, message",
    [
        ({"categories": []}, "at least one category"),
        ({"categories": ["nope"]}, "unknown categories"),
        ({"categories": ["enriched"], "query_ids": "500"}, r"no question\(s\) \[500\]"),
        ({"categories": ["enriched"], "query_ids": "abc"}, "bad query id"),
        ({"categories": ["enriched"], "num_queries": "ten"}, "must be a number"),
    ],
)
def test_bad_forms_are_rejected(tpcds, form, message):
    with pytest.raises(ValueError, match=message):
        build_command(tpcds, form)


def test_parse_ids_dedupes_and_sorts():
    assert _parse_ids("3, 1 2-4") == [1, 2, 3, 4]
    assert _parse_ids("") == []
    with pytest.raises(ValueError):
        _parse_ids("9-2")


def test_options_default_to_the_shared_funnel_not_the_display_order(suites):
    payload = launch_options(suites)
    tpcds = next(s for s in payload["suites"] if s["key"] == "tpcds")
    # funnel_order on TPC-DS lists every warehouse variant; the form must not
    # preselect 25 legs.
    assert tpcds["base"] == [
        "sql_bare",
        "sql_schema",
        "ingest",
        "enriched",
        "enriched_docs",
    ]
    assert len(tpcds["categories"]) > len(tpcds["base"])
    assert len(tpcds["query_ids"]) == 99
    assert tpcds["runnable"]
    assert {p["key"] for p in payload["providers"]} >= {"deepseek", "anthropic"}
    # Key presence is advertised, never the key itself.
    assert all(set(p) == {"key", "env", "configured"} for p in payload["providers"])
