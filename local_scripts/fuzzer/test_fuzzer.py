from __future__ import annotations

import json
from pathlib import Path

from local_scripts.fuzzer.generate import generate_cases
from local_scripts.fuzzer.models import FuzzCase
from local_scripts.fuzzer.random_data import generate_random_seeds, random_seed
from local_scripts.fuzzer.runner import CaseOutcome, CaseResult, write_repro


def test_generated_corpus_is_stable_and_covers_requested_families() -> None:
    first = generate_cases()
    second = generate_cases()

    assert first == second
    assert len(first) == 174
    assert len({case.case_id for case in first}) == len(first)
    assert all(
        "where " not in case.trilogy.lower() or "where" in case.tags for case in first
    )
    assert {case.seed for case in first} == {"edge", "dense"}
    assert {case.family for case in first} == {
        "aggregate",
        "chasm",
        "composite_join",
        "coalescing_presence",
        "derived_join",
        "derived_rowset_base_where",
        "function",
        "grouping",
        "having",
        "independent_rowset_join",
        "join",
        "membership",
        "multiway_join",
        "named_grouping_window",
        "rowset",
        "rowset_boundary",
        "scalar",
        "union",
        "window",
        "where_complex",
    }
    tags = {tag for case in first for tag in case.tags}
    assert {
        "aggregate",
        "chasm",
        "fanout",
        "function",
        "grouping",
        "having",
        "membership",
        "nullable",
        "not",
        "rowset",
        "subset",
        "union",
        "where",
        "window",
    } <= tags


def test_random_datasets_are_repeatable_and_preserve_domain_invariants() -> None:
    first = random_seed(1234, 30)
    second = random_seed(1234, 30)

    assert first == second
    assert first != random_seed(1235, 30)
    assert len(first.events.rows) == 30
    nullable_values = {row[3] for row in first.events.rows}
    active_values = {row[4] for row in first.events.rows}
    assert None in nullable_values
    assert any(value is not None for value in nullable_values)
    assert active_values == {False, True}
    assert first.events.rows[2][1:3] == first.events.rows[3][1:3]
    assert first.events.rows[2][0] != first.events.rows[3][0]

    left_keys = {row[1] for row in first.left_facts.rows}
    subset_keys = {row[1] for row in first.subset_facts.rows}
    union_keys = {row[1] for row in first.union_facts.rows}
    assert subset_keys <= left_keys
    assert union_keys - left_keys

    seeds = generate_random_seeds(3, 2000, 16)
    assert [seed.name for seed in seeds] == [
        "random_002000",
        "random_002001",
        "random_002002",
    ]
    assert len(generate_cases(seeds)) == 261


def test_repro_contains_standalone_program_and_diagnostics(tmp_path: Path) -> None:
    case = FuzzCase(
        case_id="seed__family__case",
        seed="seed",
        family="family",
        description="description",
        tags=("tag",),
        trilogy="key id int;\nselect id;\n",
        oracle_sql="select 1",
    )
    result = CaseResult(
        case_id=case.case_id,
        seed=case.seed,
        family=case.family,
        tags=case.tags,
        status="mismatch",
        oracle_ms=1,
        compile_ms=2,
        execute_ms=3,
        sql_chars=8,
        sql_bytes=8,
        expected_rows=1,
        actual_rows=1,
    )
    outcome = CaseOutcome(result, [(1,)], [(2,)], "select 2")

    repro = write_repro(case, outcome, tmp_path)

    assert repro.read_text(encoding="utf-8") == case.trilogy
    assert (repro.parent / "oracle.sql").read_text(encoding="utf-8") == "select 1\n"
    assert (repro.parent / "generated.sql").read_text(encoding="utf-8") == (
        "select 2\n"
    )
    metadata = json.loads((repro.parent / "result.json").read_text(encoding="utf-8"))
    assert metadata["expected"] == [[1]]
    assert metadata["actual"] == [[2]]
    assert metadata["repro"] == str(repro)
