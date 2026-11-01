import os
import re
from functools import cache

import pytest

from tests.modeling._row_compare import values_match
from tests.modeling.tree_reporting import harness, reference
from tests.modeling.tree_reporting.expectations import SERVED, WRONG_ANSWERS

# The default run: both pages' default views for Boston and all cities, the
# nativeness and species cross-filters, and a species/cultivar selection.
# TREE_REPORTING_FULL_CATALOG=1 runs every extracted case (every cross-filter
# dimension, both views), about four times as long.
DEFAULT_GROUPS = {
    ("USBOS", "base"),
    ("ALL", "base"),
    ("USBOS", "cross_native_locality_bucket"),
    ("USBOS", "cross_species"),
    ("ALL", "cross_species"),
    ("USBOS", "species_selection"),
}
FULL = os.environ.get("TREE_REPORTING_FULL_CATALOG") == "1"
CASES = {
    f"{case['city']} {case['id']}": case
    for case in harness.QUERIES["cases"]
    if FULL or (case["city"], case["group"]) in DEFAULT_GROUPS
}


def unordered_limit(query: str) -> bool:
    """`LIMIT` with no `ORDER BY` picks arbitrary rows; only the count is comparable."""
    return bool(re.search(r"\blimit\b", query, re.IGNORECASE)) and not re.search(
        r"\border\s+by\b", query, re.IGNORECASE
    )


@cache
def compiled(key: str, summaries: bool) -> str:
    return harness.generate_sql(CASES[key], summaries)


def sort_key(row: tuple) -> tuple:
    return tuple(
        (
            (0, round(float(v), 6))
            if isinstance(v, (int, float)) and not isinstance(v, bool)
            else (1, repr(v))
        )
        for v in row
    )


def same_rows(actual: list[tuple], expected: list[tuple]) -> bool:
    if len(actual) != len(expected):
        return False
    pairs = zip(sorted(actual, key=sort_key), sorted(expected, key=sort_key))
    return all(
        len(a) == len(b) and all(values_match(x, y) for x, y in zip(a, b))
        for a, b in pairs
    )


def answer_params() -> list:
    return [
        pytest.param(
            key,
            id=key,
            marks=(
                [pytest.mark.xfail(strict=True, reason=WRONG_ANSWERS[key])]
                if key in WRONG_ANSWERS
                else []
            ),
        )
        for key in CASES
    ]


@pytest.mark.parametrize("key", answer_params())
def test_dashboard_answer(key: str) -> None:
    """Plans, runs in DuckDB, and answers as the trees do with the count summaries removed."""
    case = CASES[key]
    records = harness.execute_records(compiled(key, True))
    rows = [tuple(record.values()) for record in records]
    oracle = harness.execute(compiled(key, False))
    if unordered_limit(case["query"]):
        assert len(rows) == len(oracle)
    else:
        assert same_rows(
            rows, oracle
        ), f"summary-served:\n{sorted(rows, key=sort_key)}\ntree rows:\n{sorted(oracle, key=sort_key)}"
    assert (
        reference.check(case, harness.context(case["city"])["parameters"], records)
        is None
    )


@pytest.mark.parametrize("key", list(CASES))
def test_dashboard_served_from(key: str) -> None:
    """Pins which file a chart reads: a summary-served chart that falls back to the trees fails here."""
    assert harness.served_from(compiled(key, True)) == SERVED.get(key, "summary")


@pytest.mark.parametrize("key", [key for key in CASES if "tree-dot-map" in key])
def test_dot_map_cells_hold_every_positioned_tree(key: str) -> None:
    """The positioned cells alone, so a fan-out (#829: 13 trees where there are 9) fails even where the NULL cell is xfailed."""
    case = CASES[key]
    records = [
        r
        for r in harness.execute_records(compiled(key, True))
        if next(iter(r.values())) is not None
    ]
    assert (
        reference.check(case, harness.context(case["city"])["parameters"], records)
        is None
    )


def test_pins_name_extracted_cases() -> None:
    known = {f"{case['city']} {case['id']}" for case in harness.QUERIES["cases"]}
    assert set(SERVED) - known == set()
    assert set(WRONG_ANSWERS) - known == set()
