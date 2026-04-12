from __future__ import annotations

from pathlib import Path

import pytest

from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import parse_text as parse_text_v1
from trilogy.parsing.parse_engine_v2 import parse_text as parse_text_v2
from trilogy.parsing.v2.hydration import UnsupportedSyntaxError

working_path = Path(__file__).parent

QUERY_CASES: list[tuple[int, str]] = [
    (1, "query01.preql"),
    (2, "query02.preql"),
    (3, "query03.preql"),
    (6, "query06.preql"),
    (7, "query07.preql"),
    (8, "query08.preql"),
    (10, "query10.preql"),
    (12, "query12.preql"),
    (15, "query15.preql"),
    (16, "query16.preql"),
    (20, "query20.preql"),
    (21, "query21.preql"),
    (22, "query22.preql"),
    (24, "query24.preql"),
    (25, "query25.preql"),
    (26, "query26.preql"),
    (30, "query30.preql"),
    (32, "query32.preql"),
    (42, "query42.preql"),
    (55, "query55.preql"),
    (95, "query95.preql"),
    (97, "query97.preql"),
    (98, "query98.preql"),
    (99, "query99.preql"),
]

QUERY_IDS = [f"q{idx:02d}" for idx, _ in QUERY_CASES]


def _read_query(filename: str) -> str:
    with open(working_path / filename) as f:
        return f.read()


@pytest.fixture(scope="session")
def comparison_summary() -> dict[str, str]:
    results: dict[str, str] = {}
    yield results
    passed = sum(1 for v in results.values() if v == "match")
    unsupported = sum(1 for v in results.values() if v == "unsupported")
    failed = sum(1 for v in results.values() if v.startswith("mismatch"))
    errored = sum(1 for v in results.values() if v.startswith("error"))
    print("\n--- Parser v2 Comparison Summary ---")
    print(
        f"  match: {passed}  unsupported: {unsupported}  mismatch: {failed}  error: {errored}"
    )
    for label, status in sorted(results.items()):
        print(f"  {label}: {status}")


@pytest.mark.parametrize("idx,filename", QUERY_CASES, ids=QUERY_IDS)
def test_v2_parse_status(idx: int, filename: str) -> None:
    text = _read_query(filename)
    try:
        env = Environment(working_path=working_path)
        parse_text_v2(text, environment=env)
    except UnsupportedSyntaxError as e:
        pytest.xfail(f"v2 unsupported: {e}")
    except ImportError as e:
        if "parsing error" in str(e):
            pytest.xfail(f"v2 unsupported import: {e}")
        raise


@pytest.mark.parametrize("idx,filename", QUERY_CASES, ids=QUERY_IDS)
def test_v2_vs_v1_structural(
    idx: int, filename: str, comparison_summary: dict[str, str]
) -> None:
    label = f"q{idx:02d}"
    text = _read_query(filename)
    try:
        env_v2 = Environment(working_path=working_path)
        _, output_v2 = parse_text_v2(text, environment=env_v2)
    except UnsupportedSyntaxError:
        comparison_summary[label] = "unsupported"
        pytest.skip("v2 does not support this query yet")
        return
    except ImportError as e:
        if "parsing error" in str(e):
            comparison_summary[label] = "unsupported"
            pytest.skip(f"v2 import not supported: {e}")
            return
        comparison_summary[label] = "error:v2:ImportError"
        raise
    except Exception as e:
        comparison_summary[label] = f"error:v2:{type(e).__name__}"
        raise

    try:
        env_v1 = Environment(working_path=working_path)
        _, output_v1 = parse_text_v1(text, environment=env_v1)
    except Exception as e:
        comparison_summary[label] = f"error:v1:{type(e).__name__}"
        raise

    types_v1 = [type(item).__name__ for item in output_v1]
    types_v2 = [type(item).__name__ for item in output_v2]
    concepts_v1 = set(env_v1.concepts.keys())
    concepts_v2 = set(env_v2.concepts.keys())

    mismatches: list[str] = []
    if types_v1 != types_v2:
        mismatches.append(f"output types differ: v1={types_v1} v2={types_v2}")
    if concepts_v1 != concepts_v2:
        only_v1 = concepts_v1 - concepts_v2
        only_v2 = concepts_v2 - concepts_v1
        parts: list[str] = []
        if only_v1:
            parts.append(f"only in v1: {only_v1}")
        if only_v2:
            parts.append(f"only in v2: {only_v2}")
        mismatches.append(f"concept keys differ: {'; '.join(parts)}")

    if mismatches:
        comparison_summary[label] = f"mismatch: {'; '.join(mismatches)}"
        pytest.fail("\n".join(mismatches))
    else:
        comparison_summary[label] = "match"
