from types import SimpleNamespace

from tests.helpers.join_stubs import stub_join
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import CTE

KEY = "local.k"


def _sources(jointype: JoinType, lefts: list[str]) -> set[str]:
    join = stub_join("c", jointype, lefts, KEY)
    cte = SimpleNamespace(joins=[join], get_concept=lambda address: None)
    return CTE.inner_join_key_sources(cte, KEY)  # type: ignore[arg-type]


def test_inner_join_equates_both_sides():
    assert _sources(JoinType.INNER, ["a"]) == {"a", "c"}


def test_coalesced_left_sides_are_not_each_the_key():
    assert _sources(JoinType.INNER, ["a", "b"]) == {"c"}


def test_outer_join_equates_nothing():
    assert _sources(JoinType.LEFT_OUTER, ["a"]) == set()
