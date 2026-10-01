from types import SimpleNamespace

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import CTE, Join

KEY = "local.k"


def _pair(side: str) -> SimpleNamespace:
    concept = SimpleNamespace(address=KEY, pseudonyms=set())
    return SimpleNamespace(cte=SimpleNamespace(name=side), left=concept, right=concept)


def _sources(jointype: JoinType, lefts: list[str]) -> set[str]:
    join = Join(
        right_cte=SimpleNamespace(name="c"),  # type: ignore[arg-type]
        jointype=jointype,
        joinkey_pairs=[_pair(n) for n in lefts],  # type: ignore[misc]
    )
    cte = SimpleNamespace(joins=[join], get_concept=lambda address: None)
    return CTE.inner_join_key_sources(cte, KEY)  # type: ignore[arg-type]


def test_inner_join_equates_both_sides():
    assert _sources(JoinType.INNER, ["a"]) == {"a", "c"}


def test_coalesced_left_sides_are_not_each_the_key():
    assert _sources(JoinType.INNER, ["a", "b"]) == {"c"}


def test_outer_join_equates_nothing():
    assert _sources(JoinType.LEFT_OUTER, ["a"]) == set()
