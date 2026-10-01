from types import SimpleNamespace

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join
from trilogy.core.optimizations.preserved_join_keys import prune_preserved_join_keys


def _pair(side: str, address: str = "k") -> SimpleNamespace:
    return SimpleNamespace(
        cte=SimpleNamespace(name=side),
        left=SimpleNamespace(address=address),
        right=SimpleNamespace(address=address),
    )


def _join(right: str, jointype: JoinType, lefts: list[str]) -> Join:
    return Join(
        right_cte=SimpleNamespace(name=right),  # type: ignore[arg-type]
        jointype=jointype,
        joinkey_pairs=[_pair(n) for n in lefts],  # type: ignore[misc]
    )


def _prune(base: str, joins: list[Join]) -> list[list[str]]:
    cte = SimpleNamespace(name="c", base_name=base, joins=joins)
    prune_preserved_join_keys(cte)  # type: ignore[arg-type]
    return [[p.cte.name for p in j.joinkey_pairs] for j in joins]


def test_narrowed_full_reads_key_off_base():
    joins = [
        _join("ret", JoinType.LEFT_OUTER, ["sales"]),
        _join("dim", JoinType.INNER, ["sales", "ret"]),
    ]
    assert _prune("sales", joins)[1] == ["sales"]


def test_full_join_keeps_every_padded_side():
    joins = [
        _join("ret", JoinType.FULL, ["sales"]),
        _join("dim", JoinType.LEFT_OUTER, ["sales", "ret"]),
    ]
    assert _prune("sales", joins)[1] == ["sales", "ret"]


def test_right_join_pads_everything_before_it():
    joins = [
        _join("b", JoinType.LEFT_OUTER, ["a"]),
        _join("c", JoinType.RIGHT_OUTER, ["a"]),
        _join("d", JoinType.LEFT_OUTER, ["a", "c"]),
    ]
    assert _prune("a", joins)[2] == ["c"]
