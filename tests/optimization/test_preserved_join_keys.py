from types import SimpleNamespace

from tests.helpers.join_stubs import stub_join
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join
from trilogy.core.optimizations.join_upgrade import prune_preserved_join_keys


def _prune(base: str, joins: list[Join]) -> list[list[str]]:
    cte = SimpleNamespace(name="c", base_name=base, joins=joins)
    prune_preserved_join_keys(cte)  # type: ignore[arg-type]
    return [[p.node.name for p in j.pairs] for j in joins]


def test_narrowed_full_reads_key_off_base():
    joins = [
        stub_join("ret", JoinType.LEFT_OUTER, ["sales"]),
        stub_join("dim", JoinType.INNER, ["sales", "ret"]),
    ]
    assert _prune("sales", joins)[1] == ["sales"]


def test_full_join_keeps_every_padded_side():
    joins = [
        stub_join("ret", JoinType.FULL, ["sales"]),
        stub_join("dim", JoinType.LEFT_OUTER, ["sales", "ret"]),
    ]
    assert _prune("sales", joins)[1] == ["sales", "ret"]


def test_right_join_pads_everything_before_it():
    joins = [
        stub_join("b", JoinType.LEFT_OUTER, ["a"]),
        stub_join("c", JoinType.RIGHT_OUTER, ["a"]),
        stub_join("d", JoinType.LEFT_OUTER, ["a", "c"]),
    ]
    assert _prune("a", joins)[2] == ["c"]
