from tests.optimization.test_join_upgrade import _build_concept, _build_cte
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import ConceptPair, Join, UnnestJoin
from trilogy.core.optimizations.value_set_join_upgrade import _lefts_alone_before

KEY = _build_concept("KEY")
OTHER = _build_concept("OTHER")


def _onto(right, *lefts, keys=(KEY,)) -> Join:
    return Join(
        join_type=JoinType.FULL,
        right=right,
        pairs=[
            ConceptPair(left=key, right=key, node=left)
            for left in lefts
            for key in keys
        ],
    )


def test_members_joined_before_their_anchor_are_every_entering_row():
    web, cat, store, dim = (
        _build_cte(name, [KEY, OTHER]) for name in ("web", "cat", "store", "dim")
    )
    cte = _build_cte("root", [KEY])
    target = _onto(store, cat, web)
    cte.joins = [_onto(web, cat), target]
    assert _lefts_alone_before(cte, target)

    cte.joins = [_onto(dim, cat), _onto(web, cat), target]
    assert not _lefts_alone_before(cte, target)

    two_keys = _onto(store, cat, web, keys=(KEY, OTHER))
    cte.joins = [_onto(web, cat), two_keys]
    assert not _lefts_alone_before(cte, two_keys)

    unnest = UnnestJoin(concepts=[OTHER], parent=None, alias="u")  # type: ignore[arg-type]
    cte.joins = [unnest, target]
    assert not _lefts_alone_before(cte, target)
