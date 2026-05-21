from trilogy.constants import MagicConstants
from trilogy.core.enums import ComparisonOperator, JoinType, Modifier, Purpose
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import (
    CTE,
    BuildDatasource,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.null_safe_join import (
    SimplifyNullSafeJoins,
    _join_pads_null,
    _proven_non_null,
)


def _build_concept(name: str):
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _build_cte(name: str, columns, nullable=None):
    ds = BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=c.name, concept=c) for c in columns],
        address=name,
        namespace="test",
        grain=BuildGrain(),
    )
    cte = CTE.from_datasource(ds)
    cte.name = name
    cte.nullable_concepts = list(nullable or [])
    return cte


def _root_with_join(jointype: JoinType, left_nullable, right_nullable):
    key = _build_concept("KEY")
    left_cte = _build_cte("left_src", [key], nullable=left_nullable and [key])
    right_cte = _build_cte("right_src", [key], nullable=right_nullable and [key])
    root = _build_cte("root", [key])
    root.parent_ctes = [left_cte, right_cte]
    root.joins = [
        Join(
            jointype=jointype,
            right_cte=right_cte,
            modifiers=[Modifier.NULLABLE],
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=left_cte.source,
                    cte=left_cte,
                    modifiers=[Modifier.NULLABLE],
                )
            ],
        )
    ]
    return root, key


def test_proven_non_null_helper():
    key = _build_concept("KEY")
    clean = _build_cte("clean", [key])
    dirty = _build_cte("dirty", [key], nullable=[key])
    missing = _build_cte("missing", [_build_concept("OTHER")])
    assert _proven_non_null(key, clean) is True
    assert _proven_non_null(key, dirty) is False
    # not sourced from the cte → cannot prove
    assert _proven_non_null(key, missing) is False


def test_inner_join_stripped_when_a_side_non_null():
    root, _ = _root_with_join(JoinType.INNER, left_nullable=True, right_nullable=False)
    changed, _ = SimplifyNullSafeJoins().optimize(root, {})
    assert changed
    join = root.joins[0]
    assert Modifier.NULLABLE not in join.modifiers
    assert Modifier.NULLABLE not in join.joinkey_pairs[0].modifiers


def test_full_join_preserves_null_safe_form():
    """OUTER align wants IS NOT DISTINCT FROM — never touched."""
    root, _ = _root_with_join(JoinType.FULL, left_nullable=False, right_nullable=False)
    changed, _ = SimplifyNullSafeJoins().optimize(root, {})
    assert not changed
    join = root.joins[0]
    assert Modifier.NULLABLE in join.modifiers
    assert Modifier.NULLABLE in join.joinkey_pairs[0].modifiers


def test_inner_kept_when_both_sides_nullable():
    root, _ = _root_with_join(JoinType.INNER, left_nullable=True, right_nullable=True)
    changed, _ = SimplifyNullSafeJoins().optimize(root, {})
    assert not changed
    assert Modifier.NULLABLE in root.joins[0].joinkey_pairs[0].modifiers


def test_proven_non_null_via_cte_condition():
    """A nullable column becomes non-null when the CTE's own WHERE rejects NULLs.

    Mirrors what predicate-pushdown leaves behind on a producing CTE."""
    key = _build_concept("KEY")
    cte = _build_cte("source", [key], nullable=[key])
    cte.condition = BuildComparison(
        left=key, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
    )
    assert _proven_non_null(key, cte) is True


def _union_cte(name: str, branches):
    qds = QueryDatasource(
        input_concepts=[],
        output_concepts=list(branches[0].output_columns),
        datasources=[],
        source_map={c.address: set() for c in branches[0].output_columns},
        grain=branches[0].grain,
        joins=[],
    )
    return UnionCTE(
        name=name,
        source=qds,
        parent_ctes=list(branches),
        internal_ctes=list(branches),
        output_columns=list(branches[0].output_columns),
        grain=branches[0].grain,
    )


def test_union_cte_proven_non_null_when_every_branch_clean():
    key = _build_concept("KEY")
    branch_a = _build_cte("branch_a", [key])
    branch_b = _build_cte("branch_b", [key])
    union = _union_cte("union", [branch_a, branch_b])
    assert _proven_non_null(key, union) is True


def test_union_cte_not_proven_when_one_branch_nullable():
    key = _build_concept("KEY")
    branch_a = _build_cte("branch_a", [key])
    branch_b = _build_cte("branch_b", [key], nullable=[key])
    union = _union_cte("union", [branch_a, branch_b])
    assert _proven_non_null(key, union) is False


def test_union_cte_proven_via_branch_conditions():
    """Each branch's own WHERE rejects NULLs, even though build-time tracking
    still lists the key as nullable on both branches."""
    key = _build_concept("KEY")
    cond = BuildComparison(
        left=key, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
    )
    branch_a = _build_cte("branch_a", [key], nullable=[key])
    branch_a.condition = cond
    branch_b = _build_cte("branch_b", [key], nullable=[key])
    branch_b.condition = cond
    union = _union_cte("union", [branch_a, branch_b])
    assert _proven_non_null(key, union) is True


def _pair(key, cte):
    return CTEConceptPair(
        left=key,
        right=key,
        existing_datasource=cte.source,
        cte=cte,
        modifiers=[],
    )


class TestJoinPadsNull:
    def test_no_joins_does_not_pad(self):
        key = _build_concept("KEY")
        cte = _build_cte("cte", [key])
        assert _join_pads_null(cte, key.equivalent_addresses) is False

    def test_inner_join_does_not_pad(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [key])
        cte = _build_cte("cte", [key])
        cte.joins = [Join(right_cte=padded, jointype=JoinType.INNER)]
        assert _join_pads_null(cte, key.equivalent_addresses) is False

    def test_left_outer_pads_right_side(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [key])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte.joins = [Join(right_cte=padded, jointype=JoinType.LEFT_OUTER)]
        assert _join_pads_null(cte, key.equivalent_addresses) is True

    def test_left_outer_unrelated_column_not_padded(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [_build_concept("OTHER")])
        cte = _build_cte("cte", [key])
        cte.joins = [Join(right_cte=padded, jointype=JoinType.LEFT_OUTER)]
        assert _join_pads_null(cte, key.equivalent_addresses) is False

    def test_right_outer_pads_left_cte(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [key])
        anchor = _build_cte("anchor", [_build_concept("OTHER")])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte.joins = [
            Join(right_cte=anchor, left_cte=padded, jointype=JoinType.RIGHT_OUTER)
        ]
        assert _join_pads_null(cte, key.equivalent_addresses) is True

    def test_right_outer_pads_joinkey_pair_cte(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [key])
        anchor = _build_cte("anchor", [_build_concept("OTHER")])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte.joins = [
            Join(
                right_cte=anchor,
                jointype=JoinType.RIGHT_OUTER,
                joinkey_pairs=[_pair(key, padded)],
            )
        ]
        assert _join_pads_null(cte, key.equivalent_addresses) is True

    def test_right_outer_tolerates_pairless_join(self):
        key = _build_concept("KEY")
        anchor = _build_cte("anchor", [_build_concept("OTHER")])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte.joins = [Join(right_cte=anchor, jointype=JoinType.RIGHT_OUTER)]
        assert _join_pads_null(cte, key.equivalent_addresses) is False

    def test_right_outer_skips_pair_with_no_cte(self):
        key = _build_concept("KEY")
        anchor = _build_cte("anchor", [_build_concept("OTHER")])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte_less_pair = CTEConceptPair(
            left=key,
            right=key,
            existing_datasource=anchor.source,
            cte=None,
            modifiers=[],
        )
        cte.joins = [
            Join(
                right_cte=anchor,
                jointype=JoinType.RIGHT_OUTER,
                joinkey_pairs=[cte_less_pair],
            )
        ]
        assert _join_pads_null(cte, key.equivalent_addresses) is False

    def test_full_join_pads(self):
        key = _build_concept("KEY")
        padded = _build_cte("padded", [key])
        cte = _build_cte("cte", [_build_concept("OTHER")])
        cte.joins = [Join(right_cte=padded, jointype=JoinType.FULL)]
        assert _join_pads_null(cte, key.equivalent_addresses) is True


def test_proven_non_null_refused_when_local_join_pads():
    """A nullable key whose only nullability is a local LEFT join can't be
    rescued by a parent proof — the local join is the NULL source."""
    key = _build_concept("KEY")
    padded = _build_cte("padded", [key])
    cte = _build_cte("consumer", [key], nullable=[key])
    cte.joins = [Join(right_cte=padded, jointype=JoinType.LEFT_OUTER)]
    assert _proven_non_null(key, cte) is False


def test_proven_non_null_walks_to_clean_parent():
    """A nullable-flagged key is proven when every contributing parent CTE
    proves it non-null and no local outer join re-pads it."""
    key = _build_concept("KEY")
    clean_parent = _build_cte("clean_parent", [key])
    consumer = _build_cte("consumer", [key], nullable=[key])
    consumer.parent_ctes = [clean_parent]
    assert _proven_non_null(key, consumer) is True


def test_proven_non_null_cycle_safe():
    """A self-referential parent chain terminates instead of recursing
    forever, and reports unproven."""
    key = _build_concept("KEY")
    cte = _build_cte("cyclic", [key], nullable=[key])
    cte.parent_ctes = [cte]
    assert _proven_non_null(key, cte) is False


def test_proven_non_null_empty_union_unproven():
    key = _build_concept("KEY")
    qds = QueryDatasource(
        input_concepts=[],
        output_concepts=[key],
        datasources=[],
        source_map={key.address: set()},
        grain=BuildGrain(),
        joins=[],
    )
    empty_union = UnionCTE(
        name="empty",
        source=qds,
        parent_ctes=[],
        internal_ctes=[],
        output_columns=[key],
        grain=BuildGrain(),
    )
    assert _proven_non_null(key, empty_union) is False


def test_proven_non_null_non_cte_unproven():
    """Defensive: a node that is neither CTE nor UnionCTE can't be proven."""
    assert _proven_non_null(_build_concept("KEY"), object()) is False


def test_optimize_skips_union_cte():
    """The rule only rewrites plain CTE joins; a UnionCTE is a no-op."""
    key = _build_concept("KEY")
    union = _union_cte("union", [_build_cte("branch", [key])])
    changed, merged = SimplifyNullSafeJoins().optimize(union, {})
    assert changed is False
    assert merged is None
