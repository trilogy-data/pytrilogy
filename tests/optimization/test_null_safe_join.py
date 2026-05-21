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
