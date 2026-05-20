from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    JoinType,
    Modifier,
    Purpose,
)
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import (
    CTE,
    BuildDatasource,
    CTEConceptPair,
    Join,
    QueryDatasource,
)
from trilogy.core.optimizations.value_set_join_upgrade import (
    UpgradeOuterFromKeySetEquivalence,
    _accumulate_filter,
    _filters_equivalent,
    _leaf_datasource_addresses,
    _pair_key_sets_equivalent,
)


def _concept(name: str):
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(components={f"test.{name}"}),
        pseudonyms=set(),
    )


def _datasource_cte(name: str, columns):
    ds = BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=c.name, concept=c) for c in columns],
        address=name,
        namespace="test",
        grain=BuildGrain(components={c.address for c in columns}),
    )
    cte = CTE.from_datasource(ds)
    cte.name = name
    return cte


def _grouped_child(name: str, parent: CTE, grain_concept: BuildConcept):
    """A child CTE that projects ``grain_concept`` from ``parent`` and groups
    to its grain — emulates the twin-rollup shape from TPC-DS q98 where the
    rolled-up CTE has no datasource of its own, only an upstream CTE.
    """
    qds = QueryDatasource(
        input_concepts=[grain_concept],
        output_concepts=[grain_concept],
        datasources=[parent.source],
        source_map={grain_concept.address: {parent.source}},
        grain=BuildGrain(components={grain_concept.address}),
        joins=[],
    )
    cte = CTE(
        name=name,
        source=qds,
        output_columns=[grain_concept],
        source_map={grain_concept.address: [parent.name]},
        grain=BuildGrain(components={grain_concept.address}),
        parent_ctes=[parent],
        group_to_grain=True,
    )
    return cte


def _outer_join(parent_holder: CTE, jointype: JoinType, left: CTE, right: CTE, key):
    """Wire ``left`` and ``right`` under ``parent_holder`` with a single-key
    outer join in the null-safe form, mirroring how the renderer would see it."""
    parent_holder.parent_ctes = [left, right]
    parent_holder.joins = [
        Join(
            jointype=jointype,
            right_cte=right,
            modifiers=[Modifier.NULLABLE],
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=left.source,
                    cte=left,
                    modifiers=[Modifier.NULLABLE],
                )
            ],
        )
    ]


def test_twin_rollup_full_join_upgraded_to_inner():
    """Both sides are GROUP BY rollups of the same filtered source CTE with
    the same effective filter — FULL becomes INNER. (TPC-DS q98 shape.)"""
    cls = _concept("CLASS")
    other = _concept("OTHER")
    source = _datasource_cte("source", [cls, other])
    source.condition = BuildComparison(
        left=other, right="X", operator=ComparisonOperator.EQ
    )
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert changed
    assert root.joins[0].jointype == JoinType.INNER
    # The null-safe modifier stays — keys themselves are still nullable; this
    # rule only changes the join type, not the equality form.
    assert Modifier.NULLABLE in root.joins[0].modifiers


def test_parent_child_rollup_upgrades():
    """One side is a GROUP BY rollup of the other (the rollup's only parent
    IS the other side). Same shape as TPC-DS q12 / q20 / q63."""
    cls = _concept("CLASS")
    other = _concept("OTHER")
    parent = _datasource_cte("parent", [cls, other])
    parent.condition = BuildComparison(
        left=other, right="X", operator=ComparisonOperator.EQ
    )
    # ``parent`` itself counts as group_to_grain over (CLASS, OTHER); the
    # rolled-up child collapses to CLASS alone.
    parent.group_to_grain = True
    parent.grain = BuildGrain(components={cls.address, other.address})

    child = _grouped_child("child", parent, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, child, parent, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert changed
    assert root.joins[0].jointype == JoinType.INNER


def test_divergent_filter_blocks_upgrade():
    """Sides apply different upstream WHEREs — the value sets aren't
    provably equal, FULL must stay. (Mimics TPC-DS q04: year-over-year /
    HAVING comparison.)"""
    cls = _concept("CLASS")
    other = _concept("OTHER")
    source_left = _datasource_cte("source_left", [cls, other])
    source_left.condition = BuildComparison(
        left=other, right="X", operator=ComparisonOperator.EQ
    )
    source_right = _datasource_cte("source_right", [cls, other])
    source_right.condition = BuildComparison(
        left=other, right="Y", operator=ComparisonOperator.EQ
    )
    left = _grouped_child("left", source_left, cls)
    right = _grouped_child("right", source_right, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_non_group_side_blocks_upgrade():
    """A side without group_to_grain doesn't carry "every distinct value"
    cardinality — can't claim the key sets match."""
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    # ``right`` is a plain projection (no group_to_grain) — multiple rows
    # per class are possible, so distinct-cardinality unknown.
    right = _datasource_cte("right_plain", [cls])
    right.parent_ctes = [source]
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_distinct_source_concepts_blocks_upgrade():
    """Sides carry the same alias on the join surface but their underlying
    canonical sources differ (e.g. ``store_sales.customer.id`` vs
    ``catalog_sales.customer.id``). Refuse the upgrade."""
    left_key = _concept("CUSTOMER_SS")
    right_key = _concept("CUSTOMER_CS")
    source_left = _datasource_cte("ss_src", [left_key])
    source_right = _datasource_cte("cs_src", [right_key])
    left = _grouped_child("left", source_left, left_key)
    right = _grouped_child("right", source_right, right_key)
    root = _datasource_cte("root", [left_key])
    root.parent_ctes = [left, right]
    root.joins = [
        Join(
            jointype=JoinType.FULL,
            right_cte=right,
            modifiers=[Modifier.NULLABLE],
            joinkey_pairs=[
                CTEConceptPair(
                    left=left_key,
                    right=right_key,
                    existing_datasource=left.source,
                    cte=left,
                    modifiers=[Modifier.NULLABLE],
                )
            ],
        )
    ]

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert not changed


def test_left_outer_also_upgrades_on_equivalence():
    """LEFT OUTER joins benefit too — same reasoning: when both key sets
    match, the right side has every left key, so there are no NULL-padded
    rows for OUTER to preserve."""
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.LEFT_OUTER, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert changed
    assert root.joins[0].jointype == JoinType.INNER


def test_inner_join_is_left_alone():
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.INNER, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})

    assert not changed
    assert root.joins[0].jointype == JoinType.INNER


def test_filter_equivalence_uses_mutual_implication():
    """``A AND B`` and ``B AND A`` are equivalent: ``condition_implies`` treats
    them as the same atom set in either direction."""
    x = _concept("X")
    y = _concept("Y")
    a = BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ)
    b = BuildComparison(left=y, right=2, operator=ComparisonOperator.EQ)
    ab = BuildConditional(left=a, right=b, operator=BooleanOperator.AND)
    ba = BuildConditional(left=b, right=a, operator=BooleanOperator.AND)

    assert _filters_equivalent(ab, ba)
    assert _filters_equivalent(None, None)
    assert not _filters_equivalent(a, ab)


def test_accumulate_filter_walks_chain():
    """Filters from every ancestor in the chain compose into the side's
    effective predicate. ``None`` conditions don't contribute."""
    cls = _concept("CLASS")
    other = _concept("OTHER")
    grand = _datasource_cte("grand", [cls, other])
    grand.condition = BuildComparison(
        left=other, right="X", operator=ComparisonOperator.EQ
    )
    middle = _grouped_child("middle", grand, cls)
    # middle adds its own condition layered on grand's
    middle.condition = BuildComparison(
        left=cls, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
    )
    leaf = _grouped_child("leaf", middle, cls)

    acc = _accumulate_filter(leaf)
    assert acc is not None
    # Both filters must be reachable as atoms of the accumulated expression.
    from trilogy.core.processing.condition_utility import decompose_condition

    atoms = decompose_condition(acc)
    assert grand.condition in atoms
    assert middle.condition in atoms


def test_different_base_tables_block_upgrade():
    """Same canonical concept, but each side materialises it from a
    different base table (web_sales vs catalog_sales). The "complete
    distinct values" claim is per-table, so the two sides are NOT the same
    set. (TPC-DS q10 regression.)"""
    cls = _concept("CUSTOMER_ID")
    # Two datasources, each "containing" the same concept conceptually but
    # representing different physical row populations.
    source_left = _datasource_cte("web_sales", [cls])
    source_right = _datasource_cte("catalog_sales", [cls])
    left = _grouped_child("left", source_left, cls)
    right = _grouped_child("right", source_right, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    # Leaf-set equality guard fires before filter equivalence even gets a
    # chance to mis-conclude.
    assert _leaf_datasource_addresses(left) != _leaf_datasource_addresses(right)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})
    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_pair_helper_resolves_canonical_concept_aliases():
    """The pair check matches on ``canonical_address`` of the join key
    concepts, so different *local* aliases pointing to the same underlying
    column count as the same source. Backing rows come from the same single
    base table on both sides — this is the rollup-on-the-same-source
    pattern."""
    cls = _concept("CLASS")
    other = _concept("OTHER")
    source = _datasource_cte("source", [cls, other])
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    # Even though both sides reference the same ``cls`` concept object, the
    # pair helper compares by canonical address — the equality holds.
    assert _pair_key_sets_equivalent(cls, left, cls, right)
