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
    _complete_distinct,
    _filters_equivalent,
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


def test_partial_concept_blocks_upgrade():
    """Each side projects the same canonical concept but flags it as
    partial — a conceptual subset of the full value space, not the full
    distinct set. Two partial sides may be GROUP BY-distinct within their
    respective subsets, but those subsets needn't coincide.

    Partial-ness can arrive via any upstream mechanism (a partial
    datasource binding, a ``MERGE`` alignment, ``Modifier.PARTIAL`` on a
    column assignment, …); the rule only reads the propagated
    ``partial_concepts`` and doesn't care how the flag was set. The TPC-DS
    q10 regression that motivated this test happens to use ``MERGE``, but
    the same logic governs any partial source. We mark the flag directly
    here to keep the test mechanism-agnostic.
    """
    cls = _concept("CUSTOMER_ID")
    source_left = _datasource_cte("a_src", [cls])
    source_right = _datasource_cte("b_src", [cls])
    left = _grouped_child("left", source_left, cls)
    right = _grouped_child("right", source_right, cls)
    left.partial_concepts = [cls]
    right.partial_concepts = [cls]
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    assert not _complete_distinct(cls, left)
    assert not _complete_distinct(cls, right)

    changed, _ = UpgradeOuterFromKeySetEquivalence().optimize(root, {})
    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_full_join_key_veto_blocks_upgrade():
    """A key registered as a query-scoped FULL/UNION join key never upgrades,
    even when the completeness tests would pass — the canonical collapse hides
    that the two sides are independent populations."""
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence(
        full_join_keys={cls.address}
    ).optimize(root, {})

    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_equal_join_key_releases_full_veto():
    """The same key declared EQUAL (non-partial `merge a into b`) releases the
    veto: the canonical genuinely names one value space, so the standard
    completeness tests arbitrate and the join narrows."""
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    right = _grouped_child("right", source, cls)
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence(
        full_join_keys={cls.address},
        equal_join_keys={cls.address},
    ).optimize(root, {})

    assert changed
    assert root.joins[0].jointype == JoinType.INNER


def test_equal_join_key_still_requires_completeness():
    """EQUAL only releases the veto — the completeness tests still gate the
    narrowing (a non-grouped side keeps the OUTER join)."""
    cls = _concept("CLASS")
    source = _datasource_cte("source", [cls])
    left = _grouped_child("left", source, cls)
    right = _datasource_cte("right_plain", [cls])
    right.parent_ctes = [source]
    root = _datasource_cte("root", [cls])
    _outer_join(root, JoinType.FULL, left, right, cls)

    changed, _ = UpgradeOuterFromKeySetEquivalence(
        full_join_keys={cls.address},
        equal_join_keys={cls.address},
    ).optimize(root, {})

    assert not changed
    assert root.joins[0].jointype == JoinType.FULL


def test_narrow_equal_domain_joins_config_gates_plan():
    """`narrow_equal_domain_joins` (default ON) controls whether the rule plan
    forwards EQUAL keys — off, an EQUAL-declared key keeps the FULL veto."""
    from trilogy.constants import CONFIG
    from trilogy.core.optimization import build_optimization_rule_plan

    def _rule(plan):
        return next(
            p for p in plan if p.name == "upgrade_outer_key_set_equivalence"
        ).rule_factory()

    assert CONFIG.optimizations.narrow_equal_domain_joins is True
    on = _rule(
        build_optimization_rule_plan(
            full_join_keys={"test.CLASS"}, equal_join_keys={"test.CLASS"}
        )
    )
    assert on.full_join_keys == set()
    CONFIG.optimizations.narrow_equal_domain_joins = False
    try:
        off = _rule(
            build_optimization_rule_plan(
                full_join_keys={"test.CLASS"}, equal_join_keys={"test.CLASS"}
            )
        )
        assert off.full_join_keys == {"test.CLASS"}
    finally:
        CONFIG.optimizations.narrow_equal_domain_joins = True


_TWO_SOURCE_MODEL = """
key ka int;
key kb int;
property ka.va int;
property kb.vb int;

datasource dsa (a: ka, v: va) grain (ka) address dsa;
datasource dsb (b: kb, v: vb) grain (kb) address dsb;
"""


def _generated_sql(model: str, query: str) -> str:
    from trilogy import Dialects
    from trilogy.parsing.parse_engine_v2 import clear_parse_cache

    clear_parse_cache()
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    return executor.generate_sql(query)[-1]


def _with_narrowing(model: str, query: str) -> str:
    return _generated_sql(model, query)


def _without_narrowing(model: str, query: str) -> str:
    from trilogy.constants import CONFIG

    CONFIG.optimizations.narrow_equal_domain_joins = False
    try:
        return _generated_sql(model, query)
    finally:
        CONFIG.optimizations.narrow_equal_domain_joins = True


def test_equal_merge_narrows_full_to_inner_end_to_end():
    """A non-partial `merge` declares EQUAL domains: by default the merged
    key's FULL join between two authoritative scans renders INNER; opting out
    of `narrow_equal_domain_joins` keeps the preserving FULL."""
    model = _TWO_SOURCE_MODEL + "\nmerge kb into ka;"
    query = "select ka, sum(va) -> ta, sum(vb) -> tb;"
    assert "FULL JOIN" in _without_narrowing(model, query)
    narrowed = _with_narrowing(model, query)
    assert "FULL JOIN" not in narrowed
    assert "INNER JOIN" in narrowed


def test_union_join_never_narrows_end_to_end():
    """A query-scoped `union join` declares that NEITHER domain contains the
    other — the FULL join must survive narrowing even under the flag."""
    query = "select ka, va, vb union join ka = kb;"
    assert "FULL JOIN" in _generated_sql(_TWO_SOURCE_MODEL, query)
    assert "FULL JOIN" in _with_narrowing(_TWO_SOURCE_MODEL, query)


def test_union_join_veto_beats_equal_merge_on_same_key():
    """A key both merged (EQUAL) and authored as a query-scoped UNION keeps
    the preservation veto — the stronger in-query declaration wins."""
    model = _TWO_SOURCE_MODEL + "\nmerge kb into ka;"
    query = "select ka, va, vb union join ka = kb;"
    assert "FULL JOIN" in _with_narrowing(model, query)


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
