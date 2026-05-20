from trilogy import Dialects, Environment
from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    JoinType,
    Modifier,
    Purpose,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
)
from trilogy.core.optimizations.join_upgrade import (
    UpgradeJoinOnGuards,
    _accumulated_left_addresses,
    _cte_addresses,
    _gather_proofs,
    _proves_non_null,
    _seed_addresses,
)
from trilogy.core.processing.condition_utility import concepts_implied_non_null


def _persist_setup(executor):
    """Two datasources joining on a nullable key. Without a guard, the
    asymmetric-nullable rule emits a FULL join."""
    executor.execute_text("""
        key id int;
        property id.region string;
        property id.amount int;

        datasource fact (
            id: id,
            region: ~?region,
            amount: amount,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'NA' AS region, 10 AS amount UNION ALL
        SELECT 2, 'EU', 20 UNION ALL
        SELECT 3, NULL, 30
        ''';

        datasource region_dim (
            region: region,
        )
        grain (region)
        query '''
        SELECT 'NA' AS region UNION ALL
        SELECT 'EU' UNION ALL
        SELECT 'AS'
        ''';
    """)


def _rows(executor, text: str) -> set[tuple]:
    queries = executor.parse_text(text)
    return {tuple(r) for r in executor.execute_query(queries[-1]).fetchall()}


def test_full_unchanged_without_guard():
    """No filter that proves either side non-null — FULL is preserved, and
    every row (fact NULL-region, dim-only AS) survives."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    text = "SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "FULL JOIN" in sql, sql
    assert _rows(executor, text) == {
        ("NA", 10),
        ("EU", 20),
        (None, 30),
        ("AS", None),
    }


def test_full_to_inner_when_both_sides_proven():
    """A WHERE atom that touches concepts unique to each side proves both
    sides must contribute non-null data → INNER."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    # `region = 'NA'` proves the merged region (the join key) non-null;
    # `amount > 5` proves a left-only concept non-null. Together they rule
    # out both unmatched directions → INNER.
    text = "WHERE region = 'NA' and amount > 5 SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert "INNER JOIN" in sql, sql
    assert _rows(executor, text) == {("NA", 10)}


def test_full_to_one_sided_outer_when_only_one_side_proven():
    """A filter on a concept unique to one side drops unmatched rows on the
    other side (their fill columns are NULL); FULL collapses to whichever
    one-sided OUTER preserves the proven side. The exact LEFT-vs-RIGHT
    direction depends on which side trilogy picks as the join driver, but
    the FULL must go away."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    text = "WHERE amount > 5 SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert "LEFT OUTER JOIN" in sql or "RIGHT OUTER JOIN" in sql, sql
    # The NULL-region fact row (amount 30) passes `amount > 5` and the dim is
    # not nullable, so an INNER would wrongly drop it — it must survive.
    assert _rows(executor, text) == {("NA", 10), ("EU", 20), (None, 30)}


def test_full_kept_when_only_coalesced_key_proven():
    """`region IS NOT NULL` on the merged join-key concept materializes as
    `coalesce(left.region, right.region) IS NOT NULL`; coalesce is null-
    opaque, so we can't prove either side individually → leave FULL."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    text = "WHERE region is not null SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    # The coalesce form keeps left-unmatched and right-unmatched rows alike,
    # so a downgrade would lose data — verify we don't emit the wrong shape.
    assert "FULL JOIN" in sql, sql
    assert _rows(executor, text) == {("NA", 10), ("EU", 20), ("AS", None)}


def test_proves_non_null_helpers():
    """Direct unit coverage of the proof extractor."""

    env = Environment()
    env.parse("key x int; key y int;")
    build_env = env.materialize_for_select()
    x = build_env.concepts["local.x"]
    y = build_env.concepts["local.y"]

    # x IS NOT NULL → {x.address}
    assert _proves_non_null(
        BuildComparison(
            left=x, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
        )
    ) == {x.address}

    # x IS NULL → empty (we want non-nulls, not nulls)
    assert (
        _proves_non_null(
            BuildComparison(
                left=x, right=MagicConstants.NULL, operator=ComparisonOperator.IS
            )
        )
        == set()
    )

    # x = 1 → {x.address}; literal side ignored
    assert _proves_non_null(
        BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ)
    ) == {x.address}

    # x = y → both
    assert _proves_non_null(
        BuildComparison(left=x, right=y, operator=ComparisonOperator.EQ)
    ) == {x.address, y.address}

    # x > 1.2 * y inside a comparison → both (multiply is not null-opaque)
    multiply = BuildFunction(
        operator=FunctionType.MULTIPLY,
        arguments=[1.2, y],
        output_data_type=DataType.FLOAT,
        output_purpose=Purpose.PROPERTY,
        arg_count=2,
    )
    assert _proves_non_null(
        BuildComparison(left=x, right=multiply, operator=ComparisonOperator.GT)
    ) == {x.address, y.address}

    # coalesce(x, y) IS NOT NULL → empty (null-opaque function)
    coalesce = BuildFunction(
        operator=FunctionType.COALESCE,
        arguments=[x, y],
        output_data_type=DataType.INTEGER,
        output_purpose=Purpose.PROPERTY,
        arg_count=2,
    )
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce,
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            )
        )
        == set()
    )

    # concepts_implied_non_null also stops at coalesce
    assert concepts_implied_non_null(coalesce) == set()
    assert concepts_implied_non_null(multiply) == {y.address}

    # _gather_proofs walks AND-decomposed atoms
    cond = BuildConditional(
        left=BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ),
        right=BuildComparison(
            left=y, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
        ),
        operator=BooleanOperator.AND,
    )
    assert _gather_proofs(cond) == {x.address, y.address}

    # NULL IS NOT x → mirror form, same result as x IS NOT NULL
    assert _proves_non_null(
        BuildComparison(
            left=MagicConstants.NULL, right=x, operator=ComparisonOperator.IS_NOT
        )
    ) == {x.address}

    # x IS NOT y (neither side a NULL literal) → empty
    assert (
        _proves_non_null(
            BuildComparison(left=x, right=y, operator=ComparisonOperator.IS_NOT)
        )
        == set()
    )

    # Aggregate wrapper around a non-opaque function → recurses through to args.
    sum_y = BuildAggregateWrapper(
        function=BuildFunction(
            operator=FunctionType.SUM,
            arguments=[y],
            output_data_type=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            arg_count=1,
        )
    )
    assert concepts_implied_non_null(sum_y) == {y.address}

    # Operators outside IS/IS_NOT/NULL_PROPAGATING_OPS (e.g. ELSE) fall through
    # to the empty-set guard.
    assert (
        _proves_non_null(
            BuildComparison(left=x, right=y, operator=ComparisonOperator.ELSE)
        )
        == set()
    )

    # BETWEEN proves every concept inside left/low/high non-null.
    from trilogy.core.models.build import BuildBetween

    assert _proves_non_null(BuildBetween(left=x, low=1, high=10)) == {x.address}
    assert _proves_non_null(BuildBetween(left=x, low=y, high=10)) == {
        x.address,
        y.address,
    }


def test_proves_non_null_comparison_shaped_like():
    """``X LIKE 'lit'`` is parsed as a ``Comparison`` with ``operator=LIKE``
    (LIKE is in ``NULL_PROPAGATING_OPS``), so its concept is proven non-null
    by the standard comparison path — both standalone and AND-chained next
    to another comparison (q91 shape)."""

    env = Environment()
    env.parse("key x int; property x.s string;")
    build_env = env.materialize_for_select()
    x = build_env.concepts["local.x"]
    s = build_env.concepts["local.s"]

    like = BuildComparison(left=s, right="Unknown%", operator=ComparisonOperator.LIKE)

    assert _proves_non_null(like) == {s.address}
    assert _gather_proofs(like) == {s.address}

    cond = BuildConditional(
        left=like,
        right=BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert _gather_proofs(cond) == {s.address, x.address}


def test_proves_non_null_coalesce_default_rejection():
    """``coalesce(PRIMARY, default) <op> rhs`` proves PRIMARY non-null when
    every default is a literal that statically *fails* ``<op> rhs`` — the
    surviving rows can only have come from the PRIMARY branch.

    Reproduces the q41 pattern: ``count`` aggregates get wrapped in
    ``coalesce(..., 0)`` to make ``count(empty)=0`` instead of NULL, and the
    downstream filter ``... > 0`` then null-rejects the underlying column."""
    env = Environment()
    env.parse("key x int; key y int;")
    build_env = env.materialize_for_select()
    x = build_env.concepts["local.x"]
    y = build_env.concepts["local.y"]

    def coalesce(*args):
        return BuildFunction(
            operator=FunctionType.COALESCE,
            arguments=list(args),
            output_data_type=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            arg_count=len(args),
        )

    # coalesce(x, 0) > 0 — 0 > 0 is FALSE, so x must be non-null.
    assert _proves_non_null(
        BuildComparison(left=coalesce(x, 0), right=0, operator=ComparisonOperator.GT)
    ) == {x.address}

    # coalesce(x, 0) >= 0 — 0 >= 0 is TRUE, so x-null rows survive: no proof.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, 0), right=0, operator=ComparisonOperator.GTE
            )
        )
        == set()
    )

    # coalesce(x, 100) > 0 — 100 > 0 is TRUE, so x-null rows survive: no proof.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, 100), right=0, operator=ComparisonOperator.GT
            )
        )
        == set()
    )

    # coalesce(x, 0) IS NOT NULL is a tautology (coalesce always non-null with
    # non-null default) — must NOT claim x non-null. This goes through the
    # IS_NOT branch, not the null-propagating one, so it stays opaque.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, 0),
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            )
        )
        == set()
    )

    # coalesce(x, y) > 0 — non-literal default; can't fold, no proof.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, y), right=0, operator=ComparisonOperator.GT
            )
        )
        == set()
    )

    # Mirror form: 0 < coalesce(x, 0) — same proof via the flipped operator.
    assert _proves_non_null(
        BuildComparison(left=0, right=coalesce(x, 0), operator=ComparisonOperator.LT)
    ) == {x.address}

    # Multiple defaults, all literals, all failing: still proves PRIMARY.
    assert _proves_non_null(
        BuildComparison(
            left=coalesce(x, 0, -1), right=0, operator=ComparisonOperator.GT
        )
    ) == {x.address}

    # Multiple defaults, one of them satisfies the comparison → no proof.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, 0, 5), right=0, operator=ComparisonOperator.GT
            )
        )
        == set()
    )

    # Equality: coalesce(x, 0) = 5 — 0 = 5 is FALSE, so proves x non-null.
    assert _proves_non_null(
        BuildComparison(left=coalesce(x, 0), right=5, operator=ComparisonOperator.EQ)
    ) == {x.address}

    # Equality where default matches: coalesce(x, 5) = 5 — 5 = 5 TRUE, no proof.
    assert (
        _proves_non_null(
            BuildComparison(
                left=coalesce(x, 5), right=5, operator=ComparisonOperator.EQ
            )
        )
        == set()
    )


def test_cte_addresses_none_returns_empty():
    """Defensive guard: a None CTE has no addresses."""
    assert _cte_addresses(None) == set()


def test_left_address_helpers_skip_non_join_entries():
    """``_seed_addresses`` and ``_accumulated_left_addresses`` must
    short-circuit when the relevant join slot is an UnnestJoin (or other
    non-Join entry) rather than a Join."""
    concept = BuildConcept(
        name="c",
        canonical_name="c",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )
    ds = BuildDatasource(
        name="ds",
        columns=[BuildColumnAssignment(alias="c", concept=concept)],
        address="ds",
        namespace="test",
        grain=BuildGrain(),
    )
    cte = CTE.from_datasource(ds)

    unnest = InstantiatedUnnestJoin(object_to_unnest=concept, alias="u")

    # First "join" slot is an UnnestJoin → no seed can be derived from it.
    cte.joins = [unnest]
    assert _seed_addresses(cte) == set()

    # idx > 0 with a non-Join prior contributes nothing to the accumulated
    # left, and the (still non-Join) first slot gives no seed either.
    cte.joins = [unnest, unnest]
    assert _accumulated_left_addresses(cte, 1) == set()


def _build_concept(name: str, namespace: str = "test"):
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace=namespace,
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _build_cte(name: str, columns):
    ds = BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=c.name, concept=c) for c in columns],
        address=name,
        namespace="test",
        grain=BuildGrain(),
    )
    cte = CTE.from_datasource(ds)
    cte.name = name
    return cte


def _condition_for(*concepts: BuildConcept):
    condition = BuildComparison(
        left=concepts[0], right=1, operator=ComparisonOperator.GT
    )
    for concept in concepts[1:]:
        condition += BuildComparison(
            left=concept, right=1, operator=ComparisonOperator.GT
        )
    return condition


def test_inner_join_key_proofs_are_source_bound():
    """A key address appearing on several sources is still safe to propagate
    when a null-rejecting INNER join proves one specific source's key."""
    key = _build_concept("KEY")
    a_marker = _build_concept("A_MARKER")
    dim_attr = _build_concept("DIM_ATTR")

    a_cte = _build_cte("a_source", [key, a_marker])
    b_cte = _build_cte("b_source", [key])
    dim_cte = _build_cte("dim_source", [key, dim_attr])
    cte = _build_cte("root", [key, a_marker, dim_attr])
    cte.parent_ctes = [a_cte, b_cte, dim_cte]
    cte.condition = _condition_for(a_marker, dim_attr)
    cte.joins = [
        Join(
            jointype=JoinType.FULL,
            right_cte=b_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=a_cte.source,
                    cte=a_cte,
                )
            ],
        ),
        Join(
            jointype=JoinType.LEFT_OUTER,
            right_cte=dim_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=b_cte.source,
                    cte=b_cte,
                )
            ],
        ),
    ]

    changed, _ = UpgradeJoinOnGuards().optimize(cte, {})

    assert changed
    assert cte.joins[0].jointype == JoinType.INNER
    assert cte.joins[1].jointype == JoinType.INNER


def test_coalesced_left_key_does_not_prove_each_branch():
    """COALESCE(a.key, b.key) = dim.key proves the dim key, not each branch key."""
    key = _build_concept("KEY")
    a_marker = _build_concept("A_MARKER")
    dim_attr = _build_concept("DIM_ATTR")

    a_cte = _build_cte("a_source", [key, a_marker])
    b_cte = _build_cte("b_source", [key])
    dim_cte = _build_cte("dim_source", [key, dim_attr])
    cte = _build_cte("root", [key, a_marker, dim_attr])
    cte.parent_ctes = [a_cte, b_cte, dim_cte]
    cte.condition = _condition_for(a_marker, dim_attr)
    cte.joins = [
        Join(
            jointype=JoinType.FULL,
            right_cte=b_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=a_cte.source,
                    cte=a_cte,
                )
            ],
        ),
        Join(
            jointype=JoinType.FULL,
            right_cte=dim_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=a_cte.source,
                    cte=a_cte,
                ),
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=b_cte.source,
                    cte=b_cte,
                ),
            ],
        ),
    ]

    changed, _ = UpgradeJoinOnGuards().optimize(cte, {})

    assert changed
    assert cte.joins[0].jointype == JoinType.LEFT_OUTER
    assert cte.joins[1].jointype == JoinType.INNER


def test_nullable_inner_join_key_does_not_prove_non_null():
    """An INNER join rendered as IS NOT DISTINCT FROM can still match NULL keys."""
    key = _build_concept("KEY")
    a_marker = _build_concept("A_MARKER")
    dim_attr = _build_concept("DIM_ATTR")

    a_cte = _build_cte("a_source", [key, a_marker])
    b_cte = _build_cte("b_source", [key])
    dim_cte = _build_cte("dim_source", [key, dim_attr])
    cte = _build_cte("root", [key, a_marker, dim_attr])
    cte.parent_ctes = [a_cte, b_cte, dim_cte]
    cte.condition = _condition_for(a_marker, dim_attr)
    cte.joins = [
        Join(
            jointype=JoinType.FULL,
            right_cte=b_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=a_cte.source,
                    cte=a_cte,
                )
            ],
        ),
        Join(
            jointype=JoinType.LEFT_OUTER,
            right_cte=dim_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=b_cte.source,
                    cte=b_cte,
                    modifiers=[Modifier.NULLABLE],
                )
            ],
        ),
    ]

    changed, _ = UpgradeJoinOnGuards().optimize(cte, {})

    assert changed
    assert cte.joins[0].jointype == JoinType.LEFT_OUTER
    assert cte.joins[1].jointype == JoinType.INNER


def test_existing_inner_base_join_proves_nullable_source_present():
    """A downstream INNER dim join can prove an upstream LEFT-joined source
    is present without pretending the source's original join keys were proven."""
    fact_item = _build_concept("ITEM")
    fact_ticket = _build_concept("TICKET")
    return_item = _build_concept("ITEM")
    return_ticket = _build_concept("TICKET")
    return_reason = _build_concept("REASON")
    reason_id = _build_concept("REASON")
    reason_desc = _build_concept("REASON_DESC")

    fact_cte = _build_cte("fact", [fact_item, fact_ticket])
    returns_cte = _build_cte("returns", [return_item, return_ticket, return_reason])
    reason_cte = _build_cte("reason", [reason_id, reason_desc])
    fact_ds = fact_cte.source.base_datasource
    returns_ds = returns_cte.source.base_datasource
    reason_ds = reason_cte.source.base_datasource
    assert fact_ds is not None
    assert returns_ds is not None
    assert reason_ds is not None

    root = _build_cte("root", [fact_item, fact_ticket, return_reason, reason_desc])
    root.condition = _condition_for(reason_desc)
    root.source.datasources = [fact_ds, returns_ds, reason_ds]
    root.source.joins = [
        BaseJoin(
            left_datasource=fact_ds,
            right_datasource=returns_ds,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=fact_item,
                    right=return_item,
                    existing_datasource=fact_ds,
                ),
                ConceptPair(
                    left=fact_ticket,
                    right=return_ticket,
                    existing_datasource=fact_ds,
                ),
            ],
        ),
        BaseJoin(
            left_datasource=returns_ds,
            right_datasource=reason_ds,
            join_type=JoinType.INNER,
            concept_pairs=[
                ConceptPair(
                    left=return_reason,
                    right=reason_id,
                    existing_datasource=returns_ds,
                )
            ],
        ),
    ]

    changed, _ = UpgradeJoinOnGuards(base_join_only=True).optimize(root, {})

    assert changed
    assert root.source.joins[0].join_type == JoinType.INNER
    assert root.source.joins[1].join_type == JoinType.INNER


def test_seed_addresses_inlined_left_via_joinkey_pair():
    """When a join's left side has been inlined (no explicit ``left_cte``,
    no parent CTE), ``_seed_addresses`` must recover it from the ``cte``
    field on a ``CTEConceptPair`` — the "left" CTE is attached to the
    join-key pair instead of the join itself."""
    left_concept = _build_concept("L_KEY")
    right_concept = _build_concept("R_KEY")
    left_cte = _build_cte("inlined_left", [left_concept])
    right_cte = _build_cte("right", [right_concept])

    seed_cte = _build_cte("seed", [left_concept, right_concept])
    seed_cte.parent_ctes = []
    seed_cte.joins = [
        Join(
            jointype=JoinType.LEFT_OUTER,
            left_cte=None,
            right_cte=right_cte,
            joinkey_pairs=[
                CTEConceptPair(
                    left=left_concept,
                    right=right_concept,
                    existing_datasource=left_cte.source,
                    cte=left_cte,
                )
            ],
        )
    ]

    assert _seed_addresses(seed_cte) == {left_concept.address}


def test_seed_addresses_base_datasource_fallback():
    """When neither ``left_cte``, parent CTEs, nor a CTE-bearing
    joinkey_pair are available, ``_seed_addresses`` falls back to
    ``cte.source.base_datasource`` — the literal FROM-clause table.
    Without this branch, ``FROM table LEFT JOIN dim …`` chains never get
    a left side and the rule silently skips ``idx == 0``."""
    base_concept = _build_concept("BASE_COL")
    right_concept = _build_concept("R_KEY")
    base_cte = _build_cte("base", [base_concept])
    # ``from_datasource`` already set ``source.base_datasource`` to the
    # underlying BuildDatasource, which is what we want.
    right_cte = _build_cte("right", [right_concept])

    base_cte.joins = [
        Join(
            jointype=JoinType.LEFT_OUTER,
            left_cte=None,
            right_cte=right_cte,
            joinkey_pairs=[
                # ConceptPair (no `cte` field) — the inlined-left branch
                # must skip it and fall through to base_datasource.
                ConceptPair(
                    left=base_concept,
                    right=right_concept,
                    existing_datasource=base_cte.source.base_datasource,
                )
            ],
        )
    ]

    assert _seed_addresses(base_cte) == {base_concept.address}


def test_seed_addresses_returns_empty_when_no_fallback_resolves():
    """All four resolution paths fail: no explicit left, no eligible parent,
    no CTE-bearing joinkey_pair, no base datasource. Must return an empty
    set rather than raise — caller treats empty seed as "no left forced"."""
    right_concept = _build_concept("R_KEY")
    right_cte = _build_cte("right", [right_concept])

    cte = _build_cte("orphan", [right_concept])
    cte.source.base_datasource = None  # strip the only remaining fallback
    cte.joins = [
        Join(
            jointype=JoinType.LEFT_OUTER,
            left_cte=None,
            right_cte=right_cte,
            joinkey_pairs=[
                ConceptPair(
                    left=right_concept,
                    right=right_concept,
                    existing_datasource=right_cte.source,
                )
            ],
        )
    ]

    assert _seed_addresses(cte) == set()
