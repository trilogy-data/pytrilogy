from trilogy import Dialects, Environment
from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    JoinType,
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
    ConceptPair,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
)
from trilogy.core.optimizations.join_upgrade import (
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


def test_full_unchanged_without_guard():
    """No filter that proves either side non-null — FULL is preserved."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    queries = executor.parse_text("""
        SELECT region, sum(amount) as total;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    assert "FULL JOIN" in sql, sql


def test_full_to_inner_when_both_sides_proven():
    """A WHERE atom that touches concepts unique to each side proves both
    sides must contribute non-null data → INNER."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    # `region = 'NA'` proves the merged region (the join key) non-null;
    # `amount > 5` proves a left-only concept non-null. Together they rule
    # out both unmatched directions → INNER.
    queries = executor.parse_text("""
        WHERE region = 'NA' and amount > 5
        SELECT region, sum(amount) as total;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert "INNER JOIN" in sql, sql


def test_full_to_one_sided_outer_when_only_one_side_proven():
    """A filter on a concept unique to one side drops unmatched rows on the
    other side (their fill columns are NULL); FULL collapses to whichever
    one-sided OUTER preserves the proven side. The exact LEFT-vs-RIGHT
    direction depends on which side trilogy picks as the join driver, but
    the FULL must go away."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    queries = executor.parse_text("""
        WHERE amount > 5
        SELECT region, sum(amount) as total;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert "LEFT OUTER JOIN" in sql or "RIGHT OUTER JOIN" in sql, sql


def test_full_kept_when_only_coalesced_key_proven():
    """`region IS NOT NULL` on the merged join-key concept materializes as
    `coalesce(left.region, right.region) IS NOT NULL`; coalesce is null-
    opaque, so we can't prove either side individually → leave FULL."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    queries = executor.parse_text("""
        WHERE region is not null
        SELECT region, sum(amount) as total;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    # The coalesce form keeps left-unmatched and right-unmatched rows alike,
    # so a downgrade would lose data — verify we don't emit the wrong shape.
    assert "FULL JOIN" in sql, sql


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
