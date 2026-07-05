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
from trilogy.core.models.datasource import RawColumnExpr
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
    QueryDatasource,
)
from trilogy.core.optimizations.join_upgrade import (
    UpgradeJoinOnGuards,
    _accumulated_left_addresses,
    _blocked_partials,
    _cte_addresses,
    _gather_proofs,
    _partial_addresses,
    _proves_non_null,
    _seed_addresses,
    _source_datasources,
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


def _returns_setup(executor):
    """A fact with a partial (`~`) NULL-padded returns source — the q94 shape.
    `is_returned` is derived from the padded column, so "not returned" rows
    exist only while the padding join stays OUTER."""
    executor.execute_raw_sql("""
        CREATE TABLE lines AS
        SELECT 1 AS o, 10 AS i, 5 AS q UNION ALL
        SELECT 1, 11, 3 UNION ALL
        SELECT 2, 10, 7 UNION ALL
        SELECT 3, 12, 2;
        CREATE TABLE returns AS
        SELECT 1 AS o, 10 AS i, 1 AS ro;
        """)
    executor.execute_text("""
        key order_id int;
        key item_id int;

        properties <order_id, item_id> (
            qty int,
            _ret_order int?,
        );
        auto is_returned <- _ret_order is not null;

        datasource lines (
            o: order_id,
            i: item_id,
            q: qty,
        )
        grain (order_id, item_id)
        address lines;

        datasource returns (
            o: ~order_id,
            i: ~item_id,
            ro: _ret_order,
        )
        grain (order_id, item_id)
        address returns;
    """)


def test_semijoin_on_shared_key_keeps_padded_returns_rows():
    """q94: a semijoin membership on the shared join key (`order_id in
    big_orders.order_id`) proves the key non-null, but the key renders as
    COALESCE across both sides — non-null on the padded (non-returned) rows
    too — so it must NOT upgrade the padding join to INNER. The rows with
    `is_returned = false` have to stay reachable at order grain."""
    executor = Dialects.DUCK_DB.default_executor()
    _returns_setup(executor)

    text = """
        with big_orders as
        where qty > 2
        select order_id;

        where order_id in big_orders.order_id
        select order_id, bool_or(is_returned) as has_return;
    """
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    for line in sql.splitlines():
        assert not ("INNER JOIN" in line and "returns" in line), sql
    # Order 2 has no return row: it exists only via the padded side.
    assert _rows(executor, text) == {(1, True), (2, False)}


def test_plain_filter_keeps_padded_returns_rows():
    """Sibling guard (q94 P1): an ordinary filter on a fact attribute leaves
    the padding join OUTER and `is_returned = false` reachable."""
    executor = Dialects.DUCK_DB.default_executor()
    _returns_setup(executor)

    text = "where qty > 2 select order_id, bool_or(is_returned) as has_return;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    for line in sql.splitlines():
        assert not ("INNER JOIN" in line and "returns" in line), sql
    assert _rows(executor, text) == {(1, True), (2, False)}


def test_flag_is_true_with_semijoin_upgrades_padding_join():
    """q95: alongside the semijoin, `is_returned is True` forces the padded
    column non-null THROUGH the flag's lineage — a sound licence to tighten
    the padding join even though the semijoin key proof alone is not."""
    executor = Dialects.DUCK_DB.default_executor()
    _returns_setup(executor)

    text = """
        with big_orders as
        where qty > 2
        select order_id;

        where order_id in big_orders.order_id and is_returned is True
        select order_id, count(item_id) as items;
    """
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert _rows(executor, text) == {(1, 1)}


def test_flag_is_true_lineage_proofs():
    """`flag IS/= True` where flag's lineage is a null-check proves the
    underlying column non-null; `IS False` must not."""
    env = Environment()
    env.parse("key x int; property x.ret int; auto flag <- ret is not null;")
    build_env = env.materialize_for_select()
    flag = build_env.concepts["local.flag"]
    ret = build_env.concepts["local.ret"]

    assert ret.address in _proves_non_null(
        BuildComparison(left=flag, right=True, operator=ComparisonOperator.IS)
    )
    assert ret.address in _proves_non_null(
        BuildComparison(left=True, right=flag, operator=ComparisonOperator.EQ)
    )
    assert ret.address not in _proves_non_null(
        BuildComparison(left=flag, right=False, operator=ComparisonOperator.IS)
    )
    assert ret.address not in _proves_non_null(
        BuildComparison(left=flag, right=False, operator=ComparisonOperator.EQ)
    )


def test_right_only_padded_column_proof_still_upgrades():
    """Regression guard: a WHERE proof on a column that renders ONLY from the
    padded side (the raw padded key itself, not the shared COALESCE key) still
    legitimately rejects the padded rows → the join may go INNER."""
    executor = Dialects.DUCK_DB.default_executor()
    _returns_setup(executor)

    text = "where _ret_order = 1 select order_id, count(item_id) as items;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert not any(
        j in sql for j in ("FULL JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN")
    ), sql
    assert _rows(executor, text) == {(1, 1)}


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


def _build_flag_scenario(flag_alias):
    """fact LEFT_OUTER returns on KEY; returns carries a FLAG bound via
    ``flag_alias``. Query filters ``FLAG = False`` and the join is evaluated
    by ``UpgradeJoinOnGuards`` — returns the resulting join type."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    flag = _build_concept("FLAG")

    fact_ds = BuildDatasource(
        name="fact",
        columns=[
            BuildColumnAssignment(alias="KEY", concept=key),
            BuildColumnAssignment(alias="MEASURE", concept=measure),
        ],
        address="fact",
        namespace="test",
        grain=BuildGrain(),
    )
    returns_ds = BuildDatasource(
        name="returns",
        columns=[
            BuildColumnAssignment(alias="KEY", concept=key),
            BuildColumnAssignment(alias=flag_alias, concept=flag),
        ],
        address="returns",
        namespace="test",
        grain=BuildGrain(),
    )

    root = _build_cte("root", [key, measure, flag])
    root.condition = BuildComparison(
        left=flag, right=False, operator=ComparisonOperator.EQ
    )
    root.source.datasources = [fact_ds, returns_ds]
    root.source.joins = [
        BaseJoin(
            left_datasource=fact_ds,
            right_datasource=returns_ds,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(left=key, right=key, existing_datasource=fact_ds)
            ],
        )
    ]

    UpgradeJoinOnGuards(base_join_only=True).optimize(root, {})
    return root.source.joins[0].join_type


def test_raw_derived_flag_eq_false_keeps_left_outer():
    """``FLAG = false`` where FLAG is a ``CASE``-style raw column (non-null even
    on the join's unmatched rows) must NOT upgrade LEFT_OUTER→INNER: the
    predicate is satisfied by exactly the NULL-padded rows, so an INNER drops
    the whole result set. (``is_returned = false`` silent-zero-rows bug.)"""
    raw_flag = RawColumnExpr(
        text="CASE WHEN RET_ORDER_NUMBER IS NOT NULL THEN True ELSE False END"
    )
    assert _build_flag_scenario(raw_flag) == JoinType.LEFT_OUTER


def test_plain_column_flag_eq_false_still_upgrades():
    """Control: a genuine plain right-side column IS NULL-padded on unmatched
    rows, so ``flag = false`` legitimately rejects them — INNER is sound and
    the gate must still fire. Confirms the fix is scoped to opaque bindings."""
    assert _build_flag_scenario("WR_FLAG") == JoinType.INNER


def test_source_datasources_normalizes_to_safe_identifier_tokens():
    """``_source_datasources`` must yield the same ``safe_identifier`` tokens
    stored in ``CTE.source_map`` (what ``_blocked_partials`` intersects). A
    namespaced datasource's ``identifier`` keeps dots while ``source_map``
    stores the underscored form, and a ``QueryDatasource`` maps to datasource
    *objects*, never strings — both must normalize to ``safe_identifier`` or
    the partial-block check silently never matches on this path."""
    key = _build_concept("KEY")
    bd = BuildDatasource(
        name="returns",
        columns=[BuildColumnAssignment(alias="KEY", concept=key)],
        address="returns",
        namespace="test",
        grain=BuildGrain(),
    )
    # The exact mismatch the fix closes: dotted identifier vs underscored token.
    assert bd.identifier == "test.returns"
    assert bd.safe_identifier == "test_returns"

    # Bare BuildDatasource → its own safe_identifier (underscored, not dotted).
    assert _source_datasources(bd) == {"test_returns"}

    # QueryDatasource wrapping it → the BD's safe_identifier, not the object.
    qds = QueryDatasource(
        input_concepts=[key],
        output_concepts=[key],
        datasources=[bd],
        source_map={key.address: {bd}},
        grain=BuildGrain(components={key.address}),
        joins=[],
    )
    assert _source_datasources(qds) == {"test_returns"}


def test_source_datasources_cte_returns_source_map_tokens():
    """A CTE/UnionCTE source_map already holds string tokens — returned as-is."""
    key = _build_concept("KEY")
    cte = _build_cte("c", [key])
    cte.source_map = {key.address: ["test_c", "other_src"]}
    assert _source_datasources(cte) == {"test_c", "other_src"}


def test_blocked_partials_intersects_operand_tokens():
    """A partial bound exclusively to the operand renders from it (not
    blocked); one bound only elsewhere is blocked; one bound to BOTH sides
    renders as COALESCE — non-null on the operand's padded rows via the other
    side — so it is blocked too (q94); an unbound one is skipped."""
    cte = _build_cte("root", [_build_concept("X")])
    cte.source_map = {
        "test.from_operand": ["test_returns"],
        "test.from_elsewhere": ["test_fact"],
        "test.from_both": ["test_fact", "test_returns"],
        "test.unbound": [],
    }
    blocked = _blocked_partials(
        cte,
        {"test.from_operand", "test.from_elsewhere", "test.from_both", "test.unbound"},
        {"test_returns"},
    )
    assert blocked == {"test.from_elsewhere", "test.from_both"}


def test_partial_addresses_per_source_type():
    """``partial_concepts`` lives on CTE/UnionCTE/QueryDatasource; a bare
    BuildDatasource has none, so it contributes no partial addresses."""
    key = _build_concept("KEY")
    other = _build_concept("OTHER")
    bd = BuildDatasource(
        name="d",
        columns=[BuildColumnAssignment(alias="KEY", concept=key)],
        address="d",
        namespace="test",
        grain=BuildGrain(),
    )
    assert _partial_addresses(bd) == set()

    cte = _build_cte("c", [key, other])
    cte.partial_concepts = [other]
    assert _partial_addresses(cte) == {other.address}

    qds = QueryDatasource(
        input_concepts=[key],
        output_concepts=[key],
        datasources=[bd],
        source_map={key.address: {bd}},
        grain=BuildGrain(components={key.address}),
        joins=[],
        partial_concepts=[key],
    )
    assert _partial_addresses(qds) == {key.address}


def _join_producer(base, agg, key, measure):
    """A CTE that LEFT-joins ``agg`` onto ``base`` and passes the agg measure
    through as a plain single-source projection."""
    producer = _build_cte("producer", [key, measure])
    producer.parent_ctes = [base, agg]
    producer.source_map = {
        key.address: [base.name],
        measure.address: [agg.name],
    }
    producer.joins = [
        Join(
            jointype=JoinType.LEFT_OUTER,
            right_cte=agg,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=base.source,
                    cte=base,
                )
            ],
        )
    ]
    return producer


def _passthrough_consumer(name, producer, concepts, condition=None):
    consumer = _build_cte(name, concepts)
    consumer.parent_ctes = [producer]
    consumer.source_map = {c.address: [producer.name] for c in concepts}
    consumer.condition = condition
    return consumer


def test_cross_cte_null_rejection_upgrades_producer_join():
    """A consumer's WHERE that rejects NULLs on a column padded by the
    producer's LEFT join upgrades that join to INNER (the q64 shape: the
    null-rejecting filter lives CTEs downstream of the outer join)."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    base = _build_cte("base_source", [key])
    agg = _build_cte("agg_source", [key, measure])
    producer = _join_producer(base, agg, key, measure)
    consumer = _passthrough_consumer(
        "consumer", producer, [key, measure], _condition_for(measure)
    )

    rule = UpgradeJoinOnGuards()
    inverse_map = {producer.name: [consumer]}
    changed, _ = rule.optimize(producer, inverse_map)
    assert changed, "expected cross-CTE proof to upgrade the join"
    assert producer.joins[0].jointype == JoinType.INNER


def test_cross_cte_null_rejection_propagates_through_passthrough():
    """The rejection carries through an intermediate plain-passthrough CTE:
    grandparent joins, parent projects, child filters."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    base = _build_cte("base_source", [key])
    agg = _build_cte("agg_source", [key, measure])
    producer = _join_producer(base, agg, key, measure)
    middle = _passthrough_consumer("middle", producer, [key, measure])
    final = _passthrough_consumer(
        "final", middle, [key, measure], _condition_for(measure)
    )

    rule = UpgradeJoinOnGuards()
    inverse_map = {producer.name: [middle], middle.name: [final]}
    changed, _ = rule.optimize(producer, inverse_map)
    assert changed, "expected transitive cross-CTE proof to upgrade the join"
    assert producer.joins[0].jointype == JoinType.INNER


def test_cross_cte_null_rejection_requires_every_consumer():
    """A second consumer with no rejecting condition reads the padded rows —
    the producer's join must stay preserving."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    base = _build_cte("base_source", [key])
    agg = _build_cte("agg_source", [key, measure])
    producer = _join_producer(base, agg, key, measure)
    filtering = _passthrough_consumer(
        "filtering", producer, [key, measure], _condition_for(measure)
    )
    reading = _passthrough_consumer("reading", producer, [key, measure])

    rule = UpgradeJoinOnGuards()
    inverse_map = {producer.name: [filtering, reading]}
    changed, _ = rule.optimize(producer, inverse_map)
    assert not changed
    assert producer.joins[0].jointype == JoinType.LEFT_OUTER


def test_cross_cte_null_rejection_blocked_by_existence_read():
    """A consumer reading the producer existentially (EXISTS subselect) is
    satisfied by ANY row — dropping padded rows could flip it, so no upgrade."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    base = _build_cte("base_source", [key])
    agg = _build_cte("agg_source", [key, measure])
    producer = _join_producer(base, agg, key, measure)
    consumer = _passthrough_consumer(
        "consumer", producer, [key, measure], _condition_for(measure)
    )
    consumer.existence_source_map = {key.address: [producer.name]}

    rule = UpgradeJoinOnGuards()
    inverse_map = {producer.name: [consumer]}
    changed, _ = rule.optimize(producer, inverse_map)
    assert not changed
    assert producer.joins[0].jointype == JoinType.LEFT_OUTER


def test_cross_cte_null_rejection_blocked_by_coalesced_projection():
    """A consumer that renders the address from several sources (COALESCE)
    masks a one-sided NULL — no rejection carries back to the producer."""
    key = _build_concept("KEY")
    measure = _build_concept("MEASURE")
    base = _build_cte("base_source", [key])
    agg = _build_cte("agg_source", [key, measure])
    other = _build_cte("other_source", [measure])
    producer = _join_producer(base, agg, key, measure)
    consumer = _passthrough_consumer(
        "consumer", producer, [key, measure], _condition_for(measure)
    )
    consumer.parent_ctes = [producer, other]
    consumer.source_map[measure.address] = [producer.name, other.name]

    rule = UpgradeJoinOnGuards()
    inverse_map = {producer.name: [consumer]}
    changed, _ = rule.optimize(producer, inverse_map)
    assert not changed
    assert producer.joins[0].jointype == JoinType.LEFT_OUTER
