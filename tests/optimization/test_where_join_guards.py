from trilogy import Dialects, Environment
from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
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
    QueryDatasource,
)
from trilogy.core.processing.condition_utility import (
    _atom_proves_non_null,
    concepts_implied_non_null,
    gather_non_null_proofs,
    partial_addresses,
)


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


def _block_pin_heal(executor):
    """A sibling source carrying `region` complete inside a larger grain: it
    anchors the `~region` binding, so `decide_heal` leaves the FULL
    join in place for the optimizer passes under test (otherwise the WHERE
    proof heals the partial pre-discovery and no join is ever emitted). It
    also carries `amount`: an anchor whose rows the WHERE proof (`amount > 5`)
    can never kill is not dispensable, which is what keeps the anchor
    blocking. A sibling itself `~` on `region` would anchor nothing: two
    partial bindings have no defined relationship."""
    executor.execute_text("""
        key rm_id int;
        property <rm_id, region>.rm_note string;

        datasource region_notes (
            rm_id: rm_id,
            region: region,
            rm_note: rm_note,
            amount: amount,
        )
        grain (rm_id, region)
        query '''
        SELECT 1 AS rm_id, 'NA' AS region, 'x' AS rm_note, 10 AS amount
        ''';
    """)


def test_full_to_inner_when_both_sides_proven():
    """A WHERE atom that touches concepts unique to each side proves both
    sides must contribute non-null data → INNER."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)
    _block_pin_heal(executor)

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
    _block_pin_heal(executor)

    text = "WHERE amount > 5 SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "FULL JOIN" not in sql, sql
    assert "LEFT OUTER JOIN" in sql or "RIGHT OUTER JOIN" in sql, sql
    # The NULL-region fact row (amount 30) passes `amount > 5` and the dim is
    # not nullable, so an INNER would wrongly drop it — it must survive.
    assert _rows(executor, text) == {("NA", 10), ("EU", 20), (None, 30)}


def test_full_kept_when_only_coalesced_key_proven():
    """`region IS NOT NULL` on the merged join-key concept proves neither side
    alone, so no join may be downgraded on it. The region domain is read off
    `region_dim` alone (no coalesced key to prove) and the region the fact
    lacks survives."""
    executor = Dialects.DUCK_DB.default_executor()
    _persist_setup(executor)

    text = "WHERE region is not null SELECT region, sum(amount) as total;"
    sql = executor.generate_sql(executor.parse_text(text)[-1])[0]
    assert "INNER JOIN" not in sql, sql
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

    assert ret.address in _atom_proves_non_null(
        BuildComparison(left=flag, right=True, operator=ComparisonOperator.IS),
        between=True,
    )
    assert ret.address in _atom_proves_non_null(
        BuildComparison(left=True, right=flag, operator=ComparisonOperator.EQ),
        between=True,
    )
    assert ret.address not in _atom_proves_non_null(
        BuildComparison(left=flag, right=False, operator=ComparisonOperator.IS),
        between=True,
    )
    assert ret.address not in _atom_proves_non_null(
        BuildComparison(left=flag, right=False, operator=ComparisonOperator.EQ),
        between=True,
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
    assert _atom_proves_non_null(
        BuildComparison(
            left=x, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
        ),
        between=True,
    ) == {x.address}

    # x IS NULL → empty (we want non-nulls, not nulls)
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=x, right=MagicConstants.NULL, operator=ComparisonOperator.IS
            ),
            between=True,
        )
        == set()
    )

    # x = 1 → {x.address}; literal side ignored
    assert _atom_proves_non_null(
        BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ), between=True
    ) == {x.address}

    # x = y → both
    assert _atom_proves_non_null(
        BuildComparison(left=x, right=y, operator=ComparisonOperator.EQ), between=True
    ) == {x.address, y.address}

    # x > 1.2 * y inside a comparison → both (multiply is not null-opaque)
    multiply = BuildFunction(
        operator=FunctionType.MULTIPLY,
        arguments=[1.2, y],
        output_data_type=DataType.FLOAT,
        output_purpose=Purpose.PROPERTY,
        arg_count=2,
    )
    assert _atom_proves_non_null(
        BuildComparison(left=x, right=multiply, operator=ComparisonOperator.GT),
        between=True,
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
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce,
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            ),
            between=True,
        )
        == set()
    )

    # concepts_implied_non_null also stops at coalesce
    assert concepts_implied_non_null(coalesce) == set()
    assert concepts_implied_non_null(multiply) == {y.address}

    # gather_non_null_proofs walks AND-decomposed atoms
    cond = BuildConditional(
        left=BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ),
        right=BuildComparison(
            left=y, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
        ),
        operator=BooleanOperator.AND,
    )
    assert gather_non_null_proofs(cond) == {x.address, y.address}

    # NULL IS NOT x → mirror form, same result as x IS NOT NULL
    assert _atom_proves_non_null(
        BuildComparison(
            left=MagicConstants.NULL, right=x, operator=ComparisonOperator.IS_NOT
        ),
        between=True,
    ) == {x.address}

    # x IS NOT y (neither side a NULL literal) → empty
    assert (
        _atom_proves_non_null(
            BuildComparison(left=x, right=y, operator=ComparisonOperator.IS_NOT),
            between=True,
        )
        == set()
    )

    # sum skips NULL rows: a non-null sum proves no row's y non-null
    sum_y = BuildAggregateWrapper(
        function=BuildFunction(
            operator=FunctionType.SUM,
            arguments=[y],
            output_data_type=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            arg_count=1,
        )
    )
    assert concepts_implied_non_null(sum_y) == set()

    # Operators outside IS/IS_NOT/NULL_PROPAGATING_OPS (e.g. ELSE) fall through
    # to the empty-set guard.
    assert (
        _atom_proves_non_null(
            BuildComparison(left=x, right=y, operator=ComparisonOperator.ELSE),
            between=True,
        )
        == set()
    )

    # BETWEEN proves every concept inside left/low/high non-null.
    from trilogy.core.models.build import BuildBetween

    assert _atom_proves_non_null(
        BuildBetween(left=x, low=1, high=10), between=True
    ) == {x.address}
    assert _atom_proves_non_null(
        BuildBetween(left=x, low=y, high=10), between=True
    ) == {
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

    assert _atom_proves_non_null(like, between=True) == {s.address}
    assert gather_non_null_proofs(like) == {s.address}

    cond = BuildConditional(
        left=like,
        right=BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert gather_non_null_proofs(cond) == {s.address, x.address}


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
    assert _atom_proves_non_null(
        BuildComparison(left=coalesce(x, 0), right=0, operator=ComparisonOperator.GT),
        between=True,
    ) == {x.address}

    # coalesce(x, 0) >= 0 — 0 >= 0 is TRUE, so x-null rows survive: no proof.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, 0), right=0, operator=ComparisonOperator.GTE
            ),
            between=True,
        )
        == set()
    )

    # coalesce(x, 100) > 0 — 100 > 0 is TRUE, so x-null rows survive: no proof.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, 100), right=0, operator=ComparisonOperator.GT
            ),
            between=True,
        )
        == set()
    )

    # coalesce(x, 0) IS NOT NULL is a tautology (coalesce always non-null with
    # non-null default) — must NOT claim x non-null. This goes through the
    # IS_NOT branch, not the null-propagating one, so it stays opaque.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, 0),
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            ),
            between=True,
        )
        == set()
    )

    # coalesce(x, y) > 0 — non-literal default; can't fold, no proof.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, y), right=0, operator=ComparisonOperator.GT
            ),
            between=True,
        )
        == set()
    )

    # Mirror form: 0 < coalesce(x, 0) — same proof via the flipped operator.
    assert _atom_proves_non_null(
        BuildComparison(left=0, right=coalesce(x, 0), operator=ComparisonOperator.LT),
        between=True,
    ) == {x.address}

    # Multiple defaults, all literals, all failing: still proves PRIMARY.
    assert _atom_proves_non_null(
        BuildComparison(
            left=coalesce(x, 0, -1), right=0, operator=ComparisonOperator.GT
        ),
        between=True,
    ) == {x.address}

    # Multiple defaults, one of them satisfies the comparison → no proof.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, 0, 5), right=0, operator=ComparisonOperator.GT
            ),
            between=True,
        )
        == set()
    )

    # Equality: coalesce(x, 0) = 5 — 0 = 5 is FALSE, so proves x non-null.
    assert _atom_proves_non_null(
        BuildComparison(left=coalesce(x, 0), right=5, operator=ComparisonOperator.EQ),
        between=True,
    ) == {x.address}

    # Equality where default matches: coalesce(x, 5) = 5 — 5 = 5 TRUE, no proof.
    assert (
        _atom_proves_non_null(
            BuildComparison(
                left=coalesce(x, 5), right=5, operator=ComparisonOperator.EQ
            ),
            between=True,
        )
        == set()
    )


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
    assert partial_addresses(bd) == set()

    cte = _build_cte("c", [key, other])
    cte.partial_concepts = [other]
    assert partial_addresses(cte) == {other.address}

    qds = QueryDatasource(
        input_concepts=[key],
        output_concepts=[key],
        datasources=[bd],
        source_map={key.address: {bd}},
        grain=BuildGrain(components={key.address}),
        joins=[],
        partial_concepts=[key],
    )
    assert partial_addresses(qds) == {key.address}
