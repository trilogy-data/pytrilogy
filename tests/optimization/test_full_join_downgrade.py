"""Coverage for `DowngradeFullJoinOnGuards`.

The asymmetric-nullable rule in `get_join_type` promotes some joins to FULL.
This pass undoes that promotion when the surrounding WHERE makes the
preserved-by-FULL rows unreachable (their NULL fills can never satisfy the
filter).
"""

from trilogy import Dialects


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
    from trilogy import Environment
    from trilogy.core.enums import ComparisonOperator, FunctionType
    from trilogy.core.models.build import (
        BuildComparison,
        BuildFunction,
    )
    from trilogy.core.models.core import DataType
    from trilogy.core.optimizations.full_join_downgrade import (
        _concepts_in_expression,
        _gather_proofs,
        _proves_non_null,
    )
    from trilogy.constants import MagicConstants

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

    from trilogy.core.enums import Purpose

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

    # _concepts_in_expression also stops at coalesce
    assert _concepts_in_expression(coalesce) == set()
    assert _concepts_in_expression(multiply) == {y.address}

    # _gather_proofs walks AND-decomposed atoms
    from trilogy.core.enums import BooleanOperator
    from trilogy.core.models.build import BuildConditional

    cond = BuildConditional(
        left=BuildComparison(left=x, right=1, operator=ComparisonOperator.EQ),
        right=BuildComparison(
            left=y, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
        ),
        operator=BooleanOperator.AND,
    )
    assert _gather_proofs(cond) == {x.address, y.address}
