"""BigQuery will not plan a FULL OUTER JOIN unless its ON clause carries "an
equality of fields from both sides of the join". A top-level ``X = Y`` counts
whatever X and Y are, but the base dialect's null-safe expansion
``(l = r or (l is null and r is null))`` is an OR, and BigQuery only reads an
OR back as a join key while *both* operands are plain column references -- an
expression on either side is rejected, as is ``IS NOT DISTINCT FROM``.

The expression case is the ordinary one, not an exotic one: a join key merged
across several row-preserving sources renders as
``coalesce(a.x, b.x, c.x) = d.x``. That is how the thelook ``sales_reporting``
refresh started failing in production with

    BadRequest: 400 FULL OUTER JOIN cannot be used without a condition that is
    an equality of fields from both sides of the join.

BigQuery therefore encodes those keys with ``TO_JSON_STRING`` on both sides --
a top-level equality, which it accepts, and null-safe for free. Everything
else, including a non-nullable merged key that already renders as a bare
equality, keeps the base form, and no other dialect changes.

The acceptance rules asserted here were established against real BigQuery
tables; ``tests/engine/bigquery/test_bigquery_full_join_keys_live.py`` re-runs
that evidence wherever credentials exist."""

import re

from trilogy import parse
from trilogy.core.enums import JoinType, Modifier
from trilogy.dialect.bigquery import BigqueryDialect, null_wrapper
from trilogy.dialect.duckdb import DuckDBDialect

# One fact split across two sources over partial keys, with aggregates that
# have to be computed apart and merged back on the shared grain -- the shape
# that makes a join key a COALESCE over several CTEs.
MERGED_KEY_MODEL = """
key order_id int;
key user_id int;
key product_id int;
key item_id int;

property order_id.item_count int;
property item_id.sale_price float;
property product_id.unit_cost float;
property product_id.brand string;
property user_id.state string;

auto revenue <- item_count * sale_price;
auto cost <- unit_cost * item_count;
auto total_revenue <- sum(revenue);
auto total_margin <- sum(revenue) - sum(cost);
auto item_quantity <- group(item_count) by item_id;
auto total_quantity <- sum(item_quantity);

datasource order_items (
    id: item_id,
    order_id: order_id,
    user_id: ~user_id,
    product_id: ~product_id,
    sale_price: sale_price,
)
grain (item_id)
address order_items;

datasource orders (
    order_id: order_id,
    user_id: ~user_id,
    num_of_item: item_count,
)
grain (order_id)
address orders;

datasource products (
    id: product_id,
    cost: unit_cost,
    brand: brand,
)
grain (product_id)
address products;

datasource users (
    id: user_id,
    state: state,
)
grain (user_id)
address users;

select
    order_id,
    user_id,
    product_id,
    item_id,
    brand,
    state,
    total_revenue,
    total_quantity,
    total_margin,
;
"""

# `alias`.`column`, or a bare `column`. Anything else is an expression to
# BigQuery's join planner.
FIELD = r"`[^`]+`(?:\.`[^`]+`)*"
FULL_JOIN_CLAUSE = re.compile(r"FULL JOIN .*", re.IGNORECASE)
# the base dialect's null-safe expansion, with either operand free-form
NULL_SAFE_OR = re.compile(r"\((?P<left>.+?) = (?P<right>.+?) or \(.+?is null\)\)")


def render(dialect, source: str) -> str:
    env, statements = parse(source)
    return dialect.compile_statement(
        dialect.generate_queries(env.duplicate(), [statements[-1]])[0]
    )


def bigquery_illegal_full_join_keys(sql: str) -> list[str]:
    """FULL JOIN ON clauses BigQuery will refuse to plan."""
    offenders = []
    for clause in FULL_JOIN_CLAUSE.findall(sql):
        _, _, on = clause.partition(" on ")
        # `on 1=1` is not here: a constant folds to a cross product, which
        # BigQuery plans fine (test_bigquery_full_join_keys_live.py).
        if "is not distinct from" in on:
            offenders.append(clause)
            continue
        for match in NULL_SAFE_OR.finditer(on):
            if not (
                re.fullmatch(FIELD, match.group("left"))
                and re.fullmatch(FIELD, match.group("right"))
            ):
                offenders.append(clause)
                break
    return offenders


def test_null_wrapper_encodes_only_illegal_full_join_keys():
    nullable = [Modifier.NULLABLE]
    field, other = "`a`.`x`", "`b`.`x`"
    expr = "coalesce(`a`.`x`, `b`.`x`)"

    # not nullable: a bare equality, which BigQuery accepts on either operand
    # shape -- the merged key that needs no null-safety needs no encoding
    assert null_wrapper(field, other, [], JoinType.FULL) == f"{field} = {other}"
    assert null_wrapper(expr, other, [], JoinType.FULL) == f"{expr} = {other}"

    # nullable fields: BigQuery reads the expansion as an equality of fields
    assert null_wrapper(field, other, nullable, JoinType.FULL) == (
        f"({field} = {other} or ({field} is null and {other} is null))"
    )

    # nullable expression, but not a FULL join -- BigQuery only restricts FULL
    for jointype in (JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
        assert null_wrapper(expr, other, nullable, jointype) == (
            f"({expr} = {other} or ({expr} is null and {other} is null))"
        )

    # no join at all (membership rendering shares the wrapper): base form
    assert null_wrapper(expr, other, nullable) == (
        f"({expr} = {other} or ({expr} is null and {other} is null))"
    )

    # nullable expression on a FULL join: the one case that will not compile
    assert null_wrapper(expr, other, nullable, JoinType.FULL) == (
        f"TO_JSON_STRING({expr}) = TO_JSON_STRING({other})"
    )
    assert null_wrapper(field, expr, nullable, JoinType.FULL) == (
        f"TO_JSON_STRING({field}) = TO_JSON_STRING({expr})"
    )


def test_merged_full_join_key_compiles_for_bigquery():
    sql = render(BigqueryDialect(), MERGED_KEY_MODEL)
    # the model still produces the coalesced key this is all about
    assert re.search(r"FULL JOIN .*coalesce", sql), sql
    assert "TO_JSON_STRING(coalesce(" in sql, sql
    assert bigquery_illegal_full_join_keys(sql) == []


def test_field_keyed_full_joins_keep_the_cheaper_form():
    """The encoding is not applied wholesale -- a FULL join whose keys are
    plain columns keeps the expansion BigQuery already accepts, so the common
    case pays nothing for it."""
    sql = render(BigqueryDialect(), MERGED_KEY_MODEL)
    field_keyed = [c for c in FULL_JOIN_CLAUSE.findall(sql) if "coalesce(" not in c]
    assert field_keyed, sql
    for clause in field_keyed:
        assert "TO_JSON_STRING" not in clause, clause


def test_other_dialects_are_untouched():
    sql = render(DuckDBDialect(), MERGED_KEY_MODEL)
    assert "TO_JSON_STRING" not in sql
    # duckdb spells the same key with the operator BigQuery lacks
    assert re.search(r"FULL JOIN .*coalesce.*is not distinct from", sql), sql
