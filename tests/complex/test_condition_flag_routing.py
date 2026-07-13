"""A named boolean flag used both as a `? flag` filter operand and inside a
`where (f1 or f2)` OR is a derived row-argument of the condition: routing the
condition into the flag's own build is circular (the condition's row args
re-force the flag forever). The flag builds first and the condition applies
once above, over the joined rows — but only when every condition input is a
row-grain scalar can the lifted condition be pushed below windows/aggregates.
Standalone version of the enriched-q77 shape (tpc_ds
test_or_filter_over_differently_filtered_aggregates_no_recursion)."""

from typing import Any

from trilogy import Dialects, Environment

FLAG_SCHEMA = """key id int;
property id.d1 int;
property id.d2 int;
property id.chan string;
property id.profit float;
property id.loss float;
datasource t ( id, d1, d2, chan, profit, loss ) grain (id)
query '''select 1 as id, 3 as d1, 9 as d2, 'web' as chan, 10.0 as profit, 1.0 as loss
union all select 2, -1, 3, 'web', 20.0, 2.0
union all select 3, 9, 9, 'store', 30.0, 3.0''';
auto f1 <- d1 * 2 > d1;
auto f2 <- d2 between 2 and 5;
auto m <- coalesce(profit ? f1, 0) - coalesce(loss ? f2, 0);
"""


def _rows(model: str, query: str) -> list[tuple[Any, ...]]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return sorted(
        (tuple(r) for r in executor.execute_query(query).fetchall()),  # type: ignore[union-attr]
        key=str,
    )


def test_or_over_scalar_flags_lifts_condition():
    # f1/f2 closures are pure row scalars (f1 references d1 twice); every row
    # passes the OR: web = 10 + (0 - 2) = 8, store = 30.
    rows = _rows(
        FLAG_SCHEMA,
        "where chan is not null and (f1 or f2) select chan, sum(m) as t;",
    )
    assert rows == [("store", 30.0), ("web", 8.0)], rows


def test_or_with_filter_derived_input_not_row_scalar():
    # fp is filter-derived, so the condition inputs are NOT all row scalars;
    # `fp > 0` drops id=2 (fp null): web = 10, store = 30.
    rows = _rows(
        FLAG_SCHEMA + "auto fp <- profit ? f1;\n",
        "where (f1 or f2) and fp > 0 select chan, sum(m) as t;",
    )
    assert rows == [("store", 30.0), ("web", 10.0)], rows
