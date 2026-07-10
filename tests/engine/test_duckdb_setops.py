"""Relational except(...)/intersect(...) TVFs — set-operator siblings of the
table-valued union(...).

Semantics under test: SQL set operators (deduplicated output, NULL-safe row
comparison — NULL matches NULL, unlike `not in`), EXCEPT arm order is semantic
(left-fold over 3+ arms), and the existing union(...) stays a bag (UNION ALL).
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment, parse
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.query_processor import process_query
from trilogy.dialect.bigquery import BigqueryDialect

_SETOP_FIXTURE = """
key line_id int;
property line_id.item_id int;
property line_id.cat string?;
property line_id.yr int;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    cat: cat,
    yr: yr,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 'a' as cat, 2001 as yr union all
select 2 as line_id, 1 as item_id, 'a' as cat, 2001 as yr union all
select 3 as line_id, 2 as item_id, cast(null as varchar) as cat, 2001 as yr union all
select 4 as line_id, 3 as item_id, 'b' as cat, 2001 as yr union all
select 5 as line_id, 1 as item_id, 'a' as cat, 2002 as yr union all
select 6 as line_id, 2 as item_id, cast(null as varchar) as cat, 2002 as yr union all
select 7 as line_id, 4 as item_id, 'c' as cat, 2003 as yr union all
select 8 as line_id, 1 as item_id, 'a' as cat, 2011 as yr union all
select 9 as line_id, 2 as item_id, 'b' as cat, 2011 as yr union all
select 10 as line_id, 1 as item_id, 'b' as cat, 2012 as yr union all
select 11 as line_id, 2 as item_id, 'a' as cat, 2012 as yr
''';
"""

_EXCEPT_ITEMS = """
with combined as except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
select combined.k order by combined.k asc;
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_SETOP_FIXTURE)
    return executor


def _rows(executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[0].fetchall()]


def test_except_named_deduplicates_and_renders_except():
    # 2001 items are a bag {1, 1, 2, 3}; 2002 items {1, 2}. EXCEPT is a set
    # operator: the duplicate 1 is removed with its match, 3 survives once.
    executor = _executor()
    sql = executor.generate_sql(_EXCEPT_ITEMS)[-1]
    assert "\nEXCEPT\n" in sql, sql
    assert "UNION ALL" not in sql, sql
    assert _rows(executor, _EXCEPT_ITEMS) == [(3,)]


def test_except_null_matches_null():
    # cats 2001 = {'a','a',NULL,'b'}, cats 2002 = {'a',NULL}. EXCEPT compares
    # NULL=NULL as a match (unlike NOT IN), so only 'b' survives.
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (where yr = 2001 select cat -> c),
    (where yr = 2002 select cat -> c)
) -> (c);
select combined.c;
""",
    )
    assert rows == [("b",)]


def test_intersect_null_matches_null():
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as intersect(
    (where yr = 2001 select cat -> c),
    (where yr = 2002 select cat -> c)
) -> (c);
select combined.c;
""",
    )
    assert set(rows) == {("a",), (None,)}
    assert len(rows) == 2


def test_except_composite_mixed_type_columns():
    # The q87 shape composite membership cannot express: a multi-column row
    # comparison over mixed types (int + string), with dedup + NULL safety.
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (where yr = 2001 select item_id -> k, cat -> c),
    (where yr = 2002 select item_id -> k, cat -> c)
) -> (k, c);
select combined.k, combined.c;
""",
    )
    assert rows == [(3, "b")]


def test_except_multi_arm_left_folds():
    # all items {1,2,3,4} except 2002 {1,2} except 2003 {4} -> {3}
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (select item_id -> k),
    (where yr = 2002 select item_id -> k),
    (where yr = 2003 select item_id -> k)
) -> (k);
select combined.k;
""",
    )
    assert rows == [(3,)]


def test_except_arm_order_is_semantic():
    # 2002 items {1,2} are a subset of 2001 items {1,2,3}: reversing the arms
    # must yield the empty set, not {3}. Guards arm-order preservation through
    # QueryDatasource (which sorts datasources for commutative UNION ALL).
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (where yr = 2002 select item_id -> k),
    (where yr = 2001 select item_id -> k)
) -> (k);
select combined.k;
""",
    )
    assert rows == []


def test_intersect_multi_arm():
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as intersect(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k),
    (select item_id -> k)
) -> (k);
select combined.k order by combined.k asc;
""",
    )
    assert rows == [(1,), (2,)]


def test_inline_except_aggregate():
    executor = _executor()
    rows = _rows(
        executor,
        """
from except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k)
select count(k) -> total;
""",
    )
    assert rows == [(1,)]


def test_grouped_aggregate_over_except():
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (where yr = 2001 select item_id -> k, cat -> c),
    (where yr = 2002 select item_id -> k, cat -> c)
) -> (k, c);
select combined.c, count(combined.k) -> items;
""",
    )
    assert rows == [("b", 1)]


def test_except_subset_consumer_keeps_tuple_identity():
    # q87 shape: 2011 tuples {(1,'a'),(2,'b')} vs 2012 {(1,'b'),(2,'a')} — no
    # tuple matches, so EXCEPT keeps both rows even though every k (and every
    # c) appears in both arms. A consumer reading only one declared output must
    # not prune the arms' projections: set-op row identity is the full tuple.
    executor = _executor()
    query = """
with combined as except(
    (where yr = 2011 select item_id -> k, cat -> c),
    (where yr = 2012 select item_id -> k, cat -> c)
) -> (k, c);
select count(combined.k) -> total;
"""
    assert _rows(executor, query) == [(2,)]


def test_intersect_subset_consumer_keeps_tuple_identity():
    executor = _executor()
    query = """
with combined as intersect(
    (where yr = 2011 select item_id -> k, cat -> c),
    (where yr = 2012 select item_id -> k, cat -> c)
) -> (k, c);
select count(combined.k) -> total;
"""
    assert _rows(executor, query) == [(0,)]


def test_union_subset_consumer_still_prunes():
    # For a UNION ALL bag an unused column can't change row multiplicity, so
    # demand-driven pruning stays on — only set operators keep the full tuple.
    executor = _executor()
    query = """
with combined as union(
    (where yr = 2011 select item_id -> k, cat -> c),
    (where yr = 2012 select item_id -> k, cat -> c)
) -> (k, c);
select count(combined.k) -> total;
"""
    sql = executor.generate_sql(query)[-1]
    assert _rows(executor, query) == [(4,)]
    assert sql.count('as "c"') == 0, sql


def test_filter_over_except_output():
    executor = _executor()
    rows = _rows(
        executor,
        """
with combined as except(
    (where yr = 2001 select item_id -> k, cat -> c),
    (where yr = 2002 select item_id -> k, cat -> c)
) -> (k, c);
where combined.c = 'b'
select combined.k, combined.c;
""",
    )
    assert rows == [(3, "b")]


def test_union_tvf_remains_union_all_bag():
    # Regression: the union(...) TVF is a bag stack, untouched by the set-op
    # plumbing. Each arm is a grain-deduped select ({1,2,3} and {1,2}); the
    # stack keeps the cross-arm duplicates an EXCEPT/INTERSECT would collapse.
    executor = _executor()
    query = """
from union(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k)
select count(k) -> total;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql, sql
    assert "\nEXCEPT\n" not in sql, sql
    assert _rows(executor, query) == [(5,)]


def test_except_and_union_over_same_arms_do_not_merge():
    # Same arms, different operators, one query: the identifier must keep the
    # two combinators as distinct CTEs instead of merging them.
    executor = _executor()
    rows = _rows(
        executor,
        """
with kept as except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
with stacked as union(
    (where yr = 2001 select item_id -> ku),
    (where yr = 2002 select item_id -> ku)
) -> (ku);
select count(kept.k) -> except_count, count(stacked.ku) -> union_count;
""",
    )
    assert rows == [(1, 5)]


def test_except_requires_two_arms():
    executor = _executor()
    with pytest.raises(InvalidSyntaxException, match="at least two relational arms"):
        executor.generate_sql("""
with combined as except(
    (where yr = 2001 select item_id -> k)
) -> (k);
select combined.k;
""")


def test_except_arm_arity_mismatch():
    executor = _executor()
    with pytest.raises(InvalidSyntaxException, match="except arm 1 projects"):
        executor.generate_sql("""
with combined as except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k, cat -> c)
) -> (k);
select combined.k;
""")


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_setops_parse_on_both_backends(backend: ParserBackend):
    prior = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        executor = _executor()
        assert _rows(executor, _EXCEPT_ITEMS) == [(3,)]
    finally:
        CONFIG.parser_backend = prior


def test_except_survives_import_namespace(tmp_path: Path):
    (tmp_path / "setops_model.preql").write_text(_SETOP_FIXTURE + """
with combined as except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
""")
    env = Environment(working_path=tmp_path)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = _rows(
        executor,
        """
import setops_model as m;
select m.combined.k;
""",
    )
    assert rows == [(3,)]


def test_bigquery_spells_except_distinct():
    env, parsed = parse(_SETOP_FIXTURE + _EXCEPT_ITEMS)
    query = process_query(statement=parsed[-1], environment=env)
    sql = BigqueryDialect().compile_statement(query)
    assert "EXCEPT DISTINCT" in sql, sql
