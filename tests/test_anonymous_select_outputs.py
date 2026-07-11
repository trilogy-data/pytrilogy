"""Unaliased (anonymous) select expressions: `select a, b+1;` materializes a
virtual concept named from the expression text (`count(x)` -> `x_count`),
with a content-hash suffix on collision. Bare concept references keep their
plain-projection semantics, and the ambiguous bare-`by`-list comma form is
refused rather than silently gobbled."""

from __future__ import annotations

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.models.author import ConceptRef
from trilogy.core.statements.author import ConceptTransform
from trilogy.parsing.v2.model import HydrationError

MODEL = """
key id int;
property id.val int;
property id.name string;

datasource rows (
    id: id,
    val: val,
    name: name
)
grain (id)
query '''
select 1 as id, 10 as val, 'a' as name
union all
select 2, 20, 'b'
union all
select 3, 30, 'c'
''';
"""


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def executor(request) -> Executor:
    prior = CONFIG.parser_backend
    CONFIG.parser_backend = request.param
    try:
        executor = Dialects.DUCK_DB.default_executor()
        executor.execute_text(MODEL)
        yield executor
    finally:
        CONFIG.parser_backend = prior


def _run(executor: Executor, query: str):
    rs = executor.execute_text(query)[-1]
    return list(rs.keys()), rs.fetchall()


def test_anonymous_scalar_expression(executor):
    cols, rows = _run(executor, "select id, val + 1 order by id asc;")
    assert cols == ["id", "val_1"]
    assert rows == [(1, 11), (2, 21), (3, 31)]


def test_anonymous_aggregate(executor):
    cols, rows = _run(executor, "select count(id);")
    assert cols == ["id_count"]
    assert rows == [(3,)]


def test_anonymous_aggregate_with_grain(executor):
    cols, rows = _run(executor, "select name, sum(val) by name order by name asc;")
    assert cols == ["name", "sum_val_by_name"]
    assert rows == [("a", 10), ("b", 20), ("c", 30)]


def test_anonymous_paren_by_grain(executor):
    cols, _ = _run(executor, "select sum(val) by (name, id), id;")
    assert cols == ["sum_val_by_name_id", "id"]


def test_order_by_generated_name(executor):
    cols, rows = _run(
        executor, "select name, sum(val) by name order by sum_val_by_name desc;"
    )
    assert cols == ["name", "sum_val_by_name"]
    assert rows == [("c", 30), ("b", 20), ("a", 10)]


def test_hide_modifier_on_anonymous(executor):
    cols, _ = _run(executor, "select --val + 1, id;")
    assert cols == ["id"]


def test_name_collision_falls_back_to_hash(executor):
    cols, _ = _run(executor, "select val / 2, val * 2;")
    assert cols[0] == "val_2"
    assert cols[1] != "val_2" and cols[1].startswith("val_2")


def test_same_expression_reuses_concept(executor):
    cols1, _ = _run(executor, "select id, val + 1;")
    cols2, _ = _run(executor, "select name, val + 1;")
    assert cols1[1] == cols2[1] == "val_1"


def test_rowset_anonymous_output(executor):
    cols, rows = _run(
        executor,
        "with r as select name, sum(val) by name; "
        "select r.name, r.sum_val_by_name order by r.name asc;",
    )
    assert cols == ["r_name", "r_sum_val_by_name"]
    assert rows == [("a", 10), ("b", 20), ("c", 30)]


def test_having_matches_anonymous_aggregate(executor):
    cols, rows = _run(
        executor,
        "select name, count(id) having count(id) > 0 order by name asc;",
    )
    assert cols == ["name", "id_count"]
    assert rows == [("a", 1), ("b", 1), ("c", 1)]


def test_bare_reference_stays_plain_projection(executor):
    from trilogy.parser import parse_text

    _, stmts = parse_text("select id, val + 1;", executor.environment)
    select = stmts[-1]
    assert isinstance(select.selection[0].content, ConceptRef)
    assert isinstance(select.selection[1].content, ConceptTransform)


def test_bare_by_list_comma_is_refused(executor):
    with pytest.raises(HydrationError, match="Ambiguous unaliased"):
        executor.execute_text("select sum(val) by name, name;")


def test_anonymous_name_does_not_shadow_user_concept(executor):
    executor.execute_text("auto val_1 <- val * 100;")
    cols, rows = _run(executor, "select id, val + 1 order by id asc;")
    assert cols[1] != "val_1"
    assert rows[0][1] == 11
    cols, rows = _run(executor, "select id, val_1 order by id asc;")
    assert rows[0][1] == 1000
