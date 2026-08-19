"""An output alias must not change the result grain.

`select dim as name` over a fact-grain row source with an existence predicate
in the `where` used to lose the output GROUP BY: the alias adds a BASIC
projection on top of the fact merge, and the dedup check read that projection's
*declared* grain (built from its own outputs) instead of the fact rows
underneath it, so the projection looked like it was already at output grain.

Both ingredients are required — a bare `select dim` grouped correctly, and an
alias without the membership resolved against the dimension alone — which is
why the benchmark corpus never caught it.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# 3 items, 2 of them cheap; the fact carries several snapshots per item so any
# lost dedup fans the dimension projection out.
MODEL = """
key item_sk int;
property item_sk.item_id string;
property item_sk.price int;
datasource items (s: item_sk, i: item_id, p: price) grain (item_sk)
query '''
select 1 s, 'I1' i, 70 p union all select 2 s, 'I2' i, 80 p
union all select 3 s, 'I3' i, 500 p
''';

key inv_id int;
property inv_id.inv_item int;
property inv_id.qoh int;
datasource inventory (d: inv_id, s: inv_item, q: qoh) grain (inv_id)
query '''
select 1 d, 1 s, 200 q union all select 2 d, 1 s, 300 q
union all select 3 d, 1 s, 400 q union all select 4 d, 2 s, 250 q
union all select 5 d, 2 s, 350 q union all select 6 d, 3 s, 260 q
union all select 7 d, 3 s, 270 q
''';
merge inv_item into item_sk;

auto cheap_items <- item_sk ? price between 68 and 98;
"""

FACT_WHERE = "where qoh >= 100 and item_sk in cheap_items "
DIM_WHERE = "where item_sk in cheap_items "
CHEAP = [("I1",), ("I2",)]


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_fact_source_membership_bare_output(tmp_path: Path):
    assert _run(tmp_path, FACT_WHERE + "select item_id;") == sort_rows(CHEAP)


def test_fact_source_membership_aliased_output(tmp_path: Path):
    assert _run(tmp_path, FACT_WHERE + "select item_id as item_code;") == sort_rows(
        CHEAP
    )


def test_fact_source_membership_aliased_output_not_in(tmp_path: Path):
    assert _run(
        tmp_path, "where qoh >= 100 and item_sk not in cheap_items select item_id as c;"
    ) == sort_rows([("I3",)])


def test_fact_source_aliased_output_no_membership(tmp_path: Path):
    assert _run(
        tmp_path, "where qoh >= 100 and price < 100 select item_id as item_code;"
    ) == sort_rows(CHEAP)


def test_dimension_source_membership_aliased_output(tmp_path: Path):
    assert _run(tmp_path, DIM_WHERE + "select item_id as item_code;") == sort_rows(
        CHEAP
    )


def test_alias_does_not_change_grain_with_second_output(tmp_path: Path):
    # aliasing any single output is enough to trigger it; the alias need not be
    # the only projected column.
    assert _run(
        tmp_path, FACT_WHERE + "select item_id as item_code, price;"
    ) == sort_rows([("I1", 70), ("I2", 80)])
