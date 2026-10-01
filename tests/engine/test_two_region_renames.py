"""Renames of several `~` regions' columns beside the fact (TPC-DS agent run
2026-10-01, R1/R2). Each rename is its region's rows; reading the other
regions' domains too evaluated it over a (customer, store) stream that FINAL
joined back on one key: the fact fanned out, and through a rowset it was
INNER-joined and lost rows."""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

_SALES = """select 1 as ticket, 1 as item, 1 as customer_id, 10 as store_id, 100 as date_id
union all select 1, 2, 1, 10, 100
union all select 2, 1, 2, 20, 200
union all select 3, 1, null, 10, 100
union all select 4, 2, 2, null, 200
union all select 5, 1, 3, 10, null
union all select 6, 1, null, null, 100"""

MODEL = f"""
key customer_id int;
key store_id int;
property store_id.store_name string;
key date_id int;
property date_id.day string;
key ticket int;
key item int;

datasource customers (customer_id: customer_id)
grain (customer_id)
query '''select 1 as customer_id union all select 2 union all select 3''';

datasource stores (store_id: store_id, store_name: store_name)
grain (store_id)
query '''select 10 as store_id, 'north' as store_name
union all select 20, 'south' union all select 30, 'east' ''';

datasource dates (date_id: date_id, day: day)
grain (date_id)
query '''select 100 as date_id, 'mon' as day
union all select 200, 'tue' union all select 300, 'wed' ''';

datasource sales (
    ticket: ticket, item: item, customer_id: ~?customer_id,
    store_id: ~?store_id, date_id: ~?date_id,
)
grain (ticket, item)
query '''{_SALES}''';
"""

# 'wed' and 'east' have no sale: each is an extension row
EXPECTED = sorted(
    [
        (1, 1, 1, "mon", "north"),
        (1, 2, 1, "mon", "north"),
        (2, 1, 2, "tue", "south"),
        (3, 1, None, "mon", "north"),
        (4, 2, 2, "tue", None),
        (5, 1, 3, None, "north"),
        (6, 1, None, "mon", None),
        (None, None, None, "wed", None),
        (None, None, None, None, "east"),
    ],
    key=str,
)


@pytest.fixture
def executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    return executor


def _rows(executor: Executor, text: str) -> list[tuple]:
    return sorted(
        (tuple(r) for r in executor.execute_text(text)[-1].fetchall()), key=str
    )


def test_renamed_columns_of_three_regions(executor: Executor):
    renamed = (
        "select ticket as t, item as i, customer_id as c, day as d, store_name as s;"
    )
    assert _rows(executor, "select ticket, item, customer_id, day, store_name;") == (
        EXPECTED
    )
    assert _rows(executor, renamed) == EXPECTED
    assert executor.generate_sql(renamed)[-1].count(_SALES) == 1


def test_rowset_of_region_renames_keeps_fact_rows(executor: Executor):
    assert (
        _rows(
            executor,
            """rowset r <- select ticket as t, item as i, customer_id as c, day as d, store_name as s;
select count(r.t) as n, count(r.d) as d;""",
        )
        == [(6, 7)]
    )
