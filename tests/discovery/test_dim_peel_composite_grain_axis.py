"""Lock: a dimension reached through a foreign key OFF a composite fact grain
re-attaches to an aggregate at that grain.

`sales` is keyed `(item_id, ticket)` and binds `customer_id` beside it, so the
pair determines `region` jointly while neither key does alone. The peeled
`region` scan kept a merge key only when that single key determined an output,
so it carried none and the FINAL merge was a keyless join.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key item_id int;
property item_id.brand string;
key ticket int;
key customer_id int;
property customer_id.region string;
properties <ticket, item_id> (amount float);

datasource items (item_id, brand) grain (item_id)
query '''select 1 as item_id, 'acme' as brand union all select 2, 'zed' ''';

datasource customers (customer_id, region) grain (customer_id)
query '''
select 101 as customer_id, 'east' as region
union all select 102, 'west'
union all select 103, 'north'
''';

datasource sales (ticket, item_id, ?customer_id, amount) grain (item_id, ticket)
query '''
select 1 as ticket, 1 as item_id, 101 as customer_id, 6.0 as amount
union all select 1, 2, 101, 4.0
union all select 2, 1, 102, 4.0
union all select 3, 2, null, 1.0
''';
"""

_ORDER = "order by ticket asc, item_id asc"


def _rows(query: str) -> list[tuple]:
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL)
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("total", ["sum(amount)", "sum(amount) by item_id, ticket"])
def test_dimension_through_off_grain_foreign_key(total: str):
    assert _rows(f"select item_id, ticket, region, {total} as total {_ORDER};") == [
        (1, 1, "east", 6.0),
        (2, 1, "east", 4.0),
        (1, 2, "west", 4.0),
        (2, 3, None, 1.0),
    ]


def test_both_dimensions_beside_by_key_aggregate():
    query = (
        "select item_id, ticket, brand, region,"
        f" sum(amount) by item_id, ticket as total {_ORDER};"
    )
    assert _rows(query) == [
        (1, 1, "acme", "east", 6.0),
        (2, 1, "zed", "east", 4.0),
        (1, 2, "acme", "west", 4.0),
        (2, 3, "zed", None, 1.0),
    ]
