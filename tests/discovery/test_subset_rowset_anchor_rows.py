"""A `subset join` anchor keeps every row of the anchor's complete source.

`subset join buyers.id = id` declares the rowset's `id` a subset of the key. A
sibling that renders `id` only through that member holds it as a subset, so a
complete scan of `id` must not be folded into it (`_renders_no_less_completely`
in `_fold_passthrough_parents`). Folded, the statement's rows were the subset's
rows: an order with no buyer in the filtered rowset vanished. The plan reads
`id` off `orders` and LEFT JOINs each subset onto it.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key id int;
key bill_id int;
key ship_id int;

datasource orders (
    id: id,
    bill: bill_id,
    ship: ship_id,
)
grain (id)
query '''
select * from (values (1, 10, 100), (2, 20, 200), (3, 30, 300)) t(id, bill, ship)
''';

with buyers_a as
SELECT
    id,
    bill_id as cust_id
where bill_id > 10
;

with buyers_b as
SELECT
    id,
    ship_id as cust_id
where ship_id < 300
;

SELECT
    id,
    buyers_a.cust_id as a_cust,
    buyers_b.cust_id as b_cust,
subset join buyers_a.id = id and buyers_b.id = id
order by id asc
;
"""


def test_filtered_subsets_keep_every_anchor_row():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = executor.execute_text(_MODEL)[-1].fetchall()
    assert [tuple(row) for row in rows] == [
        (1, None, 100),
        (2, 20, 200),
        (3, 30, None),
    ]
