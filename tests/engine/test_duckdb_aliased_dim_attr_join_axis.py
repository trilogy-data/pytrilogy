"""Aliasing an output must not cost the planner the FK join axis.

`select fact.key as k, dim.attr as a` mints a BASIC rename per output. That
gave every root a non-empty lineage reach, which skipped the conservative
one-bucket bailout in `partition_roots`; the dim attribute then islanded into
its own scan bucket because the co-source connectivity test only related roots
sharing a datasource or a REQUESTED key. The FK was dropped, and the FINAL
merge either raised the keyless-join guard or silently cross-joined. The bare
(un-aliased) spelling of each query below always planned correctly.
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key state_sk int;
properties state_sk (state_name string, city string);

datasource states (s_sk: state_sk, s_name: state_name, s_city: city)
grain (state_sk)
query '''select 1 as s_sk, 'CA' as s_name, 'LA' as s_city
union all select 2, 'NY', 'NYC' ''';

key customer_sk int;
properties customer_sk (customer_id string, first_name string);

datasource customers (
    c_sk: customer_sk,
    c_id: customer_id,
    c_fn: first_name,
    c_state: ~state_sk,
)
grain (customer_sk)
query '''select 1 as c_sk, 'AAA' as c_id, 'Ann' as c_fn, 1 as c_state
union all select 2, 'BBB', 'Bob', 2''';

key item_sk int;
property item_sk.item_id string;

datasource items (i_sk: item_sk, i_id: item_id)
grain (item_sk)
query '''select 7 as i_sk, 'ITEM7' as i_id
union all select 8, 'ITEM8' ''';

key ticket_number int;
properties <ticket_number, item_sk> (return_amt float);

datasource returns (
    t_num: ticket_number,
    i_sk: ~item_sk,
    cust: ~customer_sk,
    amt: return_amt,
)
grain (ticket_number, item_sk)
query '''select 10 as t_num, 7 as i_sk, 1 as cust, 5.0 as amt
union all select 11, 8, 2, 7.0
union all select 12, 7, 1, 9.0''';
"""

CASES = [
    (
        "fact_key_beside_dim_attr",
        "select ticket_number as t, return_amt as a, customer_id as c order by t asc;",
        [(10, 5.0, "AAA"), (11, 7.0, "BBB"), (12, 9.0, "AAA")],
    ),
    (
        "dim_attr_without_fact_key",
        "select return_amt as a, customer_id as c order by a asc;",
        [(5.0, "AAA"), (7.0, "BBB"), (9.0, "AAA")],
    ),
    (
        "two_hop_dim_attr",
        "select ticket_number as t, state_name as s order by t asc;",
        [(10, "CA"), (11, "NY"), (12, "CA")],
    ),
    (
        "pinned_aggregate_extra_dim_attrs",
        """select customer_id as cid, first_name as fn, state_name as st, city as ct,
    sum(return_amt) by customer_id, state_name as tot
order by cid asc;""",
        [("AAA", "Ann", "CA", "LA", 14.0), ("BBB", "Bob", "NY", "NYC", 7.0)],
    ),
    (
        "rowset_subset_join_dim_attr",
        """rowset r <- select ticket_number as ticket, item_sk as isk, return_amt as amt;
select r.ticket as ticket, r.isk as isk, item_id as label
subset join r.isk = item_sk order by ticket asc;""",
        [(10, 7, "ITEM7"), (11, 8, "ITEM8"), (12, 7, "ITEM7")],
    ),
]


@pytest.mark.parametrize("name,query,expected", CASES, ids=[c[0] for c in CASES])
def test_aliased_outputs_keep_the_fk_axis(name, query, expected):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.execute_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    assert "on 1=1" not in sql, sql
    rows = executor.execute_text(query)[-1].fetchall()
    assert [tuple(r) for r in rows] == [
        tuple(float(v) if isinstance(v, float) else v for v in row) for row in expected
    ]
