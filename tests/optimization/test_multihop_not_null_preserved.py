from trilogy import Dialects, Environment

# A nullable FK (anonymous sales) on the fact, chained sale -> customer -> address.
NULLABLE_FK_MODEL = """
key sale_id int;
property sale_id.customer_sk int?;
key customer_sk int;
property customer_sk.address_sk int;
key address_sk int;
property address_sk.state string;
datasource sales ( sale_id: sale_id, customer_sk: customer_sk, )
grain (sale_id)
query '''
select 1 as sale_id, 101 as customer_sk union all
select 2 as sale_id, 102 as customer_sk union all
select 3 as sale_id, null as customer_sk union all
select 4 as sale_id, null as customer_sk
''';
datasource customers ( customer_sk: customer_sk, address_sk: address_sk, )
grain (customer_sk)
query '''select 101 as customer_sk, 201 as address_sk union all select 102 as customer_sk, 202 as address_sk''';
datasource addresses ( address_sk: address_sk, state: state, )
grain (address_sk)
query '''select 201 as address_sk, 'CA' as state union all select 202 as address_sk, 'NY' as state''';
"""

NULLABLE_COLUMN_MODEL = """
key id int;
property id.opt_val string?;
datasource t ( id: id, opt_val: opt_val, )
grain (id)
query '''select 1 as id, 'a' as opt_val union all select 2 as id, null as opt_val''';
"""


def _exec(model: str):
    env = Environment()
    env.parse(model)
    return Dialects.DUCK_DB.default_executor(environment=env)


# regression: an `IS NOT NULL` on a multi-hop path reached via a nullable FK used
# to be dropped pre-resolution as "tautological" (the column is non-null in its
# own table), leaking outer-join-padded rows into a NULL group. Keeping the
# predicate through build now drives an INNER join, so the anonymous sales are
# excluded — regardless of whether the (now-redundant) guard is later stripped.
def test_multihop_nullable_fk_no_null_leak():
    exec = _exec(NULLABLE_FK_MODEL)
    rows = exec.execute_query("""
select state, count(sale_id) as n
where address_sk is not null
order by state;
""").fetchall()
    assert all(r[0] is not None for r in rows)
    assert sorted(rows) == [("CA", 1), ("NY", 1)]


# the StripRedundantNotNull rule drops a guard on a non-null output column once it
# is provably redundant on the built tree (a key reached via INNER joins).
def test_redundant_not_null_on_output_stripped():
    exec = _exec(NULLABLE_FK_MODEL)
    sql = exec.generate_sql("""
select address_sk
where address_sk is not null
order by address_sk;
""")[-1]
    assert "is not null" not in sql.lower(), sql


# a genuinely nullable column can be NULL in the output, so its IS NOT NULL is a
# real filter and must survive.
def test_nullable_column_not_null_preserved():
    exec = _exec(NULLABLE_COLUMN_MODEL)
    body = """
select id
where opt_val is not null
order by id;
"""
    sql = exec.generate_sql(body)[-1]
    assert "is not null" in sql.lower(), sql
    assert exec.execute_query(body).fetchall() == [(1,)]
