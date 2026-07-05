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


def _channel(p: str) -> str:
    # a fact with a binding-level (`?`) nullable FK joined to a date dim; the
    # nullability lives only on the datasource column, not the concept
    return f"""
key {p}d_id int;
property {p}d_id.{p}yr int;
datasource {p}dates ( {p}d_id: {p}d_id, {p}yr: {p}yr, )
grain ({p}d_id)
query '''select 1 as {p}d_id, 2000 as {p}yr union all select 2 as {p}d_id, 2001 as {p}yr''';

key {p}cust int;
key {p}id int;
property {p}id.{p}qty int;
property {p}id.{p}wc int;
property {p}id.{p}date int;
datasource {p}tbl ( {p}id: {p}id, {p}cust: ?{p}cust, {p}qty: {p}qty, {p}wc: {p}wc, {p}date: {p}date, )
grain ({p}id)
query '''select 1 as {p}id, 10 as {p}cust, 2 as {p}qty, 10 as {p}wc, 1 as {p}date
union all select 2 as {p}id, null as {p}cust, 4 as {p}qty, 20 as {p}wc, 1 as {p}date''';

merge {p}date into ~{p}d_id;
"""


UNION_CHANNEL_MODEL = _channel("s_") + _channel("w_")

UNION_ARMS = """
with chans as union(
    (where s_yr = 2000 and s_cust is not null
     select 'store' as channel, s_cust as cu, s_qty as qty, s_wc as wc),
    (where w_yr = 2000 and w_cust is not null
     select 'web' as channel, w_cust as cu, w_qty as qty, w_wc as wc)
) -> (channel, cu, qty, wc);
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


# q78 regression: an authored `is not null` on a `?`-bound FK vanished from every
# union arm once the downstream aggregate consumed two or more union measure
# columns. The arm node's own condition proved the FK non-null, build-time
# refinement dropped it from nullable_concepts, and StripRedundantNotNull read
# that absence as never-nullable — stripping the very filter that proved it and
# silently leaking null-customer rows.
def test_union_multi_measure_keeps_binding_nullable_fk_guard():
    exec = _exec(UNION_CHANNEL_MODEL)
    body = UNION_ARMS + """
select
    chans.cu as cust_id,
    sum(chans.qty ? chans.channel = 'store') as store_qty,
    sum(chans.wc ? chans.channel != 'store') as other_wc
order by cust_id asc;
"""
    sql = exec.generate_sql(body)[-1]
    assert sql.lower().count("is not null") >= 2, sql
    rows = exec.execute_query(body).fetchall()
    assert rows == [(10, 2, 10)], rows


# one consumed measure column is the shape that never lost the guard; keep it
# covered so the two shapes can't diverge again
def test_union_single_measure_keeps_binding_nullable_fk_guard():
    exec = _exec(UNION_CHANNEL_MODEL)
    body = UNION_ARMS + """
select
    chans.cu as cust_id,
    sum(chans.qty ? chans.channel = 'store') as store_qty,
    sum(chans.qty ? chans.channel != 'store') as other_qty
order by cust_id asc;
"""
    sql = exec.generate_sql(body)[-1]
    assert sql.lower().count("is not null") >= 2, sql
    rows = exec.execute_query(body).fetchall()
    assert rows == [(10, 2, 2)], rows


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
