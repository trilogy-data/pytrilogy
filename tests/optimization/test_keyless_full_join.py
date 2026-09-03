from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key oid int;
property oid.amt float;
key pid int;
property pid.qty int;
property pid.region string;
datasource orders (oid:oid, amt:amt) grain (oid)
  query '''select * from (values (1, 10.0), (2, 20.0)) as t(oid, amt)''';
datasource parts (pid:pid, qty:qty, region:region) grain (pid)
  query '''select * from (values (1, 7, 'east'), (2, 8, 'west')) as t(pid, qty, region)''';
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    return executor


def test_grand_total_cartesian_narrows_to_inner():
    executor = _executor()
    query = "select sum(amt) as total_amt, sum(qty) as total_qty;"
    sql = executor.generate_sql(query)[-1]

    assert "INNER JOIN" in sql, sql
    assert "FULL JOIN" not in sql, sql
    assert [tuple(r) for r in executor.execute_text(query)[0].fetchall()] == [
        (30.0, 15)
    ]


def test_planner_narrows_before_the_optimizer():
    # The planner emits the narrowed type itself, so the optimizer rule has
    # nothing left to do: the SQL is identical with the rule switched off.
    executor = _executor()
    query = "select sum(amt) as total_amt, sum(qty) as total_qty;"
    narrowed_sql = executor.generate_sql(query)[-1]
    narrowed = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]

    assert "FULL JOIN" not in narrowed_sql, narrowed_sql
    full_sql = narrowed_sql.replace("INNER JOIN", "FULL JOIN")
    unnarrowed = [tuple(r) for r in executor.execute_raw_sql(full_sql).fetchall()]
    assert narrowed == unnarrowed


def test_having_on_aggregate_keeps_row_semantics():
    # The planner joins the two grand totals INNER before the HAVING is pushed
    # into one side's CTE; INNER commutes with that pushdown (a FULL would
    # resurrect the other side's row with a NULL), so the filtered row and
    # only the filtered row survives, with or without the optimizer rule.
    executor = _executor()
    failing = (
        "select sum(amt) as total_amt, sum(qty) as total_qty having total_qty > 1000;"
    )
    passing = (
        "select sum(amt) as total_amt, sum(qty) as total_qty having total_qty > 10;"
    )
    sql = executor.generate_sql(failing)[-1]
    assert "FULL JOIN" not in sql, sql
    assert executor.execute_text(failing)[0].fetchall() == []
    assert [tuple(r) for r in executor.execute_text(passing)[0].fetchall()] == [
        (30.0, 15)
    ]


def test_keyed_full_join_is_untouched():
    # The rule only ever collapses a keyless cartesian; a real union join keeps
    # its FULL.
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text("""
key cid int;
property cid.cname string;
key sid int;
property sid.scust int;
datasource customers (cid:cid, cname:cname) grain (cid)
  query '''select * from (values (1, 'a'), (2, 'b')) as t(cid, cname)''';
datasource signups (sid:sid, scust:scust) grain (sid)
  query '''select * from (values (10, 2), (11, 3)) as t(sid, scust)''';
""")
    sql = executor.generate_sql(
        "select cid, cname, count(sid) as signups union join scust = cid;"
    )[-1]

    assert "FULL JOIN" in sql, sql
