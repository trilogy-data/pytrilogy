"""A dim bridge (supplying the binding key) combined with two OUTER (LEFT) rowset
joins off the same anchor key must render valid SQL — the downstream coalesce must
reference a column the dim-bridge CTE actually projects, not a hidden binding key.

Surfaced on TPC-DS q11: three aggregate rowsets per year joined to `customer` for
the binding key, and two LEFT joins to lay the other years' revenue side by side.

`QueryDatasource.get_alias` walks the pseudonym closure and prefers a NON-hidden
member, so the merged OUTER key renders from the dim-bridge CTE's actually-projected
join-key alias instead of its hidden binding key.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

CUSTOMER = """key id int;
property id.name string;
property id.code string;
datasource customers (cid: id, nm: name, cd: code)
grain (id)
query '''
select 1 as cid, 'Alice' as nm, 'A001' as cd union all
select 2, 'Bob', 'B002' union all
select 3, 'Cara', 'C003' ''';
"""

SALES = """import customer as customer;
key sale_id int;
property sale_id.amt float;
property sale_id.yr int;
datasource sales (sid: sale_id, cust: customer.id, amt: amt, yr: yr)
grain (sale_id)
query '''
select 1 as sid, 1 as cust, 10.0 as amt, 2001 as yr union all
select 2, 2, 20.0, 2001 union all
select 3, 1, 5.0, 2002 union all
select 4, 3, 7.0, 2002 union all
select 5, 1, 3.0, 2000 union all
select 6, 2, 4.0, 2000''';
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "customer.preql").write_text(CUSTOMER)
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


_QUERY = """
import sales as ss;
import customer as c;

rowset sr01 <- where ss.yr = 2001
    select ss.customer.id as cust_id, sum(ss.amt) as s_rev;
rowset sr02 <- where ss.yr = 2002
    select ss.customer.id as cust_id, sum(ss.amt) as s_rev;
rowset sr03 <- where ss.yr = 2000
    select ss.customer.id as cust_id, sum(ss.amt) as s_rev;

where sr01.s_rev > 0
select c.code as code, c.name,
    sr02.s_rev, sr03.s_rev
{j1} join sr01.cust_id = c.id
{j2} join sr01.cust_id = sr02.cust_id
{j3} join sr01.cust_id = sr03.cust_id
order by c.code asc;
"""


def _sql(models: Path, j1: str, j2: str, j3: str) -> str:
    return _executor(models).generate_sql(_QUERY.format(j1=j1, j2=j2, j3=j3))[-1]


def _rows(models: Path, j1: str, j2: str, j3: str) -> list[tuple]:
    res = _executor(models).execute_text(_QUERY.format(j1=j1, j2=j2, j3=j3))[0]
    return [tuple(r) for r in res.fetchall()]


def test_all_left_unaffected(models: Path):
    sql = _sql(models, "left", "left", "left")
    assert "INVALID" not in sql
    assert _rows(models, "left", "left", "left") == [
        ("A001", "Alice", 5.0, 3.0),
        ("B002", "Bob", None, 4.0),
    ]
