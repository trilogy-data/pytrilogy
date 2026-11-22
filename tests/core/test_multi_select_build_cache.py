from collections import Counter

import pytest

from trilogy import Dialects, Environment
from trilogy.core.models.build import Factory

MODEL = """
key ws_id int;
property ws_id.ws_cust int;
property ws_id.ws_amt int;
property ws_id.ws_day int;
datasource web_sales (id: ws_id, c: ws_cust, a: ws_amt, d: ws_day) grain (ws_id)
query '''select 1 id, 1 c, 10 a, 1 d union all select 2, 2, 20, 2 union all select 3, 2, 30, 3''';

key cs_id int;
property cs_id.cs_cust int;
property cs_id.cs_amt int;
property cs_id.cs_day int;
datasource catalog_sales (id: cs_id, c: cs_cust, a: cs_amt, d: cs_day) grain (cs_id)
query '''select 1 id, 1 c, 5 a, 4 d union all select 2, 3, 7, 5''';
"""

UNION = """
with both as union(
    (select ws_id as id, ws_cust as cust, ws_amt as amt, ws_day as day),
    (select cs_id as id, cs_cust as cust, cs_amt as amt, cs_day as day)
) -> (id, cust, amt, day);
"""

QUERY = """
select both.cust, sum(both.amt) as total, count(both.id) as n, max(both.day) as last
order by both.cust asc;
"""


@pytest.fixture
def builds(monkeypatch: pytest.MonkeyPatch) -> Counter[str]:
    calls: Counter[str] = Counter()
    for name in ("_build_multi_select_lineage", "_build_multi_select_lineage_once"):
        original = getattr(Factory, name)

        def counted(self, *args, _name=name, _original=original, **kwargs):
            calls[_name] += 1
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(Factory, name, counted)
    return calls


def test_a_union_is_built_once_for_all_its_outputs(builds: Counter[str]):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL + UNION)
    rows = [tuple(r) for r in executor.execute_text(QUERY)[-1].fetchall()]
    assert rows == [(1, 15, 1, 4), (2, 50, 2, 3), (3, 7, 1, 5)]
    assert builds["_build_multi_select_lineage"] > 4
    assert builds["_build_multi_select_lineage_once"] == 1


def test_a_rebound_union_is_built_again(builds: Counter[str]):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL + UNION)
    executor.generate_sql(QUERY)
    executor.parse_text(UNION.replace("cs_amt as amt", "cs_amt + 1 as amt"))
    rows = [tuple(r) for r in executor.execute_text(QUERY)[-1].fetchall()]
    assert rows == [(1, 16, 1, 4), (2, 50, 2, 3), (3, 8, 1, 5)]
    assert builds["_build_multi_select_lineage_once"] == 2
