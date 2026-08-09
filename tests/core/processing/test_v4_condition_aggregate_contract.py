import re

from trilogy import Dialects, Environment

_MODEL = """
key item_id int;
property item_id.item_size int;

key supplier_id int;
property supplier_id.supplier_name string;
property supplier_id.region string;

property <item_id, supplier_id>.supply_cost int;

datasource items (id: item_id, size: item_size)
grain (item_id)
query '''select 1 id, 15 size union all select 2 id, 10 size''';

datasource suppliers (id: supplier_id, n: supplier_name, r: region)
grain (supplier_id)
query '''
select 1 id, 'eu-low' n, 'EUROPE' r union all
select 2 id, 'eu-high' n, 'EUROPE' r union all
select 3 id, 'other' n, 'ASIA' r
''';

datasource supplies (item: item_id, supplier: supplier_id, c: supply_cost)
grain (item_id, supplier_id)
query '''
select 1 item, 1 supplier, 10 c union all
select 1 item, 2 supplier, 20 c union all
select 1 item, 3 supplier, 5 c union all
select 2 item, 1 supplier, 1 c
''';

auto min_europe_cost <- min(supply_cost ? region = 'EUROPE') by item_id;
"""

_QUERY = """
where item_size = 15
    and region = 'EUROPE'
    and supply_cost = min_europe_cost
select item_id, supplier_name;
"""


def test_aggregate_condition_feeder_keeps_only_value_and_grain_contract():
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(_QUERY)[-1]
    rows = executor.execute_text(_QUERY)[-1].fetchall()

    assert [tuple(row) for row in rows] == [(1, "eu-low")]
    assert len(re.findall(r"\bWITH\b|,\s*\w+\s+as\s*\(", sql, re.IGNORECASE)) == 5
    assert sql.count("'EUROPE'") == 4
    assert sql.count("min_europe_cost") == 2
