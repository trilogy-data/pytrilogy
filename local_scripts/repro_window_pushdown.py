"""Exact failing combo: filter=base_name, select=vehicle_name, window=rank_row_desc."""

import trilogy.core.processing.discovery_utility as du
import trilogy.core.processing.v4_node_generators.nested_select as m
from tests.test_window_where_pushdown_matrix import (
    MODEL,
    _oracle_rows,
    _oracle_sql,
    _sorted,
)
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_node_generators import multiselect, rowset, union_select

calls = {"plan": 0, "excluded": 0}
_op, _oc = m.plan_nested_select, du._component_map
traced_plan = lambda *a, **k: (
    calls.__setitem__("plan", calls["plan"] + 1),
    _op(*a, **k),
)[1]
# consumers bind the name at import, so patch each of them too
for mod in (m, rowset, multiselect, union_select):
    mod.plan_nested_select = traced_plan


def traced_cm(environment, g=None, island_rowsets=True, excluded_addresses=frozenset()):
    if excluded_addresses:
        calls["excluded"] += 1
    return _oc(environment, g, island_rowsets, excluded_addresses)


du._component_map = traced_cm

FILTER = "vehicle_name != 'B'"
WINDOW = "rank launch_id order by orb_pay desc"
WINDOW_SQL = "rank() over (order by orb_pay desc)"
QUERY = f"where {FILTER}\nselect vehicle_name, {WINDOW} as w;"

env = Environment()
env.parse(MODEL)
exe = Dialects.DUCK_DB.default_executor(environment=env)
actual = _sorted([tuple(r) for r in exe.execute_query(QUERY).fetchall()])
expected = _oracle_rows(_oracle_sql("vehicle_name", WINDOW_SQL, "row", FILTER))
print(f"plan calls={calls['plan']} excluded_nonempty={calls['excluded']}")
print(f"actual   {actual}")
print(f"expected {expected}")
print("MATCH" if actual == expected else "MISMATCH")
print("\n--- SQL ---")
print(exe.generate_sql(QUERY)[-1][:1200])
