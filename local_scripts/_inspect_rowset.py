"""Probe how q05's rowset is structured at the BuildConcept level so we
know how to traverse it under a label."""

import sys

sys.path.insert(0, "local_scripts")
from pathlib import Path

from discovery_v4 import _find_select, _materialize_for_query

from trilogy import Environment
from trilogy.core.models.build import BuildRowsetItem, BuildRowsetLineage
from trilogy.core.processing.concept_strategies_v4 import History

TPCDS = Path(__file__).resolve().parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
text = (TPCDS / "query05.preql").read_text()
env = Environment(working_path=TPCDS)
_, queries = env.parse(text)
select = _find_select(queries)
history = History(base_environment=env)
build_stmt, build_env, conditions = _materialize_for_query(env, select, history)

mandatory = list(build_stmt.output_components)
print("=== MANDATORY (outer SELECT) ===")
for c in mandatory:
    print(f"  {c.address}  derivation={c.derivation.value}")
    if isinstance(c.lineage, BuildRowsetItem):
        print(
            f"    -> ROWSET wrap, content={c.lineage.content.address}, rowset={c.lineage.rowset.name}"
        )
    elif c.lineage is not None:
        args = getattr(c.lineage, "concept_arguments", [])
        print(
            f"    -> lineage {type(c.lineage).__name__} args=[{', '.join(a.address for a in args)}]"
        )

# walk all rowset concepts reachable
seen: set[str] = set()
rowsets: dict[str, BuildRowsetLineage] = {}


def walk(c):
    if c.address in seen:
        return
    seen.add(c.address)
    if isinstance(c.lineage, BuildRowsetItem):
        rowsets[c.lineage.rowset.name] = c.lineage.rowset
        walk(c.lineage.content)
        return
    if c.lineage is None:
        return
    for arg in c.lineage.concept_arguments:
        ac = build_env.concepts.get(arg.address, arg)
        walk(ac)


for c in mandatory:
    walk(c)

for name, rs in rowsets.items():
    print(f"\n=== ROWSET '{name}' ===")
    print(f"  derived_concepts: {rs.derived_concepts}")
    print(f"  select type: {type(rs.select).__name__}")
    if hasattr(rs.select, "output_components"):
        print(
            f"  select.output_components addresses: {[c.address for c in rs.select.output_components]}"
        )
    if hasattr(rs.select, "selection"):
        print(
            f"  select.selection addresses: {[c.address for c in rs.select.selection]}"
        )
    if hasattr(rs.select, "where_clause") and rs.select.where_clause:
        print(f"  where_clause: {rs.select.where_clause}")
