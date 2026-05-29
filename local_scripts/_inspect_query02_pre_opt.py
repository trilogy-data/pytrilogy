import sys

sys.path.insert(0, "local_scripts")
from discovery_v4 import run_tpcds_query

from trilogy.core.processing.nodes import SelectNode
from trilogy.core.query_processor import datasource_to_cte, flatten_ctes
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.dialect.duckdb import DuckDBDialect

info, benv, _, stmt = run_tpcds_query("02")
node = info.strategy_node.copy()
if stmt is not None and getattr(stmt, "having_clause", None):
    from trilogy.core.enums import BooleanOperator
    from trilogy.core.models.build import BuildConditional

    having = stmt.having_clause.conditional
    combined = (
        BuildConditional(
            left=node.conditions, right=having, operator=BooleanOperator.AND
        )
        if node.conditions
        else having
    )
    node = SelectNode(
        output_concepts=list(stmt.output_components),
        input_concepts=list(node.usable_outputs),
        parents=[node],
        environment=node.environment,
        partial_concepts=list(node.partial_concepts),
        conditions=combined,
    )
node.hidden_concepts = set(stmt.hidden_components) if stmt else set()
node.ordering = stmt.order_by if stmt else None
node.rebuild_cache()
qds = node.resolve()
root_cte = datasource_to_cte(qds, benv.cte_name_map)
raw = list(reversed(flatten_ctes(root_cte)))
seen = {}
for cte in raw:
    if cte.name not in seen:
        seen[cte.name] = cte
    else:
        seen[cte.name] = seen[cte.name] + cte
for cte in raw:
    cte.parent_ctes = [seen[x.name] for x in cte.parent_ctes]
deduped = list(seen.values())
if stmt is not None:
    root_cte.limit = stmt.limit
    root_cte.hidden_concepts = set(stmt.hidden_components)
outputs = [c for c in node.output_concepts if c.address not in node.hidden_concepts]
pq = ProcessedQuery(output_columns=outputs, ctes=deduped, base=root_cte)
sql = DuckDBDialect().compile_statement(pq)
print("=== SQL WITHOUT optimize_ctes ===")
print(sql)
print()
print(f"has WEB filter: {'WEB' in sql}")
print(f"has 2001 filter: {'2001' in sql}")
