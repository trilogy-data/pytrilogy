## Query Planning

Query planning is divided into 3 core phases: discovery builds an abstract node
tree, each node is then resolved to an abstract CTE, and finally each CTE is
rendered to a backend-appropriate query.

Discovery is a staged planner: concept graph, group graph, per-group
materialization, final assembly. It is documented in
[`docs/v4_network_discovery_design.md`](../../../docs/v4_network_discovery_design.md);
the entrypoint is `concept_strategies_v4.search_concepts`, the stage
implementations live in `v4_helper/`, and the per-derivation node builders live
in `v4_node_generators/`.

The final CTE, or the `base`, contains all required columns for the final
output. The last select applies only query-level filters + ordering; no joins
take place there.

## Debugging

Base query derivation accepts the `DebuggingHook` defined under hooks, which
prints each step of the plan to the console. This is a great first step to
figure out what might be going wrong with discovery in a query.

Example usage

```python
from trilogy import parse
from trilogy.core.query_processor import process_query
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.core.statements.author import SelectStatement

declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);


datasource posts (
user_id: user_id,
id: post_id
)
grain (post_id)
address bigquery-public-data.stackoverflow.post_history
;

select
user_id,
count(post_id) -> user_post_count
;

metric avg_user_post_count <- avg(user_post_count);


datasource users (
id: user_id,
display_name: display_name,
about_me: about_me,
)
grain (user_id)
address bigquery-public-data.stackoverflow.users
;


select
avg_user_post_count
;


"""
env, parsed = parse(declarations)
select: SelectStatement = parsed[-1]

query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

```
