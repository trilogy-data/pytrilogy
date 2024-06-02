
## Query Planning

Query planning is divided into 3 core phases.

The first phase builds an abstract node tree by looping through every combination of
output concept and keys in the output query grain and recursively searching for sources.

It will begin with aggregations if those exist, then window functions, then filtration functions,
rowsets, and finally look for bare selects.

Each type of complex node will generate a new recursive node search for required parents,
until a set of terminal nodes with base concept selection is reached. 

A default merge node is injected between every recursion. The overall loop will terminate early 
if an output node is returned with all required query concepts. If not, the merge node will
handle a join between the returned subtrees. If there are not multiple nodes to merge,
the merge node will simply return the single parent node and prune itself from the graph.

In the second pass, each node is resolved to an abstract CTE. At this phase, CTEs that 
reference the same tables, parent CTEs, and filtering can be merged.

Finally, in query rendering each CTE is rendered to a backend appropriate query. The final
CTE, or the `base`, will contain all required columns for the final output. The last
select will only apply any query level filters + ordering, no joins will take place.

## Aug 2023 Update

For complex derivations, propogating the "full" context upstream is an issue. Instead, we need to adjust logic to prune the optional nodes in each search pattern.

For filter nodes -> we should have these generate a node with _just_ the filtered column + the new concept. 

## Debugging

Base query derivation accepts the 'DebugHook' defined under hooks, which will print to console
each step of the plan. This is a great first step to figure out what might be going
wrong with discovery in a query. 

Example usage

```python
from preql import parse
from preql.core.query_processor import process_query
from preql.hooks.query_debugger import DebuggingHook
from preql.core.models import Select

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
select: Select = parsed[-1]

query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

```