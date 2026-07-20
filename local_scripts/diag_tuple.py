import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from discovery_v4 import (
    BuildInfo,
    V4History,
    _find_select,
    _materialize_for_query,
    generate_graph,
    search_concepts,
    write_diagnostics,
)
from trilogy import Environment
from trilogy.constants import CONFIG

CONFIG.use_v4_discovery = True

MODEL = """
key id int;
property id.val int;
property id.cat int;
datasource t (id: id, val: val, cat: cat) grain (id)
  query '''
    select 1 as id, 10 as val, 1 as cat
    union all select 2, 20, 1
    union all select 3, 30, 2
  ''';

with pairs as select val, cat where val = 20;
select (20, 1) in (pairs.val, pairs.cat) as present, (20, 2) in (pairs.val, pairs.cat) as cross_pair_absent;
"""

env = Environment()
env, queries = env.parse(MODEL)
statement = _find_select(queries)
history = V4History(base_environment=env)
build_statement, benv, where = _materialize_for_query(env, statement, history)

info: BuildInfo = search_concepts(
    mandatory_list=list(build_statement.output_components),
    history=history,
    environment=benv,
    depth=0,
    g=generate_graph(benv),
    conditions=[where] if where else [],
)
write_diagnostics(info, "tuple_grainless", Path("local_scripts/v4_diagnostics"))
print("done")
