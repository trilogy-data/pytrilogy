"""Print rowset handle pseudonyms with and without a declared scoped join (s25)."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from discovery_v4 import _find_select, _materialize_for_query  # noqa: E402

from trilogy import Environment  # noqa: E402
from trilogy.constants import CONFIG  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import V4History  # noqa: E402

CONFIG.use_v4_discovery = True

A = """key aid int;
property aid.av float;
property aid.aw float;
datasource a (i: aid, v: av, w: aw) grain (aid) address a_tbl;
"""

BARE = """
import a as a;
with rs as select a.aid as k, a.av as sv;
select a.aid, a.aw, rs.sv
order by a.aid;
"""

JOINED = """
import a as a;
with rs as select a.aid as k, a.av as sv;
select a.aid, rs.sv
subset join rs.k = a.aid
order by a.aid;
"""

for label, q in (("bare", BARE), ("joined", JOINED)):
    tmp = Path(tempfile.mkdtemp())
    (tmp / "a.preql").write_text(A)
    env = Environment(working_path=tmp)
    _, queries = env.parse(q)
    select = _find_select(queries)
    history = V4History(base_environment=env)
    if select.join_clauses:
        history.build_caches.scoped_joins = [
            (j.source_address, j.target_address, j.join_type)
            for j in select.join_clauses
        ]
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
    print(f"=== {label} ===")
    print("scoped_join_key_groups:", dict(build_env.scoped_join_key_groups))
    for addr in ("rs.k", "rs.sv", "a.aid"):
        c = build_env.concepts.get(addr)
        if c is None:
            print(addr, "<missing>")
            continue
        print(
            addr, "| derivation:", c.derivation, "| pseudonyms:", sorted(c.pseudonyms)
        )
