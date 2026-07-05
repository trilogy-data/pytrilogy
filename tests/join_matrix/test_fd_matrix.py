"""FD/cardinality tier (docs/domain_graph_design.md step 4): join on A,
carry B where A → B.

`dim` binds a and b completely at grain (a), declaring the FD a → b globally
(the domain graph's complete-binding rule). A fact and an enrichment source
both bind {a, b}; the enrichment's own grain (its row id) is NOT the
connecting key set, so before the FD closure the planner joined on BOTH keys.
The closure proves the b pair redundant — equality on a implies equality on
b — and the join must ride on a alone, with rows identical on honest data
(b is a function of a by declaration; violating data is a lying declaration,
the author-error ruling).
"""

import re
from pathlib import Path

from tests.join_matrix.harness import make_engine, sort_rows

DIM = """key a int;
key b int;

datasource dim (ka: a, kb: b) grain (a)
query '''select 1 ka, 10 kb union all select 2, 10 union all select 3, 20''';
"""

FACT = """import dim as dim;
key f_id int;
property f_id.v1 int;

datasource f1 (i: f_id, fa: dim.a, fb: dim.b, v: v1) grain (f_id)
query '''select 1 i, 1 fa, 10 fb, 100 v union all select 2, 1, 10, 200
union all select 3, 2, 10, 400 union all select 4, 3, 20, 800''';
"""

# enrichment keyed by its own row id — (a, b) is NOT its grain, so the grain
# restriction stays out of the way and the FD prune is what decides the keys
ENRICH = """import dim as dim;
key e_id int;
property e_id.attr string;

datasource enrich (i: e_id, ea: dim.a, eb: dim.b, t: attr) grain (e_id)
query '''select 1 i, 1 ea, 10 eb, 'x' t union all select 2 i, 2 ea, 10 eb, 'y' t
union all select 3 i, 3 ea, 20 eb, 'z' t''';
"""

QUERY = """
import fact as f;
import enrich as e;
merge e.dim.a into f.dim.a;
merge e.dim.b into f.dim.b;

select
    e.attr,
    sum(f.v1) as s1
order by e.attr asc;
"""

# attr per a: 1→x, 2→y, 3→z; v1 per a: 300, 400, 800
ORACLE = [("x", 300), ("y", 400), ("z", 800)]


def _write_models(tmp_path: Path) -> Path:
    (tmp_path / "dim.preql").write_text(DIM)
    (tmp_path / "fact.preql").write_text(FACT)
    (tmp_path / "enrich.preql").write_text(ENRICH)
    return tmp_path


def test_fd_prunes_redundant_join_key_no_fanout(tmp_path: Path):
    engine = make_engine(_write_models(tmp_path))
    statements = engine.parse_text(QUERY)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    assert rows == ORACLE, (rows, sql)
    # the fact↔enrich join must ride on `a` alone: the b pair is functionally
    # redundant (a → b) and pruning it is the observable FD-closure behavior
    join_clauses = re.findall(r"JOIN[^\n]+ on ([^\n]+)", sql)
    assert join_clauses, sql
    assert not any(" AND " in clause for clause in join_clauses), sql
    assert any("dim_a" in clause for clause in join_clauses), sql
