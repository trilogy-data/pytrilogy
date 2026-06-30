"""Minimal repro of the canonical-address collision that the merge_node
passthrough currently papers over (faithful reduction of
tests/modeling/gcat/test_gcat.py::test_extra_filter).

`s1` and `s2` are two date_spine keys with identical lineage, so they share a
canonical_address. Each is merged to a different physical column of one
datasource (`d1`, `d2`). A multiselect needs both, sourced from the single
shared `facts` scan. The correct plan is one `facts` scan exposing both columns;
the bug drops one of them (canonical collision) and renders INVALID_REFERENCE.
"""

from trilogy import CONFIG, Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key id int;
property id.d1 date;
property id.d2 date;
datasource facts (
    id: id,
    d1: ?d1,
    d2: ?d2,
) grain (id) address facts;
key s1 <- date_spine(date_add(current_date(), day, -10), current_date());
key s2 <- date_spine(date_add(current_date(), day, -10), current_date());
merge d1 into ~s1;
merge d2 into ~s2;
auto m1 <- count(id) by d1;
auto m2 <- count(id) by d2;
"""

BASIC_MODEL = """
key id int;
key base int;
property id.d1 int;
property id.d2 int;
datasource facts (
    id: id,
    d1: ?d1,
    d2: ?d2,
) grain (id) address facts;
datasource dim_base (
    base: base,
) grain (base) address dim_base;
property base.s1 <- base + 1;
property base.s2 <- base + 1;
merge d1 into ~s1;
merge d2 into ~s2;
auto m1 <- count(id) by d1;
auto m2 <- count(id) by d2;
"""

QUERY = """
select s1, m1
merge
select s2, m2
align d:s1,s2;
"""


def _generate(query: str = QUERY, model: str = MODEL):
    env = Environment()
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(model)
    queries = base.parse_text(query)
    strict_mode = CONFIG.strict_mode
    CONFIG.strict_mode = False
    try:
        return base.generate_sql(queries[-1])[0]
    finally:
        CONFIG.strict_mode = strict_mode


def test_basic_canonical_collision_s1_arm_sources_own_physical_column():
    sql = _generate("select s1, m1;", model=BASIC_MODEL)
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert sql.count('FROM\n    "facts"') == 1, sql
    assert sql.count('FROM\n    "dim_base"') == 1, sql
    assert '"facts"."d1"' in sql, sql
    assert '"facts"."d2" as "d2"' not in sql, sql


def test_canonical_collision_s1_arm_sources_own_physical_column():
    sql = _generate("select s1, m1;")
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    # One facts scan exposing its own physical column `d1` for `s1`, never the
    # sibling's `d2`. The single-consumer fact scan inlines into the spine merge,
    # so the merge key `facts.d1` renders inside `coalesce(...)` (its complete
    # domain is the spine). Rows verified in test_merge_unnest_partial_join.
    assert sql.count('FROM\n    "facts"') == 1, sql
    assert '"facts"."d2" as "d2"' not in sql, sql
    assert 'coalesce("facts"."d1","quizzical"."s1") as "s1"' in sql, sql


def test_canonical_collision_s2_arm_sources_own_physical_column():
    sql = _generate("select s2, m2;")
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert sql.count('FROM\n    "facts"') == 1, sql
    assert '"facts"."d1" as "d1"' not in sql, sql
    assert 'coalesce("facts"."d2","quizzical"."s2") as "s2"' in sql, sql


def test_canonical_collision_single_source_both_columns():
    sql = _generate()
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    # one unified scan of `facts` exposing both merged date columns
    assert sql.count('FROM\n    "facts"') == 1, sql
    assert '"facts"."d1" as "s1"' in sql, sql
    assert '"facts"."d2" as "s2"' in sql, sql
