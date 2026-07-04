"""Unit + e2e coverage for the BASIC-into-GROUP fold gate in
``CollapseSingleParent`` (`basic_fold_into_group_is_safe`).

A scalar row projection over a GROUP parent (e.g. `sum(a)/sum(b)`) folds into the
GROUP's SELECT list -- valid SQL and the single-CTE shape v3 emits natively. The
gate restricts that to the row-preserving subset: every output the child derives
*locally* must be a scalar, while aggregate/window columns merely passed through
from the GROUP parent are fine."""

from types import SimpleNamespace

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.optimizations.collapse_single_parent import (
    basic_fold_into_group_is_safe,
)


def _col(address: str, derivation: Derivation, lineage=None) -> SimpleNamespace:
    return SimpleNamespace(address=address, derivation=derivation, lineage=lineage)


def _parent(output_lcl: set[str]) -> SimpleNamespace:
    return SimpleNamespace(output_lcl=output_lcl)


def _child(*columns: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(output_columns=list(columns))


def test_gate_allows_scalar_over_group():
    parent = _parent({"local.week", "local.agg_a", "local.agg_b"})
    child = _child(
        _col("local.week", Derivation.ROOT),
        _col("local.agg_a", Derivation.AGGREGATE),  # passthrough
        _col("local.agg_b", Derivation.AGGREGATE),  # passthrough
        _col("local.ratio", Derivation.BASIC),  # new scalar over the aggregates
    )
    assert basic_fold_into_group_is_safe(parent, child)


def test_gate_allows_dimension_join_passthrough_aggregates():
    parent = _parent({"local.key", "local.total"})
    child = _child(
        _col("local.key", Derivation.ROOT),
        _col("local.total", Derivation.AGGREGATE),  # passthrough
        _col("local.label", Derivation.BASIC),  # joined-on dimension label
    )
    assert basic_fold_into_group_is_safe(parent, child)


def test_gate_blocks_new_aggregate():
    parent = _parent({"local.key"})
    child = _child(
        _col("local.key", Derivation.ROOT),
        _col("local.new_agg", Derivation.AGGREGATE),  # NOT in parent -> derived anew
    )
    assert not basic_fold_into_group_is_safe(parent, child)


def test_gate_blocks_new_window_and_unnest():
    parent = _parent({"local.key"})
    for unsafe in (Derivation.WINDOW, Derivation.UNNEST, Derivation.RECURSIVE):
        child = _child(
            _col("local.key", Derivation.ROOT),
            _col("local.derived", unsafe),
        )
        assert not basic_fold_into_group_is_safe(parent, child)


# --- collapse of a rowset rename into an unbound merge key ------------------
# An unbound merge/scoped-join key (a `merge`-canonical with no datasource
# binding, no lineage) renders only through a materialized pseudonym-rename
# column in a parent CTE. `CollapseSingleParent` must not fold the rename's
# inline (empty-source) computation into the same CTE as a reference to the key
# under a rename address the merge cannot carry (guard-1 in `optimize`); that
# would emit INVALID_REFERENCE_BUG.


FUNNEL_MERGE_MODEL = """
key step int;
property step.name <- CASE WHEN step = 1 THEN 'one' WHEN step = 2 THEN 'two' END;
key actor int;
key event_id int;
property event_id.event_name string;

datasource events (id: event_id, ename: event_name)
grain (event_id)
query '''select 1 id, 'a' as ename union all select 2 id, 'a' as ename
         union all select 3 id, 'b' as ename''';

with stages as
SELECT
    CASE WHEN event_name = 'a' THEN 1 ELSE 2 END -> stage,
    event_id
;
merge stages.stage into step;
merge stages.event_id into actor;
"""


def _funnel_executor():
    env = Environment()
    env, _ = env.parse(FUNNEL_MERGE_MODEL)
    return env, Dialects.DUCK_DB.default_executor(environment=env)


def test_funnel_count_over_unbound_merge_key_executes():
    # `count(actor)` grouped by the unbound merge key `step` (via its rowset
    # rename): guard-1 keeps the rename boundary so the key resolves. Regression
    # for the funnel-remap CI failure.
    env, executor = _funnel_executor()
    _, statements = env.parse(
        "SELECT --step,\n name, count(actor) -> event_count ORDER BY step asc;"
    )
    sql = executor.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [tuple(r) for r in executor.execute_query(statements[-1]).fetchall()]
    assert rows == [("one", 2), ("two", 1)]


@pytest.mark.xfail(
    reason="Selecting ONLY an unbound merge-key's derived property (no aggregate "
    "or other anchor) fully collapses the rowset-rename boundary; the key's only "
    "materialized alias is pruned and the bare key (no datasource binding, no "
    "lineage) dead-ends at render. Detecting this needs full collapse-cascade / "
    "downstream-survival knowledge no single-collapse guard has; a broad guard "
    "over-blocks legitimate rowset collapses (TPC-DS q29/q64). Documented limit.",
    strict=True,
)
def test_bare_unbound_merge_key_property_only_select():
    env, executor = _funnel_executor()
    _, statements = env.parse("SELECT name ORDER BY name asc;")
    sql = executor.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql


def test_downstream_used_merge_key_alias_stays_collapsed():
    # An unbound merge key whose rowset alias IS consumed downstream (here a
    # semijoin membership) renders and executes normally. Companion to the xfail
    # above: this shape has a surviving alias, so guard-1 leaves the collapse
    # alone -- exercising the merge+rowset+downstream path end-to-end (the shape
    # a broad strand guard over-blocked in TPC-DS q64).
    model = """
key oid int;
property oid.label string;
key item int;

datasource orders (o: oid, i: item, lbl: label)
grain (oid)
query '''select 1 o, 10 i, 'x' as lbl union all select 2 o, 20 i, 'y' as lbl''';

with picked as
SELECT item as ritem, oid
;
merge picked.ritem into item;

SELECT label
WHERE item in picked.ritem
ORDER BY label asc;
"""
    env = Environment()
    env, statements = env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [tuple(r) for r in executor.execute_query(statements[-1]).fetchall()]
    assert rows == [("x",), ("y",)]


def test_basic_ratio_over_group_renders_single_group_cte():
    query = """
key id int;
property id.amount float;
key bucket string;

datasource facts (
    id: id,
    amount: amount,
    bucket: bucket
)
grain (id)
address facts_table;

select
    bucket,
    sum(amount ? bucket = 'a') / sum(amount ? bucket = 'b') as ratio
;
"""
    env = Environment()
    env, statements = env.parse(query)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(statements[-1])[-1]
    # The division folds into the grouped SELECT -- no trailing projection CTE.
    assert sql.count("GROUP BY") == 1, sql
    assert "/ " in sql or "/" in sql
