"""Equivalence lock for baseline+delta nested materialization.

`build_nested_select` materializes each nested arm's environment as a cached
context-free baseline plus an overlay delta (`Environment.materialize_delta`).
`materialize_for_select` remains the reference spelling: for every nested
materialization these queries trigger, the delta result must equal the full
build — same concepts, same datasources, same aliases, same insertion order.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.models.environment import Environment as AuthorEnvironment

_MODEL = """
key line_id int;
property line_id.item_id int;
property line_id.cat string?;
property line_id.yr int;
property line_id.other_id int;
auto cat_b <- cat;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    cat: cat,
    yr: yr,
    other_id: other_id,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 'a' as cat, 2001 as yr, 1 as other_id union all
select 2 as line_id, 1 as item_id, 'a' as cat, 2001 as yr, 2 as other_id union all
select 3 as line_id, 2 as item_id, cast(null as varchar) as cat, 2002 as yr, 1 as other_id union all
select 4 as line_id, 3 as item_id, 'b' as cat, 2003 as yr, 2 as other_id
''';

property other_id.oname string;
datasource others (
    other_id: other_id,
    oname: oname,
)
grain (other_id)
query '''select 1 as other_id, 'x' as oname union all select 2 as other_id, 'y' as oname''';
"""

_ARMS_MODEL = """
key sid int;
property sid.s_cust int;
datasource store_fact (r: sid, c: s_cust) grain (sid)
query '''select 1 r, 1 c union all select 2 r, 2 c''';

key cid int;
property cid.c_cust int;
datasource catalog_fact (r: cid, c: c_cust) grain (cid)
query '''select 1 r, 1 c union all select 2 r, 3 c''';
"""

CASES = (
    # q12 shape: a share-of-total auto whose lineage hash depends on the
    # overlay's grain-stamped builds — caught the weak-__eq__ change-detection
    # defect (BuildConcept equality ignores lineage/canonical_name).
    (
        _MODEL,
        "auto class_share <- count(line_id) / (count(line_id) by cat) * 100;\n"
        "select yr, cat, class_share order by class_share desc limit 5;",
    ),
    (_MODEL, "with rs as select yr, count(line_id) -> c having c > 2;\nselect rs.yr;"),
    (_MODEL, "with rs as select yr order by yr desc limit 2;\nselect rs.yr;"),
    (_MODEL, "with rs as select yr, oname;\nselect rs.yr, rs.oname;"),
    (
        _MODEL,
        "with combined as union(\n"
        "    (select yr, count(line_id) -> c having c > 2),\n"
        "    (where cat = 'a' select yr, count(line_id) -> c)\n"
        ") -> (y, c);\n"
        "select combined.y, sum(combined.c) -> total;",
    ),
    (_ARMS_MODEL, "where s_cust is null select c_cust union join s_cust = c_cust;"),
    (
        _ARMS_MODEL,
        "select c_cust, sum(sid) -> s_rows, sum(cid) -> c_rows"
        " union join s_cust = c_cust;",
    ),
)


def _signature(concept):
    # BuildConcept.__eq__ is deliberately weak (ignores lineage and
    # canonical_name), so the lock compares a structural signature instead.
    return (
        concept.canonical_address,
        str(concept.grain),
        str(concept.lineage),
        str(concept.keys),
        tuple(sorted(concept.pseudonyms)),
        concept.derivation,
        concept.granularity,
    )


def _signatures(concepts):
    return {k: _signature(v) for k, v in dict(concepts).items()}


def _assert_env_equal(delta, full, context):
    assert _signatures(delta.concepts) == _signatures(full.concepts), context
    assert list(delta.concepts) == list(full.concepts), context
    assert _signatures(delta.canonical_concepts) == _signatures(
        full.canonical_concepts
    ), context
    assert dict(delta.datasources) == dict(full.datasources), context
    assert _signatures(delta.alias_origin_lookup) == _signatures(
        full.alias_origin_lookup
    ), context
    assert delta.materialized_concepts == full.materialized_concepts, context
    assert delta.scoped_partial_derived == full.scoped_partial_derived, context
    assert delta.scoped_join_key_groups == full.scoped_join_key_groups, context


@pytest.fixture
def v4():
    yield


def test_session_caches_invalidate_on_env_mutation(v4):
    """The cross-statement BuildCaches bundle is stamped by the env mutation
    counters: a statement that adds concepts/datasources must not be answered
    from the prior state's caches, and an unchanged repeat must still plan
    identically."""
    env = Environment()
    env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql_first = executor.generate_sql("select yr, count(line_id) -> c;")[-1]
    executor.parse_text(
        "key extra_id int;\n"
        "datasource extras (e: extra_id) grain (extra_id)"
        " query '''select 5 as e''';"
    )
    sql_new = executor.generate_sql("select extra_id;")[-1]
    assert "extras" in sql_new
    assert executor.generate_sql("select yr, count(line_id) -> c;")[-1] == sql_first


def test_nested_arm_delta_matches_full_materialization(monkeypatch, v4):
    real = AuthorEnvironment.materialize_delta
    checks = {"count": 0}

    def checking(self, baseline, local_concepts, **kwargs):
        snapshot = dict(local_concepts)
        delta_env = real(self, baseline, local_concepts, **kwargs)
        full_env = self.materialize_for_select(snapshot, **kwargs)
        _assert_env_equal(delta_env, full_env, checks["count"])
        checks["count"] += 1
        return delta_env

    monkeypatch.setattr(AuthorEnvironment, "materialize_delta", checking)
    for model, query in CASES:
        env = Environment()
        env.parse(model)
        Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)
    assert checks["count"] >= len(CASES)
