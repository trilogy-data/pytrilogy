"""Discovery dead-ends where the requested concepts span unconnected models
should raise a `DisconnectedConceptsException` naming the disconnected subgraphs
and asking whether a join/merge is missing — not the opaque internal
"No remaining priority concepts" candidate-set dump.

Covers the message formatter plus end-to-end queries that two unrelated models
(no merge) cannot resolve. The partition logic itself (`disconnected_components`,
graph-reachability based) is exercised end-to-end here and in
`test_disconnected_components_e2e.py`.
"""

import pytest

from trilogy import Dialects
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import (
    format_disconnected_subgraphs_error,
)


def _concept(
    name: str,
    grain: set[str] | None = None,
    pseudonyms: set[str] | None = None,
    derivation: Derivation = Derivation.ROOT,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        derivation=derivation,
        granularity=Granularity.MULTI_ROW,
        grain=BuildGrain(components=set(grain or set())),
        pseudonyms=set(pseudonyms or set()),
    )


def test_format_message_lists_subgraphs_and_hint():
    a_id = _concept("a_id", grain={"local.a_id"})
    sb = _concept("sb", grain={"local.b_id"})

    message = format_disconnected_subgraphs_error([[a_id], [sb]])

    assert "2 disconnected" in message
    # default namespace is stripped from the rendered message
    assert "{a_id}" in message
    assert "{sb}" in message
    assert "local." not in message
    assert "join or merge" in message


_DISCONNECTED_MODEL = """
key a_id int;
property a_id.av int;
datasource a (id: a_id, v: av) grain (a_id)
query '''select 1 id, 10 v union all select 2 id, 20 v''';

key b_id int;
property b_id.bv int;
datasource b (id: b_id, v: bv) grain (b_id)
query '''select 1 id, 100 v union all select 2 id, 200 v''';

auto sa <- sum(av) by a_id;
auto sb <- sum(bv) by b_id;
select a_id, sa, sb;
"""


def test_disconnected_query_raises_typed_exception():
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_DISCONNECTED_MODEL)
    message = str(exc.value)
    assert "Are you missing a join or merge" in message
    # the two unrelated models each form their own subgraph
    groups = {frozenset(g) for g in exc.value.subgraphs}
    assert frozenset({"local.a_id", "local.sa"}) in groups
    assert frozenset({"local.sb"}) in groups


_DISCONNECTED_CROSS_CTE_AGG = """
key a_id int;
property a_id.av int;
datasource a (id: a_id, v: av) grain (a_id)
query '''select 1 id, 10 v union all select 2 id, 20 v''';

key b_id int;
property b_id.bv int;
datasource b (id: b_id, v: bv) grain (b_id)
query '''select 1 id, 100 v union all select 2 id, 200 v''';

with a_agg as select a_id, sum(av) as sa;
with b_agg as select b_id, sum(bv) as sb;
with combined as
select a_agg.a_id as id, sum(a_agg.sa) as ta, sum(b_agg.sb) as tb;
select combined.id, combined.ta, combined.tb;
"""


def test_cross_cte_aggregate_grain_only_bridge_raises():
    # `combined` groups by a_agg.a_id but aggregates b_agg.sb — whose only tie to
    # a_agg.a_id is that the aggregate is grouped `by` it. That grain edge must not
    # bridge the (unrelated) a and b models, so building the `combined` rowset
    # raises the helpful subgraph error rather than dead-ending in the raw "No
    # remaining priority concepts" dump. The rowset inner select runs the exact same
    # connectivity check as a top-level select (no rowset-specific path), so the
    # b-side aggregate splits off from the a-side group key + aggregate. The two
    # engines name the split differently — v3 by source (`b_agg.sb`), v4 by the
    # rowset's own aliased columns (`combined.tb`) — so assert the structural
    # invariant, not engine-specific addresses.
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_DISCONNECTED_CROSS_CTE_AGG)
    message = str(exc.value)
    assert "Are you missing a join or merge" in message
    assert "_virt" not in message
    assert len(exc.value.subgraphs) == 2
    assert all(group for group in exc.value.subgraphs)


_DISCONNECTED_ROOT_PROPERTIES = """
key cust int;
property cust.cname string;
key prod int;
property prod.pname string;
datasource customers (cust: cust, cname: cname) grain (cust)
query '''select 1 as cust, 'a' as cname''';
datasource products (prod: prod, pname: pname) grain (prod)
query '''select 1 as prod, 'b' as pname''';
select cname, pname;
"""


_FILTER_OVER_SCOPED_JOIN_ROWSET_MEASURE = """
key sale_id int;
property sale_id.s_qty int;
property sale_id.s_dow int;
property sale_id.s_week int;
datasource sales (id: sale_id, q: s_qty, d: s_dow, w: s_week) grain (sale_id)
query '''select 1 id, 10 q, 1 d, 1 w union all select 2 id, 5 q, 2 d, 1 w''';

rowset cur <- select s_week as w, s_dow as dow, sum(s_qty) as total;
rowset nxt <- select s_week as w2, s_dow as dow2, sum(s_qty) as total2;
def ratio(x) -> sum(cur.total ? cur.dow = x) by cur.w
  / sum(nxt.total2 ? nxt.dow2 = x) by cur.w;
select cur.w, @ratio(0) as r
  subset join nxt.w2 = cur.w + 1
  subset join nxt.dow2 = cur.dow;
"""


def test_filter_over_scoped_join_rowset_measure_not_islanded():
    # Two sibling rowsets related by a scoped join; a filtered aggregate over each
    # rowset's MEASURE mints a `_virt_filter` over a rowset output. Islanding
    # severed each filter from its rowset (a boundary-crossing edge) and wrongly
    # orphaned it into its own singleton subgraph (q02 Bug #2 -- the confusing
    # `{_virt_filter_...}` subgraph in the discovery error). Reconnecting
    # downstream consumers of declared outputs to the rowset hub keeps each filter
    # in its rowset's component; the only legitimate split here is cur vs nxt
    # (the cross-rowset relationship is the scoped join, not graph reachability).
    from trilogy.core.models.build import BuildFilterItem
    from trilogy.core.processing.discovery_utility import disconnected_components

    env = Environment()
    env.parse(_FILTER_OVER_SCOPED_JOIN_ROWSET_MEASURE)
    be = env.materialize_for_select()

    filters = [
        c
        for c in be.concepts.values()
        if isinstance(getattr(c, "lineage", None), BuildFilterItem)
        and "_virt_filter" in c.address
    ]
    assert filters, "expected _virt_filter concepts over the rowset measures"
    # Each filter must be co-located with a rowset measure, never its own island.
    measures = [be.concepts["cur.total"], be.concepts["nxt.total2"]]
    groups = disconnected_components(be, filters + measures, island_rowsets=True)
    singletons = [g for g in groups if len(g) == 1]
    assert not singletons, [sorted(c.address for c in g) for g in groups]


def test_disconnected_root_properties_raise_typed_exception():
    # Distinct surface from the aggregate case above: a plain ROOT-property select
    # dead-ends in `source_query_concepts` (no priority exhaustion in
    # get_priority_concept), which previously emitted a bare "Could not resolve
    # connections" message. It must now name the disconnected subgraphs too.
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_DISCONNECTED_ROOT_PROPERTIES)
    assert "Are you missing a join or merge" in str(exc.value)
    groups = {frozenset(g) for g in exc.value.subgraphs}
    assert groups == {frozenset({"local.cname"}), frozenset({"local.pname"})}
