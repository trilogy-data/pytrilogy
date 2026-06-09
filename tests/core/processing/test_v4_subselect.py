"""v4 correlated-subselect generator (`gen_subselect`).

The cross-datasource branch is the interesting one: when a subselect concept
carries `outer_arguments`, the INNER select reads a *separate* datasource and
must be planned recursively and attached as a second parent. These drive the
real `search_concepts` planner so the dispatch wiring (history/g/source_policy
threading) and the generator body both run on a genuine plan.
"""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core.enums import Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts
from trilogy.core.processing.nodes import SubselectNode

CROSS_DATASOURCE_MODEL = """
key customer_id int;
property customer_id.customer_name string;
property customer_id.customer_lat float;
property customer_id.customer_lon float;

key warehouse_id int;
property warehouse_id.warehouse_name string;
property warehouse_id.warehouse_lat float;
property warehouse_id.warehouse_lon float;

datasource customers(
    customer_id: customer_id,
    customer_name: customer_name,
    customer_lat: customer_lat,
    customer_lon: customer_lon
)
grain (customer_id)
query '''
select 1 as customer_id, 'alice' as customer_name, 40.0 as customer_lat, -74.0 as customer_lon
union all select 2, 'bob', 34.0, -118.0
''';

datasource warehouses(
    warehouse_id: warehouse_id,
    warehouse_name: warehouse_name,
    warehouse_lat: warehouse_lat,
    warehouse_lon: warehouse_lon
)
grain (warehouse_id)
query '''
select 10 as warehouse_id, 'east' as warehouse_name, 40.1 as warehouse_lat, -74.1 as warehouse_lon
union all select 20, 'west', 34.2, -118.2
''';

def table close_warehouses(lat, long) -> select warehouse_name order by sqrt(
        (lat - warehouse_lat) * (lat - warehouse_lat)
        + (long - warehouse_lon) * (long - warehouse_lon)
    ) asc limit 1;

select
    customer_name,
    @close_warehouses(customer_lat, customer_lon) as nearest_warehouses
;
"""

# Non-correlated subselect: a constant-array output with no `outer_arguments`,
# so `gen_subselect` plans no inner parent (the `not outer_arguments` skip).
NON_CORRELATED_MODEL = """
key id int;
property id.val int;
datasource nums(
    id: id,
    val: val
)
grain (id)
query '''
select 1 id, 10 val
union all select 2, 20
union all select 3, 30
''';
auto top2 <- subselect(val order by val desc limit 2);
select id, top2;
"""


def _benv(model: str) -> tuple[Environment, BuildEnvironment]:
    env = Environment()
    env.parse(model)
    return env, env.materialize_for_select()


def _subselect_address(benv: BuildEnvironment) -> str:
    for address, concept in benv.concepts.items():
        if concept.derivation == Derivation.SUBSELECT:
            return address
    raise AssertionError("no SUBSELECT concept in environment")


def _search(env, benv, addresses):
    return search_concepts(
        mandatory_list=[benv.concepts[a] for a in addresses],
        history=V4History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=[],
    )


def _walk(node):
    yield node
    for parent in node.parents:
        yield from _walk(parent)


def test_cross_datasource_subselect_plans_inner_parent():
    env, benv = _benv(CROSS_DATASOURCE_MODEL)
    addr = _subselect_address(benv)
    info = _search(env, benv, ["local.customer_name", addr])
    assert info.strategy_node is not None
    subnodes = [n for n in _walk(info.strategy_node) if isinstance(n, SubselectNode)]
    assert subnodes, "expected a SubselectNode in the plan"
    sub = subnodes[0]
    # Two parents: the outer customer row source plus the recursively-planned
    # inner warehouse select. The inner columns are inputs, never outputs.
    assert len(sub.parents) == 2
    inner_sources = {d.name for p in sub.parents[1:] for d in p.resolve().datasources}
    assert any("warehouse" in n for n in inner_sources)
    output_addrs = {o.address for o in sub.output_concepts}
    assert "warehouse_lat" not in {a.split(".")[-1] for a in output_addrs}


def test_cross_datasource_subselect_end_to_end():
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        executor = Dialects.DUCK_DB.default_executor()
        rows = executor.execute_query(CROSS_DATASOURCE_MODEL).fetchall()
    finally:
        CONFIG.use_v4_discovery = prior
    by_customer = {r.customer_name: r.nearest_warehouses for r in rows}
    assert by_customer == {"alice": ["east"], "bob": ["west"]}


def test_non_correlated_subselect_has_single_parent():
    """No `outer_arguments`: the inner-search branch is skipped and the node
    keeps only its outer parent."""
    env, benv = _benv(NON_CORRELATED_MODEL)
    addr = _subselect_address(benv)
    info = _search(env, benv, ["local.id", addr])
    assert info.strategy_node is not None
    subnodes = [n for n in _walk(info.strategy_node) if isinstance(n, SubselectNode)]
    assert subnodes
    assert all(len(n.parents) == 1 for n in subnodes)
