"""A plan's bindings are decided when its reference graph is generated and
owned by that graph (`ReferenceGraph.scope`); the environment keeps the
bindings as authored."""

import pytest

from tests.engine.test_enum_unions import PREQL as UNION_FAMILY
from tests.engine.test_partition_source_exclusion import THREE_WAY
from tests.helpers.models import CUSTOMERS_DERIVED
from trilogy import Dialects
from trilogy.core import query_processor
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.processing import statement_scope
from trilogy.core.processing.v4_helper import keyspace
from trilogy.core.processing.v4_node_generators import nested_select


class _Graphs:
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.seen: list[tuple] = []

    def __call__(self, build_environment, *args):
        before = {
            name: (ds, tuple(c.modifiers for c in ds.columns))
            for name, ds in build_environment.datasources.items()
        }
        graph = self.wrapped(build_environment, *args)
        self.seen.append((build_environment, before, graph))
        return graph


class _Heals:
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.seen: list[tuple] = []

    def __call__(self, environment, scope, *args):
        replacements = self.wrapped(environment, scope, *args)
        self.seen.append((scope, replacements))
        return replacements


def _plan(monkeypatch, module, model: str, query: str) -> _Graphs:
    graphs = _Graphs(module.generate_scope_graph)
    monkeypatch.setattr(module, "generate_scope_graph", graphs)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    executor.generate_sql(query)
    return graphs


@pytest.mark.parametrize(
    "model, query",
    [
        (CUSTOMERS_DERIVED, "select customer_id, status where status = 'delivered';"),
        (THREE_WAY, "where channel in ('WEB', 'CATALOG') select sum(amount) as total;"),
    ],
)
def test_planning_leaves_the_authored_bindings(monkeypatch, model, query):
    graphs = _plan(monkeypatch, query_processor, model, query)
    ((environment, before, graph),) = graphs.seen
    after = {
        name: (ds, tuple(c.modifiers for c in ds.columns))
        for name, ds in environment.datasources.items()
    }
    assert after == before
    assert [id(ds) for ds in graph.scope.datasources] != [
        id(ds) for ds, _ in before.values()
    ]


def test_exclusion_records_the_ruled_out_domain_on_the_scope(monkeypatch):
    graphs = _plan(
        monkeypatch,
        query_processor,
        THREE_WAY,
        "where channel in ('WEB', 'CATALOG') select sum(amount) as total;",
    )
    ((_, _, graph),) = graphs.seen
    assert {ds.identifier for ds in graph.scope.datasources} == {"web", "catalog"}
    assert graph.scope.excluded_enum_values["local.channel"] == frozenset({"STORE"})


def test_heal_changing_nothing_keeps_the_authored_scope_and_its_facts(monkeypatch):
    heals = _Heals(statement_scope.decide_heal)
    monkeypatch.setattr(statement_scope, "decide_heal", heals)
    graphs = _plan(
        monkeypatch,
        query_processor,
        CUSTOMERS_DERIVED,
        "select name where name = 'ann';",
    )
    ((scope, replacements),) = heals.seen
    ((_, _, graph),) = graphs.seen
    assert replacements == {}
    assert graph.scope is scope
    assert scope in keyspace._FACTS_CACHE


def test_nested_select_graph_holds_the_partition_union(monkeypatch):
    graphs = _plan(
        monkeypatch,
        nested_select,
        UNION_FAMILY,
        "with r as select category, sales; select r.category, r.sales;",
    )
    assert graphs.seen
    for _, _, graph in graphs.seen:
        assert any(
            isinstance(ds, BuildUnionDatasource) for ds in graph.datasources.values()
        )
