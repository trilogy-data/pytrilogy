from typing import Union

import networkx as nx

from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildWhereClause


def get_graph_exact_match(
    g: Union[nx.DiGraph, "ReferenceGraph"],
    accept_partial: bool,
    conditions: BuildWhereClause | None,
) -> set[str]:
    exact: set[str] = set()
    for node, ds in g.datasources.items():
        if isinstance(ds, list):
            exact.add(node)
            continue

        if not conditions and not ds.non_partial_for:
            exact.add(node)
            continue
        elif not conditions and accept_partial and ds.non_partial_for:
            exact.add(node)
            continue
        elif conditions:
            if not ds.non_partial_for:
                continue
            if ds.non_partial_for and conditions == ds.non_partial_for:
                exact.add(node)
                continue
        else:
            continue

    return exact


def prune_sources_for_conditions(
    g: "ReferenceGraph",
    accept_partial: bool,
    conditions: BuildWhereClause | None,
):
    complete = get_graph_exact_match(g, accept_partial, conditions)
    to_remove = []
    for node in g.datasources:
        if node not in complete:
            to_remove.append(node)

    for node in to_remove:
        g.remove_node(node)


def concept_to_node(input: BuildConcept) -> str:
    # if input.purpose == Purpose.METRIC:
    #     return f"c~{input.namespace}.{input.name}@{input.grain}"
    return f"c~{input.address}@{input.grain.str_no_condition}"


def datasource_to_node(input: BuildDatasource) -> str:
    # if isinstance(input, JoinedDataSource):
    #     return "ds~join~" + ",".join(
    #         [datasource_to_node(sub) for sub in input.datasources]
    #     )
    return f"ds~{input.identifier}"


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.concepts: dict[str, BuildConcept] = {}
        self.datasources: dict[str, BuildDatasource] = {}
        self.pseudonyms: set[tuple[str, str]] = set()

    def copy(self) -> "ReferenceGraph":
        g = ReferenceGraph()
        g.concepts = self.concepts.copy()
        g.datasources = self.datasources.copy()
        g.pseudonyms = {*self.pseudonyms}
        # g.add_nodes_from(self.nodes(data=True))
        for node in self.nodes:
            g.add_node(node, fast=True)
        for edge in self.edges:
            g.add_edge(edge[0], edge[1], fast=True)
        # g.add_edges_from(self.edges(data=True))
        return g

    def remove_node(self, n) -> None:
        if n in self.concepts:
            del self.concepts[n]
        if n in self.datasources:
            del self.datasources[n]
        super().remove_node(n)

    def add_node(self, node_for_adding, fast: bool = False, **attr):
        if fast:
            return super().add_node(node_for_adding, **attr)
        node_name = node_for_adding
        if attr.get("datasource"):
            self.datasources[node_name] = attr["datasource"]
        super().add_node(node_name, **attr)

    def add_datasource_node(self, node_name, datasource) -> None:
        self.datasources[node_name] = datasource
        super().add_node(node_name, datasource=datasource)

    def add_edge(self, u_of_edge, v_of_edge, fast: bool = False, **attr):
        return super().add_edge(u_of_edge, v_of_edge, **attr)
