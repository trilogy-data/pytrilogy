import networkx as nx

from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildWhereClause


def get_graph_exact_match(
    g: nx.DiGraph, accept_partial: bool, conditions: BuildWhereClause | None
) -> set[str]:
    datasources: dict[str, BuildDatasource | list[BuildDatasource]] = (
        nx.get_node_attributes(g, "datasource")
    )
    exact: set[str] = set()
    for node in g.nodes:
        if node in datasources:
            ds = datasources[node]
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
    g: nx.DiGraph,
    accept_partial: bool,
    conditions: BuildWhereClause | None,
):

    complete = get_graph_exact_match(g, accept_partial, conditions)
    to_remove = []
    for node in g.nodes:
        if node.startswith("ds~") and node not in complete:
            to_remove.append(node)

    for node in to_remove:
        g.remove_node(node)


def concept_to_node(input: BuildConcept) -> str:
    # if input.purpose == Purpose.METRIC:
    #     return f"c~{input.namespace}.{input.name}@{input.grain}"
    return f"c~{input.address}@{input.grain.without_condition()}"


def datasource_to_node(input: BuildDatasource) -> str:
    # if isinstance(input, JoinedDataSource):
    #     return "ds~join~" + ",".join(
    #         [datasource_to_node(sub) for sub in input.datasources]
    #     )
    return f"ds~{input.identifier}"


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_node(self, node_for_adding, **attr):
        if isinstance(node_for_adding, BuildConcept):
            node_name = concept_to_node(node_for_adding)
            attr["type"] = "concept"
            attr["concept"] = node_for_adding
            attr["grain"] = node_for_adding.grain
        elif isinstance(node_for_adding, BuildDatasource):
            node_name = datasource_to_node(node_for_adding)
            attr["type"] = "datasource"
            attr["ds"] = node_for_adding
            attr["grain"] = node_for_adding.grain
        else:
            node_name = node_for_adding
        super().add_node(node_name, **attr)

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        if isinstance(u_of_edge, BuildConcept):
            orig = u_of_edge
            u_of_edge = concept_to_node(u_of_edge)
            if u_of_edge not in self.nodes:
                self.add_node(orig)
        elif isinstance(u_of_edge, BuildDatasource):
            u_of_edge = datasource_to_node(u_of_edge)

        if isinstance(v_of_edge, BuildConcept):
            orig = v_of_edge
            v_of_edge = concept_to_node(v_of_edge)
            if v_of_edge not in self.nodes:
                self.add_node(orig)
        elif isinstance(v_of_edge, BuildDatasource):
            v_of_edge = datasource_to_node(v_of_edge)
        super().add_edge(u_of_edge, v_of_edge, **attr)
