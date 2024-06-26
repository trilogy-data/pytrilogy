import networkx as nx

from trilogy.core.models import Concept, Datasource


def concept_to_node(input: Concept) -> str:
    # if input.purpose == Purpose.METRIC:
    #     return f"c~{input.namespace}.{input.name}@{input.grain}"
    return f"c~{input.namespace}.{input.name}@{input.grain}"


def datasource_to_node(input: Datasource) -> str:
    # if isinstance(input, JoinedDataSource):
    #     return "ds~join~" + ",".join(
    #         [datasource_to_node(sub) for sub in input.datasources]
    #     )
    return f"ds~{input.namespace}.{input.identifier}"


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_node(self, node_for_adding, **attr):
        if isinstance(node_for_adding, Concept):
            node_name = concept_to_node(node_for_adding)
            attr["type"] = "concept"
            attr["concept"] = node_for_adding
            attr["grain"] = node_for_adding.grain
        elif isinstance(node_for_adding, Datasource):
            node_name = datasource_to_node(node_for_adding)
            attr["type"] = "datasource"
            attr["ds"] = node_for_adding
            attr["grain"] = node_for_adding.grain
        else:
            node_name = node_for_adding
        super().add_node(node_name, **attr)

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        if isinstance(u_of_edge, Concept):
            orig = u_of_edge
            u_of_edge = concept_to_node(u_of_edge)
            if u_of_edge not in self.nodes:
                self.add_node(orig)
        elif isinstance(u_of_edge, Datasource):
            u_of_edge = datasource_to_node(u_of_edge)

        if isinstance(v_of_edge, Concept):
            orig = v_of_edge
            v_of_edge = concept_to_node(v_of_edge)
            if v_of_edge not in self.nodes:
                self.add_node(orig)
        elif isinstance(v_of_edge, Datasource):
            v_of_edge = datasource_to_node(v_of_edge)
        super().add_edge(u_of_edge, v_of_edge, **attr)
