import sys
from os import environ

import networkx as nx

from trilogy.hooks.base_hook import BaseHook

if not environ.get("TCL_LIBRARY"):
    minor = sys.version_info.minor
    if minor == 13:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python313\tcl\tcl8.6"
    elif minor == 12:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python312\tcl\tcl8.6"
    else:
        pass


class GraphHook(BaseHook):
    def __init__(self):
        super().__init__()
        try:
            pass
        except ImportError:
            raise ImportError("GraphHook requires matplotlib and scipy to be installed")

        # https://github.com/python/cpython/issues/125235#issuecomment-2412948604

    def query_graph_built(
        self,
        graph: nx.DiGraph,
        target: str | None = None,
        highlight_nodes: list[str] | None = None,
        remove_isolates: bool = True,
    ):
        from matplotlib import pyplot as plt

        graph = graph.copy()
        nodes = [*graph.nodes]
        for node in nodes:
            if "__preql_internal" in node:
                graph.remove_node(node)
        if remove_isolates:
            graph.remove_nodes_from(list(nx.isolates(graph)))
        color_map = []
        highlight_nodes = highlight_nodes or []
        for node in graph:
            if node in highlight_nodes:
                color_map.append("orange")
            elif str(node).startswith("ds"):
                color_map.append("blue")
            else:
                color_map.append("green")
        # pos = nx.kamada_kawai_layout(graph, scale=2)
        pos = nx.spring_layout(graph)
        kwargs = {}
        if target:
            edge_colors = []
            descendents = nx.descendants(graph, target)
            for edge in graph.edges():
                if edge[0] == target:
                    edge_colors.append("blue")
                elif edge[1] == target:
                    edge_colors.append("blue")
                elif edge[1] in descendents:
                    edge_colors.append("green")
                else:
                    edge_colors.append("black")
            kwargs["edge_color"] = edge_colors
        nx.draw(
            graph,
            pos=pos,
            node_color=color_map,
            connectionstyle="arc3, rad = 0.1",
            **kwargs
        )  # Draw the original graph
        # Please note, the code below uses the original idea of re-calculating a dictionary of adjusted label positions per node.
        pos_labels = {}
        # For each node in the Graph
        for aNode in graph.nodes():
            # Get the node's position from the layout
            x, y = pos[aNode]
            # pos_labels[aNode] = (x+slopeX*label_ratio, y+slopeY*label_ratio)
            pos_labels[aNode] = (x, y)
        # Finally, redraw the labels at their new position.
        nx.draw_networkx_labels(graph, pos=pos_labels, font_size=10)
        plt.show()
