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

        # Draw the graph without labels first
        nx.draw(
            graph,
            pos=pos,
            node_color=color_map,
            connectionstyle="arc3, rad = 0.1",
            with_labels=False,  # Important: don't draw labels with nx.draw
            **kwargs
        )

        # Draw labels with manual spacing
        self._draw_labels_with_manual_spacing(graph, pos)

        plt.show()

    def _draw_labels_with_manual_spacing(self, graph, pos):
        import numpy as np

        pos_labels = {}
        node_positions = list(pos.values())

        # Calculate average distance between nodes to determine spacing
        if len(node_positions) > 1:
            distances = []
            for i, (x1, y1) in enumerate(node_positions):
                for j, (x2, y2) in enumerate(node_positions[i + 1 :], i + 1):
                    dist = np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
                    distances.append(dist)

            avg_distance = np.mean(distances)
            min_spacing = max(
                0.1, avg_distance * 0.3
            )  # Minimum spacing as fraction of average distance
        else:
            min_spacing = 0.1

        # Simple spacing algorithm - offset labels that are too close
        for i, node in enumerate(graph.nodes()):
            x, y = pos[node]

            # Check for nearby labels and adjust position
            adjusted_x, adjusted_y = x, y
            for j, other_node in enumerate(
                list(graph.nodes())[:i]
            ):  # Only check previous nodes
                other_x, other_y = pos_labels.get(other_node, pos[other_node])
                distance = np.sqrt(
                    (adjusted_x - other_x) ** 2 + (adjusted_y - other_y) ** 2
                )

                if distance < min_spacing:
                    # Calculate offset direction
                    if distance > 0:
                        offset_x = (adjusted_x - other_x) / distance * min_spacing
                        offset_y = (adjusted_y - other_y) / distance * min_spacing
                    else:
                        # If nodes are at exact same position, use random offset
                        angle = np.random.random() * 2 * np.pi
                        offset_x = np.cos(angle) * min_spacing
                        offset_y = np.sin(angle) * min_spacing

                    adjusted_x = other_x + offset_x
                    adjusted_y = other_y + offset_y

            pos_labels[node] = (adjusted_x, adjusted_y)

        # Draw the labels at adjusted positions
        nx.draw_networkx_labels(graph, pos=pos_labels, font_size=10)
