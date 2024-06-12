from preql.hooks.base_hook import BaseHook
from networkx import DiGraph


class GraphHook(BaseHook):
    def __init__(self):
        super().__init__()
        try:
            pass
        except ImportError:
            raise ImportError("GraphHook requires matplotlib and scipy to be installed")

    def query_graph_built(self, graph: DiGraph):
        from networkx import draw_kamada_kawai
        from matplotlib import pyplot as plt

        graph = graph.copy()
        nodes = [*graph.nodes]
        for node in nodes:
            if "__preql_internal" in node:
                graph.remove_node(node)
        draw_kamada_kawai(graph, with_labels=True, connectionstyle="arc3, rad = 0.1")
        # draw_spring(graph, with_labels=True, connectionstyle='arc3, rad = 0.1')
        plt.show()
