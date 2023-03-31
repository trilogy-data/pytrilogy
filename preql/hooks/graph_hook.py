from preql.hooks.base_hook import BaseHook
from networkx import DiGraph


class GraphHook(BaseHook):
    def query_graph_built(self, graph: DiGraph):
        from networkx import draw_kamada_kawai
        from matplotlib import pyplot as plt

        draw_kamada_kawai(graph, with_labels=True, connectionstyle="arc3, rad = 0.1")
        # draw_spring(graph, with_labels=True, connectionstyle='arc3, rad = 0.1')
        plt.show()
