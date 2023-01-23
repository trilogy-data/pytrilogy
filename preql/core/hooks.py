from networkx import DiGraph


class BaseProcessingHook(object):
    def query_graph_built(self, graph: DiGraph):
        pass


class GraphHook(BaseProcessingHook):
    def query_graph_built(self, graph: DiGraph):
        from networkx import draw_kamada_kawai
        from matplotlib import pyplot as plt

        draw_kamada_kawai(graph, with_labels=True, connectionstyle="arc3, rad = 0.1")
        # draw_spring(graph, with_labels=True, connectionstyle='arc3, rad = 0.1')
        plt.show()


def print_select_graph(input: str, G, indentation: int = 1):
    if input.startswith("select-"):
        output = "SELECT"
    else:
        output = input
    print("\t" * indentation + output)
    for node in G.predecessors(input):
        print_select_graph(node, G, indentation + 1)


class QueryPlanHook(BaseProcessingHook):
    def query_graph_built(self, graph: DiGraph):
        return
        # print_select_graph(root, graph)
