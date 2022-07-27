from networkx import DiGraph

class BaseProcessingHook(object):

    def query_graph_built(self, graph:DiGraph):
        pass


class GraphHook(BaseProcessingHook):

    def query_graph_built(self, graph:DiGraph):
        from networkx import draw_spring
        from matplotlib import pyplot as plt
        draw_spring(graph, with_labels=True)
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

    def query_graph_built(self, graph:DiGraph):
        return
        #print_select_graph(root, graph)