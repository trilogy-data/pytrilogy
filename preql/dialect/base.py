from preql.core.models import Concept, Environment, Select, ProcessedQuery
from typing import Dict, List, Any
from preql.core.processor import generate_graph, graph_to_query


class BaseDialect():
    def generate_queries(self, environment:Environment, statements)->List[ProcessedQuery]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                graph = generate_graph(environment, statement.input_components, statement.output_components)
                output.append(graph_to_query(environment, graph, statement))
        return output


    def compile_statement(self, query:ProcessedQuery)->List[str]:
        raise NotImplementedError