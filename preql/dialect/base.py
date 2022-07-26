from typing import List

from preql.core.models import Environment, Select, ProcessedQuery
from preql.core.processor import process_query


class BaseDialect:
    def generate_queries(
        self, environment: Environment, statements
    ) -> List[ProcessedQuery]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                output.append(process_query(environment, statement))
                # graph = generate_graph(environment, statement)
                # output.append(graph_to_query(environment, graph, statement))
        return output

    def compile_statement(self, query: ProcessedQuery) -> str:
        raise NotImplementedError
