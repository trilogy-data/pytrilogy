from typing import List, Optional

from preql.core.models import Environment, Select, ProcessedQuery
from preql.core.processor import process_query
from preql.core.hooks import BaseProcessingHook

class BaseDialect:
    def generate_queries(
        self, environment: Environment, statements, hooks:Optional[List[BaseProcessingHook]]=None
    ) -> List[ProcessedQuery]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                output.append(process_query(environment, statement, hooks))
                # graph = generate_graph(environment, statement)
                # output.append(graph_to_query(environment, graph, statement))
        return output

    def compile_statement(self, query: ProcessedQuery) -> str:
        raise NotImplementedError
