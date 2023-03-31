from typing import Union
from preql.core.models import QueryDatasource, CTE, Datasource

from preql.hooks.base_hook import BaseHook
from preql.constants import logger
from logging import StreamHandler, DEBUG
from preql.core.processing.concept_strategies_v2 import StrategyNode


from preql.dialect.bigquery import BigqueryDialect

renderer = BigqueryDialect()


def print_recursive_resolved(input: Union[QueryDatasource, Datasource], depth=0):
    print(
        "  " * depth,
        input.identifier,
        "->",
        input.group_required,
        "->",
        [c.address for c in input.output_concepts],
    )
    if isinstance(input, QueryDatasource):
        for child in input.datasources:
            print_recursive_resolved(child, depth + 1)


def print_recursive_nodes(input: StrategyNode, depth=0):
    print(
        "  " * depth,
        input,
        "->",
        input.resolve().grain,
        "->",
        input.resolve().identifier,
    )
    for child in input.parents:
        print_recursive_nodes(child, depth + 1)


def print_recursive_ctes(input: CTE, depth=0):
    select_statement = [c.address for c in input.output_columns]
    print("  " * depth, input.name, "->", input.group_to_grain, "->", select_statement)
    sql = renderer.render_cte(input).statement
    for line in sql.split("\n"):
        print("  " * (depth), line)
    print("-----")
    if isinstance(input, CTE):
        for child in input.parent_ctes:
            print_recursive_ctes(child, depth + 1)


class DebuggingHook(BaseHook):
    def __init__(self):
        if not any([isinstance(x, StreamHandler) for x in logger.handlers]):
            logger.addHandler(StreamHandler())
        logger.setLevel(DEBUG)

    def process_root_datasource(self, datasource: QueryDatasource):
        print_recursive_resolved(datasource)

    def process_root_cte(self, cte: CTE):
        print_recursive_ctes(cte)

    def process_root_strategy_node(self, node: StrategyNode):
        print_recursive_nodes(node)
