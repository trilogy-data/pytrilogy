from typing import Union
from preql.core.models import QueryDatasource, CTE, Datasource, Select

from preql.hooks.base_hook import BaseHook
from preql.constants import logger
from logging import StreamHandler, DEBUG
from preql.core.processing.concept_strategies_v2 import StrategyNode


from preql.dialect.bigquery import BigqueryDialect

renderer = BigqueryDialect()


def print_recursive_resolved(input: Union[QueryDatasource, Datasource], depth=0):
    display = [
        (
            "  " * depth,
            input.full_name,
            "->",
            input.group_required,
            "->",
            [c.address for c in input.output_concepts],
        )
    ]
    if isinstance(input, QueryDatasource):
        for child in input.datasources:
            display += print_recursive_resolved(child, depth + 1)
    return display


def print_recursive_nodes(input: StrategyNode, depth=0):
    resolved = input.resolve()
    display = [
        (
            "  " * depth,
            input,
            "->",
            resolved.grain,
            "->",
            [c.address for c in resolved.output_concepts],
        )
    ]
    for child in input.parents:
        display += print_recursive_nodes(child, depth + 1)
    return display


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

    def process_select_info(self, select: Select):
        print(f"grain: {str(select.grain)}")

    def process_root_datasource(self, datasource: QueryDatasource):
        printed = print_recursive_resolved(datasource)
        for row in printed:
            print("".join([str(v) for v in row]))

    def process_root_cte(self, cte: CTE):
        print_recursive_ctes(cte)

    def process_root_strategy_node(self, node: StrategyNode):
        printed = print_recursive_nodes(node)
        for row in printed:
            print("".join([str(v) for v in row]))
