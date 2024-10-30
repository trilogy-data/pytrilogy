from typing import Union
from trilogy.core.models import QueryDatasource, CTE, Datasource, SelectStatement

from trilogy.hooks.base_hook import BaseHook
from trilogy.constants import logger
from logging import StreamHandler, DEBUG
from trilogy.core.processing.nodes import StrategyNode

from trilogy.dialect.bigquery import BigqueryDialect

from enum import Enum


class PrintMode(Enum):
    OFF = False
    BASIC = True
    FULL = 3


renderer = BigqueryDialect()


def print_recursive_resolved(
    input: Union[QueryDatasource, Datasource], mode: PrintMode, depth: int = 0
):
    extra = []
    if isinstance(input, QueryDatasource):
        if input.joins:
            extra.append("join")
        if input.condition:
            extra.append("filter")
    if input.group_required:
        extra.append("group")
    output = [c.address for c in input.output_concepts[:3]]
    if len(input.output_concepts) > 3:
        output.append("...")
    display = [
        (
            "  " * depth,
            input.__class__.__name__,
            "<",
            ",".join(extra),
            ">",
            # [c.address for c in input.input_concepts],
            "->",
            output,
        )
    ]
    if isinstance(input, QueryDatasource):
        for child in input.datasources:
            display += print_recursive_resolved(child, mode=mode, depth=depth + 1)
    return display


def print_recursive_nodes(
    input: StrategyNode, mode: PrintMode = PrintMode.BASIC, depth: int = 0
):
    resolved = input.resolve()
    if mode == PrintMode.FULL:
        display = [
            [
                "  " * depth,
                input,
                "->",
                resolved.grain,
                "->",
                [c.address for c in resolved.output_concepts],
            ]
        ]
    elif mode == PrintMode.BASIC:
        display = [
            [
                "  " * depth,
                input,
                "->",
                resolved.grain,
            ]
        ]
    for child in input.parents:
        display += print_recursive_nodes(
            child,
            mode=mode,
            depth=depth + 1,
        )
    return display


def print_recursive_ctes(input: CTE, depth: int = 0, max_depth: int | None = None):
    if max_depth and depth > max_depth:
        return
    select_statement = [c.address for c in input.output_columns]
    print("  " * depth, input.name, "->", input.group_to_grain, "->", select_statement)
    sql = renderer.render_cte(input).statement
    for line in sql.split("\n"):
        logger.debug("  " * (depth) + line)
    if isinstance(input, CTE):
        for child in input.parent_ctes:
            print_recursive_ctes(child, depth + 1)


class DebuggingHook(BaseHook):
    def __init__(
        self,
        level=DEBUG,
        max_depth: int | None = None,
        process_ctes: PrintMode | bool = True,
        process_nodes: PrintMode | bool = True,
        process_datasources: PrintMode | bool = True,
        process_other: bool = True,
    ):
        if not any([isinstance(x, StreamHandler) for x in logger.handlers]):
            logger.addHandler(StreamHandler())
        logger.setLevel(level)

        self.max_depth = max_depth
        self.process_ctes = PrintMode(process_ctes)
        self.process_nodes = PrintMode(process_nodes)
        self.process_datasources = PrintMode(process_datasources)
        self.process_other = PrintMode(process_other)

    def process_select_info(self, select: SelectStatement):
        if self.process_datasources != PrintMode.OFF:
            print(f"grain: {str(select.grain)}")

    def process_root_datasource(self, datasource: QueryDatasource):
        if self.process_datasources != PrintMode.OFF:
            printed = print_recursive_resolved(datasource, self.process_datasources)
            for row in printed:
                print("".join([str(v) for v in row]))

    def process_root_cte(self, cte: CTE):
        if self.process_ctes != PrintMode.OFF:
            print_recursive_ctes(cte, max_depth=self.max_depth)

    def process_root_strategy_node(self, node: StrategyNode):
        if self.process_nodes != PrintMode.OFF:
            printed = print_recursive_nodes(node, mode=self.process_nodes)
            for row in printed:
                # logger.info("".join([str(v) for v in row]))
                print("".join([str(v) for v in row]))
