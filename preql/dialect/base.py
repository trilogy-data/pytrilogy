from typing import List, Union, Optional, Dict, Any

from jinja2 import Template
from preql.utility import string_to_hash

from preql.constants import CONFIG, logger
from preql.core.enums import Purpose, DataType, FunctionType, WindowType
from preql.core.models import (
    Concept,
    CTE,
    ProcessedQuery,
    CompiledCTE,
    Conditional,
    Comparison,
    OrderItem,
    WindowItem,
    FilterItem,
    Function,
    AggregateWrapper,
    Parenthetical,
    CaseWhen,
    CaseElse,
)
from preql.core.models import Environment, Select
from preql.core.query_processor import process_query_v2
from preql.dialect.common import render_join
from preql.hooks.base_hook import BaseHook
from preql.utility import unique

LOGGER_PREFIX = "[RENDERING]"


def INVALID_REFERENCE_STRING(x: Any):
    return f"INVALID_REFERENCE_BUG<{x}>"


WINDOW_FUNCTION_MAP = {
    WindowType.LAG: lambda concept, window, sort: (
        f"lag({concept}) over (partition by {window} order by {sort} )"
    )
    if window
    else f"lag({concept}) over (order by {sort})",
    WindowType.LEAD: lambda concept, window, sort: (
        f"lead({concept}) over (partition by {window} order by {sort})"
    )
    if window
    else f"lead({concept}) over (order by {sort})",
    WindowType.RANK: lambda concept, window, sort: (
        f"rank() over (partition by {window} order by {sort})"
    )
    if window
    else f"rank() over (order by {sort} )",
    WindowType.ROW_NUMBER: lambda concept, window, sort: (
        f"row_number() over (partition by {window} order by {sort})"
    )
    if window
    else f"row_number() over (order by {sort})",
}

DATATYPE_MAP = {
    DataType.STRING: "string",
    DataType.INTEGER: "int",
    DataType.FLOAT: "float",
    DataType.BOOL: "bool",
}


def render_case(args):
    return "CASE\n\t\t" + "\n\t\t".join(args) + "\n\tEND"


FUNCTION_MAP = {
    # generic types
    FunctionType.CONSTANT: lambda x: f"{x[0]}",
    FunctionType.CAST: lambda x: f"cast({x[0]} as {x[1]})",
    FunctionType.CASE: lambda x: render_case(x),
    # math
    FunctionType.ADD: lambda x: f"({x[0]} + {x[1]})",
    FunctionType.SUBTRACT: lambda x: f"({x[0]} - {x[1]})",
    FunctionType.DIVIDE: lambda x: f"({x[0]} / {x[1]})",
    FunctionType.MULTIPLY: lambda x: f"({x[0]} * {x[1]})",
    FunctionType.ROUND: lambda x: f"round({x[0]},{x[1]})",
    # aggregate types
    FunctionType.COUNT_DISTINCT: lambda x: f"count(distinct {x[0]})",
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x: f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.MAX: lambda x: f"max({x[0]})",
    FunctionType.MIN: lambda x: f"min({x[0]})",
    # string types
    FunctionType.LIKE: lambda x: f" {x[0]} like {x[1]} ",
    FunctionType.UPPER: lambda x: f"UPPER({x[0]}) ",
    FunctionType.LOWER: lambda x: f"LOWER({x[0]}) ",
    # FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
    # date types
    FunctionType.DATE: lambda x: f"date({x[0]})",
    FunctionType.DATETIME: lambda x: f"datetime({x[0]})",
    FunctionType.TIMESTAMP: lambda x: f"timestamp({x[0]})",
    FunctionType.SECOND: lambda x: f"second({x[0]})",
    FunctionType.MINUTE: lambda x: f"minute({x[0]})",
    FunctionType.HOUR: lambda x: f"hour({x[0]})",
    FunctionType.DAY: lambda x: f"day({x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x: f"day_of_week({x[0]})",
    FunctionType.WEEK: lambda x: f"week({x[0]})",
    FunctionType.MONTH: lambda x: f"month({x[0]})",
    FunctionType.QUARTER: lambda x: f"quarter({x[0]})",
    FunctionType.YEAR: lambda x: f"year({x[0]})",
    # string types
    FunctionType.CONCAT: lambda x: f"concat({','.join(x)})",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args: f"{args[0]}",
    FunctionType.COUNT: lambda args: f"{args[0]}",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
    FunctionType.MAX: lambda args: f"{args[0]}",
    FunctionType.MIN: lambda args: f"{args[0]}",
}


GENERIC_SQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}{% for join in joins %}
{{ join }}{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}{%- if group_by %}GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
"""
)


def check_lineage(c: Concept, cte: CTE) -> bool:
    checks = []
    if not c.lineage:
        return True
    # logger.debug(
    #     f"{LOGGER_PREFIX} [{c.address}] Checking lineage for rendering in {cte.name}"
    # )
    for sub_c in c.lineage.arguments:
        if not isinstance(sub_c, Concept):
            continue
        if sub_c.address in cte.source_map or (
            sub_c.lineage and check_lineage(sub_c, cte)
        ):
            checks.append(True)
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{sub_c.address}] not found in source map for"
                f" {cte.name}, have cte keys {[c for c in cte.source_map.keys()]} and"
                f" datasource keys {[c for c in cte.source.source_map.keys()]}"
            )
            checks.append(False)
    return all(checks)


def safe_quote(string: str, quote_char: str):
    # split dotted identifiers
    # TODO: evaluate if we need smarter parsing for strings that could actually include .
    components = string.split(".")
    return ".".join([f"{quote_char}{string}{quote_char}" for string in components])


class BaseDialect:
    WINDOW_FUNCTION_MAP = WINDOW_FUNCTION_MAP
    FUNCTION_MAP = FUNCTION_MAP
    FUNCTION_GRAIN_MATCH_MAP = FUNCTION_GRAIN_MATCH_MAP
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = GENERIC_SQL_TEMPLATE
    DATATYPE_MAP = DATATYPE_MAP

    def render_order_item(self, order_item: OrderItem, ctes: List[CTE]) -> str:
        matched_ctes = [
            cte
            for cte in ctes
            if order_item.expr.address in [a.address for a in cte.output_columns]
        ]
        if not matched_ctes:
            raise ValueError(f"No source found for concept {order_item.expr}")
        selected = matched_ctes[0]
        return (
            f"{selected.name}.{order_item.expr.safe_address} {order_item.order.value}"
        )

    def render_concept_sql(self, c: Concept, cte: CTE, alias: bool = True) -> str:
        # only recurse while it's in sources of the current cte
        logger.debug(
            f"{LOGGER_PREFIX} [{c.address}] Attempting rendering on"
            f" {cte.name} alias={alias}"
        )

        if (c.lineage and check_lineage(c, cte)) and not cte.source_map.get(
            c.address, ""
        ).startswith("cte"):
            logger.debug(f"{LOGGER_PREFIX} [{c.address}] rendering lineage concept")
            if isinstance(c.lineage, WindowItem):
                # args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments] +[c.lineage.sort_concepts]
                self.render_concept_sql(c.lineage.arguments[0], cte, alias=False)
                rendered_order_components = [
                    f"{self.render_concept_sql(x.expr, cte, alias=False)} {x.order.value}"
                    for x in c.lineage.order_by
                ]
                rendered_over_components = [
                    self.render_concept_sql(x, cte, alias=False) for x in c.lineage.over
                ]
                rval = f"{self.WINDOW_FUNCTION_MAP[c.lineage.type](concept = self.render_concept_sql(c.lineage.content, cte=cte, alias=False), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
            elif isinstance(c.lineage, FilterItem):
                rval = f"{self.render_concept_sql(c.lineage.content, cte=cte, alias=False)}"
            elif isinstance(c.lineage, AggregateWrapper):
                args = [
                    self.render_expr(v, cte)  # , alias=False)
                    for v in c.lineage.function.arguments
                ]
                if cte.group_to_grain:
                    rval = f"{self.FUNCTION_MAP[c.lineage.function.operator](args)}"
                else:
                    logger.info(
                        f"{LOGGER_PREFIX} [{c.address}] ignoring aggregate, already at"
                        " target grain"
                    )
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.function.operator](args)}"
            else:
                args = [
                    self.render_expr(v, cte)  # , alias=False)
                    for v in c.lineage.arguments
                ]
                if cte.group_to_grain:
                    rval = f"{self.FUNCTION_MAP[c.lineage.operator](args)}"
                else:
                    logger.info(
                        f"{LOGGER_PREFIX} [{c.address}] ignoring aggregate, already at"
                        " target grain"
                    )
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
        # else if it's complex, just reference it from the source
        elif c.lineage:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Complex reference falling back to"
                " source address"
            )
            if not cte.source_map.get(c.address, None):
                logger.debug(
                    f"{LOGGER_PREFIX} [{c.address}] Cannot render from {cte.name}, have"
                    f" {cte.source_map.keys()} only"
                )
            missing = [
                sub_c.address
                for sub_c in c.lineage.arguments
                if isinstance(sub_c, Concept) and sub_c not in cte.source_map
            ]
            rval = f"{cte.source_map.get(c.address, INVALID_REFERENCE_STRING(f'Missing complex sources {missing}, have {cte.source_map.keys()}'))}.{safe_quote(c.safe_address, self.QUOTE_CHARACTER)}"
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Basic reference, using source address"
                f" for {c.address}"
            )
            if not cte.source_map.get(c.address, None):
                logger.debug(
                    f"{LOGGER_PREFIX} [{c.address}] Cannot render {c.address} from"
                    f" {cte.name}, have {cte.source_map.keys()} only"
                )
            rval = f"{cte.source_map.get(c.address, INVALID_REFERENCE_STRING('Missing basic reference'))}.{safe_quote(cte.get_alias(c), self.QUOTE_CHARACTER)}"

        if alias:
            return (
                f"{rval} as"
                f" {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
            )
        return rval

    def render_expr(
        self,
        e: Union[
            Function,
            Conditional,
            Comparison,
            Concept,
            str,
            int,
            list,
            bool,
            float,
            DataType,
            Function,
            Parenthetical,
            AggregateWrapper
            # FilterItem
        ],
        cte: Optional[CTE] = None,
        cte_map: Optional[Dict[str, CTE]] = None,
    ) -> str:
        # if isinstance(e, Concept):
        #     cte = cte or cte_map.get(e.address, None)

        if isinstance(e, Comparison):
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)}"
        elif isinstance(e, Conditional):
            # conditions need to be nested in parentheses
            return f"( {self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)} ) "
        elif isinstance(e, Parenthetical):
            # conditions need to be nested in parentheses
            return f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map)} ) "
        elif isinstance(e, CaseWhen):
            return f"WHEN {self.render_expr(e.comparison, cte=cte, cte_map=cte_map) } THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map) }"
        elif isinstance(e, CaseElse):
            return f"ELSE {self.render_expr(e.expr, cte=cte, cte_map=cte_map) }"
        # elif isinstance(e, FilterItem):
        #     return f"{self.render_expr}"

        # elif isinstance(e, Parenthetical):
        #     # conditions need to be nested in parentheses
        #     return (
        #         f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map)} ) "
        #     )
        elif isinstance(e, Function):
            if cte and cte.group_to_grain:
                return self.FUNCTION_MAP[e.operator](
                    [self.render_expr(z, cte=cte, cte_map=cte_map) for z in e.arguments]
                )
            return self.FUNCTION_GRAIN_MATCH_MAP[e.operator](
                [self.render_expr(z, cte=cte, cte_map=cte_map) for z in e.arguments]
            )
        elif isinstance(e, AggregateWrapper):
            return self.render_expr(e.function, cte, cte_map=cte_map)
        elif isinstance(e, Concept):
            if cte:
                return self.render_concept_sql(e, cte, False)
                # return f"{cte.source_map[e.address]}.{self.QUOTE_CHARACTER}{cte.get_alias(e)}{self.QUOTE_CHARACTER}"
            elif cte_map:
                return f"{cte_map[e.address].name}.{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
            return f"{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
        elif isinstance(e, bool):
            return f"{e}"
        elif isinstance(e, str):
            return f"'{e}'"
        elif isinstance(e, (int, float)):
            return str(e)
        elif isinstance(e, list):
            return f"[{','.join([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e])}]"
        elif isinstance(e, DataType):
            return str(e.value)
        raise ValueError(f"Unable to render type {type(e)} {e}")

    def render_cte(self, cte: CTE):
        return CompiledCTE(
            name=cte.name,
            statement=self.SQL_TEMPLATE.render(
                select_columns=[
                    self.render_concept_sql(c, cte) for c in cte.output_columns
                ],
                base=f"{cte.base_name} as {cte.base_alias}"
                if cte.render_from_clause
                else None,
                grain=cte.grain,
                limit=None,
                joins=[
                    render_join(join, self.QUOTE_CHARACTER)
                    for join in (cte.joins or [])
                ],
                where=self.render_expr(cte.condition, cte)
                if cte.condition
                else None,  # source_map=cte_output_map)
                # where=self.render_expr(where_assignment[cte.name], cte)
                # if cte.name in where_assignment
                # else None,
                group_by=[
                    self.render_concept_sql(c, cte, alias=False)
                    for c in unique(
                        cte.grain.components
                        + [
                            c
                            for c in cte.output_columns
                            if c.purpose == Purpose.PROPERTY
                            and c not in cte.grain.components
                        ]
                        + [
                            c
                            for c in cte.output_columns
                            if c.purpose == Purpose.METRIC
                            and any(
                                [
                                    c.with_grain(cte.grain) in cte.output_columns
                                    for cte in cte.parent_ctes
                                ]
                            )
                        ],
                        "address",
                    )
                ]
                if cte.group_to_grain
                else None,
            ),
        )

    def generate_ctes(
        self, query: ProcessedQuery, where_assignment: Dict[str, Conditional]
    ):
        return [self.render_cte(cte) for cte in query.ctes]

    def generate_queries(
        self,
        environment: Environment,
        statements,
        hooks: Optional[List[BaseHook]] = None,
    ) -> List[ProcessedQuery]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                if hooks:
                    for hook in hooks:
                        hook.process_select_info(statement)
                output.append(process_query_v2(environment, statement, hooks=hooks))
                # graph = generate_graph(environment, statement)
                # output.append(graph_to_query(environment, graph, statement))
        return output

    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns: Dict[str, str] = {}
        cte_output_map = {}
        selected = set()
        output_addresses = [c.address for c in query.output_columns]
        for c in query.base.output_columns:
            if c.address not in selected and c.address in output_addresses:
                select_columns[
                    c.address
                ] = f"{query.base.name}.{safe_quote(c.safe_address, self.QUOTE_CHARACTER)}"
                cte_output_map[c.address] = query.base
                selected.add(c.address)
        if not all([x in selected for x in output_addresses]):
            missing = [x for x in output_addresses if x not in selected]
            raise ValueError(
                f"Did not get all output addresses in select - missing: {missing}, have"
                f" {selected}"
            )

        # where assignment
        output_where = False
        if query.where_clause:
            found = False
            filter = set([str(x.address) for x in query.where_clause.concept_arguments])
            query_output = set([str(z.address) for z in query.output_columns])
            if filter.issubset(query_output):
                output_where = True
                found = True
            # we can't push global filters up to CTEs
            # because they may reference all columns in the output
            # only some of which are joined
            # TODO: optimize this?
            # if not found:
            #     for cte in output_ctes:
            #         cte_filter = set([str(z.with_grain()) for z in cte.output_columns])
            #         if filter.issubset(cte_filter):
            #             # 2023-01-16 - removing related columns to look at output columns
            #             # will need to backport pushing where columns into original output search
            #             # if set([x.name for x in query.where_clause.input]).issubset(
            #             #     [z.name for z in cte.related_columns]
            #             # ):
            #             where_assignment[cte.name] = query.where_clause.conditional
            #             found = True
            #             break

            if not found:
                raise NotImplementedError(
                    f"Cannot generate query with filtering on grain {filter} that is"
                    f" not a subset of the query output grain {query_output}. Use a"
                    " filtered concept instead."
                )
        # 2023-03-31 - this needs to be moved up into query building
        # for join in query.joins:
        #
        #     if (
        #         join.left_cte.grain.issubset(query.grain)
        #         and join.left_cte.grain != query.grain
        #     ):
        #         join.jointype = JoinType.FULL
        compiled_ctes = self.generate_ctes(query, {})

        # want to return columns in the order the user wrote
        sorted_select = []
        for c in query.output_columns:
            sorted_select.append(select_columns[c.address])
        final = self.SQL_TEMPLATE.render(
            select_columns=sorted_select,
            base=query.base.name,
            joins=[render_join(join, self.QUOTE_CHARACTER) for join in query.joins],
            ctes=compiled_ctes,
            limit=query.limit,
            # move up to CTEs
            where=self.render_expr(
                query.where_clause.conditional, cte_map=cte_output_map
            )
            if query.where_clause and output_where
            else None,
            order_by=[
                self.render_order_item(i, [query.base]) for i in query.order_by.items
            ]
            if query.order_by
            else None,
        )

        if CONFIG.strict_mode and INVALID_REFERENCE_STRING(1) in final:
            raise ValueError(
                f"Invalid reference string found in query: {final}, this should never"
                " occur. Please report this issue."
            )
        if CONFIG.hash_identifiers:
            for cte in query.ctes:
                new_name = f"rhash_{string_to_hash(cte.name)}"
                final = final.replace(cte.name, new_name)
        return final
