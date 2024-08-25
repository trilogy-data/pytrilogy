from typing import List, Union, Optional, Dict, Any, Sequence, Callable

from jinja2 import Template

from trilogy.constants import CONFIG, logger, MagicConstants
from trilogy.core.internal import DEFAULT_CONCEPTS
from trilogy.core.enums import (
    FunctionType,
    WindowType,
    DatePart,
    PurposeLineage,
    ComparisonOperator,
)
from trilogy.core.models import (
    ListType,
    DataType,
    Concept,
    CTE,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedShowStatement,
    CompiledCTE,
    Conditional,
    Comparison,
    SubselectComparison,
    OrderItem,
    WindowItem,
    FilterItem,
    Function,
    AggregateWrapper,
    Parenthetical,
    CaseWhen,
    CaseElse,
    SelectStatement,
    PersistStatement,
    Environment,
    RawColumnExpr,
    ListWrapper,
    ShowStatement,
    RowsetItem,
    MultiSelectStatement,
    RowsetDerivationStatement,
    ConceptDeclarationStatement,
    ImportStatement,
    RawSQLStatement,
    ProcessedRawSQLStatement,
    NumericType,
    MergeStatementV2,
)
from trilogy.core.query_processor import process_query, process_persist
from trilogy.dialect.common import render_join
from trilogy.hooks.base_hook import BaseHook
from trilogy.core.enums import UnnestMode

LOGGER_PREFIX = "[RENDERING]"


def INVALID_REFERENCE_STRING(x: Any, callsite: str = ""):
    return f"INVALID_REFERENCE_BUG_{callsite}<{x}>"


def window_factory(string: str, include_concept: bool = False) -> Callable:
    def render_window(
        concept: str, window: str, sort: str, offset: int | None = None
    ) -> str:
        if not include_concept:
            concept = ""
        if offset:
            base = f"{string}({concept}, {offset})"
        else:
            base = f"{string}({concept})"
        if window and sort:
            return f"{base} over (partition by {window} order by {sort} )"
        elif window:
            return f"{base} over (partition by {window})"
        elif sort:
            return f"{base} over (order by {sort} )"
        else:
            return f"{base} over ()"

    return render_window


WINDOW_FUNCTION_MAP = {
    WindowType.LAG: window_factory("lag", include_concept=True),
    WindowType.LEAD: window_factory("lead", include_concept=True),
    WindowType.RANK: window_factory("rank"),
    WindowType.ROW_NUMBER: window_factory("row_number"),
    WindowType.SUM: window_factory("sum", include_concept=True),
    WindowType.COUNT: window_factory("count", include_concept=True),
    WindowType.AVG: window_factory("avg", include_concept=True),
}

DATATYPE_MAP = {
    DataType.STRING: "string",
    DataType.INTEGER: "int",
    DataType.FLOAT: "float",
    DataType.BOOL: "bool",
    DataType.NUMERIC: "numeric",
}


def render_case(args):
    return "CASE\n\t" + "\n\t".join(args) + "\n\tEND"


FUNCTION_MAP = {
    # generic types
    FunctionType.ALIAS: lambda x: f"{x[0]}",
    FunctionType.GROUP: lambda x: f"{x[0]}",
    FunctionType.CONSTANT: lambda x: f"{x[0]}",
    FunctionType.COALESCE: lambda x: f"coalesce({','.join(x)})",
    FunctionType.CAST: lambda x: f"cast({x[0]} as {x[1]})",
    FunctionType.CASE: lambda x: render_case(x),
    FunctionType.SPLIT: lambda x: f"split({x[0]}, {x[1]})",
    FunctionType.IS_NULL: lambda x: f"isnull({x[0]})",
    # complex
    FunctionType.INDEX_ACCESS: lambda x: f"{x[0]}[{x[1]}]",
    FunctionType.UNNEST: lambda x: f"unnest({x[0]})",
    # math
    FunctionType.ADD: lambda x: f"{x[0]} + {x[1]}",
    FunctionType.SUBTRACT: lambda x: f"{x[0]} - {x[1]}",
    FunctionType.DIVIDE: lambda x: f"{x[0]} / {x[1]}",
    FunctionType.MULTIPLY: lambda x: f"{x[0]} * {x[1]}",
    FunctionType.ROUND: lambda x: f"round({x[0]},{x[1]})",
    FunctionType.MOD: lambda x: f"({x[0]} % {x[1]})",
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
    FunctionType.SUBSTRING: lambda x: f"SUBSTRING({x[0]},{x[1]},{x[2]})",
    FunctionType.STRPOS: lambda x: f"STRPOS({x[0]},{x[1]})",
    # FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
    # date types
    FunctionType.DATE_TRUNCATE: lambda x: f"date_trunc({x[0]},{x[1]})",
    FunctionType.DATE_PART: lambda x: f"date_part({x[0]},{x[1]})",
    FunctionType.DATE_ADD: lambda x: f"date_add({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE_DIFF: lambda x: f"date_diff({x[0]},{x[1]}, {x[2]})",
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
    # constant types
    FunctionType.CURRENT_DATE: lambda x: "current_date()",
    FunctionType.CURRENT_DATETIME: lambda x: "current_datetime()",
    FunctionType.ATTR_ACCESS: lambda x: f"""{x[0]}.{x[1].replace("'", "")}""",
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
{%- if full_select -%}
{{full_select}}
{%- else -%}
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
\t{{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
\t{{ base }}{% endif %}{% if joins %}{% for join in joins %}
\t{{ join }}{% endfor %}{% endif %}
{% if where %}WHERE
\t{{ where }}
{% endif %}{%- if group_by %}GROUP BY {% for group in group_by %}
\t{{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}
{% endif %}{% endif %}
"""
)


def safe_quote(string: str, quote_char: str):
    # split dotted identifiers
    # TODO: evaluate if we need smarter parsing for strings that could actually include .
    components = string.split(".")
    return ".".join([f"{quote_char}{string}{quote_char}" for string in components])


def safe_get_cte_value(coalesce, cte: CTE, c: Concept, quote_char: str):
    address = c.address
    raw = cte.source_map.get(address, None)

    if not raw:
        return INVALID_REFERENCE_STRING("Missing source reference")
    if isinstance(raw, str):
        rendered = cte.get_alias(c, raw)
        return f"{raw}.{safe_quote(rendered, quote_char)}"
    if isinstance(raw, list) and len(raw) == 1:
        rendered = cte.get_alias(c, raw[0])
        return f"{raw[0]}.{safe_quote(rendered, quote_char)}"
    return coalesce([f"{x}.{safe_quote(cte.get_alias(c, x), quote_char)}" for x in raw])


class BaseDialect:
    WINDOW_FUNCTION_MAP = WINDOW_FUNCTION_MAP
    FUNCTION_MAP = FUNCTION_MAP
    FUNCTION_GRAIN_MATCH_MAP = FUNCTION_GRAIN_MATCH_MAP
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = GENERIC_SQL_TEMPLATE
    DATATYPE_MAP = DATATYPE_MAP
    UNNEST_MODE = UnnestMode.CROSS_APPLY

    def render_order_item(
        self, order_item: OrderItem, cte: CTE, final: bool = False
    ) -> str:
        if final:
            return f"{cte.name}.{self.QUOTE_CHARACTER}{order_item.expr.safe_address}{self.QUOTE_CHARACTER} {order_item.order.value}"

        return f"{self.render_concept_sql(order_item.expr, cte=cte, alias=False)} {order_item.order.value}"

    def render_concept_sql(self, c: Concept, cte: CTE, alias: bool = True) -> str:
        # only recurse while it's in sources of the current cte
        logger.debug(
            f"{LOGGER_PREFIX} [{c.address}] Starting rendering loop on cte: {cte.name}"
        )

        if c.lineage and cte.source_map.get(c.address, []) == []:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] rendering concept with lineage that is not already existing"
            )
            if isinstance(c.lineage, WindowItem):
                rendered_order_components = [
                    f"{self.render_concept_sql(x.expr, cte, alias=False)} {x.order.value}"
                    for x in c.lineage.order_by
                ]
                rendered_over_components = [
                    self.render_concept_sql(x, cte, alias=False) for x in c.lineage.over
                ]
                rval = f"{self.WINDOW_FUNCTION_MAP[c.lineage.type](concept = self.render_concept_sql(c.lineage.content, cte=cte, alias=False), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
            elif isinstance(c.lineage, FilterItem):
                # for cases when we've optimized this
                if (
                    len(cte.output_columns) == 1
                    and cte.condition == c.lineage.where.conditional
                ):
                    rval = self.render_expr(c.lineage.content, cte=cte)
                else:
                    rval = f"CASE WHEN {self.render_expr(c.lineage.where.conditional, cte=cte)} THEN {self.render_concept_sql(c.lineage.content, cte=cte, alias=False)} ELSE NULL END"
            elif isinstance(c.lineage, RowsetItem):
                rval = f"{self.render_concept_sql(c.lineage.content, cte=cte, alias=False)}"
            elif isinstance(c.lineage, MultiSelectStatement):
                rval = f"{self.render_concept_sql(c.lineage.find_source(c, cte), cte=cte, alias=False)}"
            elif isinstance(c.lineage, AggregateWrapper):
                args = [
                    self.render_expr(v, cte)  # , alias=False)
                    for v in c.lineage.function.arguments
                ]
                if cte.group_to_grain:
                    rval = self.FUNCTION_MAP[c.lineage.function.operator](args)
                else:
                    logger.debug(
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
                    logger.debug(
                        f"{LOGGER_PREFIX} [{c.address}] ignoring optimazable aggregate function, at grain so optimizing"
                    )
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Rendering basic lookup from {cte.source_map.get(c.address, INVALID_REFERENCE_STRING('Missing source reference'))}"
            )

            raw_content = cte.get_alias(c)
            if isinstance(raw_content, RawColumnExpr):
                rval = raw_content.text
            elif isinstance(raw_content, Function):
                rval = self.render_expr(raw_content, cte=cte)
            else:
                rval = f"{safe_get_cte_value(self.FUNCTION_MAP[FunctionType.COALESCE], cte, c, self.QUOTE_CHARACTER)}"
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
            SubselectComparison,
            Concept,
            str,
            int,
            list,
            bool,
            float,
            DataType,
            Function,
            Parenthetical,
            AggregateWrapper,
            MagicConstants,
            NumericType,
            ListType,
            ListWrapper[int],
            ListWrapper[str],
            ListWrapper[float],
            DatePart,
            CaseWhen,
            CaseElse,
            WindowItem,
            FilterItem,
            # FilterItem
        ],
        cte: Optional[CTE] = None,
        cte_map: Optional[Dict[str, CTE]] = None,
    ) -> str:

        if isinstance(e, SubselectComparison):

            if isinstance(e.right, Concept):
                # we won't always have an existnce map
                # so fall back to the normal map
                lookup_cte = cte
                if cte_map and not lookup_cte:
                    lookup_cte = cte_map.get(e.right.address)
                assert lookup_cte, "Subselects must be rendered with a CTE in context"
                if e.right.address not in lookup_cte.existence_source_map:
                    lookup = lookup_cte.source_map[e.right.address]
                else:
                    lookup = lookup_cte.existence_source_map[e.right.address]

                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} (select {lookup[0]}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} from {lookup[0]})"
            elif isinstance(e.right, (ListWrapper, Parenthetical, list)):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)}"

            elif isinstance(
                e.right,
                (
                    str,
                    int,
                    bool,
                    float,
                ),
            ):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} ({self.render_expr(e.right, cte=cte, cte_map=cte_map)})"
            else:
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)}"
        elif isinstance(e, Comparison):
            if e.operator == ComparisonOperator.BETWEEN:
                right_comp = e.right
                assert isinstance(right_comp, Conditional)
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(right_comp.left, cte=cte, cte_map=cte_map) and self.render_expr(right_comp.right, cte=cte, cte_map=cte_map)}"
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)}"
        elif isinstance(e, Conditional):
            # conditions need to be nested in parentheses
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map)}"
        elif isinstance(e, WindowItem):
            rendered_order_components = [
                f"{self.render_expr(x.expr, cte, cte_map=cte_map)} {x.order.value}"
                for x in e.order_by
            ]
            rendered_over_components = [
                self.render_expr(x, cte, cte_map=cte_map) for x in e.over
            ]
            return f"{self.WINDOW_FUNCTION_MAP[e.type](concept = self.render_expr(e.content, cte=cte, cte_map=cte_map), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
        elif isinstance(e, Parenthetical):
            # conditions need to be nested in parentheses
            if isinstance(e.content, list):
                return f"( {','.join([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e.content])} )"
            return f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map)} )"
        elif isinstance(e, CaseWhen):
            return f"WHEN {self.render_expr(e.comparison, cte=cte, cte_map=cte_map) } THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map) }"
        elif isinstance(e, CaseElse):
            return f"ELSE {self.render_expr(e.expr, cte=cte, cte_map=cte_map) }"
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
        elif isinstance(e, FilterItem):
            return f"CASE WHEN {self.render_expr(e.where.conditional,cte=cte, cte_map=cte_map)} THEN {self.render_expr(e.content, cte, cte_map=cte_map)} ELSE NULL END"
        elif isinstance(e, Concept):
            if cte:
                return self.render_concept_sql(e, cte, alias=False)
            elif cte_map:
                return f"{cte_map[e.address].name}.{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
            return f"{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
        elif isinstance(e, bool):
            return f"{e}"
        elif isinstance(e, str):
            return f"'{e}'"
        elif isinstance(e, (int, float)):
            return str(e)
        elif isinstance(e, ListWrapper):
            return f"[{','.join([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e])}]"
        elif isinstance(e, list):
            return f"[{','.join([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e])}]"
        elif isinstance(e, DataType):
            return str(e.value)
        elif isinstance(e, DatePart):
            return str(e.value)
        elif isinstance(e, NumericType):
            return f"{self.DATATYPE_MAP[DataType.NUMERIC]}({e.precision},{e.scale})"
        elif isinstance(e, MagicConstants):
            if e == MagicConstants.NULL:
                return "null"
        else:
            raise ValueError(f"Unable to render type {type(e)} {e}")

    def render_cte(self, cte: CTE):
        if self.UNNEST_MODE in (UnnestMode.CROSS_APPLY, UnnestMode.CROSS_JOIN):
            # for a cross apply, derviation happens in the join
            # so we only use the alias to select
            select_columns = [
                self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in [y.address for y in cte.join_derived_concepts]
                and c.address not in [y.address for y in cte.hidden_concepts]
            ] + [
                f"{self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
                for c in cte.join_derived_concepts
            ]
        else:
            # otherwse, assume we are unnesting directly in the select
            select_columns = [
                self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in [y.address for y in cte.hidden_concepts]
            ]
        if cte.base_name == cte.base_alias:
            source = cte.base_name
        else:
            source = f"{cte.base_name} as {cte.base_alias}"
        return CompiledCTE(
            name=cte.name,
            statement=self.SQL_TEMPLATE.render(
                select_columns=select_columns,
                base=(f"{source}" if cte.render_from_clause else None),
                grain=cte.grain,
                limit=cte.limit,
                # some joins may not need to be rendered
                joins=[
                    j
                    for j in [
                        render_join(
                            join,
                            self.QUOTE_CHARACTER,
                            self.render_concept_sql,
                            cte,
                            self.UNNEST_MODE,
                        )
                        for join in (cte.joins or [])
                    ]
                    if j
                ],
                where=(
                    self.render_expr(cte.condition, cte) if cte.condition else None
                ),  # source_map=cte_output_map)
                order_by=(
                    [self.render_order_item(i, cte) for i in cte.order_by.items]
                    if cte.order_by
                    else None
                ),
                group_by=(
                    list(
                        set(
                            [
                                self.render_concept_sql(c, cte, alias=False)
                                for c in cte.group_concepts
                            ]
                        )
                    )
                    if cte.group_to_grain
                    else None
                ),
            ),
        )

    def generate_ctes(
        self,
        query: ProcessedQuery,
    ):
        return [self.render_cte(cte) for cte in query.ctes]

    def generate_queries(
        self,
        environment: Environment,
        statements: Sequence[
            SelectStatement
            | MultiSelectStatement
            | PersistStatement
            | ShowStatement
            | ConceptDeclarationStatement
            | RowsetDerivationStatement
            | ImportStatement
            | RawSQLStatement
            | MergeStatementV2
        ],
        hooks: Optional[List[BaseHook]] = None,
    ) -> List[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
    ]:
        output: List[
            ProcessedQuery
            | ProcessedQueryPersist
            | ProcessedShowStatement
            | ProcessedRawSQLStatement
        ] = []
        for statement in statements:
            if isinstance(statement, PersistStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_persist_info(statement)
                persist = process_persist(environment, statement, hooks=hooks)
                output.append(persist)
            elif isinstance(statement, SelectStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_select_info(statement)
                output.append(process_query(environment, statement, hooks=hooks))
            elif isinstance(statement, MultiSelectStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_multiselect_info(statement)
                output.append(process_query(environment, statement, hooks=hooks))
            elif isinstance(statement, RowsetDerivationStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_rowset_info(statement)
            elif isinstance(statement, ShowStatement):
                # TODO - encapsulate this a little better
                if isinstance(statement.content, SelectStatement):
                    output.append(
                        ProcessedShowStatement(
                            output_columns=[
                                environment.concepts[
                                    DEFAULT_CONCEPTS["query_text"].address
                                ]
                            ],
                            output_values=[
                                process_query(
                                    environment, statement.content, hooks=hooks
                                )
                            ],
                        )
                    )
                else:
                    raise NotImplementedError(type(statement))
            elif isinstance(statement, RawSQLStatement):
                output.append(ProcessedRawSQLStatement(text=statement.text))
            elif isinstance(
                statement,
                (
                    ConceptDeclarationStatement,
                    MergeStatementV2,
                    ImportStatement,
                    RowsetDerivationStatement,
                ),
            ):
                continue
            else:
                raise NotImplementedError(type(statement))
        return output

    def compile_statement(
        self,
        query: (
            ProcessedQuery
            | ProcessedQueryPersist
            | ProcessedShowStatement
            | ProcessedRawSQLStatement
        ),
    ) -> str:
        if isinstance(query, ProcessedShowStatement):
            return ";\n".join([str(x) for x in query.output_values])
        elif isinstance(query, ProcessedRawSQLStatement):
            return query.text
        select_columns: Dict[str, str] = {}
        cte_output_map = {}
        selected = set()
        hidden_addresses = [c.address for c in query.hidden_columns]
        output_addresses = [
            c.address for c in query.output_columns if c.address not in hidden_addresses
        ]

        for c in query.base.output_columns:
            if c.address not in selected:
                select_columns[c.address] = (
                    f"{query.base.name}.{safe_quote(c.safe_address, self.QUOTE_CHARACTER)}"
                )
                cte_output_map[c.address] = query.base
                if c.address not in hidden_addresses:
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
            # found = False
            filter = set(
                [
                    str(x.address)
                    for x in query.where_clause.row_arguments
                    if not x.derivation == PurposeLineage.CONSTANT
                ]
            )
            query_output = set([str(z.address) for z in query.output_columns])
            # if it wasn't an output
            # we would have forced it up earlier and we don't need to render at this point
            if filter.issubset(query_output):
                output_where = True
            for ex_set in query.where_clause.existence_arguments:
                for c in ex_set:
                    if c.address not in cte_output_map:
                        cts = [
                            ct
                            for ct in query.ctes
                            if ct.name in query.base.existence_source_map[c.address]
                        ]
                        if not cts:
                            raise ValueError(query.base.existence_source_map[c.address])
                        cte_output_map[c.address] = cts[0]

        compiled_ctes = self.generate_ctes(query)

        # restort selections by the order they were written in
        sorted_select: List[str] = []
        for output_c in output_addresses:
            sorted_select.append(select_columns[output_c])
        if not query.base.requires_nesting:
            final = self.SQL_TEMPLATE.render(
                output=(
                    query.output_to
                    if isinstance(query, ProcessedQueryPersist)
                    else None
                ),
                full_select=compiled_ctes[-1].statement,
                ctes=compiled_ctes[:-1],
            )
        else:
            final = self.SQL_TEMPLATE.render(
                output=(
                    query.output_to
                    if isinstance(query, ProcessedQueryPersist)
                    else None
                ),
                select_columns=sorted_select,
                base=query.base.name,
                joins=[
                    render_join(join, self.QUOTE_CHARACTER, None)
                    for join in query.joins
                ],
                ctes=compiled_ctes,
                limit=query.limit,
                # move up to CTEs
                where=(
                    self.render_expr(
                        query.where_clause.conditional, cte_map=cte_output_map
                    )
                    if query.where_clause and output_where
                    else None
                ),
                order_by=(
                    [
                        self.render_order_item(i, query.base, final=True)
                        for i in query.order_by.items
                    ]
                    if query.order_by
                    else None
                ),
            )

        if CONFIG.strict_mode and INVALID_REFERENCE_STRING(1) in final:
            raise ValueError(
                f"Invalid reference string found in query: {final}, this should never"
                " occur. Please report this issue."
            )
        logger.info(f"{LOGGER_PREFIX} Compiled query: {final}")
        return final
