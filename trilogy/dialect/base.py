from typing import List, Union, Optional, Dict, Any, Sequence, Callable

from jinja2 import Template

from trilogy.core.processing.utility import (
    is_scalar_condition,
    decompose_condition,
    sort_select_output,
)
from trilogy.constants import CONFIG, logger, MagicConstants
from trilogy.core.internal import DEFAULT_CONCEPTS
from trilogy.core.enums import (
    FunctionType,
    WindowType,
    DatePart,
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
    TupleWrapper,
    MapWrapper,
    ShowStatement,
    RowsetItem,
    MultiSelectStatement,
    RowsetDerivationStatement,
    ConceptDeclarationStatement,
    ImportStatement,
    RawSQLStatement,
    ProcessedRawSQLStatement,
    NumericType,
    MapType,
    StructType,
    MergeStatementV2,
    Datasource,
    CopyStatement,
    ProcessedCopyStatement,
)
from trilogy.core.query_processor import process_query, process_persist, process_copy
from trilogy.dialect.common import render_join, render_unnest
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
    DataType.MAP: "map",
}


def render_case(args):
    return "CASE\n\t" + "\n\t".join(args) + "\n\tEND"


def struct_arg(args):
    return [f"{x[0]}: {x[1]}" for x in zip(args[::2], args[1::2])]


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
    FunctionType.BOOL: lambda x: f"CASE WHEN {x[0]} THEN TRUE ELSE FALSE END",
    # complex
    FunctionType.INDEX_ACCESS: lambda x: f"{x[0]}[{x[1]}]",
    FunctionType.MAP_ACCESS: lambda x: f"{x[0]}[{x[1]}][1]",
    FunctionType.UNNEST: lambda x: f"unnest({x[0]})",
    FunctionType.ATTR_ACCESS: lambda x: f"""{x[0]}.{x[1].replace("'", "")}""",
    FunctionType.STRUCT: lambda x: f"{{{', '.join(struct_arg(x))}}}",
    FunctionType.ARRAY: lambda x: f"[{', '.join(x)}]",
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
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
    FunctionType.MAX: lambda args: f"{args[0]}",
    FunctionType.MIN: lambda args: f"{args[0]}",
}


GENERIC_SQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as (
{{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{% else -%}
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
\t{{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
\t{{ base }}{% endif %}{% if joins %}{% for join in joins %}
\t{{ join }}{% endfor %}{% endif %}{% if where %}
WHERE
\t{{ where }}{% endif %}{%- if group_by %}
GROUP BY {% for group in group_by %}
\t{{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}{%- if order_by %}
ORDER BY{% for order in order_by %}
\t{{ order }}{% if not loop.last %},{% endif %}{% endfor %}
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
        return None
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

    def render_concept_sql(
        self, c: Concept, cte: CTE, alias: bool = True, raise_invalid: bool = False
    ) -> str:
        result = None
        if c.pseudonyms:
            candidates = [y for y in [cte.get_concept(x) for x in c.pseudonyms] if y]
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] pseudonym candidates are {[x.address for x in candidates]}"
            )
            for candidate in [c] + candidates:
                try:
                    logger.debug(
                        f"{LOGGER_PREFIX} [{c.address}] Attempting rendering w/ candidate {candidate.address}"
                    )
                    result = self._render_concept_sql(
                        candidate, cte, raise_invalid=True
                    )
                    if result:
                        break
                except ValueError:
                    continue
        if not result:
            result = self._render_concept_sql(c, cte, raise_invalid=raise_invalid)
        if alias:
            return f"{result} as {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
        return result

    def _render_concept_sql(
        self, c: Concept, cte: CTE, raise_invalid: bool = False
    ) -> str:
        # only recurse while it's in sources of the current cte
        logger.debug(
            f"{LOGGER_PREFIX} [{c.address}] Starting rendering loop on cte: {cte.name}"
        )

        # check if it's not inherited AND no pseudonyms are inherited
        if c.lineage and cte.source_map.get(c.address, []) == []:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] rendering concept with lineage that is not already existing, have {cte.source_map}"
            )
            if isinstance(c.lineage, WindowItem):
                rendered_order_components = [
                    f"{self.render_concept_sql(x.expr, cte, alias=False, raise_invalid=raise_invalid)} {x.order.value}"
                    for x in c.lineage.order_by
                ]
                rendered_over_components = [
                    self.render_concept_sql(
                        x, cte, alias=False, raise_invalid=raise_invalid
                    )
                    for x in c.lineage.over
                ]
                rval = f"{self.WINDOW_FUNCTION_MAP[c.lineage.type](concept = self.render_concept_sql(c.lineage.content, cte=cte, alias=False, raise_invalid=raise_invalid), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
            elif isinstance(c.lineage, FilterItem):
                # for cases when we've optimized this
                if cte.condition == c.lineage.where.conditional:
                    rval = self.render_expr(c.lineage.content, cte=cte)
                else:
                    rval = f"CASE WHEN {self.render_expr(c.lineage.where.conditional, cte=cte)} THEN {self.render_concept_sql(c.lineage.content, cte=cte, alias=False, raise_invalid=raise_invalid)} ELSE NULL END"
            elif isinstance(c.lineage, RowsetItem):
                rval = f"{self.render_concept_sql(c.lineage.content, cte=cte, alias=False, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, MultiSelectStatement):
                rval = f"{self.render_concept_sql(c.lineage.find_source(c, cte), cte=cte, alias=False, raise_invalid=raise_invalid)}"
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
            elif (
                isinstance(c.lineage, Function)
                and c.lineage.operator == FunctionType.CONSTANT
                and CONFIG.rendering.parameters is True
                and c.datatype.data_type != DataType.MAP
            ):
                rval = f":{c.safe_address}"
            else:
                args = [
                    self.render_expr(
                        v, cte=cte, raise_invalid=raise_invalid
                    )  # , alias=False)
                    for v in c.lineage.arguments
                ]

                if cte.group_to_grain:
                    rval = f"{self.FUNCTION_MAP[c.lineage.operator](args)}"
                else:
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Rendering basic lookup from {cte.source_map.get(c.address, INVALID_REFERENCE_STRING('Missing source reference'))}"
            )

            raw_content = cte.get_alias(c)
            if isinstance(raw_content, RawColumnExpr):
                rval = raw_content.text
            elif isinstance(raw_content, Function):
                rval = self.render_expr(
                    raw_content, cte=cte, raise_invalid=raise_invalid
                )
            else:
                rval = safe_get_cte_value(
                    self.FUNCTION_MAP[FunctionType.COALESCE],
                    cte,
                    c,
                    self.QUOTE_CHARACTER,
                )
                if not rval:
                    if raise_invalid:
                        raise ValueError(
                            f"Invalid reference string found in query: {rval}, this should never occur. Please report this issue."
                        )
                    rval = INVALID_REFERENCE_STRING(
                        f"Missing source reference to {c.address}"
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
            MapWrapper[Any, Any],
            MapType,
            NumericType,
            StructType,
            ListType,
            ListWrapper[Any],
            TupleWrapper[Any],
            DatePart,
            CaseWhen,
            CaseElse,
            WindowItem,
            FilterItem,
            # FilterItem
        ],
        cte: Optional[CTE] = None,
        cte_map: Optional[Dict[str, CTE]] = None,
        raise_invalid: bool = False,
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
                    lookup = lookup_cte.source_map.get(
                        e.right.address,
                        [
                            INVALID_REFERENCE_STRING(
                                f"Missing source reference to {e.right.address}"
                            )
                        ],
                    )
                else:
                    lookup = lookup_cte.existence_source_map[e.right.address]
                if len(lookup) > 0:
                    target = lookup[0]
                else:
                    target = INVALID_REFERENCE_STRING(
                        f"Missing source CTE for {e.right.address}"
                    )
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} (select {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} from {target} where {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} is not null)"
            elif isinstance(e.right, (ListWrapper, TupleWrapper, Parenthetical, list)):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"

            elif isinstance(
                e.right,
                (
                    str,
                    int,
                    bool,
                    float,
                ),
            ):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} ({self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)})"
            else:
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
        elif isinstance(e, Comparison):
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
        elif isinstance(e, Conditional):
            # conditions need to be nested in parentheses
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
        elif isinstance(e, WindowItem):
            rendered_order_components = [
                f"{self.render_expr(x.expr, cte, cte_map=cte_map, raise_invalid=raise_invalid)} {x.order.value}"
                for x in e.order_by
            ]
            rendered_over_components = [
                self.render_expr(x, cte, cte_map=cte_map, raise_invalid=raise_invalid)
                for x in e.over
            ]
            return f"{self.WINDOW_FUNCTION_MAP[e.type](concept = self.render_expr(e.content, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
        elif isinstance(e, Parenthetical):
            # conditions need to be nested in parentheses
            if isinstance(e.content, list):
                return f"( {','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e.content])} )"
            return f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} )"
        elif isinstance(e, CaseWhen):
            return f"WHEN {self.render_expr(e.comparison, cte=cte, cte_map=cte_map) } THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) }"
        elif isinstance(e, CaseElse):
            return f"ELSE {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) }"
        elif isinstance(e, Function):

            if cte and cte.group_to_grain:
                return self.FUNCTION_MAP[e.operator](
                    [
                        self.render_expr(
                            z, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                        )
                        for z in e.arguments
                    ]
                )

            return self.FUNCTION_GRAIN_MATCH_MAP[e.operator](
                [
                    self.render_expr(
                        z, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                    )
                    for z in e.arguments
                ]
            )
        elif isinstance(e, AggregateWrapper):
            return self.render_expr(
                e.function, cte, cte_map=cte_map, raise_invalid=raise_invalid
            )
        elif isinstance(e, FilterItem):
            return f"CASE WHEN {self.render_expr(e.where.conditional,cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} THEN {self.render_expr(e.content, cte, cte_map=cte_map, raise_invalid=raise_invalid)} ELSE NULL END"
        elif isinstance(e, Concept):
            if cte:
                return self.render_concept_sql(
                    e, cte, alias=False, raise_invalid=raise_invalid
                )
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
            return f"[{','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])}]"
        elif isinstance(e, TupleWrapper):
            return f"({','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])})"
        elif isinstance(e, MapWrapper):
            return f"MAP {{{','.join([f'{self.render_expr(k, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}:{self.render_expr(v, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}' for k, v in e.items()])}}}"
        elif isinstance(e, list):
            return f"{self.FUNCTION_MAP[FunctionType.ARRAY]([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])}"
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

    def render_cte(self, cte: CTE, auto_sort: bool = True) -> CompiledCTE:
        if self.UNNEST_MODE in (
            UnnestMode.CROSS_APPLY,
            UnnestMode.CROSS_JOIN,
            UnnestMode.CROSS_JOIN_ALIAS,
        ):
            # for a cross apply, derivation happens in the join
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
        if auto_sort:
            select_columns = sorted(select_columns, key=lambda x: x)
        source: str | None = cte.base_name
        if not cte.render_from_clause:
            if len(cte.joins) > 0:
                if cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.CROSS_JOIN_ALIAS,
                    UnnestMode.CROSS_JOIN,
                    UnnestMode.CROSS_APPLY,
                ):
                    source = f"{render_unnest(self.UNNEST_MODE, self.QUOTE_CHARACTER, cte.join_derived_concepts[0], self.render_concept_sql, cte)}"
                # direct - eg DUCK DB - can be directly selected inline
                elif (
                    cte.join_derived_concepts and self.UNNEST_MODE == UnnestMode.DIRECT
                ):
                    source = None
                else:
                    raise SyntaxError("CTE has joins but no from clause")
            else:
                source = None
        else:
            if cte.quote_address:
                source = f"{self.QUOTE_CHARACTER}{cte.base_name}{self.QUOTE_CHARACTER}"
            else:
                source = cte.base_name
            if cte.base_name != cte.base_alias:
                source = f"{source} as {cte.base_alias}"
        if not cte.render_from_clause:
            final_joins = []
        else:
            final_joins = cte.joins or []
        where: Conditional | Parenthetical | Comparison | None = None
        having: Conditional | Parenthetical | Comparison | None = None
        materialized = {x for x, v in cte.source_map.items() if v}
        if cte.condition:
            if not cte.group_to_grain or is_scalar_condition(
                cte.condition, materialized=materialized
            ):
                where = cte.condition

            else:
                components = decompose_condition(cte.condition)
                for x in components:
                    if is_scalar_condition(x, materialized=materialized):
                        where = where + x if where else x
                    else:
                        having = having + x if having else x

        logger.info(f"{len(final_joins)} joins for cte {cte.name}")
        return CompiledCTE(
            name=cte.name,
            statement=self.SQL_TEMPLATE.render(
                select_columns=select_columns,
                base=f"{source}" if source else None,
                grain=cte.grain,
                limit=cte.limit,
                # some joins may not need to be rendered
                comment=cte.comment if CONFIG.show_comments else None,
                joins=[
                    j
                    for j in [
                        render_join(
                            join,
                            self.QUOTE_CHARACTER,
                            self.render_concept_sql,
                            self.render_expr,
                            cte,
                            self.UNNEST_MODE,
                        )
                        for join in final_joins
                    ]
                    if j
                ],
                where=(self.render_expr(where, cte) if where else None),
                having=(self.render_expr(having, cte) if having else None),
                order_by=(
                    [self.render_order_item(i, cte) for i in cte.order_by.items]
                    if cte.order_by
                    else None
                ),
                group_by=(
                    sorted(
                        list(
                            set(
                                [
                                    self.render_concept_sql(c, cte, alias=False)
                                    for c in cte.group_concepts
                                ]
                            )
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
    ) -> List[CompiledCTE]:
        return [self.render_cte(cte) for cte in query.ctes[:-1]] + [
            # last CTE needs to respect the user output order
            self.render_cte(sort_select_output(query.ctes[-1], query), auto_sort=False)
        ]

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
            | CopyStatement
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
            | ProcessedCopyStatement
        ] = []
        for statement in statements:
            if isinstance(statement, PersistStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_persist_info(statement)
                persist = process_persist(environment, statement, hooks=hooks)
                output.append(persist)
            elif isinstance(statement, CopyStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_select_info(statement.select)
                copy = process_copy(environment, statement, hooks=hooks)
                output.append(copy)
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
                    Datasource,
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

        compiled_ctes = self.generate_ctes(query)

        final = self.SQL_TEMPLATE.render(
            output=(
                query.output_to if isinstance(query, ProcessedQueryPersist) else None
            ),
            full_select=compiled_ctes[-1].statement,
            ctes=compiled_ctes[:-1],
        )

        if CONFIG.strict_mode and INVALID_REFERENCE_STRING(1) in final:
            raise ValueError(
                f"Invalid reference string found in query: {final}, this should never"
                " occur. Please report this issue."
            )
        logger.info(f"{LOGGER_PREFIX} Compiled query: {final}")
        return final
