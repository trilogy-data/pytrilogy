from collections import defaultdict
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from jinja2 import Template

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    MagicConstants,
    Rendering,
    logger,
)
from trilogy.core.constants import UNNEST_NAME
from trilogy.core.enums import (
    ComparisonOperator,
    DatePart,
    FunctionType,
    GroupMode,
    Modifier,
    Ordering,
    ShowCategory,
    UnnestMode,
    WindowType,
)
from trilogy.core.internal import DEFAULT_CONCEPTS
from trilogy.core.models.author import ArgBinding
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFilterItem,
    BuildFunction,
    BuildMultiSelectLineage,
    BuildOrderItem,
    BuildParamaterizedConceptReference,
    BuildParenthetical,
    BuildRowsetItem,
    BuildSubselectComparison,
    BuildWindowItem,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
)
from trilogy.core.models.datasource import Datasource, RawColumnExpr
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import CTE, CompiledCTE, RecursiveCTE, UnionCTE
from trilogy.core.processing.utility import (
    decompose_condition,
    is_scalar_condition,
    sort_select_output,
)
from trilogy.core.query_processor import process_copy, process_persist, process_query
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    CopyStatement,
    FunctionDeclaration,
    ImportStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectStatement,
    ShowStatement,
    ValidateStatement,
)
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedCopyStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
    ProcessedValidateStatement,
)
from trilogy.core.utility import safe_quote
from trilogy.dialect.common import render_join, render_unnest
from trilogy.hooks.base_hook import BaseHook


def null_wrapper(lval: str, rval: str, modifiers: list[Modifier]) -> str:

    if Modifier.NULLABLE in modifiers:
        return f"({lval} = {rval} or ({lval} is null and {rval} is null))"
    return f"{lval} = {rval}"


LOGGER_PREFIX = "[RENDERING]"

WINDOW_ITEMS = (BuildWindowItem,)
FILTER_ITEMS = (BuildFilterItem,)
AGGREGATE_ITEMS = (BuildAggregateWrapper,)
FUNCTION_ITEMS = (BuildFunction,)
PARENTHETICAL_ITEMS = (BuildParenthetical,)
CASE_WHEN_ITEMS = (BuildCaseWhen,)
CASE_ELSE_ITEMS = (BuildCaseElse,)
SUBSELECT_COMPARISON_ITEMS = (BuildSubselectComparison,)
COMPARISON_ITEMS = (BuildComparison,)
CONDITIONAL_ITEMS = (BuildConditional,)


def INVALID_REFERENCE_STRING(x: Any, callsite: str = ""):
    # if CONFIG.validate_missing:
    #     raise SyntaxError(f"INVALID_REFERENCE_BUG_{callsite}<{x}>")

    return f"INVALID_REFERENCE_BUG_{callsite}<{x}>"


def window_factory(string: str, include_concept: bool = False) -> Callable:
    def render_window(
        concept: str, window: str, sort: str, offset: int | None = None
    ) -> str:
        if not include_concept:
            concept = ""
        if offset is not None:
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

DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "string",
    DataType.INTEGER: "int",
    DataType.FLOAT: "float",
    DataType.BOOL: "bool",
    DataType.NUMERIC: "numeric",
    DataType.MAP: "map",
    DataType.DATE: "date",
    DataType.DATETIME: "datetime",
    DataType.ARRAY: "list",
}

COMPLEX_DATATYPE_MAP = {
    DataType.ARRAY: lambda x: f"{x}[]",
}


def render_case(args):
    return "CASE\n\t" + "\n\t".join(args) + "\n\tEND"


def struct_arg(args):
    return [f"{x[1]}: {x[0]}" for x in zip(args[::2], args[1::2])]


def hash_from_args(val, hash_type):
    hash_type = hash_type[1:-1]
    if hash_type.lower() == "md5":
        return f"md5({val})"
    elif hash_type.lower() == "sha1":
        return f"sha1({val})"
    elif hash_type.lower() == "sha256":
        return f"sha256({val})"
    elif hash_type.lower() == "sha512":
        return f"sha512({val})"
    else:
        raise ValueError(f"Unsupported hash type: {hash_type}")


FUNCTION_MAP = {
    # generic types
    FunctionType.ALIAS: lambda x: f"{x[0]}",
    FunctionType.GROUP: lambda x: f"{x[0]}",
    FunctionType.CONSTANT: lambda x: f"{x[0]}",
    FunctionType.TYPED_CONSTANT: lambda x: f"{x[0]}",
    FunctionType.COALESCE: lambda x: f"coalesce({','.join(x)})",
    FunctionType.NULLIF: lambda x: f"nullif({x[0]},{x[1]})",
    FunctionType.CAST: lambda x: f"cast({x[0]} as {x[1]})",
    FunctionType.CASE: lambda x: render_case(x),
    FunctionType.SPLIT: lambda x: f"split({x[0]}, {x[1]})",
    FunctionType.IS_NULL: lambda x: f"{x[0]} is null",
    FunctionType.BOOL: lambda x: f"CASE WHEN {x[0]} THEN TRUE ELSE FALSE END",
    FunctionType.PARENTHETICAL: lambda x: f"({x[0]})",
    # Complex
    FunctionType.INDEX_ACCESS: lambda x: f"{x[0]}[{x[1]}]",
    FunctionType.MAP_ACCESS: lambda x: f"{x[0]}[{x[1]}]",
    FunctionType.UNNEST: lambda x: f"unnest({x[0]})",
    FunctionType.DATE_SPINE: lambda x: f"""unnest(
        generate_series(
            {x[0]},
            {x[1]},
            INTERVAL '1 day'
        )
    )""",
    FunctionType.RECURSE_EDGE: lambda x: f"CASE WHEN {x[1]} IS NULL THEN {x[0]} ELSE {x[1]} END",
    FunctionType.ATTR_ACCESS: lambda x: f"""{x[0]}.{x[1].replace("'", "")}""",
    FunctionType.STRUCT: lambda x: f"{{{', '.join(struct_arg(x))}}}",
    FunctionType.ARRAY: lambda x: f"[{', '.join(x)}]",
    FunctionType.DATE_LITERAL: lambda x: f"date '{x}'",
    FunctionType.DATETIME_LITERAL: lambda x: f"datetime '{x}'",
    # MAP
    FunctionType.MAP_KEYS: lambda x: f"map_keys({x[0]})",
    FunctionType.MAP_VALUES: lambda x: f"map_values({x[0]})",
    # ARRAY
    FunctionType.ARRAY_SUM: lambda x: f"array_sum({x[0]})",
    FunctionType.ARRAY_DISTINCT: lambda x: f"array_distinct({x[0]})",
    FunctionType.ARRAY_SORT: lambda x: f"array_sort({x[0]})",
    FunctionType.ARRAY_TRANSFORM: lambda args: (
        f"array_transform({args[0]}, {args[1]} -> {args[2]})"
    ),
    FunctionType.ARRAY_TO_STRING: lambda args: (
        f"array_to_string({args[0]}, {args[1]})"
    ),
    FunctionType.ARRAY_FILTER: lambda args: (
        f"array_filter({args[0]}, {args[1]} -> {args[2]})"
    ),
    # math
    FunctionType.ADD: lambda x: " + ".join(x),
    FunctionType.ABS: lambda x: f"abs({x[0]})",
    FunctionType.SUBTRACT: lambda x: " - ".join(x),
    FunctionType.DIVIDE: lambda x: " / ".join(x),
    FunctionType.MULTIPLY: lambda x: " * ".join(x),
    FunctionType.ROUND: lambda x: f"round({x[0]},{x[1]})",
    FunctionType.FLOOR: lambda x: f"floor({x[0]})",
    FunctionType.CEIL: lambda x: f"ceil({x[0]})",
    FunctionType.MOD: lambda x: f"({x[0]} % {x[1]})",
    FunctionType.SQRT: lambda x: f"sqrt({x[0]})",
    FunctionType.RANDOM: lambda x: "random()",
    FunctionType.LOG: lambda x: (
        f"log({x[0]})" if x[1] == 10 else f"log({x[0]}, {x[1]})"
    ),
    # aggregate types
    FunctionType.COUNT_DISTINCT: lambda x: f"count(distinct {x[0]})",
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x: f"sum({x[0]})",
    FunctionType.ARRAY_AGG: lambda x: f"array_agg({x[0]})",
    FunctionType.LENGTH: lambda x: f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.MAX: lambda x: f"max({x[0]})",
    FunctionType.MIN: lambda x: f"min({x[0]})",
    FunctionType.ANY: lambda x: f"any_value({x[0]})",
    # string types
    FunctionType.LIKE: lambda x: f" {x[0]} like {x[1]} ",
    FunctionType.UPPER: lambda x: f"UPPER({x[0]}) ",
    FunctionType.LOWER: lambda x: f"LOWER({x[0]}) ",
    FunctionType.SUBSTRING: lambda x: f"SUBSTRING({x[0]},{x[1]},{x[2]})",
    FunctionType.STRPOS: lambda x: f"STRPOS({x[0]},{x[1]})",
    FunctionType.CONTAINS: lambda x: f"CONTAINS({x[0]},{x[1]})",
    FunctionType.REGEXP_CONTAINS: lambda x: f"REGEXP_CONTAINS({x[0]},{x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x: f"REGEXP_EXTRACT({x[0]},{x[1]})",
    FunctionType.REGEXP_REPLACE: lambda x: f"REGEXP_REPLACE({x[0]},{x[1]}, {x[2]})",
    FunctionType.TRIM: lambda x: f"TRIM({x[0]})",
    FunctionType.REPLACE: lambda x: f"REPLACE({x[0]},{x[1]},{x[2]})",
    FunctionType.HASH: lambda x: hash_from_args(x[0], x[1]),
    # FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
    # date types
    FunctionType.DATE_TRUNCATE: lambda x: f"date_trunc({x[0]},{x[1]})",
    FunctionType.DATE_PART: lambda x: f"date_part({x[0]},{x[1]})",
    FunctionType.DATE_ADD: lambda x: f"date_add({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE_SUB: lambda x: f"date_sub({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE_DIFF: lambda x: f"date_diff({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE: lambda x: f"date({x[0]})",
    FunctionType.DATETIME: lambda x: f"datetime({x[0]})",
    FunctionType.TIMESTAMP: lambda x: f"timestamp({x[0]})",
    FunctionType.SECOND: lambda x: f"second({x[0]})",
    FunctionType.MINUTE: lambda x: f"minute({x[0]})",
    FunctionType.HOUR: lambda x: f"hour({x[0]})",
    FunctionType.DAY: lambda x: f"day({x[0]})",
    FunctionType.DAY_NAME: lambda x: f"dayname({x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x: f"day_of_week({x[0]})",
    FunctionType.WEEK: lambda x: f"week({x[0]})",
    FunctionType.MONTH: lambda x: f"month({x[0]})",
    FunctionType.MONTH_NAME: lambda x: f"monthname({x[0]})",
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
    FunctionType.ANY: lambda args: f"{args[0]}",
}


GENERIC_SQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% if recursive%} RECURSIVE {% endif %}{% for cte in ctes %}
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


def safe_get_cte_value(
    coalesce,
    cte: CTE | UnionCTE,
    c: BuildConcept,
    quote_char: str,
    render_expr: Callable,
    use_map: dict[str, set[str]],
) -> Optional[str]:
    address = c.address
    raw = cte.source_map.get(address, None)

    if not raw:
        return None
    if isinstance(raw, str):
        rendered = cte.get_alias(c, raw)
        use_map[raw].add(c.address)
        return f"{quote_char}{raw}{quote_char}.{safe_quote(rendered, quote_char)}"
    if isinstance(raw, list) and len(raw) == 1:
        rendered = cte.get_alias(c, raw[0])
        if isinstance(rendered, FUNCTION_ITEMS):
            # if it's a function, we need to render it as a function
            return f"{render_expr(rendered, cte=cte, raise_invalid=True)}"
        use_map[raw[0]].add(c.address)
        return f"{quote_char}{raw[0]}{quote_char}.{safe_quote(rendered, quote_char)}"
    for x in raw:
        use_map[x].add(c.address)
    return coalesce(
        sorted(
            [
                f"{quote_char}{x}{quote_char}.{safe_quote(cte.get_alias(c, x), quote_char)}"
                for x in raw
            ]
        )
    )


class BaseDialect:
    WINDOW_FUNCTION_MAP = WINDOW_FUNCTION_MAP
    FUNCTION_MAP = FUNCTION_MAP
    FUNCTION_GRAIN_MATCH_MAP = FUNCTION_GRAIN_MATCH_MAP
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = GENERIC_SQL_TEMPLATE
    DATATYPE_MAP = DATATYPE_MAP
    COMPLEX_DATATYPE_MAP = COMPLEX_DATATYPE_MAP
    UNNEST_MODE = UnnestMode.CROSS_APPLY
    GROUP_MODE = GroupMode.AUTO
    EXPLAIN_KEYWORD = "EXPLAIN"
    NULL_WRAPPER = staticmethod(null_wrapper)

    def __init__(self, rendering: Rendering | None = None):
        self.rendering = rendering or CONFIG.rendering
        self.used_map: dict[str, set[str]] = defaultdict(set)

    def render_order_item(
        self,
        order_item: BuildOrderItem,
        cte: CTE | UnionCTE,
    ) -> str:
        # if final:
        #     if not alias:
        #         return f"{self.QUOTE_CHARACTER}{order_item.expr.safe_address}{self.QUOTE_CHARACTER} {order_item.order.value}"

        #     return f"{cte.name}.{self.QUOTE_CHARACTER}{order_item.expr.safe_address}{self.QUOTE_CHARACTER} {order_item.order.value}"

        return (
            f"{self.render_expr(order_item.expr, cte=cte, )} {order_item.order.value}"
        )

    def render_concept_sql(
        self,
        c: BuildConcept,
        cte: CTE | UnionCTE,
        alias: bool = True,
        raise_invalid: bool = False,
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
                        candidate,
                        cte,
                        raise_invalid=True,
                    )
                    if result:
                        break
                except ValueError:
                    continue
        if not result:
            result = self._render_concept_sql(
                c,
                cte,
                raise_invalid=raise_invalid,
            )
        if alias:
            return f"{result} as {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
        return result

    def _render_concept_sql(
        self,
        c: BuildConcept,
        cte: CTE | UnionCTE,
        raise_invalid: bool = False,
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
            if isinstance(c.lineage, WINDOW_ITEMS):
                rendered_order_components = [
                    f"{self.render_expr(x.expr, cte, raise_invalid=raise_invalid)} {x.order.value}"
                    for x in c.lineage.order_by
                ]
                rendered_over_components = [
                    self.render_concept_sql(
                        x, cte, alias=False, raise_invalid=raise_invalid
                    )
                    for x in c.lineage.over
                ]

                rval = self.WINDOW_FUNCTION_MAP[c.lineage.type](
                    concept=self.render_concept_sql(
                        c.lineage.content,
                        cte=cte,
                        alias=False,
                        raise_invalid=raise_invalid,
                    ),
                    window=",".join(rendered_over_components),
                    sort=",".join(rendered_order_components),
                    offset=c.lineage.index,
                )
            elif isinstance(c.lineage, FILTER_ITEMS):
                # for cases when we've optimized this
                if cte.condition == c.lineage.where.conditional:
                    rval = self.render_expr(
                        c.lineage.content, cte=cte, raise_invalid=raise_invalid
                    )
                else:
                    rval = f"CASE WHEN {self.render_expr(c.lineage.where.conditional, cte=cte)} THEN {self.render_expr(c.lineage.content, cte=cte, raise_invalid=raise_invalid)} ELSE NULL END"
            elif isinstance(c.lineage, BuildRowsetItem):
                rval = f"{self.render_concept_sql(c.lineage.content, cte=cte, alias=False, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, BuildMultiSelectLineage):
                if c.address in c.lineage.calculated_derivations:
                    assert c.lineage.derive is not None
                    for x in c.lineage.derive.items:
                        if x.address == c.address:
                            rval = self.render_expr(
                                x.expr,
                                cte=cte,
                                raise_invalid=raise_invalid,
                            )
                            break
                else:
                    rval = f"{self.render_concept_sql(c.lineage.find_source(c, cte), cte=cte, alias=False, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, BuildComparison):
                rval = f"{self.render_expr(c.lineage.left, cte=cte, raise_invalid=raise_invalid)} {c.lineage.operator.value} {self.render_expr(c.lineage.right, cte=cte, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, AGGREGATE_ITEMS):
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
                isinstance(c.lineage, FUNCTION_ITEMS)
                and c.lineage.operator == FunctionType.UNION
            ):
                local_matched = [
                    x
                    for x in c.lineage.arguments
                    if isinstance(x, BuildConcept) and x.address in cte.output_columns
                ]
                # if we're sorting by the output of the union
                if not local_matched:
                    rval = c.safe_address
                else:
                    rval = self.render_expr(local_matched[0], cte)
            elif (
                isinstance(c.lineage, FUNCTION_ITEMS)
                and c.lineage.operator == FunctionType.CONSTANT
                and self.rendering.parameters is True
                and c.datatype.data_type != DataType.MAP
            ):
                rval = f":{c.safe_address}"
            else:
                args = []
                for arg in c.lineage.arguments:
                    if (
                        isinstance(arg, BuildConcept)
                        and arg.lineage
                        and isinstance(arg.lineage, FUNCTION_ITEMS)
                        and arg.lineage.operator
                        in (
                            FunctionType.ADD,
                            FunctionType.SUBTRACT,
                            FunctionType.DIVIDE,
                            FunctionType.MULTIPLY,
                        )
                    ):
                        args.append(
                            self.render_expr(
                                BuildParenthetical(content=arg),
                                cte=cte,
                                raise_invalid=raise_invalid,
                            )
                        )
                    else:
                        args.append(
                            self.render_expr(arg, cte=cte, raise_invalid=raise_invalid)
                        )

                if cte.group_to_grain:
                    rval = f"{self.FUNCTION_MAP[c.lineage.operator](args)}"
                else:
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Rendering basic lookup from {cte.source_map.get(c.address,None)}"
            )

            raw_content = cte.get_alias(c)
            parent = cte.source_map.get(c.address, None)
            if parent:
                self.used_map[parent[0]].add(c.address)
            if isinstance(raw_content, RawColumnExpr):
                rval = raw_content.text
            elif isinstance(raw_content, FUNCTION_ITEMS):
                rval = self.render_expr(
                    raw_content, cte=cte, raise_invalid=raise_invalid
                )
            else:
                rval = safe_get_cte_value(
                    self.FUNCTION_MAP[FunctionType.COALESCE],
                    cte,
                    c,
                    self.QUOTE_CHARACTER,
                    self.render_expr,
                    self.used_map,
                )
                if not rval:
                    # unions won't have a specific source mapped; just use a generic column reference
                    # we shouldn't ever have an expression at this point, so will be safe
                    if isinstance(cte, UnionCTE):
                        rval = c.safe_address
                    else:
                        if raise_invalid:
                            raise ValueError(
                                f"Invalid reference string found in query: {rval}, this should never occur. Please report this issue."
                            )
                        rval = INVALID_REFERENCE_STRING(
                            f"Missing source reference to {c.address}"
                        )
        return rval

    def render_array_unnest(
        self,
        left,
        right,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
    ):
        return f"{self.render_expr(left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {operator.value} {self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"

    def render_expr(
        self,
        e: Union[
            BuildConcept,
            BuildFunction,
            BuildConditional,
            BuildAggregateWrapper,
            BuildComparison,
            BuildCaseWhen,
            BuildCaseElse,
            BuildSubselectComparison,
            BuildWindowItem,
            BuildFilterItem,
            BuildParenthetical,
            BuildParamaterizedConceptReference,
            BuildMultiSelectLineage,
            BuildRowsetItem,
            str,
            int,
            list,
            bool,
            float,
            date,
            datetime,
            DataType,
            TraitDataType,
            MagicConstants,
            MapWrapper[Any, Any],
            MapType,
            NumericType,
            StructType,
            ArrayType,
            ListWrapper[Any],
            TupleWrapper[Any],
            DatePart,
        ],
        cte: Optional[CTE | UnionCTE] = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
    ) -> str:
        if isinstance(e, SUBSELECT_COMPARISON_ITEMS):

            if isinstance(e.right, BuildConcept):
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
                assert cte, "CTE must be provided for inlined CTEs"
                self.used_map[target].add(e.right.address)
                if target in cte.inlined_ctes:
                    info = cte.inlined_ctes[target]
                    return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} (select {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} from {info.new_base} as {target} where {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} is not null)"
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} (select {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} from {target} where {target}.{self.QUOTE_CHARACTER}{e.right.safe_address}{self.QUOTE_CHARACTER} is not null)"
            elif isinstance(e.right, BuildParamaterizedConceptReference):
                if isinstance(e.right.concept.lineage, BuildFunction) and isinstance(
                    e.right.concept.lineage.arguments[0], ListWrapper
                ):
                    return self.render_array_unnest(
                        e.left,
                        e.right,
                        e.operator,
                        cte=cte,
                        cte_map=cte_map,
                        raise_invalid=raise_invalid,
                    )
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
            elif isinstance(
                e.right,
                (ListWrapper, TupleWrapper, BuildParenthetical),
            ):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"

            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} ({self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)})"
        elif isinstance(e, COMPARISON_ITEMS):
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
        elif isinstance(e, CONDITIONAL_ITEMS):
            # conditions need to be nested in parentheses
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
        elif isinstance(e, WINDOW_ITEMS):
            rendered_order_components = [
                f"{self.render_expr(x.expr, cte, cte_map=cte_map, raise_invalid=raise_invalid)} {x.order.value}"
                for x in e.order_by
            ]
            rendered_over_components = [
                self.render_expr(x, cte, cte_map=cte_map, raise_invalid=raise_invalid)
                for x in e.over
            ]
            return f"{self.WINDOW_FUNCTION_MAP[e.type](concept = self.render_expr(e.content, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid), window=','.join(rendered_over_components), sort=','.join(rendered_order_components))}"  # noqa: E501
        elif isinstance(e, PARENTHETICAL_ITEMS):
            # conditions need to be nested in parentheses
            if isinstance(e.content, list):
                return f"( {','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e.content])} )"
            return f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} )"
        elif isinstance(e, CASE_WHEN_ITEMS):
            return f"WHEN {self.render_expr(e.comparison, cte=cte, cte_map=cte_map) } THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) }"
        elif isinstance(e, CASE_ELSE_ITEMS):
            return f"ELSE {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) }"
        elif isinstance(e, FUNCTION_ITEMS):
            arguments = []
            for arg in e.arguments:
                if (
                    isinstance(arg, BuildConcept)
                    and arg.lineage
                    and isinstance(arg.lineage, FUNCTION_ITEMS)
                    and arg.lineage.operator
                    in (
                        FunctionType.ADD,
                        FunctionType.SUBTRACT,
                        FunctionType.DIVIDE,
                        FunctionType.MULTIPLY,
                    )
                ):
                    arguments.append(
                        self.render_expr(
                            BuildParenthetical(content=arg),
                            cte=cte,
                            cte_map=cte_map,
                            raise_invalid=raise_invalid,
                        )
                    )
                else:
                    arguments.append(
                        self.render_expr(
                            arg, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                        )
                    )

            if cte and cte.group_to_grain:
                return self.FUNCTION_MAP[e.operator](arguments)

            return self.FUNCTION_GRAIN_MATCH_MAP[e.operator](arguments)
        elif isinstance(e, AGGREGATE_ITEMS):
            return self.render_expr(
                e.function, cte, cte_map=cte_map, raise_invalid=raise_invalid
            )
        elif isinstance(e, FILTER_ITEMS):
            return f"CASE WHEN {self.render_expr(e.where.conditional,cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} THEN {self.render_expr(e.content, cte, cte_map=cte_map, raise_invalid=raise_invalid)} ELSE NULL END"
        elif isinstance(e, BuildConcept):
            if (
                isinstance(e.lineage, FUNCTION_ITEMS)
                and e.lineage.operator == FunctionType.CONSTANT
                and self.rendering.parameters is True
                and e.datatype.data_type != DataType.MAP
            ):
                return f":{e.safe_address}"
            if cte:
                return self.render_concept_sql(
                    e,
                    cte,
                    alias=False,
                    raise_invalid=raise_invalid,
                )
            elif cte_map:
                self.used_map[cte_map[e.address].name].add(e.address)
                return f"{cte_map[e.address].name}.{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
            return f"{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
        elif isinstance(e, bool):
            return f"{e}"
        elif isinstance(e, str):
            return f"'{e}'"
        elif isinstance(e, (int, float)):
            return str(e)
        elif isinstance(e, TupleWrapper):
            return f"({','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])})"
        elif isinstance(e, MapWrapper):
            return f"MAP {{{','.join([f'{self.render_expr(k, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}:{self.render_expr(v, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}' for k, v in e.items()])}}}"
        elif isinstance(e, ListWrapper):
            return f"{self.FUNCTION_MAP[FunctionType.ARRAY]([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])}"
        elif isinstance(e, DataType):
            return self.DATATYPE_MAP.get(e, e.value)
        elif isinstance(e, DatePart):
            return str(e.value)
        elif isinstance(e, NumericType):
            return f"{self.DATATYPE_MAP[DataType.NUMERIC]}({e.precision},{e.scale})"
        elif isinstance(e, MagicConstants):
            if e == MagicConstants.NULL:
                return "null"
            return str(e.value)
        elif isinstance(e, date):
            return self.FUNCTION_MAP[FunctionType.DATE_LITERAL](e)
        elif isinstance(e, datetime):
            return self.FUNCTION_MAP[FunctionType.DATETIME_LITERAL](e)
        elif isinstance(e, TraitDataType):
            return self.render_expr(e.type, cte=cte, cte_map=cte_map)
        elif isinstance(e, ArgBinding):
            return e.name
        elif isinstance(e, Ordering):
            return str(e.value)
        elif isinstance(e, ArrayType):
            return f"{self.COMPLEX_DATATYPE_MAP[DataType.ARRAY](self.render_expr(e.value_data_type, cte=cte, cte_map=cte_map))}"
        elif isinstance(e, list):
            return f"{self.FUNCTION_MAP[FunctionType.ARRAY]([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e])}"
        elif isinstance(e, BuildParamaterizedConceptReference):
            if self.rendering.parameters:
                if e.concept.namespace == DEFAULT_NAMESPACE:
                    return f":{e.concept.name}"
                return f":{e.concept.address.replace('.', '_')}"
            elif e.concept.lineage:
                return self.render_expr(e.concept.lineage, cte=cte, cte_map=cte_map)
            return f"{self.QUOTE_CHARACTER}{e.concept.address}{self.QUOTE_CHARACTER}"

        else:
            raise ValueError(f"Unable to render type {type(e)} {e}")

    def render_cte_group_by(
        self, cte: CTE | UnionCTE, select_columns
    ) -> Optional[list[str]]:

        if not cte.group_to_grain:
            return None
        base = set(
            [self.render_concept_sql(c, cte, alias=False) for c in cte.group_concepts]
        )
        if self.GROUP_MODE == GroupMode.AUTO:
            return sorted(list(base))

        else:
            # find the index of each column in the select columns
            final = []
            found = []
            for idx, c in enumerate(select_columns):
                pre_alias = c.split(" as ")[0]
                if pre_alias in base:
                    final.append(str(idx + 1))
                    found.append(pre_alias)
            if not all(c in found for c in base):
                raise ValueError(
                    f"Group by columns {base} not found in select columns {select_columns}"
                )
            return final

    def render_cte(self, cte: CTE | UnionCTE, auto_sort: bool = True) -> CompiledCTE:
        if isinstance(cte, UnionCTE):
            base_statement = f"\n{cte.operator}\n".join(
                [
                    self.render_cte(child, auto_sort=False).statement
                    for child in cte.internal_ctes
                ]
            )
            if cte.order_by:

                ordering = [self.render_order_item(i, cte) for i in cte.order_by.items]
                base_statement += "\nORDER BY " + ",".join(ordering)
            return CompiledCTE(name=cte.name, statement=base_statement)
        elif isinstance(cte, RecursiveCTE):
            base_statement = "\nUNION ALL\n".join(
                [self.render_cte(child, False).statement for child in cte.internal_ctes]
            )
            return CompiledCTE(name=cte.name, statement=base_statement)
        if self.UNNEST_MODE in (
            UnnestMode.CROSS_APPLY,
            UnnestMode.CROSS_JOIN,
            UnnestMode.CROSS_JOIN_ALIAS,
            UnnestMode.SNOWFLAKE,
        ):
            # for a cross apply, derivation happens in the join
            # so we only use the alias to select
            select_columns = [
                self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in [y.address for y in cte.join_derived_concepts]
                and c.address not in cte.hidden_concepts
            ] + [
                f"{self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
                for c in cte.join_derived_concepts
                if c.address not in cte.hidden_concepts
            ]
        elif self.UNNEST_MODE in (UnnestMode.CROSS_JOIN_UNNEST, UnnestMode.PRESTO):
            select_columns = [
                self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in [y.address for y in cte.join_derived_concepts]
                and c.address not in cte.hidden_concepts
            ] + [
                f"{UNNEST_NAME} as {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
                for c in cte.join_derived_concepts
                if c.address not in cte.hidden_concepts
            ]
        else:
            # otherwse, assume we are unnesting directly in the select
            select_columns = [
                self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in cte.hidden_concepts
            ]
        if auto_sort:
            select_columns = sorted(select_columns, key=lambda x: x)
        source: str | None = cte.base_name
        if not cte.render_from_clause:
            if len(cte.joins) > 0:
                if cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.CROSS_JOIN_ALIAS,
                    # UnnestMode.CROSS_JOIN_UNNEST,
                    UnnestMode.CROSS_JOIN,
                    UnnestMode.CROSS_APPLY,
                ):

                    source = f"{render_unnest(self.UNNEST_MODE, self.QUOTE_CHARACTER, cte.join_derived_concepts[0], self.render_expr, cte)}"
                elif cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.CROSS_JOIN_UNNEST,
                ):
                    source = f"{self.render_expr(cte.join_derived_concepts[0], cte)} as {self.QUOTE_CHARACTER}{UNNEST_NAME}{self.QUOTE_CHARACTER}"
                elif cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.PRESTO,
                ):
                    source = f"{self.render_expr(cte.join_derived_concepts[0], cte)} as t({self.QUOTE_CHARACTER}{UNNEST_NAME}{self.QUOTE_CHARACTER})"
                elif (
                    cte.join_derived_concepts
                    and self.UNNEST_MODE == UnnestMode.SNOWFLAKE
                ):
                    source = f"{render_unnest(self.UNNEST_MODE, self.QUOTE_CHARACTER, cte.join_derived_concepts[0], self.render_expr, cte)}"
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
                source = safe_quote(cte.base_name, self.QUOTE_CHARACTER)
            else:
                source = cte.base_name
            if cte.base_name != cte.base_alias:
                source = f"{source} as {self.QUOTE_CHARACTER}{cte.base_alias}{self.QUOTE_CHARACTER}"
        if not cte.render_from_clause:
            final_joins = []
        else:
            final_joins = cte.joins or []
        where: BuildConditional | BuildParenthetical | BuildComparison | None = None
        having: BuildConditional | BuildParenthetical | BuildComparison | None = None
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

        logger.info(f"{LOGGER_PREFIX} {len(final_joins)} joins for cte {cte.name}")
        return CompiledCTE(
            name=cte.name,
            statement=self.SQL_TEMPLATE.render(
                select_columns=select_columns,
                base=f"{source}" if source else None,
                grain=cte.grain,
                limit=cte.limit,
                comment=cte.comment if CONFIG.show_comments else None,
                # some joins may not need to be rendered
                joins=[
                    j
                    for j in [
                        render_join(
                            join,
                            self.QUOTE_CHARACTER,
                            self.render_expr,
                            cte,
                            use_map=self.used_map,
                            unnest_mode=self.UNNEST_MODE,
                            null_wrapper=self.NULL_WRAPPER,
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
                group_by=self.render_cte_group_by(cte, select_columns),
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

    def create_show_output(
        self,
        environment: Environment,
        content: ShowCategory,
    ):
        if content == ShowCategory.CONCEPTS:
            output_columns = [
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_address"].address
                ].reference,
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_datatype"].address
                ].reference,
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_description"].address
                ].reference,
            ]
            output_values = [
                {
                    DEFAULT_CONCEPTS["concept_address"].address: (
                        concept.name
                        if concept.namespace == DEFAULT_NAMESPACE
                        else concept.address
                    ),
                    DEFAULT_CONCEPTS["concept_datatype"].address: str(concept.datatype),
                    DEFAULT_CONCEPTS[
                        "concept_description"
                    ].address: concept.metadata.description
                    or "",
                }
                for _, concept in environment.concepts.items()
                if not concept.is_internal
            ]
        else:
            raise NotImplementedError(f"Show category {content} not implemented")
        return ProcessedShowStatement(
            output_columns=output_columns,
            output_values=[ProcessedStaticValueOutput(values=output_values)],
        )

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
            | ValidateStatement
        ],
        hooks: Optional[List[BaseHook]] = None,
    ) -> List[PROCESSED_STATEMENT_TYPES]:
        output: List[PROCESSED_STATEMENT_TYPES] = []
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
                                ].reference
                            ],
                            output_values=[
                                process_query(
                                    environment, statement.content, hooks=hooks
                                )
                            ],
                        )
                    )
                elif isinstance(statement.content, ShowCategory):
                    output.append(
                        self.create_show_output(environment, statement.content)
                    )
                elif isinstance(statement.content, ValidateStatement):
                    output.append(
                        ProcessedShowStatement(
                            output_columns=[
                                environment.concepts[
                                    DEFAULT_CONCEPTS["label"].address
                                ].reference,
                                environment.concepts[
                                    DEFAULT_CONCEPTS["query_text"].address
                                ].reference,
                                environment.concepts[
                                    DEFAULT_CONCEPTS["expected"].address
                                ].reference,
                            ],
                            output_values=[
                                ProcessedValidateStatement(
                                    scope=statement.content.scope,
                                    targets=statement.content.targets,
                                )
                            ],
                        )
                    )
                else:
                    raise NotImplementedError(type(statement.content))
            elif isinstance(statement, RawSQLStatement):
                output.append(ProcessedRawSQLStatement(text=statement.text))
            elif isinstance(statement, ValidateStatement):
                output.append(
                    ProcessedValidateStatement(
                        scope=statement.scope,
                        targets=statement.targets,
                    )
                )
            elif isinstance(
                statement,
                (
                    ConceptDeclarationStatement,
                    MergeStatementV2,
                    ImportStatement,
                    RowsetDerivationStatement,
                    Datasource,
                    FunctionDeclaration,
                ),
            ):
                continue
            else:
                raise NotImplementedError(type(statement))
        return output

    def compile_statement(
        self,
        query: PROCESSED_STATEMENT_TYPES,
    ) -> str:
        if isinstance(query, ProcessedShowStatement):
            return ";\n".join(
                [
                    f"{self.EXPLAIN_KEYWORD} {self.compile_statement(x)}"
                    for x in query.output_values
                    if isinstance(x, (ProcessedQuery, ProcessedCopyStatement))
                ]
            )
        elif isinstance(query, ProcessedRawSQLStatement):
            return query.text

        elif isinstance(query, ProcessedValidateStatement):
            return "select 1;"

        recursive = any(isinstance(x, RecursiveCTE) for x in query.ctes)

        compiled_ctes = self.generate_ctes(query)

        final = self.SQL_TEMPLATE.render(
            recursive=recursive,
            output=(
                query.output_to if isinstance(query, ProcessedQueryPersist) else None
            ),
            full_select=compiled_ctes[-1].statement,
            ctes=compiled_ctes[:-1],
        )

        if CONFIG.strict_mode and INVALID_REFERENCE_STRING(1) in final:
            raise ValueError(
                f"Invalid reference string found in query: {final}, this should never"
                " occur. Please create an issue to report this."
            )
        logger.info(f"{LOGGER_PREFIX} Compiled query: {final}")
        return final
