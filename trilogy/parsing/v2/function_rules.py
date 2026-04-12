from __future__ import annotations

from datetime import date, datetime
from typing import Any

from trilogy.constants import MagicConstants
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    Ordering,
    WindowType,
)
from trilogy.core.internal import INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    FilterItem,
    Function,
    FunctionCallWrapper,
    SubselectItem,
    TraitDataType,
    WhereClause,
    WindowItem,
    WindowItemOrder,
    WindowItemOver,
)
from trilogy.core.models.core import DataType, MapType, arg_to_datatype
from trilogy.parsing.common import arbitrary_to_concept
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind

SIMPLE_FUNCTION_DISPATCH: dict[SyntaxNodeKind, FunctionType] = {
    # string
    SyntaxNodeKind.UPPER: FunctionType.UPPER,
    SyntaxNodeKind.FLOWER: FunctionType.LOWER,
    SyntaxNodeKind.FSPLIT: FunctionType.SPLIT,
    SyntaxNodeKind.FSTRPOS: FunctionType.STRPOS,
    SyntaxNodeKind.FSUBSTRING: FunctionType.SUBSTRING,
    SyntaxNodeKind.FCONTAINS: FunctionType.CONTAINS,
    SyntaxNodeKind.FTRIM: FunctionType.TRIM,
    SyntaxNodeKind.FLTRIM: FunctionType.LTRIM,
    SyntaxNodeKind.FRTRIM: FunctionType.RTRIM,
    SyntaxNodeKind.FREPLACE: FunctionType.REPLACE,
    SyntaxNodeKind.FREGEXP_EXTRACT: FunctionType.REGEXP_EXTRACT,
    SyntaxNodeKind.FREGEXP_CONTAINS: FunctionType.REGEXP_CONTAINS,
    SyntaxNodeKind.FREGEXP_REPLACE: FunctionType.REGEXP_REPLACE,
    SyntaxNodeKind.FHASH: FunctionType.HASH,
    SyntaxNodeKind.FHEX: FunctionType.HEX,
    SyntaxNodeKind.LIKE: FunctionType.LIKE,
    SyntaxNodeKind.ILIKE: FunctionType.ILIKE,
    SyntaxNodeKind.LEN: FunctionType.LENGTH,
    SyntaxNodeKind.CONCAT: FunctionType.CONCAT,
    SyntaxNodeKind.FCOALESCE: FunctionType.COALESCE,
    SyntaxNodeKind.FGREATEST: FunctionType.GREATEST,
    SyntaxNodeKind.FLEAST: FunctionType.LEAST,
    SyntaxNodeKind.FNULLIF: FunctionType.NULLIF,
    SyntaxNodeKind.FRECURSE_EDGE: FunctionType.RECURSE_EDGE,
    SyntaxNodeKind.UNION: FunctionType.UNION,
    # math
    SyntaxNodeKind.FADD: FunctionType.ADD,
    SyntaxNodeKind.FSUB: FunctionType.SUBTRACT,
    SyntaxNodeKind.FMUL: FunctionType.MULTIPLY,
    SyntaxNodeKind.FDIV: FunctionType.DIVIDE,
    SyntaxNodeKind.FMOD: FunctionType.MOD,
    SyntaxNodeKind.FFLOOR: FunctionType.FLOOR,
    SyntaxNodeKind.FCEIL: FunctionType.CEIL,
    SyntaxNodeKind.FABS: FunctionType.ABS,
    SyntaxNodeKind.FSQRT: FunctionType.SQRT,
    SyntaxNodeKind.FRANDOM: FunctionType.RANDOM,
    # date
    SyntaxNodeKind.FDATE: FunctionType.DATE,
    SyntaxNodeKind.FDATETIME: FunctionType.DATETIME,
    SyntaxNodeKind.FTIMESTAMP: FunctionType.TIMESTAMP,
    SyntaxNodeKind.FSECOND: FunctionType.SECOND,
    SyntaxNodeKind.FMINUTE: FunctionType.MINUTE,
    SyntaxNodeKind.FHOUR: FunctionType.HOUR,
    SyntaxNodeKind.FDAY: FunctionType.DAY,
    SyntaxNodeKind.FDAY_NAME: FunctionType.DAY_NAME,
    SyntaxNodeKind.FDAY_OF_WEEK: FunctionType.DAY_OF_WEEK,
    SyntaxNodeKind.FWEEK: FunctionType.WEEK,
    SyntaxNodeKind.FMONTH: FunctionType.MONTH,
    SyntaxNodeKind.FMONTH_NAME: FunctionType.MONTH_NAME,
    SyntaxNodeKind.FFORMAT_TIME: FunctionType.FORMAT_TIME,
    SyntaxNodeKind.FPARSE_TIME: FunctionType.PARSE_TIME,
    SyntaxNodeKind.FQUARTER: FunctionType.QUARTER,
    SyntaxNodeKind.FYEAR: FunctionType.YEAR,
    SyntaxNodeKind.FDATE_TRUNC: FunctionType.DATE_TRUNCATE,
    SyntaxNodeKind.FDATE_PART: FunctionType.DATE_PART,
    SyntaxNodeKind.FDATE_ADD: FunctionType.DATE_ADD,
    SyntaxNodeKind.FDATE_SUB: FunctionType.DATE_SUB,
    SyntaxNodeKind.FDATE_DIFF: FunctionType.DATE_DIFF,
    SyntaxNodeKind.FDATE_SPINE: FunctionType.DATE_SPINE,
    # array
    SyntaxNodeKind.FARRAY_SUM: FunctionType.ARRAY_SUM,
    SyntaxNodeKind.FARRAY_DISTINCT: FunctionType.ARRAY_DISTINCT,
    SyntaxNodeKind.FARRAY_TO_STRING: FunctionType.ARRAY_TO_STRING,
    SyntaxNodeKind.FGENERATE_ARRAY: FunctionType.GENERATE_ARRAY,
    # map
    SyntaxNodeKind.FMAP_KEYS: FunctionType.MAP_KEYS,
    SyntaxNodeKind.FMAP_VALUES: FunctionType.MAP_VALUES,
    # geo
    SyntaxNodeKind.FGEO_FROM_TEXT: FunctionType.GEO_FROM_TEXT,
    SyntaxNodeKind.FGEO_POINT: FunctionType.GEO_POINT,
    SyntaxNodeKind.FGEO_DISTANCE: FunctionType.GEO_DISTANCE,
    SyntaxNodeKind.FGEO_X: FunctionType.GEO_X,
    SyntaxNodeKind.FGEO_Y: FunctionType.GEO_Y,
    SyntaxNodeKind.FGEO_CENTROID: FunctionType.GEO_CENTROID,
    SyntaxNodeKind.FGEO_TRANSFORM: FunctionType.GEO_TRANSFORM,
    # misc
    SyntaxNodeKind.UNNEST: FunctionType.UNNEST,
    SyntaxNodeKind.FBOOL: FunctionType.BOOL,
}

AGGREGATE_DISPATCH: dict[SyntaxNodeKind, FunctionType] = {
    SyntaxNodeKind.COUNT: FunctionType.COUNT,
    SyntaxNodeKind.COUNT_DISTINCT: FunctionType.COUNT_DISTINCT,
    SyntaxNodeKind.SUM: FunctionType.SUM,
    SyntaxNodeKind.AVG: FunctionType.AVG,
    SyntaxNodeKind.MAX: FunctionType.MAX,
    SyntaxNodeKind.MIN: FunctionType.MIN,
    SyntaxNodeKind.ARRAY_AGG: FunctionType.ARRAY_AGG,
    SyntaxNodeKind.BOOL_AND: FunctionType.BOOL_AND,
    SyntaxNodeKind.BOOL_OR: FunctionType.BOOL_OR,
    SyntaxNodeKind.ANY: FunctionType.ANY,
}

WINDOW_TO_AGGREGATE_MAP: dict[WindowType, FunctionType] = {
    WindowType.SUM: FunctionType.SUM,
    WindowType.AVG: FunctionType.AVG,
    WindowType.COUNT: FunctionType.COUNT,
    WindowType.COUNT_DISTINCT: FunctionType.COUNT_DISTINCT,
    WindowType.MAX: FunctionType.MAX,
    WindowType.MIN: FunctionType.MIN,
}


def _expr_to_boolean(root: Any, context: RuleContext) -> Any:
    if not isinstance(root, Comparison):
        from trilogy.core.models.author import Conditional

        if isinstance(root, Conditional):
            return root
        if arg_to_datatype(root) == DataType.BOOL:
            return Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        return Comparison(
            left=root,
            right=MagicConstants.NULL,
            operator=ComparisonOperator.IS_NOT,
        )
    return root


# --- Table-driven simple function handler ---


def simple_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    ft = SIMPLE_FUNCTION_DISPATCH[node.kind]  # type: ignore
    return context.function_factory.create_function(args, ft)


# --- Aggregate handlers ---


def generic_aggregate(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    ft = AGGREGATE_DISPATCH[node.kind]  # type: ignore
    return context.function_factory.create_function(args, ft)


def aggregate_functions(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> AggregateWrapper:
    args = hydrated_children(node, hydrate)
    if len(args) == 2:
        return AggregateWrapper(function=args[0], by=args[1])
    return AggregateWrapper(function=args[0])


def aggregate_over(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return args[0]


def aggregate_all(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[ConceptRef]:
    return [
        ConceptRef(
            address=f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
            datatype=DataType.INTEGER,
        )
    ]


def aggregate_by(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    base = args[0]
    b_concept = str(base).split(" ")[-1]
    fargs = [context.environment.concepts[a] for a in [b_concept] + args[1:]]
    return context.function_factory.create_function(fargs, FunctionType.GROUP)


def fgroup(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    if len(args) == 2:
        fargs = [args[0]] + list(args[1])
    else:
        fargs = [args[0]]
    return context.function_factory.create_function(fargs, FunctionType.GROUP)


def over_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[Any]:
    return hydrated_children(node, hydrate)


def over_component(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptRef:
    args = hydrated_children(node, hydrate)
    addr = str(args[0]).lstrip(",").strip()
    return ConceptRef(address=addr)


# --- Special function handlers ---


def fround(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    if len(args) == 1:
        args.append(0)
    return context.function_factory.create_function(args, FunctionType.ROUND)


def flog(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    if len(args) == 1:
        args.append(10)
    return context.function_factory.create_function(args, FunctionType.LOG)


def farray_sort(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    if len(args) == 1:
        args = [args[0], Ordering.ASCENDING]
    return context.function_factory.create_function(args, FunctionType.ARRAY_SORT)


def fcast(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    value, dtype = args[0], args[1]
    if isinstance(value, str):
        processed: Any
        match dtype:
            case DataType.DATE:
                processed = date.fromisoformat(value)
            case DataType.DATETIME | DataType.TIMESTAMP:
                processed = datetime.fromisoformat(value)
            case DataType.INTEGER:
                processed = int(value)
            case DataType.FLOAT:
                processed = float(value)
            case DataType.BOOL:
                processed = value.capitalize() == "True"
            case DataType.STRING:
                processed = value
            case _:
                raise fail(node, f"Invalid cast type {dtype}")
        if isinstance(dtype, TraitDataType):
            return context.function_factory.create_function(
                [processed, dtype], FunctionType.TYPED_CONSTANT
            )
        return context.function_factory.create_function(
            [processed], FunctionType.CONSTANT
        )
    return context.function_factory.create_function(args, FunctionType.CAST)


def fcurrent_date(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.functions import CurrentDate

    return CurrentDate([])


def fcurrent_datetime(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    return context.function_factory.create_function(
        args=[], operator=FunctionType.CURRENT_DATETIME
    )


def fcurrent_timestamp(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    return context.function_factory.create_function(
        args=[], operator=FunctionType.CURRENT_TIMESTAMP
    )


def fnot(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    if arg_to_datatype(args[0]) == DataType.BOOL:
        return Comparison(
            left=context.function_factory.create_function(
                [args[0], False], FunctionType.COALESCE
            ),
            operator=ComparisonOperator.EQ,
            right=False,
        )
    return context.function_factory.create_function(args, FunctionType.IS_NULL)


# --- Case handlers ---


def fcase(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    return context.function_factory.create_function(args, FunctionType.CASE)


def fcase_when(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CaseWhen:
    args = hydrated_children(node, hydrate)
    root = _expr_to_boolean(args[0], context)
    return CaseWhen(comparison=root, expr=args[1])


def fcase_else(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CaseElse:
    args = hydrated_children(node, hydrate)
    return CaseElse(expr=args[0])


def fcase_simple_when(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CaseSimpleWhen:
    args = hydrated_children(node, hydrate)
    return CaseSimpleWhen(value_expr=args[0], expr=args[1])


def fcase_simple(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    switch_expr = args[0]
    case_args: list[CaseSimpleWhen | CaseElse] = [
        a for a in args[1:] if isinstance(a, (CaseSimpleWhen, CaseElse))
    ]
    return context.function_factory.create_function(
        [switch_expr] + case_args, FunctionType.SIMPLE_CASE  # type: ignore
    )


# --- Filter handler ---


def filter_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FilterItem:
    args = hydrated_children(node, hydrate)
    expr = args[0]
    raw = args[1]
    if isinstance(raw, WhereClause):
        where = raw
    else:
        where = WhereClause(conditional=_expr_to_boolean(raw, context))
    if isinstance(expr, str):
        expr = context.environment.concepts[expr].reference
    return FilterItem(content=expr, where=where)


# --- Subselect handlers ---


def subselect(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SubselectItem:
    from trilogy.core.models.author import OrderItem

    args = hydrated_children(node, hydrate)
    content = args[0]
    if isinstance(content, Concept):
        content = content.reference
    where = None
    order_by: list[OrderItem] = []
    limit = None
    for arg in args[1:]:
        if isinstance(arg, WhereClause):
            where = arg
        elif isinstance(arg, list):
            order_by = arg
        elif isinstance(arg, int):
            limit = arg
    return SubselectItem(content=content, where=where, order_by=order_by, limit=limit)


def subselect_where(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WhereClause:
    args = hydrated_children(node, hydrate)
    return WhereClause(conditional=_expr_to_boolean(args[0], context))


def subselect_order(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return args[0]


def subselect_limit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> int:
    args = hydrated_children(node, hydrate)
    return int(str(args[0]))


# --- Window handlers ---


def window_item_over(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItemOver:
    args = hydrated_children(node, hydrate)
    return WindowItemOver(contents=args[0])


def window_item_order(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItemOrder:
    args = hydrated_children(node, hydrate)
    return WindowItemOrder(contents=args[0])


def window_sql_partition(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItemOver:
    args = hydrated_children(node, hydrate)
    return WindowItemOver(contents=args[0])


def window_sql_order(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItemOrder:
    args = hydrated_children(node, hydrate)
    return WindowItemOrder(contents=args[0])


def window_sql_over(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, list[Any]]:
    args = hydrated_children(node, hydrate)
    over: list[Any] = []
    order: list[Any] = []
    for item in args:
        if isinstance(item, WindowItemOver):
            over = item.contents
        elif isinstance(item, WindowItemOrder):
            order = item.contents
    return {"over": over, "order": order}


def window_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItem | AggregateWrapper:
    args = hydrated_children(node, hydrate)
    item: WindowItem = args[0]
    if not item.order_by and item.type in WINDOW_TO_AGGREGATE_MAP:
        func = context.function_factory.create_function(
            [item.content], WINDOW_TO_AGGREGATE_MAP[item.type]
        )
        return AggregateWrapper(function=func, by=list(item.over))
    return item


def _parse_window_args(
    args: list[Any], context: RuleContext
) -> tuple[WindowType, Concept | None, int | None, list[Any], list[Any]]:
    wtype = WindowType.ROW_NUMBER
    concept: Concept | None = None
    index: int | None = None
    order_by: list[Any] = []
    over: list[Any] = []
    for item in args:
        if isinstance(item, int):
            index = item
        elif isinstance(item, WindowItemOrder):
            order_by = item.contents
        elif isinstance(item, WindowItemOver):
            over = item.contents
        elif isinstance(item, dict):
            over = item.get("over", [])
            order_by = item.get("order", [])
        elif isinstance(item, str):
            concept = context.environment.concepts[item]
        elif isinstance(item, ConceptRef):
            concept = context.environment.concepts[item.address]
        elif isinstance(item, WindowType):
            wtype = item
        else:
            concept = arbitrary_to_concept(item, environment=context.environment)
            context.add_concept(concept, meta=core_meta(None))
    return wtype, concept, index, order_by, over


def window_item_legacy(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItem:
    args = hydrated_children(node, hydrate)
    wtype, concept, index, order_by, over = _parse_window_args(args, context)
    if not concept:
        raise fail(node, "Window statements must be on fields, not constants")
    return WindowItem(
        type=wtype, content=concept.reference, over=over, order_by=order_by, index=index
    )


def window_item_sql(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WindowItem:
    args = hydrated_children(node, hydrate)
    wtype, concept, index, order_by, over = _parse_window_args(args, context)
    if not concept:
        raise fail(node, "Window function requires a field argument")
    return WindowItem(
        type=wtype, content=concept.reference, over=over, order_by=order_by, index=index
    )


# --- Access handlers ---


def index_access(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    base = args[0]
    if hasattr(base, "datatype") and (
        base.datatype == DataType.MAP or isinstance(base.datatype, MapType)
    ):
        return context.function_factory.create_function(args, FunctionType.MAP_ACCESS)
    return context.function_factory.create_function(args, FunctionType.INDEX_ACCESS)


def map_key_access(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    return context.function_factory.create_function(args, FunctionType.MAP_ACCESS)


def attr_access(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    return context.function_factory.create_function(args, FunctionType.ATTR_ACCESS)


def chained_access(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    base = args[0]
    for accessor in args[1:]:
        if isinstance(accessor, int):
            base = context.function_factory.create_function(
                [base, accessor], FunctionType.INDEX_ACCESS
            )
        elif hasattr(base, "datatype") and (
            base.datatype == DataType.MAP or isinstance(base.datatype, MapType)
        ):
            base = context.function_factory.create_function(
                [base, accessor], FunctionType.MAP_ACCESS
            )
        else:
            base = context.function_factory.create_function(
                [base, accessor], FunctionType.ATTR_ACCESS
            )
    return base


# --- Transform lambda + custom function ---


def transform_lambda(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return context.environment.functions[str(args[0])]


def farray_transform(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    factory = args[1]
    if not len(factory.function_arguments) == 1:
        raise fail(node, "Array transform lambda must have exactly 1 argument")
    return context.function_factory.create_function(args, FunctionType.ARRAY_TRANSFORM)


def farray_filter(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Function:
    args = hydrated_children(node, hydrate)
    return context.function_factory.create_function(args, FunctionType.ARRAY_FILTER)


def custom_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionCallWrapper:
    args = hydrated_children(node, hydrate)
    name = str(args[0])
    fn_args = args[1:]
    if name not in context.environment.functions:
        raise fail(node, f"Unknown function @{name}")
    factory = context.environment.functions[name]
    return FunctionCallWrapper(content=factory(*fn_args), name=name, args=fn_args)


# --- Build hydrator dict ---

FUNCTION_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    # aggregates
    SyntaxNodeKind.AGGREGATE_FUNCTIONS: aggregate_functions,
    SyntaxNodeKind.AGGREGATE_OVER: aggregate_over,
    SyntaxNodeKind.AGGREGATE_ALL: aggregate_all,
    SyntaxNodeKind.AGGREGATE_BY: aggregate_by,
    SyntaxNodeKind.FGROUP: fgroup,
    SyntaxNodeKind.OVER_LIST: over_list,
    SyntaxNodeKind.OVER_COMPONENT: over_component,
    # special functions
    SyntaxNodeKind.FROUND: fround,
    SyntaxNodeKind.FLOG: flog,
    SyntaxNodeKind.FARRAY_SORT: farray_sort,
    SyntaxNodeKind.FCAST: fcast,
    SyntaxNodeKind.FCURRENT_DATE: fcurrent_date,
    SyntaxNodeKind.FCURRENT_DATETIME: fcurrent_datetime,
    SyntaxNodeKind.FCURRENT_TIMESTAMP: fcurrent_timestamp,
    SyntaxNodeKind.FNOT: fnot,
    # case
    SyntaxNodeKind.FCASE: fcase,
    SyntaxNodeKind.FCASE_WHEN: fcase_when,
    SyntaxNodeKind.FCASE_ELSE: fcase_else,
    SyntaxNodeKind.FCASE_SIMPLE: fcase_simple,
    SyntaxNodeKind.FCASE_SIMPLE_WHEN: fcase_simple_when,
    # filter
    SyntaxNodeKind.FILTER_ITEM: filter_item,
    # subselect
    SyntaxNodeKind.SUBSELECT: subselect,
    SyntaxNodeKind.SUBSELECT_WHERE: subselect_where,
    SyntaxNodeKind.SUBSELECT_ORDER: subselect_order,
    SyntaxNodeKind.SUBSELECT_LIMIT: subselect_limit,
    # window
    SyntaxNodeKind.WINDOW_ITEM: window_item,
    SyntaxNodeKind.WINDOW_ITEM_LEGACY: window_item_legacy,
    SyntaxNodeKind.WINDOW_ITEM_SQL: window_item_sql,
    SyntaxNodeKind.WINDOW_ITEM_OVER: window_item_over,
    SyntaxNodeKind.WINDOW_ITEM_ORDER: window_item_order,
    SyntaxNodeKind.WINDOW_SQL_OVER: window_sql_over,
    SyntaxNodeKind.WINDOW_SQL_PARTITION: window_sql_partition,
    SyntaxNodeKind.WINDOW_SQL_ORDER: window_sql_order,
    # access
    SyntaxNodeKind.INDEX_ACCESS: index_access,
    SyntaxNodeKind.MAP_KEY_ACCESS: map_key_access,
    SyntaxNodeKind.ATTR_ACCESS: attr_access,
    SyntaxNodeKind.CHAINED_ACCESS: chained_access,
    # transform lambda
    SyntaxNodeKind.TRANSFORM_LAMBDA: transform_lambda,
    SyntaxNodeKind.FARRAY_TRANSFORM: farray_transform,
    SyntaxNodeKind.FARRAY_FILTER: farray_filter,
    # custom function
    SyntaxNodeKind.CUSTOM_FUNCTION: custom_function,
}

# Add all table-driven simple functions
for _kind in SIMPLE_FUNCTION_DISPATCH:
    FUNCTION_NODE_HYDRATORS[_kind] = simple_function

# Add all table-driven aggregates
for _kind in AGGREGATE_DISPATCH:
    FUNCTION_NODE_HYDRATORS[_kind] = generic_aggregate
