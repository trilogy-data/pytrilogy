import difflib
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from logging import getLogger
from os.path import dirname, join
from pathlib import Path
from re import IGNORECASE
from typing import Any, Callable, List, Optional, Tuple, Union, cast

from lark import Lark, ParseTree, Token, Transformer, Tree, v_args
from lark.exceptions import (
    UnexpectedCharacters,
    UnexpectedEOF,
    UnexpectedInput,
    UnexpectedToken,
    VisitError,
)
from lark.tree import Meta
from pydantic import ValidationError

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    NULL_VALUE,
    MagicConstants,
    Parsing,
)
from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ChartType,
    ComparisonOperator,
    ConceptSource,
    CreateMode,
    DatasourceState,
    DatePart,
    Derivation,
    FunctionType,
    Granularity,
    IOType,
    Modifier,
    Ordering,
    PersistMode,
    PublishAction,
    Purpose,
    ShowCategory,
    ValidationScope,
    WindowOrder,
    WindowType,
)
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    MissingParameterException,
    UndefinedConceptException,
)
from trilogy.core.functions import (
    CurrentDate,
    FunctionFactory,
)
from trilogy.core.internal import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    ArgBinding,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comment,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    CustomFunctionFactory,
    CustomType,
    DeriveClause,
    DeriveItem,
    Expr,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    OrderBy,
    OrderItem,
    Parenthetical,
    RowsetItem,
    SubselectComparison,
    SubselectItem,
    UndefinedConceptFull,
    WhereClause,
    Window,
    WindowItem,
    WindowItemOrder,
    WindowItemOver,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    DataTyped,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructComponent,
    StructType,
    TraitDataType,
    TupleWrapper,
    arg_to_datatype,
    dict_to_map_wrapper,
    is_compatible_datatype,
    list_to_wrapper,
    tuple_to_wrapper,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    File,
    Query,
    RawColumnExpr,
)
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
    Import,
)
from trilogy.core.statements.author import (
    ChartConfig,
    ChartStatement,
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ConceptTransform,
    CopyStatement,
    CreateStatement,
    FromClause,
    FunctionDeclaration,
    ImportStatement,
    Limit,
    MergeStatementV2,
    MockStatement,
    MultiSelectStatement,
    PersistStatement,
    PublishStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    ShowStatement,
    TypeDeclaration,
    ValidateStatement,
)
from trilogy.parsing.common import (
    align_item_to_concept,
    arbitrary_to_concept,
    constant_to_concept,
    derive_item_to_concept,
    process_function_args,
    rowset_to_concepts,
)
from trilogy.parsing.exceptions import NameShadowError, ParseError
from trilogy.parsing.rust_parser import (
    RustStatementBlock,
    assert_public_parse_matches_rust,
    build_direct_statement,
    parse_statement_blocks,
    parser_compare_enabled,
    rust_hybrid_enabled,
)

perf_logger = getLogger("trilogy.parse.performance")


def metadata_from_meta(
    meta: Meta,
    description: str | None = None,
    concept_source: ConceptSource = ConceptSource.MANUAL,
) -> Metadata:
    """Create Metadata from a Lark Meta object, capturing full position info."""
    return Metadata(
        description=description,
        line_number=meta.line,
        column=meta.column,
        end_line=meta.end_line,
        end_column=meta.end_column,
        concept_source=concept_source,
    )


class ParsePass(Enum):
    INITIAL = 1
    VALIDATION = 2


CONSTANT_TYPES = (int, float, str, bool, ListWrapper, TupleWrapper, MapWrapper)

# Window functions that can be converted to aggregates when no ORDER BY is specified
WINDOW_TO_AGGREGATE_MAP = {
    WindowType.SUM: FunctionType.SUM,
    WindowType.AVG: FunctionType.AVG,
    WindowType.COUNT: FunctionType.COUNT,
    WindowType.COUNT_DISTINCT: FunctionType.COUNT_DISTINCT,
    WindowType.MAX: FunctionType.MAX,
    WindowType.MIN: FunctionType.MIN,
}

SELF_LABEL = "root"

MAX_PARSE_DEPTH = 10

SUPPORTED_INCREMENTAL_TYPES: set[DataType] = set([DataType.DATE, DataType.TIMESTAMP])

STDLIB_ROOT = Path(__file__).parent.parent


@dataclass
class WholeGrainWrapper:
    where: WhereClause


@dataclass
class FunctionBindingType:
    type: DataType | TraitDataType | None = None


@dataclass
class DropOn:
    functions: List[FunctionType]


@dataclass
class AddOn:
    functions: List[FunctionType]


@dataclass
class DatasourcePartitionClause:
    columns: List[ConceptRef]


class DatasourceUpdateTrigger(Enum):
    INCREMENTAL = "incremental"
    FRESHNESS = "freshness"


@dataclass
class DatasourceUpdateTriggerClause:
    trigger_type: DatasourceUpdateTrigger
    columns: List[ConceptRef]


@dataclass
class DatasourceFreshnessProbeClause:
    path: str


_PARSER: "Lark | None" = None


def _get_parser() -> Lark:
    global _PARSER
    if _PARSER is None:
        with open(join(dirname(__file__), "trilogy.lark"), "r") as f:
            _PARSER = Lark(
                f.read(),
                start=[
                    "start",
                    "select_list",
                    "column_assignment_list",
                    "where",
                    "having",
                    "order_by",
                    "align_clause",
                    "derive_clause",
                    "datasource",
                    "grain_clause",
                    "whole_grain_clause",
                    "address",
                    "query",
                    "file",
                    "datasource_update_trigger_clause",
                    "datasource_partition_clause",
                    "datasource_status_clause",
                    "concept",
                    "concept_declaration",
                    "concept_property_declaration",
                    "concept_derivation",
                    "constant_derivation",
                    "properties_declaration",
                    "parameter_declaration",
                    "raw_function",
                    "table_function",
                    "type_declaration",
                    "data_type",
                    "expr",
                ],
                propagate_positions=True,
                g_regex_flags=IGNORECASE,
                parser="lalr",
                cache=True,
            )
    return _PARSER


class _LazyParser:
    def parse(self, text: str, *args, **kwargs):
        if "start" not in kwargs:
            kwargs["start"] = "start"
        return _get_parser().parse(text, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(_get_parser(), name)


PARSER = _LazyParser()


def parse_concept_reference(
    name: str, environment: Environment, purpose: Optional[Purpose] = None
) -> Tuple[str, str, str, str | None]:
    parent = None
    if "." in name:
        if purpose == Purpose.PROPERTY:
            parent, name = name.rsplit(".", 1)
            namespace = environment.concepts[parent].namespace or DEFAULT_NAMESPACE
            lookup = f"{namespace}.{name}"

        else:
            namespace, name = name.rsplit(".", 1)
            lookup = f"{namespace}.{name}"
    else:
        namespace = environment.namespace or DEFAULT_NAMESPACE
        lookup = name
    return lookup, namespace, name, parent


def expr_to_boolean(
    root,
    function_factory: FunctionFactory,
) -> Union[Comparison, SubselectComparison, Conditional]:
    if not isinstance(root, (Comparison, SubselectComparison, Conditional)):
        if arg_to_datatype(root) == DataType.BOOL:
            root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        elif arg_to_datatype(root) == DataType.INTEGER:
            root = Comparison(
                left=function_factory.create_function(
                    [root],
                    FunctionType.BOOL,
                ),
                right=True,
                operator=ComparisonOperator.EQ,
            )
        else:
            root = Comparison(
                left=root, right=NULL_VALUE, operator=ComparisonOperator.IS_NOT
            )

    return root


def unwrap_transformation(
    input: Expr,
    environment: Environment,
) -> (
    Function
    | FilterItem
    | WindowItem
    | AggregateWrapper
    | FunctionCallWrapper
    | Parenthetical
    | SubselectItem
):
    if isinstance(input, Function):
        return input
    elif isinstance(input, AggregateWrapper):
        return input
    elif isinstance(input, ConceptRef):
        concept = environment.concepts[input.address]
        return Function(
            operator=FunctionType.ALIAS,
            output_datatype=concept.datatype,
            output_purpose=concept.purpose,
            arguments=[input],
        )
    elif isinstance(input, FilterItem):
        return input
    elif isinstance(input, WindowItem):
        return input
    elif isinstance(input, FunctionCallWrapper):
        return input
    elif isinstance(input, Parenthetical):
        return input
    elif isinstance(input, SubselectItem):
        return input
    else:
        return Function(
            operator=FunctionType.CONSTANT,
            output_datatype=arg_to_datatype(input),
            output_purpose=Purpose.CONSTANT,
            arguments=[input],
        )


def rehydrate_lineage(
    lineage: Any, environment: Environment, function_factory: FunctionFactory
) -> Any:
    """Fix datatype propagation. This is a hack to fix the fact that we don't know the datatypes of functions until we've parsed all concepts"""
    if isinstance(lineage, Function):
        rehydrated = [
            rehydrate_lineage(x, environment, function_factory)
            for x in lineage.arguments
        ]
        return function_factory.create_function(
            rehydrated,
            operator=lineage.operator,
        )
    elif isinstance(lineage, Parenthetical):
        lineage.content = rehydrate_lineage(
            lineage.content, environment, function_factory
        )
        return lineage
    elif isinstance(lineage, WindowItem):
        # this is temporarily guaranteed until we do some upstream work
        assert isinstance(lineage.content, ConceptRef)
        lineage.content.datatype = environment.concepts[
            lineage.content.address
        ].datatype
        return lineage
    elif isinstance(lineage, AggregateWrapper):
        lineage.function = rehydrate_lineage(
            lineage.function, environment, function_factory
        )
        return lineage
    elif isinstance(lineage, RowsetItem):
        lineage.content.datatype = environment.concepts[
            lineage.content.address
        ].datatype
        return lineage
    else:
        return lineage


def rehydrate_concept_lineage(
    concept: Concept, environment: Environment, function_factory: FunctionFactory
) -> Concept:
    concept.lineage = rehydrate_lineage(concept.lineage, environment, function_factory)
    if isinstance(concept.lineage, DataTyped):
        concept.datatype = concept.lineage.output_datatype
    return concept


class ParseToObjects(Transformer):
    def __init__(
        self,
        environment: Environment,
        parse_address: str | None = None,
        token_address: Path | str | None = None,
        parsed: dict[str, "ParseToObjects"] | None = None,
        tokens: dict[Path | str, ParseTree] | None = None,
        text_lookup: dict[Path | str, str] | None = None,
        environment_lookup: dict[str, Environment] | None = None,
        import_keys: list[str] | None = None,
        parse_config: Parsing | None = None,
        max_parse_depth: int = MAX_PARSE_DEPTH,
    ):
        Transformer.__init__(self, True)
        self.environment: Environment = environment
        self.parse_address: str = parse_address or SELF_LABEL
        self.token_address: Path | str = token_address or SELF_LABEL
        self.parsed: dict[str, ParseToObjects] = parsed if parsed is not None else {}
        self.tokens: dict[Path | str, ParseTree] = tokens if tokens is not None else {}
        self.environments: dict[str, Environment] = environment_lookup or {}
        self.text_lookup: dict[Path | str, str] = (
            text_lookup if text_lookup is not None else {}
        )
        # we do a second pass to pick up circular dependencies
        # after initial parsing
        self.parse_pass = ParsePass.INITIAL
        self.function_factory = FunctionFactory(self.environment)
        self.import_keys: list[str] = import_keys or ["root"]
        self.parse_config: Parsing = parse_config or CONFIG.parsing
        self.max_parse_depth: int = max_parse_depth
        self._pending_self_imports: list[str] = []
        self._simple_arg_bindings: dict[str, ArgBinding] = {}

    def set_text(self, text: str):
        self.text_lookup[self.token_address] = text

    def _parse_fragment_text(self, start_rule: str, text: str):
        original_fail_on_missing = self.environment.concepts.fail_on_missing
        original_parse_pass = self.parse_pass
        original_text = self.text_lookup.get(self.token_address)
        try:
            self.environment.concepts.fail_on_missing = False
            self.parse_pass = (
                ParsePass.VALIDATION
                if start_rule == "datasource"
                else ParsePass.INITIAL
            )
            self.set_text(text)
            if start_rule == "expr":
                simple_expr = self._parse_simple_expr_text(text)
                if simple_expr is not None:
                    return simple_expr
            if start_rule == "data_type":
                simple_type = self._parse_simple_data_type_text(text)
                if simple_type is not None:
                    return simple_type
            if start_rule == "order_by":
                simple_order = self._parse_simple_order_by_text(text)
                if simple_order is not None:
                    return simple_order
            if start_rule == "align_clause":
                simple_align = self._parse_simple_align_clause_text(text)
                if simple_align is not None:
                    return simple_align
            if start_rule == "derive_clause":
                simple_derive = self._parse_simple_derive_clause_text(text)
                if simple_derive is not None:
                    return simple_derive
            if start_rule == "select_list":
                simple_select = self._parse_simple_select_list_text(text)
                if simple_select is not None:
                    return simple_select
            if start_rule == "having":
                simple_having = self._parse_simple_having_text(text)
                if simple_having is not None:
                    return simple_having
            if start_rule == "where":
                simple_where = self._parse_simple_where_text(text)
                if simple_where is not None:
                    return simple_where
            if start_rule == "whole_grain_clause":
                simple_whole_grain = self._parse_simple_whole_grain_clause_text(text)
                if simple_whole_grain is not None:
                    return simple_whole_grain
            return self.transform(_get_parser().parse(text, start=start_rule))
        finally:
            self.environment.concepts.fail_on_missing = original_fail_on_missing
            self.parse_pass = original_parse_pass
            if original_text is None:
                self.text_lookup.pop(self.token_address, None)
            else:
                self.text_lookup[self.token_address] = original_text

    def _parse_simple_expr_text(self, text: str):
        stripped = text.strip()
        if not stripped:
            return None
        if stripped.lower() == "null":
            return NULL_VALUE
        typed_literal = self._parse_simple_typed_literal_text(stripped)
        if typed_literal is not None:
            return typed_literal
        if stripped.startswith("'") and stripped.endswith("'") and len(stripped) >= 2:
            return stripped[1:-1]
        if stripped.isdigit():
            return int(stripped)
        try:
            if "." in stripped and stripped.replace(".", "", 1).isdigit():
                return float(stripped)
        except ValueError:
            pass
        if stripped.lower() in {"true", "false"}:
            return stripped.lower() == "true"
        tuple_expr = self._parse_simple_expr_tuple_text(stripped)
        if tuple_expr is not None:
            return tuple_expr
        list_expr = self._parse_simple_list_expr_text(stripped)
        if list_expr is not None:
            return list_expr
        parenthetical_expr = self._parse_simple_parenthetical_expr_text(stripped)
        if parenthetical_expr is not None:
            return parenthetical_expr

        if stripped[0].isalpha() or stripped[0] == "_":
            concept = self.environment.concepts.get(stripped)
            if concept is not None and not isinstance(concept, UndefinedConceptFull):
                return concept.reference
            if stripped in self._simple_arg_bindings:
                binding = self._simple_arg_bindings[stripped]
                return ConceptRef(
                    address=f"{self.environment.namespace or DEFAULT_NAMESPACE}.{stripped}",
                    datatype=binding.output_datatype,
                )

        concat_expr = self._parse_simple_concat_expr_text(stripped)
        if concat_expr is not None:
            return concat_expr
        cast_expr = self._parse_simple_postfix_cast_expr_text(stripped)
        if cast_expr is not None:
            return cast_expr
        aggregate_by_expr = self._parse_simple_aggregate_by_expr_text(stripped)
        if aggregate_by_expr is not None:
            return aggregate_by_expr
        function_expr = self._parse_simple_function_expr_text(stripped)
        if function_expr is not None:
            return function_expr
        filter_expr = self._parse_simple_filter_expr_text(stripped)
        if filter_expr is not None:
            return filter_expr
        product_expr = self._parse_simple_product_expr_text(stripped)
        if product_expr is not None:
            return product_expr

        sum_expr = self._parse_simple_sum_expr_text(stripped)
        if sum_expr is not None:
            return sum_expr
        return None

    def _parse_simple_typed_literal_text(self, text: str):
        match = re.fullmatch(r"'([^']*)'::([a-zA-Z_][a-zA-Z0-9_]*)", text)
        if not match:
            return None
        value = match.group(1)
        dtype = self._parse_simple_data_type_text(match.group(2))
        if dtype == DataType.DATE:
            return self.function_factory.create_function(
                [date.fromisoformat(value)],
                FunctionType.CONSTANT,
            )
        if dtype in {DataType.DATETIME, DataType.TIMESTAMP}:
            return self.function_factory.create_function(
                [datetime.fromisoformat(value)],
                FunctionType.CONSTANT,
            )
        if dtype == DataType.STRING:
            return self.function_factory.create_function([value], FunctionType.CONSTANT)
        return None

    def _parse_simple_expr_tuple_text(self, text: str) -> TupleWrapper | None:
        if not (text.startswith("(") and text.endswith(")")):
            return None
        inner = text[1:-1].strip()
        if not inner:
            return None
        raw_values = self._split_top_level_commas(inner)
        if len(raw_values) < 2:
            return None
        values = [self._parse_simple_expr_text(part) for part in raw_values]
        if any(value is None for value in values):
            return None
        datatypes = {arg_to_datatype(value) for value in values}
        if len(datatypes) != 1:
            return None
        datatype = datatypes.pop()
        if not isinstance(datatype, DataType):
            return None
        return TupleWrapper(val=tuple(cast(list[object], values)), type=datatype)

    def _parse_simple_list_expr_text(self, text: str) -> ListWrapper | None:
        if not (text.startswith("[") and text.endswith("]")):
            return None
        inner = text[1:-1].strip()
        if not inner:
            return list_to_wrapper([])
        raw_values = self._split_top_level_commas(inner)
        values = [self._parse_simple_expr_text(part) for part in raw_values]
        if any(value is None for value in values):
            return None
        return list_to_wrapper(cast(list[object], values))

    def _parse_simple_parenthetical_expr_text(self, text: str) -> Parenthetical | None:
        if not (text.startswith("(") and text.endswith(")")):
            return None
        inner = text[1:-1].strip()
        if not inner:
            return None
        return Parenthetical(content=self._parse_fragment_text("expr", inner))

    def _parse_simple_identifier_list_text(self, text: str) -> list[str] | None:
        stripped = text.strip()
        if not stripped:
            return []
        values = [value.strip() for value in stripped.split(",")]
        if not values or any(
            not value or any(not (ch.isalnum() or ch in {"_", "."}) for ch in value)
            for value in values
        ):
            return None
        return values

    def _parse_simple_data_type_text(self, text: str):
        stripped = text.strip().lower()
        lookup = {
            "string": DataType.STRING,
            "bool": DataType.BOOL,
            "number": DataType.NUMBER,
            "float": DataType.FLOAT,
            "numeric": DataType.NUMERIC,
            "int": DataType.INTEGER,
            "bigint": DataType.BIGINT,
            "date": DataType.DATE,
            "datetime": DataType.DATETIME,
            "timestamp": DataType.TIMESTAMP,
            "date_part": DataType.DATE_PART,
            "geography": DataType.GEOGRAPHY,
            "null": DataType.NULL,
            "any": DataType.ANY,
            "unknown": DataType.UNKNOWN,
        }
        if stripped in lookup:
            return lookup[stripped]
        numeric_match = re.fullmatch(r"numeric\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", stripped)
        if numeric_match:
            return NumericType(
                precision=int(numeric_match.group(1)),
                scale=int(numeric_match.group(2)),
            )
        return None

    def _parse_simple_select_list_text(self, text: str) -> list[SelectItem] | None:
        items: list[SelectItem] = []
        for raw_item in self._split_top_level_commas(text):
            item = self._strip_nonsemantic_comments(raw_item).strip()
            if not item:
                continue
            if "metadata(" in item.lower():
                return None
            expr_text, alias_text = self._split_top_level_assignment(item)
            if expr_text is None:
                return None
            expr = self._parse_fragment_text("expr", expr_text)
            if alias_text is None:
                if not isinstance(expr, (ConceptRef, Concept)):
                    return None
                items.append(
                    SelectItem(
                        content=expr.reference if isinstance(expr, Concept) else expr
                    )
                )
                continue
            transformation = unwrap_transformation(expr, self.environment)
            _, namespace, output, _ = parse_concept_reference(
                alias_text, self.environment
            )
            concept = arbitrary_to_concept(
                transformation,
                environment=self.environment,
                namespace=namespace,
                name=output,
                metadata=Metadata(concept_source=ConceptSource.SELECT),
            )
            items.append(
                SelectItem(
                    content=ConceptTransform(function=transformation, output=concept)
                )
            )
        return items if items else None

    def _parse_simple_align_clause_text(self, text: str) -> AlignClause | None:
        items: list[AlignItem] = []
        for raw_item in self._split_top_level_align_items(text):
            item = raw_item.strip()
            if not item:
                continue
            alias_text, concepts_text = self._split_top_level_colon_assignment(item)
            if alias_text is None or concepts_text is None:
                return None
            if any(not (char.isalnum() or char == "_") for char in alias_text):
                return None
            concepts: list[ConceptRef] = []
            for concept_name in self._split_top_level_commas(concepts_text):
                concept_key = concept_name.strip()
                if not concept_key:
                    continue
                try:
                    concepts.append(self.environment.concepts[concept_key].reference)
                except UndefinedConceptException:
                    return None
            if not concepts:
                return None
            items.append(
                AlignItem(
                    alias=alias_text,
                    namespace=self.environment.namespace,
                    concepts=concepts,
                )
            )
        return AlignClause(items=items) if items else None

    def _parse_simple_derive_clause_text(self, text: str) -> DeriveClause | None:
        items: list[DeriveItem] = []
        for raw_item in self._split_top_level_commas(text):
            item = self._strip_nonsemantic_comments(raw_item).strip().rstrip(",")
            if not item:
                continue
            split_index = self._find_simple_top_level_operator(item, " -> ")
            if split_index == -1:
                return None
            expr_text = item[:split_index].strip()
            name_text = item[split_index + 4 :].strip()
            if not expr_text or not name_text:
                return None
            if not all(char.isalnum() or char == "_" for char in name_text):
                return None
            expr = self._parse_simple_expr_text(expr_text)
            if expr is None:
                return None
            items.append(
                DeriveItem(
                    expr=expr,
                    name=name_text,
                    namespace=self.environment.namespace or DEFAULT_NAMESPACE,
                )
            )
        return DeriveClause(items=items) if items else None

    def _parse_simple_order_by_text(self, text: str) -> OrderBy | None:
        stripped = text.strip()
        match = re.match(r"(?is)^order\s+by\b", stripped)
        if not match:
            return None
        body = stripped[match.end() :].strip()
        if not body:
            return None
        items = self._parse_simple_order_items(body)
        return OrderBy(items=items) if items else None

    def _parse_simple_order_items(self, text: str) -> list[OrderItem] | None:
        items: list[OrderItem] = []
        for raw_item in self._split_top_level_commas(text):
            item = raw_item.strip()
            if not item:
                continue
            tokens = item.split()
            expr_text = item
            order_text = "asc"
            if (
                len(tokens) >= 3
                and tokens[-2].lower() == "nulls"
                and tokens[-1].lower()
                in {
                    "first",
                    "last",
                }
            ):
                base_order = "asc"
                trim = 2
                if tokens[-3].lower() in {"asc", "desc"}:
                    base_order = tokens[-3].lower()
                    trim = 3
                expr_text = " ".join(tokens[:-trim]).strip()
                order_text = f"{base_order} nulls {tokens[-1].lower()}"
            elif len(tokens) >= 2 and tokens[-1].lower() in {"asc", "desc"}:
                expr_text = " ".join(tokens[:-1]).strip()
                order_text = tokens[-1].lower()
            expr = self._parse_simple_local_alias_expr_text(expr_text)
            items.append(OrderItem(expr=expr, order=Ordering(order_text)))
        return items if items else None

    def _parse_simple_having_text(self, text: str) -> HavingClause | None:
        stripped = text.strip()
        match = re.match(r"(?is)^having\b", stripped)
        if not match:
            return None
        root = self._parse_simple_boolean_root_text(
            self._strip_nonsemantic_comments(stripped[match.end() :].strip()),
            allow_local_alias=True,
        )
        if root is None:
            return None
        return HavingClause(
            conditional=cast(
                Comparison | Conditional | Parenthetical | SubselectComparison,
                root,
            )
        )

    def _parse_simple_whole_grain_clause_text(
        self, text: str
    ) -> WholeGrainWrapper | None:
        stripped = text.strip()
        match = re.match(r"(?is)^complete\s+where\b", stripped)
        if not match:
            return None
        root = self._parse_simple_boolean_root_text(
            self._strip_nonsemantic_comments(stripped[match.end() :].strip())
        )
        if root is None:
            return None
        return WholeGrainWrapper(
            where=WhereClause(
                conditional=cast(
                    Comparison | Conditional | Parenthetical | SubselectComparison,
                    root,
                )
            )
        )

    def _parse_simple_where_text(self, text: str) -> WhereClause | None:
        stripped = text.strip()
        match = re.match(r"(?is)^where\b", stripped)
        if not match:
            return None
        root = self._parse_simple_boolean_root_text(
            self._strip_nonsemantic_comments(stripped[match.end() :].strip())
        )
        if root is None:
            return None
        return WhereClause(
            conditional=cast(
                Comparison | Conditional | Parenthetical | SubselectComparison,
                root,
            )
        )

    def _parse_simple_boolean_root_text(
        self, text: str, *, allow_local_alias: bool = False
    ) -> (
        Comparison
        | Conditional
        | Parenthetical
        | SubselectComparison
        | ConceptRef
        | None
    ):
        stripped = text.strip()
        if not stripped:
            return None
        parenthetical = self._parse_simple_parenthetical_boolean_text(
            stripped,
            allow_local_alias=allow_local_alias,
        )
        if parenthetical is not None:
            return parenthetical
        or_parts = self._split_simple_top_level_boolean_or(stripped)
        if or_parts is not None:
            or_parsed_parts: list[
                Comparison
                | Conditional
                | Parenthetical
                | SubselectComparison
                | ConceptRef
            ] = []
            for part in or_parts:
                parsed = self._parse_simple_boolean_root_text(
                    part, allow_local_alias=allow_local_alias
                )
                if parsed is None:
                    return None
                or_parsed_parts.append(parsed)
            or_root: (
                Comparison
                | Conditional
                | Parenthetical
                | SubselectComparison
                | ConceptRef
            ) = or_parsed_parts[0]
            for parsed_part in or_parsed_parts[1:]:
                or_root = Conditional(
                    left=or_root,
                    right=parsed_part,
                    operator=BooleanOperator.OR,
                )
            return or_root
        and_parts = self._split_simple_top_level_boolean_and(stripped)
        if and_parts is not None:
            and_parsed_parts: list[
                Comparison
                | Conditional
                | Parenthetical
                | SubselectComparison
                | ConceptRef
            ] = []
            for part in and_parts:
                parsed = self._parse_simple_boolean_root_text(
                    part, allow_local_alias=allow_local_alias
                )
                if parsed is None:
                    return None
                and_parsed_parts.append(parsed)
            and_root: (
                Comparison
                | Conditional
                | Parenthetical
                | SubselectComparison
                | ConceptRef
            ) = and_parsed_parts[0]
            for parsed_part in and_parsed_parts[1:]:
                and_root = Conditional(
                    left=and_root, right=parsed_part, operator=BooleanOperator.AND
                )
            return and_root
        between_index = self._find_simple_top_level_operator(stripped, " between ")
        if between_index != -1:
            left_text = stripped[:between_index].strip()
            remainder = stripped[between_index + 9 :].strip()
            and_index = self._find_simple_top_level_operator(remainder, " and ")
            if and_index == -1:
                return None
            lower_text = remainder[:and_index].strip()
            upper_text = remainder[and_index + 5 :].strip()
            if not left_text or not lower_text or not upper_text:
                return None
            left = self._parse_simple_boolean_operand_text(
                left_text, allow_local_alias=allow_local_alias
            )
            return Conditional(
                left=Comparison(
                    left=left,
                    right=self._parse_simple_boolean_rhs_text(
                        lower_text, allow_local_alias=allow_local_alias
                    ),
                    operator=ComparisonOperator.GTE,
                ),
                right=Comparison(
                    left=left,
                    right=self._parse_simple_boolean_rhs_text(
                        upper_text, allow_local_alias=allow_local_alias
                    ),
                    operator=ComparisonOperator.LTE,
                ),
                operator=BooleanOperator.AND,
            )
        for operator_text, operator in (
            (" not in ", ComparisonOperator.NOT_IN),
            (" in ", ComparisonOperator.IN),
            (" is not ", ComparisonOperator.IS_NOT),
            (" is ", ComparisonOperator.IS),
            (">=", ComparisonOperator.GTE),
            ("<=", ComparisonOperator.LTE),
            ("=", ComparisonOperator.EQ),
            (">", ComparisonOperator.GT),
            ("<", ComparisonOperator.LT),
        ):
            index = self._find_simple_top_level_operator(stripped, operator_text)
            if index == -1:
                continue
            left_text = stripped[:index].strip()
            right_text = stripped[index + len(operator_text) :].strip()
            if not left_text or not right_text:
                return None
            left = self._parse_simple_boolean_operand_text(
                left_text, allow_local_alias=allow_local_alias
            )
            right = self._parse_simple_boolean_rhs_text(
                right_text, allow_local_alias=allow_local_alias
            )
            if operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
                return SubselectComparison(left=left, right=right, operator=operator)
            return Comparison(
                left=left,
                right=right,
                operator=operator,
            )
        operand = self._parse_simple_boolean_operand_text(
            stripped, allow_local_alias=allow_local_alias
        )
        if isinstance(operand, (ConceptRef, Parenthetical)):
            return operand
        boolean_expr = expr_to_boolean(operand, self.function_factory)
        if isinstance(boolean_expr, Comparison):
            return boolean_expr
        return None

    def _parse_simple_parenthetical_boolean_text(
        self, text: str, *, allow_local_alias: bool = False
    ) -> Parenthetical | None:
        if not (text.startswith("(") and text.endswith(")")):
            return None
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for index, char in enumerate(text):
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if in_single:
                if char == "'":
                    in_single = False
                continue
            if in_double:
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                in_single = True
                continue
            if char == '"':
                in_double = True
                continue
            if char == "(":
                depth += 1
                continue
            if char == ")":
                depth -= 1
                if depth == 0 and index != len(text) - 1:
                    return None
        if depth != 0:
            return None
        inner = text[1:-1].strip()
        if not inner:
            return None
        inner_root = self._parse_simple_boolean_root_text(
            inner, allow_local_alias=allow_local_alias
        )
        if inner_root is None:
            return None
        return Parenthetical(content=inner_root)

    def _split_simple_top_level_boolean_or(self, text: str) -> list[str] | None:
        parts: list[str] = []
        start = 0
        depth = 0
        in_single = False
        in_double = False
        escape = False
        index = 0
        while index < len(text):
            char = text[index]
            if escape:
                escape = False
                index += 1
                continue
            if char == "\\":
                escape = True
                index += 1
                continue
            if in_single:
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                in_single = True
                index += 1
                continue
            if char == '"':
                in_double = True
                index += 1
                continue
            if char in "([{":
                depth += 1
                index += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                index += 1
                continue
            if depth == 0 and text[index : index + 4].lower() == " or ":
                part = text[start:index].strip()
                if not part:
                    return None
                parts.append(part)
                start = index + 4
                index += 4
                continue
            index += 1
        if not parts:
            return None
        tail = text[start:].strip()
        if not tail:
            return None
        parts.append(tail)
        return parts

    def _split_simple_top_level_boolean_and(self, text: str) -> list[str] | None:
        parts: list[str] = []
        start = 0
        depth = 0
        in_single = False
        in_double = False
        escape = False
        pending_between = False
        index = 0
        while index < len(text):
            char = text[index]
            if escape:
                escape = False
                index += 1
                continue
            if char == "\\":
                escape = True
                index += 1
                continue
            if in_single:
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                in_single = True
                index += 1
                continue
            if char == '"':
                in_double = True
                index += 1
                continue
            if char in "([{":
                depth += 1
                index += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                index += 1
                continue
            if depth == 0 and text[index : index + 9].lower() == " between ":
                pending_between = True
                index += 9
                continue
            if depth == 0 and text[index : index + 5].lower() == " and ":
                if pending_between:
                    pending_between = False
                    index += 5
                    continue
                part = text[start:index].strip()
                if not part:
                    return None
                parts.append(part)
                start = index + 5
                index += 5
                continue
            index += 1
        if not parts:
            return None
        tail = text[start:].strip()
        if not tail:
            return None
        parts.append(tail)
        return parts

    def _find_simple_top_level_operator(self, text: str, operator_text: str) -> int:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        index = 0
        while index <= len(text) - len(operator_text):
            char = text[index]
            if escape:
                escape = False
                index += 1
                continue
            if char == "\\":
                escape = True
                index += 1
                continue
            if in_single:
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                in_single = True
                index += 1
                continue
            if char == '"':
                in_double = True
                index += 1
                continue
            if char in "([{":
                depth += 1
                index += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                index += 1
                continue
            if (
                depth == 0
                and text[index : index + len(operator_text)].lower() == operator_text
            ):
                return index
            index += 1
        return -1

    def _find_simple_top_level_char(self, text: str, target: str) -> int:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for index, char in enumerate(text):
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if in_single:
                if char == "'":
                    in_single = False
                continue
            if in_double:
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                in_single = True
                continue
            if char == '"':
                in_double = True
                continue
            if char in "([{":
                depth += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                continue
            if depth == 0 and char == target:
                return index
        return -1

    def _find_simple_top_level_keyword(self, text: str, keyword: str) -> int:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        keyword_lower = keyword.lower()
        index = 0
        while index <= len(text) - len(keyword):
            char = text[index]
            if escape:
                escape = False
                index += 1
                continue
            if char == "\\":
                escape = True
                index += 1
                continue
            if in_single:
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                in_single = True
                index += 1
                continue
            if char == '"':
                in_double = True
                index += 1
                continue
            if char in "([{":
                depth += 1
                index += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                index += 1
                continue
            if (
                depth == 0
                and text[index : index + len(keyword)].lower() == keyword_lower
            ):
                before = text[index - 1] if index > 0 else " "
                after_index = index + len(keyword)
                after = text[after_index] if after_index < len(text) else " "
                if before.isspace() and after.isspace():
                    return index
            index += 1
        return -1

    def _contains_simple_top_level_token(self, text: str, token: str) -> bool:
        return self._find_simple_top_level_operator(text, token) != -1

    def _parse_simple_boolean_rhs_text(
        self, text: str, *, allow_local_alias: bool = False
    ):
        lowered = text.strip().lower()
        if lowered == "null":
            return NULL_VALUE
        return self._parse_simple_boolean_operand_text(
            text, allow_local_alias=allow_local_alias
        )

    def _parse_simple_boolean_operand_text(
        self, text: str, *, allow_local_alias: bool = False
    ):
        stripped = text.strip()
        if (
            allow_local_alias
            and stripped
            and stripped.lower() not in {"null", "true", "false"}
            and (stripped[0].isalpha() or stripped[0] == "_")
            and all(char.isalnum() or char == "_" for char in stripped)
        ):
            concept = self.environment.concepts.get(stripped)
            if concept is None or isinstance(concept, UndefinedConceptFull):
                return ConceptRef(
                    address=f"{self.environment.namespace or DEFAULT_NAMESPACE}.{stripped}"
                )
        return self._parse_fragment_text("expr", text)

    def _parse_simple_local_alias_expr_text(self, text: str):
        stripped = text.strip()
        if (
            stripped
            and stripped.lower() not in {"null", "true", "false"}
            and (stripped[0].isalpha() or stripped[0] == "_")
            and all(char.isalnum() or char == "_" for char in stripped)
        ):
            concept = self.environment.concepts.get(stripped)
            if concept is None or isinstance(concept, UndefinedConceptFull):
                return ConceptRef(
                    address=f"{self.environment.namespace or DEFAULT_NAMESPACE}.{stripped}"
                )
        return self._parse_fragment_text("expr", text)

    def _split_top_level_assignment(self, text: str) -> tuple[str | None, str | None]:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        index = 0
        while index < len(text):
            char = text[index]
            if escape:
                escape = False
                index += 1
                continue
            if char == "\\":
                escape = True
                index += 1
                continue
            if in_single:
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                in_single = True
                index += 1
                continue
            if char == '"':
                in_double = True
                index += 1
                continue
            if char in "([{":
                depth += 1
                index += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                index += 1
                continue
            if depth == 0 and text[index : index + 2] == "->":
                return text[:index].strip(), text[index + 2 :].strip()
            if depth == 0 and text[index : index + 4].lower() == " as ":
                return text[:index].strip(), text[index + 4 :].strip()
            index += 1
        return text.strip(), None

    def _split_top_level_align_items(self, text: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        in_single = False
        in_double = False
        escape = False
        index = 0
        lowered = text.lower()
        while index < len(text):
            char = text[index]
            if escape:
                current.append(char)
                escape = False
                index += 1
                continue
            if char == "\\":
                current.append(char)
                escape = True
                index += 1
                continue
            if in_single:
                current.append(char)
                if char == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                current.append(char)
                if char == '"':
                    in_double = False
                index += 1
                continue
            if char == "'":
                current.append(char)
                in_single = True
                index += 1
                continue
            if char == '"':
                current.append(char)
                in_double = True
                index += 1
                continue
            if char in "([{":
                current.append(char)
                depth += 1
                index += 1
                continue
            if char in ")]}":
                current.append(char)
                depth = max(0, depth - 1)
                index += 1
                continue
            if depth == 0 and lowered[index : index + 3] == "and":
                before_ok = index == 0 or text[index - 1].isspace()
                after_end = index + 3
                after_ok = after_end >= len(text) or text[after_end].isspace()
                if before_ok and after_ok:
                    part = "".join(current).strip()
                    if part:
                        parts.append(part)
                    current = []
                    index += 3
                    continue
            current.append(char)
            index += 1
        tail = "".join(current).strip()
        if tail:
            parts.append(tail)
        return parts

    def _split_top_level_colon_assignment(
        self, text: str
    ) -> tuple[str | None, str | None]:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for index, char in enumerate(text):
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if in_single:
                if char == "'":
                    in_single = False
                continue
            if in_double:
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                in_single = True
                continue
            if char == '"':
                in_double = True
                continue
            if char in "([{":
                depth += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                continue
            if depth == 0 and char == ":":
                return text[:index].strip(), text[index + 1 :].strip()
        return None, None

    def _parse_simple_grain_clause_text(self, text: str) -> Grain | None:
        stripped = text.strip()
        if not stripped.lower().startswith("grain") or not stripped.endswith(")"):
            return None
        open_paren = stripped.find("(")
        if open_paren == -1:
            return None
        columns = self._parse_simple_identifier_list_text(stripped[open_paren + 1 : -1])
        if columns is None:
            return None
        return Grain(
            components=set(
                self.environment.concepts[column].address for column in columns
            )
        )

    def _parse_simple_address_clause_text(self, text: str) -> Address | None:
        stripped = text.strip()
        if not stripped.lower().startswith("address "):
            return None
        body = stripped[8:].strip()
        if not body:
            return None
        if body.startswith("f`") or body.startswith('f"'):
            return None
        if body.startswith("`") and body.endswith("`"):
            return Address(location=body[1:-1], quoted=True)
        if any(not (ch.isalnum() or ch in {"_", "."}) for ch in body):
            return None
        return Address(location=body, quoted=False)

    def _parse_simple_datasource_status_clause_text(
        self, text: str
    ) -> DatasourceState | None:
        stripped = text.strip().lower()
        if not stripped.startswith("state "):
            return None
        value = stripped[6:].strip()
        if value not in {"published", "unpublished"}:
            return None
        return DatasourceState(value)

    def _split_top_level_commas(self, text: str) -> list[str]:
        parts: list[str] = []
        start = 0
        paren_depth = 0
        brace_depth = 0
        bracket_depth = 0
        in_single = False
        in_double = False
        in_backtick = False
        in_triple_single = False
        index = 0
        while index < len(text):
            current = text[index]
            if in_triple_single:
                if text.startswith("'''", index):
                    in_triple_single = False
                    index += 3
                    continue
                index += 1
                continue
            if in_single:
                if current == "\\" and index + 1 < len(text):
                    index += 2
                    continue
                if current == "'":
                    in_single = False
                index += 1
                continue
            if in_double:
                if current == "\\" and index + 1 < len(text):
                    index += 2
                    continue
                if current == '"':
                    in_double = False
                index += 1
                continue
            if in_backtick:
                if current == "`":
                    in_backtick = False
                index += 1
                continue
            if text.startswith("'''", index):
                in_triple_single = True
                index += 3
                continue
            if current == "'":
                in_single = True
                index += 1
                continue
            if current == '"':
                in_double = True
                index += 1
                continue
            if current == "`":
                in_backtick = True
                index += 1
                continue
            if current == "(":
                paren_depth += 1
            elif current == ")":
                paren_depth = max(paren_depth - 1, 0)
            elif current == "{":
                brace_depth += 1
            elif current == "}":
                brace_depth = max(brace_depth - 1, 0)
            elif current == "[":
                bracket_depth += 1
            elif current == "]":
                bracket_depth = max(bracket_depth - 1, 0)
            elif (
                current == ","
                and paren_depth == 0
                and brace_depth == 0
                and bracket_depth == 0
            ):
                part = text[start:index].strip()
                if part:
                    parts.append(part)
                start = index + 1
            index += 1
        tail = text[start:].strip()
        if tail:
            parts.append(tail)
        return parts

    def _parse_simple_column_assignment_list_text(
        self, text: str
    ) -> list[ColumnAssignment] | None:
        assignments: list[ColumnAssignment] = []
        for item in self._split_top_level_commas(text):
            item = self._strip_nonsemantic_comments(item).rstrip(",").strip()
            if not item:
                continue
            concept_part = item
            alias_part: str | None = None
            colon_index = -1
            paren_depth = 0
            brace_depth = 0
            bracket_depth = 0
            for index, char in enumerate(item):
                if char == "(":
                    paren_depth += 1
                elif char == ")":
                    paren_depth = max(paren_depth - 1, 0)
                elif char == "{":
                    brace_depth += 1
                elif char == "}":
                    brace_depth = max(brace_depth - 1, 0)
                elif char == "[":
                    bracket_depth += 1
                elif char == "]":
                    bracket_depth = max(bracket_depth - 1, 0)
                elif (
                    char == ":"
                    and paren_depth == 0
                    and brace_depth == 0
                    and bracket_depth == 0
                ):
                    colon_index = index
                    break
            if colon_index != -1:
                alias_part = item[:colon_index].strip()
                concept_part = item[colon_index + 1 :].strip()
            modifiers: list[Modifier] = []
            while concept_part.startswith(("~", "?")):
                modifiers.append(Modifier(concept_part[0]))
                concept_part = concept_part[1:].strip()
            if not concept_part or any(
                not (char.isalnum() or char in {"_", "."}) for char in concept_part
            ):
                return None
            try:
                resolved = self.environment.concepts[concept_part]
            except UndefinedConceptException:
                return None
            alias: str | Function | RawColumnExpr
            if alias_part is None:
                alias = resolved.safe_address
            elif alias_part.startswith('"') and alias_part.endswith('"'):
                alias = alias_part[1:-1]
            elif all(char.isalnum() or char == "_" for char in alias_part):
                alias = alias_part
            else:
                raw_alias = self._parse_simple_raw_column_assignment_text(alias_part)
                if raw_alias is not None:
                    alias = raw_alias
                    assignments.append(
                        ColumnAssignment(
                            alias=alias,
                            modifiers=modifiers,
                            concept=resolved.reference,
                        )
                    )
                    continue
                expr_alias = self._parse_fragment_text("expr", alias_part)
                if not isinstance(expr_alias, Function):
                    return None
                alias = expr_alias
            assignments.append(
                ColumnAssignment(
                    alias=alias,
                    modifiers=modifiers,
                    concept=resolved.reference,
                )
            )
        return assignments

    def _parse_simple_raw_column_assignment_text(
        self, text: str
    ) -> RawColumnExpr | None:
        match = re.fullmatch(r"raw\s*\(\s*'''(.*)'''\s*\)", text.strip(), re.DOTALL)
        if not match:
            return None
        return RawColumnExpr(text=match.group(1))

    def _strip_nonsemantic_comments(self, text: str) -> str:
        cleaned: list[str] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            for marker in ("#", "//"):
                index = line.find(marker)
                if index != -1:
                    line = line[:index].rstrip()
            if line:
                cleaned.append(line)
        return " ".join(cleaned).strip()

    def _parse_simple_concat_expr_text(self, text: str):
        index = self._find_simple_top_level_operator(text, " || ")
        if index == -1:
            return None
        parts = self._split_top_level_operator_sequence(text, " || ")
        if parts is None or len(parts) < 2:
            return None
        parsed = [self._parse_simple_expr_text(part) for part in parts]
        if any(part is None for part in parsed):
            return None
        result = parsed[0]
        assert result is not None
        for part in parsed[1:]:
            assert part is not None
            result = self.function_factory.create_function(
                [result, part], FunctionType.CONCAT
            )
        return result

    def _parse_simple_postfix_cast_expr_text(self, text: str):
        parts = self._split_top_level_operator_sequence(text, "::")
        if parts is None or len(parts) < 2:
            return None
        base = self._parse_simple_expr_text(parts[0])
        if base is None:
            return None
        result = base
        for dtype_text in parts[1:]:
            dtype = self._parse_simple_data_type_text(dtype_text.strip())
            if dtype is None:
                return None
            result = self.function_factory.create_function(
                [result, dtype], FunctionType.CAST
            )
        return result

    def _parse_simple_aggregate_by_expr_text(self, text: str):
        by_index = self._find_simple_top_level_keyword(text, "by")
        if by_index == -1:
            return None
        expr_text = text[:by_index].strip()
        by_text = text[by_index + 2 :].strip()
        if not expr_text or not by_text:
            return None
        base = self._parse_simple_expr_text(expr_text)
        if base is None:
            return None
        if isinstance(base, AggregateWrapper):
            function = base.function
        elif isinstance(base, Function) and base.operator == FunctionType.GROUP:
            function = base
        else:
            return None
        by_parts = self._split_top_level_commas(by_text)
        if not by_parts:
            return None
        by_items: list[ConceptRef | Concept] = []
        for part in by_parts:
            item = part.strip()
            if not item:
                continue
            if item == "*":
                by_items.append(
                    ConceptRef(
                        address=f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
                        datatype=DataType.INTEGER,
                    )
                )
                continue
            concept = self.environment.concepts.get(item)
            if concept is None or isinstance(concept, UndefinedConceptFull):
                return None
            by_items.append(concept.reference)
        if not by_items:
            return None
        return AggregateWrapper(function=function, by=by_items)

    def _split_top_level_operator_sequence(
        self, text: str, operator_text: str
    ) -> list[str] | None:
        parts: list[str] = []
        start = 0
        while True:
            index = self._find_simple_top_level_operator(text[start:], operator_text)
            if index == -1:
                tail = text[start:].strip()
                if not tail:
                    return None
                parts.append(tail)
                break
            actual_index = start + index
            part = text[start:actual_index].strip()
            if not part:
                return None
            parts.append(part)
            start = actual_index + len(operator_text)
        return parts if len(parts) > 1 else None

    def _parse_simple_function_expr_text(self, text: str):
        case_expr = self._parse_simple_case_expr_text(text)
        if case_expr is not None:
            return case_expr
        window_expr = self._parse_simple_legacy_window_expr_text(text)
        if window_expr is not None:
            return window_expr
        custom_function = self._parse_simple_custom_function_expr_text(text)
        if custom_function is not None:
            return custom_function
        open_paren = text.find("(")
        if open_paren <= 0 or not text.endswith(")"):
            return None
        name = text[:open_paren].strip().lower()
        inner = text[open_paren + 1 : -1].strip()
        if name == "cast":
            expr_text, dtype_text = self._split_top_level_as(inner)
            if expr_text is None or dtype_text is None:
                return None
            expr = self._parse_simple_expr_text(expr_text)
            if expr is None:
                expr = self._parse_fragment_text("expr", expr_text)
            dtype = self._parse_simple_data_type_text(dtype_text)
            if dtype is None:
                dtype = self._parse_fragment_text("data_type", dtype_text)
            # Fold cast(string_literal as type) to constant, matching Lark fcast behavior
            if isinstance(expr, str) and isinstance(dtype, DataType):
                try:
                    match dtype:
                        case DataType.DATE:
                            processed: object = date.fromisoformat(expr)
                        case DataType.DATETIME | DataType.TIMESTAMP:
                            processed = datetime.fromisoformat(expr)
                        case DataType.INTEGER:
                            processed = int(expr)
                        case DataType.FLOAT:
                            processed = float(expr)
                        case DataType.BOOL:
                            processed = expr.capitalize() == "True"
                        case DataType.STRING:
                            processed = expr
                        case _:
                            processed = None
                    if processed is not None:
                        return self.function_factory.create_function(
                            [processed], FunctionType.CONSTANT
                        )
                except (ValueError, TypeError):
                    pass
            return self.function_factory.create_function(
                [expr, dtype], FunctionType.CAST
            )
        aggregate_map = {
            "sum": FunctionType.SUM,
            "avg": FunctionType.AVG,
            "max": FunctionType.MAX,
            "min": FunctionType.MIN,
            "count": FunctionType.COUNT,
            "count_distinct": FunctionType.COUNT_DISTINCT,
        }
        args = self._split_top_level_commas(inner)
        if name == "concat" and args:
            parsed = [
                self._parse_simple_expr_text(arg)
                or self._parse_fragment_text("expr", arg)
                for arg in args
            ]
            return self.function_factory.create_function(parsed, FunctionType.CONCAT)
        if name == "coalesce" and args:
            parsed = [
                self._parse_simple_expr_text(arg)
                or self._parse_fragment_text("expr", arg)
                for arg in args
            ]
            return self.function_factory.create_function(parsed, FunctionType.COALESCE)
        if name == "upper" and len(args) == 1:
            return self.function_factory.create_function(
                [
                    self._parse_simple_expr_text(args[0])
                    or self._parse_fragment_text("expr", args[0])
                ],
                FunctionType.UPPER,
            )
        if name == "lower" and len(args) == 1:
            return self.function_factory.create_function(
                [
                    self._parse_simple_expr_text(args[0])
                    or self._parse_fragment_text("expr", args[0])
                ],
                FunctionType.LOWER,
            )
        if name == "substring" and len(args) in {2, 3}:
            parsed = [
                self._parse_simple_expr_text(arg)
                or self._parse_fragment_text("expr", arg)
                for arg in args
            ]
            return self.function_factory.create_function(parsed, FunctionType.SUBSTRING)
        if name == "bool" and len(args) == 1:
            return self.function_factory.create_function(
                [
                    self._parse_simple_expr_text(args[0])
                    or self._parse_fragment_text("expr", args[0])
                ],
                FunctionType.BOOL,
            )
        if name == "date_diff" and len(args) == 3:
            parsed = [
                self._parse_simple_expr_text(args[0])
                or self._parse_fragment_text("expr", args[0]),
                self._parse_simple_expr_text(args[1])
                or self._parse_fragment_text("expr", args[1]),
                DatePart(args[2].strip()),
            ]
            return self.function_factory.create_function(parsed, FunctionType.DATE_DIFF)
        if name == "round" and len(args) in {1, 2}:
            parsed = [
                self._parse_simple_expr_text(args[0])
                or self._parse_fragment_text("expr", args[0])
            ]
            if len(args) == 2:
                parsed.append(
                    self._parse_simple_expr_text(args[1])
                    or self._parse_fragment_text("expr", args[1])
                )
            else:
                parsed.append(0)
            return self.function_factory.create_function(parsed, FunctionType.ROUND)
        if name == "group" and len(args) == 1:
            parsed = self._parse_simple_expr_text(args[0]) or self._parse_fragment_text(
                "expr", args[0]
            )
            return self.function_factory.create_function([parsed], FunctionType.GROUP)
        if name == "unnest" and len(args) == 1:
            parsed = self._parse_simple_expr_text(args[0]) or self._parse_fragment_text(
                "expr", args[0]
            )
            return self.function_factory.create_function([parsed], FunctionType.UNNEST)
        if name in aggregate_map and len(args) == 1:
            simple_arg = self._parse_simple_expr_text(args[0])
            if simple_arg is not None:
                return AggregateWrapper(
                    function=self.function_factory.create_function(
                        [simple_arg],
                        aggregate_map[name],
                    )
                )
        return None

    def _parse_simple_filter_expr_text(self, text: str) -> FilterItem | None:
        lowered = text.lower()
        if lowered.startswith("filter "):
            index = self._find_simple_top_level_operator(text, " where ")
            if index == -1:
                return None
            expr_text = text[7:index].strip()
            where_text = text[index:].strip()
            if not expr_text or not where_text:
                return None
            expr = self._parse_fragment_text("expr", expr_text)
            where = self._parse_fragment_text("where", where_text)
            return FilterItem(content=expr, where=where)
        index = self._find_simple_top_level_char(text, "?")
        if index == -1:
            return None
        expr_text = text[:index].strip()
        conditional_text = text[index + 1 :].strip()
        if not expr_text or not conditional_text:
            return None
        conditional = self._parse_simple_boolean_root_text(conditional_text)
        if conditional is None:
            return None
        return FilterItem(
            content=self._parse_fragment_text("expr", expr_text),
            where=WhereClause(
                conditional=cast(
                    Comparison | Conditional | Parenthetical | SubselectComparison,
                    conditional,
                )
            ),
        )

    def _parse_simple_product_expr_text(self, text: str):
        if text.startswith(("*", "/")):
            return None
        if any(
            self._contains_simple_top_level_token(text, token)
            for token in (
                " between ",
                " when ",
                " then ",
                " else ",
                " end",
                " and ",
                " or ",
                " is ",
                "case ",
            )
        ):
            return None
        parts: list[object] = []
        current: list[str] = []
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for char in text:
            if escape:
                current.append(char)
                escape = False
                continue
            if char == "\\":
                current.append(char)
                escape = True
                continue
            if in_single:
                current.append(char)
                if char == "'":
                    in_single = False
                continue
            if in_double:
                current.append(char)
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                current.append(char)
                in_single = True
                continue
            if char == '"':
                current.append(char)
                in_double = True
                continue
            if char in "([{":
                current.append(char)
                depth += 1
                continue
            if char in ")]}":
                current.append(char)
                depth = max(0, depth - 1)
                continue
            if char in {"*", "/"} and depth == 0:
                operand = "".join(current).strip()
                if not operand:
                    return None
                parts.append(operand)
                parts.append(char)
                current = []
                continue
            current.append(char)
        operand = "".join(current).strip()
        if not operand or not parts:
            return None
        parts.append(operand)
        result = self._parse_fragment_text("expr", cast(str, parts[0]))
        for index in range(1, len(parts), 2):
            operator = parts[index]
            right = self._parse_fragment_text("expr", cast(str, parts[index + 1]))
            result = self.function_factory.create_function(
                [result, right],
                FunctionType.MULTIPLY if operator == "*" else FunctionType.DIVIDE,
            )
        return result

    def _parse_simple_case_expr_text(self, text: str):
        lowered = text.lower()
        if not (lowered.startswith("case ") and lowered.endswith(" end")):
            return None
        remaining = text[4:].strip()
        args: list[CaseWhen | CaseElse] = []
        while remaining:
            lowered_remaining = remaining.lower()
            if lowered_remaining.startswith("when "):
                then_index = self._find_simple_top_level_operator(remaining, " then ")
                if then_index == -1:
                    return None
                condition_text = remaining[5:then_index].strip()
                tail = remaining[then_index + 6 :].strip()
                next_breaks = [
                    index
                    for index in (
                        self._find_simple_top_level_operator(tail, " when "),
                        self._find_simple_top_level_operator(tail, " else "),
                        self._find_simple_top_level_operator(tail, " end"),
                    )
                    if index != -1
                ]
                expr_end = min(next_breaks) if next_breaks else len(tail)
                expr_text = tail[:expr_end].strip()
                conditional = self._parse_simple_boolean_root_text(
                    condition_text, allow_local_alias=True
                )
                if conditional is None or not expr_text:
                    return None
                if isinstance(conditional, (Parenthetical, ConceptRef)):
                    conditional = expr_to_boolean(conditional, self.function_factory)
                if not isinstance(
                    conditional, (Comparison, Conditional, SubselectComparison)
                ):
                    return None
                args.append(
                    CaseWhen(
                        comparison=conditional,
                        expr=(
                            self._parse_simple_local_alias_expr_text(expr_text)
                            or self._parse_fragment_text("expr", expr_text)
                        ),
                    )
                )
                remaining = tail[expr_end:].strip()
                continue
            if lowered_remaining.startswith("else "):
                tail = remaining[5:].strip()
                end_index = self._find_simple_top_level_operator(tail, " end")
                if end_index == -1:
                    return None
                expr_text = tail[:end_index].strip()
                if not expr_text:
                    return None
                args.append(
                    CaseElse(
                        expr=(
                            self._parse_simple_local_alias_expr_text(expr_text)
                            or self._parse_fragment_text("expr", expr_text)
                        )
                    )
                )
                remaining = tail[end_index:].strip()
                continue
            if lowered_remaining == "end":
                return self.function_factory.create_function(args, FunctionType.CASE)
            return None
        return None

    def _parse_simple_legacy_window_expr_text(self, text: str) -> WindowItem | None:
        lowered = text.lower()
        matched_type = next(
            (
                window_type
                for window_type in ("lead", "lag")
                if lowered.startswith(f"{window_type} ")
            ),
            None,
        )
        if matched_type is None:
            return None
        remainder = text[len(matched_type) :].strip()
        parts = remainder.split(maxsplit=1)
        index: int | None = None
        if len(parts) == 2 and parts[0].isdigit():
            index = int(parts[0])
            remainder = parts[1].strip()
        order_index = self._find_simple_top_level_operator(remainder, " by ")
        expr_text = remainder if order_index == -1 else remainder[:order_index].strip()
        order_text = None if order_index == -1 else remainder[order_index + 4 :].strip()
        if not expr_text:
            return None
        concept_expr = self._parse_fragment_text("expr", expr_text)
        if not isinstance(concept_expr, ConceptRef):
            return None
        order_by = (
            [] if order_text is None else self._parse_simple_order_items(order_text)
        )
        if order_text and order_by is None:
            return None
        return WindowItem(
            type=WindowType(matched_type),
            content=concept_expr,
            over=[],
            order_by=order_by or [],
            index=index,
        )

    def _parse_simple_custom_function_expr_text(
        self, text: str
    ) -> FunctionCallWrapper | None:
        stripped = text.strip()
        if not stripped.startswith("@") or not stripped.endswith(")"):
            return None
        open_paren = stripped.find("(")
        if open_paren <= 1:
            return None
        name = stripped[1:open_paren].strip()
        if not name or any(not (char.isalnum() or char == "_") for char in name):
            return None
        function = self.environment.functions.get(name)
        if function is None:
            return None
        args_text = stripped[open_paren + 1 : -1].strip()
        call_args = (
            [
                self._parse_fragment_text("expr", part)
                for part in self._split_top_level_commas(args_text)
            ]
            if args_text
            else []
        )
        return FunctionCallWrapper(
            content=function(*call_args),
            name=name,
            args=call_args,
        )

    def _parse_simple_sum_expr_text(self, text: str):
        if text.startswith(("+", "-")):
            return None
        if any(
            self._contains_simple_top_level_token(text, token)
            for token in (
                " between ",
                " when ",
                " then ",
                " else ",
                " end",
                " and ",
                " or ",
                " is ",
                "case ",
            )
        ):
            return None
        parts: list[object] = []
        current: list[str] = []
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for char in text:
            if escape:
                current.append(char)
                escape = False
                continue
            if char == "\\":
                current.append(char)
                escape = True
                continue
            if in_single:
                current.append(char)
                if char == "'":
                    in_single = False
                continue
            if in_double:
                current.append(char)
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                current.append(char)
                in_single = True
                continue
            if char == '"':
                current.append(char)
                in_double = True
                continue
            if char in "([{":
                current.append(char)
                depth += 1
                continue
            if char in ")]}":
                current.append(char)
                depth = max(0, depth - 1)
                continue
            if char in {"+", "-"} and depth == 0:
                operand = "".join(current).strip()
                if not operand:
                    return None
                parts.append(operand)
                parts.append(char)
                current = []
                continue
            current.append(char)
        operand = "".join(current).strip()
        if not operand or not parts:
            return None
        parts.append(operand)
        result = self._parse_fragment_text("expr", cast(str, parts[0]))
        for index in range(1, len(parts), 2):
            operator = parts[index]
            right = self._parse_fragment_text("expr", cast(str, parts[index + 1]))
            result = self.function_factory.create_function(
                [result, right],
                FunctionType.ADD if operator == "+" else FunctionType.SUBTRACT,
            )
        return result

    def _split_top_level_as(self, text: str) -> tuple[str | None, str | None]:
        depth = 0
        in_single = False
        in_double = False
        escape = False
        for index, char in enumerate(text):
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if in_single:
                if char == "'":
                    in_single = False
                continue
            if in_double:
                if char == '"':
                    in_double = False
                continue
            if char == "'":
                in_single = True
                continue
            if char == '"':
                in_double = True
                continue
            if char in "([{":
                depth += 1
                continue
            if char in ")]}":
                depth = max(0, depth - 1)
                continue
            if depth == 0 and text[index : index + 4].lower() == " as ":
                return text[:index].strip(), text[index + 4 :].strip()
        return None, None

    def transform(self, tree: Tree):
        results = super().transform(tree)
        self.tokens[self.token_address] = tree
        return results

    def prepare_parse(self):
        self.parse_pass = ParsePass.INITIAL
        self.environment.concepts.fail_on_missing = False
        for _, v in self.parsed.items():
            v.prepare_parse()

    def _resolve_pending_self_imports(self):
        """Resolve self-imports using existing import machinery."""
        if not self._pending_self_imports:
            return
        pending = self._pending_self_imports
        self._pending_self_imports = []
        env_file_path = self.environment.env_file_path
        if isinstance(env_file_path, str):
            env_file_path = Path(env_file_path)

        for alias in pending:
            self.environment.add_import(
                alias,
                self.environment,
                Import(
                    alias=alias,
                    path=env_file_path if env_file_path else Path("."),
                    input_path=env_file_path,
                ),
            )

    def run_second_parse_pass(self, force: bool = False):
        if self.token_address not in self.tokens:
            return []
        self.parse_pass = ParsePass.VALIDATION

        for _, v in list(self.parsed.items()):
            if v.parse_pass == ParsePass.VALIDATION:
                continue
            v.run_second_parse_pass()
        reparsed = self.transform(self.tokens[self.token_address])
        self._resolve_pending_self_imports()
        self.environment.concepts.undefined = {}
        passed = False
        passes = 0
        # output datatypes for functions may have been wrong
        # as they were derived from not fully understood upstream types
        # so loop through to recreate function lineage until all datatypes are known

        while not passed:
            new_passed = True
            for x, y in self.environment.concepts.items():
                if y.datatype == DataType.UNKNOWN and y.lineage:
                    self.environment.concepts[x] = rehydrate_concept_lineage(
                        y, self.environment, self.function_factory
                    )
                    new_passed = False
            passes += 1
            if passes > MAX_PARSE_DEPTH:
                break
            passed = new_passed

        return reparsed

    def start(self, args):
        return args

    def LINE_SEPARATOR(self, args):
        return MagicConstants.LINE_SEPARATOR

    def block(self, args):
        output = args[0]
        if isinstance(output, ConceptDeclarationStatement):
            if len(args) > 1 and args[1] != MagicConstants.LINE_SEPARATOR:
                comments = [x for x in args[1:] if isinstance(x, Comment)]
                merged = "\n".join([x.text.split("#")[1].rstrip() for x in comments])
                output.concept.metadata.description = merged
        # this is a bad plan for now;
        # because a comment after an import statement is very common
        # and it's not intuitive that it modifies the import description
        # if isinstance(output, ImportStatement):
        #     if len(args) > 1 and isinstance(args[1], Comment):
        #         comment = args[1].text.split("#")[1].strip()
        #         namespace = output.alias
        #         for _, v in self.environment.concepts.items():
        #             if v.namespace == namespace:
        #                 if v.metadata.description:
        #                     v.metadata.description = (
        #                         f"{comment}: {v.metadata.description}"
        #                     )
        #                 else:
        #                     v.metadata.description = comment

        return args[0]

    def metadata(self, args):
        pairs = {key: val for key, val in zip(args[::2], args[1::2])}
        return Metadata(**pairs)

    def IDENTIFIER(self, args) -> str:
        return args.value

    def ORDER_IDENTIFIER(self, args) -> ConceptRef:
        return self.environment.concepts[args.value.strip()].reference

    def WILDCARD_IDENTIFIER(self, args) -> str:
        return args.value

    def QUOTED_IDENTIFIER(self, args) -> str:
        return args.value[1:-1]

    @v_args(meta=True)
    def concept_lit(self, meta: Meta, args) -> ConceptRef:
        address = args[0]
        if "." not in address and self.environment.namespace == DEFAULT_NAMESPACE:
            address = f"{DEFAULT_NAMESPACE}.{address}"
        mapping = self.environment.concepts[address]
        datatype = mapping.output_datatype
        return ConceptRef(
            # this is load-bearing to handle pseudonyms
            address=mapping.address,
            metadata=metadata_from_meta(meta),
            datatype=datatype,
        )

    def _interpolate_params(self, text: str) -> str:
        def replace(match: re.Match) -> str:
            name = match.group(1).strip()
            lookup = f"{DEFAULT_NAMESPACE}.{name}" if "." not in name else name
            try:
                concept = self.environment.concepts[lookup]
            except KeyError:
                raise ParseError(
                    f"Unknown reference '{{{name}}}' in address — '{name}' is not defined."
                )
            if (
                not isinstance(concept.lineage, Function)
                or concept.lineage.operator != FunctionType.CONSTANT
            ):
                raise ParseError(
                    f"'{{{name}}}' in address must reference a constant or parameter, not {concept.purpose}."
                )
            return str(concept.lineage.arguments[0])

        return re.sub(r"\{([^}]+)\}", replace, text)

    def ADDRESS(self, args) -> Address:
        return Address(location=args.value, quoted=False)

    def QUOTED_ADDRESS(self, args) -> Address:
        return Address(location=args.value[1:-1], quoted=True)

    def FILE_PATH(self, args) -> str:
        return args.value[1:-1]

    def F_QUOTED_ADDRESS(self, args) -> Address:
        return Address(location=self._interpolate_params(args.value[2:-1]), quoted=True)

    def F_FILE_PATH(self, args) -> str:
        return self._interpolate_params(args.value[2:-1])

    def STRING_CHARS(self, args) -> str:
        return args.value

    def SINGLE_STRING_CHARS(self, args) -> str:
        return args.value

    def DOUBLE_STRING_CHARS(self, args) -> str:
        return args.value

    def MINUS(self, args) -> str:
        return "-"

    @v_args(meta=True)
    def struct_component(self, meta: Meta, args) -> StructComponent:
        modifiers = []
        for arg in args:
            if isinstance(arg, Modifier):
                modifiers.append(arg)
        return StructComponent(name=args[0], type=args[1], modifiers=modifiers)

    @v_args(meta=True)
    def struct_type(self, meta: Meta, args) -> StructType:
        final: list[
            DataType
            | MapType
            | ArrayType
            | NumericType
            | StructType
            | StructComponent
            | Concept
        ] = []
        for arg in args:
            if isinstance(arg, StructComponent):
                final.append(arg)
            else:
                new = self.environment.concepts.__getitem__(  # type: ignore
                    key=arg, line_no=meta.line
                )
                final.append(new)

        return StructType(
            fields=final,
            fields_map={
                x.name: x for x in final if isinstance(x, (Concept, StructComponent))
            },
        )

    def list_type(self, args) -> ArrayType:
        content = args[0]
        if isinstance(content, str):
            content = self.environment.concepts[content]
        return ArrayType(type=content)

    def numeric_type(self, args) -> NumericType:
        return NumericType(precision=args[0], scale=args[1])

    def map_type(self, args) -> MapType:
        key = args[0]
        value = args[1]
        if isinstance(key, str):
            key = self.environment.concepts[key]
        elif isinstance(value, str):
            value = self.environment.concepts[value]
        return MapType(key_type=key, value_type=value)

    def enum_type(self, args) -> EnumType:
        base_type = args[0]
        if not isinstance(base_type, DataType):
            raise TypeError(
                f"enum base type must be a primitive DataType, got {base_type}"
            )
        return EnumType(type=base_type, values=list(args[1:]))

    @v_args(meta=True)
    def data_type(
        self, meta: Meta, args
    ) -> (
        DataType
        | TraitDataType
        | ArrayType
        | StructType
        | MapType
        | NumericType
        | EnumType
    ):
        resolved = args[0]
        traits = args[2:]
        base: (
            DataType
            | TraitDataType
            | ArrayType
            | StructType
            | MapType
            | NumericType
            | EnumType
        )
        if isinstance(resolved, StructType):
            base = resolved
        elif isinstance(resolved, ArrayType):
            base = resolved
        elif isinstance(resolved, NumericType):
            base = resolved
        elif isinstance(resolved, MapType):
            base = resolved
        elif isinstance(resolved, EnumType):
            base = resolved
        else:
            base = DataType(args[0].lower())
        if traits:
            for trait in traits:
                if trait not in self.environment.data_types:
                    similar = difflib.get_close_matches(
                        trait, list(self.environment.data_types.keys())
                    )
                    hint = f" Did you mean: {', '.join(similar)}?" if similar else ""
                    raise TypeError(
                        f"Invalid type (trait) {trait} for {base}, line {meta.line}.{hint}"
                    )
                matched = self.environment.data_types[trait]
                if not is_compatible_datatype(matched.type, base):
                    raise TypeError(
                        f"Invalid type (trait) {trait} for {base}, line {meta.line}. Trait expects type {matched.type}, has {base}"
                    )
            return TraitDataType(type=base, traits=traits)

        return base

    def array_comparison(self, args) -> ComparisonOperator:
        return ComparisonOperator([x.value.lower() for x in args])

    def COMPARISON_OPERATOR(self, args) -> ComparisonOperator:
        return ComparisonOperator(args.strip())

    def LOGICAL_OPERATOR(self, args) -> BooleanOperator:
        return BooleanOperator(args.lower())

    def concept_assignment(self, args):
        return args

    @v_args(meta=True)
    def column_assignment(self, meta: Meta, args):
        modifiers = []
        short_binding = True
        if len(args) == 2:
            alias = args[0]
            concept_list = args[1]
            short_binding = False
        else:
            alias = args[0][-1]
            concept_list = args[0]
        # recursively collect modifiers
        if len(concept_list) > 1:
            modifiers += concept_list[:-1]
        concept = concept_list[-1]
        resolved = self.environment.concepts.__getitem__(  # type: ignore
            key=concept, line_no=meta.line, file=self.token_address
        )
        if short_binding:
            alias = resolved.safe_address
        return ColumnAssignment(
            alias=alias, modifiers=modifiers, concept=resolved.reference
        )

    def _TERMINATOR(self, args):
        return None

    def _static_functions(self, args):
        return args[0]

    def MODIFIER(self, args) -> Modifier:
        return Modifier(args.value)

    def SHORTHAND_MODIFIER(self, args) -> Modifier:
        return Modifier(args.value)

    def DATASOURCE_PARTIAL(self, args) -> Modifier:
        return Modifier.PARTIAL

    def PURPOSE(self, args) -> Purpose:
        return Purpose(args.value)

    def AUTO(self, args) -> Purpose:
        return Purpose.AUTO

    def CONST(self, args) -> Purpose:
        return Purpose.CONSTANT

    def CONSTANT(self, args) -> Purpose:
        return Purpose.CONSTANT

    def PROPERTY(self, args):
        return Purpose.PROPERTY

    def HASH_TYPE(self, args):
        return args.value

    @v_args(meta=True)
    def prop_ident(self, meta: Meta, args) -> Tuple[List[Concept], str]:
        return [self.environment.concepts[grain] for grain in args[:-1]], args[-1]

    @v_args(meta=True)
    def prop_ident_wildcard(self, meta: Meta, args) -> Tuple[List[Concept], str]:
        return [
            self.environment.concepts[f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"]
        ], str(args[0])

    @v_args(meta=True)
    def concept_property_declaration(self, meta: Meta, args) -> Concept:
        unique = False
        if not args[0] == Purpose.PROPERTY:
            unique = True
            args = args[1:]
        metadata = Metadata()
        modifiers = []
        for arg in args:
            if isinstance(arg, Metadata):
                metadata = arg
            if isinstance(arg, Modifier):
                modifiers.append(arg)

        declaration = args[1]
        if isinstance(declaration, (tuple)):
            parents, name = declaration
            if "." in name:
                namespace, name = name.split(".", 1)
            else:
                namespace = self.environment.namespace or DEFAULT_NAMESPACE
        else:
            if "." not in declaration:
                raise ParseError(
                    f"Property declaration {args[1]} must be fully qualified with a parent key"
                )
            grain, name = declaration.rsplit(".", 1)
            parent = self.environment.concepts[grain]
            parents = [parent]
            namespace = parent.namespace
        grain_components = {x.address for x in parents}
        all_rows_addr = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
        is_abstract_grain = grain_components == {all_rows_addr}
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=Purpose.PROPERTY if not unique else Purpose.UNIQUE_PROPERTY,
            metadata=metadata,
            grain=Grain(components=grain_components),
            namespace=namespace,
            keys=grain_components,
            modifiers=modifiers,
            granularity=(
                Granularity.SINGLE_ROW if is_abstract_grain else Granularity.MULTI_ROW
            ),
        )

        self.environment.add_concept(concept, meta)
        return concept

    def inline_property(self, args):
        # returns (name, datatype, nullable, metadata) for use by properties_declaration
        return args

    def inline_property_list(self, args):
        props = []
        for arg in args:
            if isinstance(arg, list):
                props.append(arg)
            elif isinstance(arg, Comment) and props:
                # comment follows a comma after the preceding property
                merged = arg.text.split("#")[1].rstrip()
                prop_args = props[-1]
                existing = next((a for a in prop_args if isinstance(a, Metadata)), None)
                if existing is None:
                    prop_args.append(Metadata(description=merged))
                elif not existing.description:
                    existing.description = merged
        return props

    def prop_ident_list(self, args):
        return [str(a) for a in args]

    @v_args(meta=True)
    def properties_declaration(self, meta: Meta, args) -> list[Concept]:
        # _PROPERTIES is filtered; args[0] = parent(s), args[1] = inline_property_list
        parents_arg = args[0]
        inline_props: list[list] = args[1]

        if isinstance(parents_arg, list):
            # <key1, key2> form
            parents = [self.environment.concepts[k] for k in parents_arg]
        else:
            parent = self.environment.concepts[str(parents_arg)]
            parents = [parent]

        grain_components = {x.address for x in parents}
        namespace = parents[0].namespace
        all_rows_addr = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
        is_abstract_grain = grain_components == {all_rows_addr}
        concepts = []
        for prop_args in inline_props:
            name = str(prop_args[0])
            datatype = prop_args[1]
            metadata = Metadata()
            modifiers = []
            for extra in prop_args[2:]:
                if isinstance(extra, Metadata):
                    metadata = extra
                elif isinstance(extra, Modifier):
                    modifiers.append(extra)
            concept = Concept(
                name=name,
                datatype=datatype,
                purpose=Purpose.PROPERTY,
                metadata=metadata,
                grain=Grain(components=grain_components),
                namespace=namespace,
                keys=grain_components,
                modifiers=modifiers,
                granularity=(
                    Granularity.SINGLE_ROW
                    if is_abstract_grain
                    else Granularity.MULTI_ROW
                ),
            )
            self.environment.add_concept(concept, meta)
            concepts.append(concept)
        return concepts

    def parameter_default(self, args):
        return args[0]

    @v_args(meta=True)
    def parameter_declaration(self, meta: Meta, args) -> ConceptDeclarationStatement:
        metadata = Metadata()
        default = None
        name = args[0]
        datatype = args[1]
        for arg in args[2:]:
            if isinstance(arg, Metadata):
                metadata = arg
            elif not isinstance(arg, Modifier):
                default = arg
        _, namespace, name, _ = parse_concept_reference(name, self.environment)
        raw = self.environment.parameters.get(name, default)
        if raw is None:
            raise MissingParameterException(
                f'This script requires parameter "{name}" to be set in environment.'
            )
        if datatype == DataType.INTEGER:
            value: Any = int(raw)
        elif datatype == DataType.FLOAT:
            value = float(raw)
        elif datatype == DataType.BOOL:
            value = bool(raw)
        elif datatype == DataType.STRING:
            value = str(raw)
        elif datatype == DataType.DATE:
            value = raw if isinstance(raw, date) else date.fromisoformat(raw)
        elif datatype == DataType.DATETIME:
            value = raw if isinstance(raw, datetime) else datetime.fromisoformat(raw)
        else:
            raise ParseError(f"Unsupported datatype {datatype} for parameter {name}.")
        return self.constant_derivation(meta, [Purpose.CONSTANT, name, value, metadata])

    @v_args(meta=True)
    def concept_declaration(self, meta: Meta, args) -> ConceptDeclarationStatement:
        metadata = Metadata()
        modifiers = []
        purpose = args[0]
        datatype = args[2]
        for arg in args:
            if isinstance(arg, Metadata):
                metadata = arg
            if isinstance(arg, Modifier):
                modifiers.append(arg)
        name = args[1]
        _, namespace, name, _ = parse_concept_reference(name, self.environment)

        concept = Concept(
            name=name,
            datatype=datatype,
            purpose=purpose,
            metadata=metadata,
            namespace=namespace,
            modifiers=modifiers,
            derivation=Derivation.ROOT,
            granularity=Granularity.MULTI_ROW,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
            concept.metadata.column = meta.column
            concept.metadata.end_line = meta.end_line
            concept.metadata.end_column = meta.end_column
        self.environment.add_concept(concept, meta=meta)
        return ConceptDeclarationStatement(concept=concept)

    @v_args(meta=True)
    def concept_derivation(self, meta: Meta, args) -> ConceptDerivationStatement:
        metadata = args[3] if len(args) > 3 else None
        purpose = args[0]
        raw_name = args[1]
        # abc.def.property pattern
        if isinstance(raw_name, str):
            lookup, namespace, name, parent_concept = parse_concept_reference(
                raw_name, self.environment, purpose
            )
            # <abc.def,zef.gf>.property pattern
            keys = (
                [self.environment.concepts[parent_concept].address]
                if parent_concept
                else []
            )
        else:
            keys, name = raw_name
            keys = [x.address for x in keys]
            namespaces = set([x.rsplit(".", 1)[0] for x in keys])
            if not len(namespaces) == 1:
                namespace = self.environment.namespace or DEFAULT_NAMESPACE
            else:
                namespace = namespaces.pop()
        source_value = args[2]
        # we need to strip off every parenthetical to see what is being assigned.
        while isinstance(source_value, Parenthetical):
            source_value = source_value.content

        concept: Concept
        if isinstance(
            source_value,
            (
                FilterItem,
                WindowItem,
                AggregateWrapper,
                Function,
                FunctionCallWrapper,
                Comparison,
                SubselectItem,
            ),
        ):
            concept = arbitrary_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                environment=self.environment,
                metadata=metadata,
            )
            # let constant purposes exist to support round-tripping
            # as a build concept may end up with a constant based on constant inlining happening recursively
            if purpose == Purpose.KEY and concept.purpose != Purpose.KEY:
                concept.purpose = Purpose.KEY
            elif (
                purpose
                and purpose != Purpose.AUTO
                and concept.purpose != purpose
                and purpose != Purpose.CONSTANT
            ):
                raise SyntaxError(
                    f'Concept {name} purpose {concept.purpose} does not match declared purpose {purpose}. Suggest defaulting to "auto"'
                )
            if purpose == Purpose.PROPERTY and keys:
                concept.keys = set(keys)
        elif isinstance(source_value, CONSTANT_TYPES):
            concept = constant_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                metadata=metadata,
            )
        elif isinstance(source_value, ConceptRef):
            concept = arbitrary_to_concept(
                self.function_factory.create_function(
                    [source_value], FunctionType.ALIAS, meta=meta
                ),
                name=name,
                namespace=namespace,
                environment=self.environment,
                metadata=metadata,
            )
        else:
            raise SyntaxError(
                f"Received invalid type {type(args[2])} {args[2]} as input to concept derivation: `{self.text_lookup[self.token_address][meta.start_pos:meta.end_pos]}`"
            )
        if concept.metadata:
            concept.metadata.line_number = meta.line
            concept.metadata.column = meta.column
            concept.metadata.end_line = meta.end_line
            concept.metadata.end_column = meta.end_column
        self.environment.add_concept(concept, meta=meta)
        return ConceptDerivationStatement(concept=concept)

    @v_args(meta=True)
    def rowset_derivation_statement(
        self, meta: Meta, args
    ) -> RowsetDerivationStatement:
        name = args[0]
        select: SelectStatement | MultiSelectStatement = args[1]
        output = RowsetDerivationStatement(
            name=name,
            select=select,
            namespace=self.environment.namespace or DEFAULT_NAMESPACE,
        )

        for new_concept in rowset_to_concepts(output, self.environment):
            if new_concept.metadata:
                new_concept.metadata.line_number = meta.line
                new_concept.metadata.column = meta.column
                new_concept.metadata.end_line = meta.end_line
                new_concept.metadata.end_column = meta.end_column
            self.environment.add_concept(new_concept, force=True)

        self.environment.add_rowset(
            output.name, output.select.as_lineage(self.environment)
        )
        return output

    @v_args(meta=True)
    def constant_derivation(
        self, meta: Meta, args: tuple[Purpose, str, Any, Optional[Metadata]]
    ) -> Concept:

        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        constant: Union[str, float, int, bool, MapWrapper, ListWrapper] = args[2]
        lookup, namespace, name, parent = parse_concept_reference(
            name, self.environment
        )
        concept = Concept(
            name=name,
            datatype=arg_to_datatype(constant),
            purpose=Purpose.CONSTANT,
            metadata=metadata_from_meta(meta) if not metadata else metadata,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                output_datatype=arg_to_datatype(constant),
                output_purpose=Purpose.CONSTANT,
                arguments=[constant],
            ),
            grain=Grain(components=set()),
            namespace=namespace,
            granularity=Granularity.SINGLE_ROW,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
            concept.metadata.column = meta.column
            concept.metadata.end_line = meta.end_line
            concept.metadata.end_column = meta.end_column
        self.environment.add_concept(concept, meta)
        return concept

    @v_args(meta=True)
    def concept(self, meta: Meta, args) -> ConceptDeclarationStatement:
        if isinstance(args[0], list):
            # properties_declaration returns a list; concepts already added to env
            concept: Concept = args[0][0]
        elif isinstance(args[0], Concept):
            concept = args[0]
        else:
            concept = args[0].concept
        if concept.metadata:
            concept.metadata.line_number = meta.line
            concept.metadata.column = meta.column
            concept.metadata.end_line = meta.end_line
            concept.metadata.end_column = meta.end_column
        return ConceptDeclarationStatement(concept=concept)

    def column_assignment_list(self, args):
        return args

    def column_list(self, args) -> List:
        return args

    def grain_clause(self, args) -> Grain:
        return Grain(
            components=set([self.environment.concepts[a].address for a in args[0]])
        )

    @v_args(meta=True)
    def aggregate_by(self, meta: Meta, args):
        base = args[0]
        b_concept = base.value.split(" ")[-1]
        args = [self.environment.concepts[a] for a in [b_concept] + args[1:]]
        return self.function_factory.create_function(args, FunctionType.GROUP, meta)

    def whole_grain_clause(self, args) -> WholeGrainWrapper:
        return WholeGrainWrapper(where=args[0])

    def MULTILINE_STRING(self, args) -> str:
        return args[3:-3]

    def raw_column_assignment(self, args):
        return RawColumnExpr(text=args[1])

    def DATASOURCE_STATUS(self, args) -> DatasourceState:
        return DatasourceState(args.value.lower())

    @v_args(meta=True)
    def datasource_status_clause(self, meta: Meta, args):
        return args[1]

    @v_args(meta=True)
    def datasource_partition_clause(self, meta: Meta, args):
        return DatasourcePartitionClause([ConceptRef(address=arg) for arg in args[0]])

    @v_args(meta=True)
    def datasource_update_trigger_clause(self, meta: Meta, args):
        trigger_type = DatasourceUpdateTrigger(args[0].lower())
        if isinstance(args[1], str):
            if trigger_type != DatasourceUpdateTrigger.FRESHNESS:
                raise ValueError(
                    f"Probe scripts are only supported for freshness triggers, not {trigger_type.value}"
                )
            p = Path(args[1])
            if not p.is_absolute():
                p = (Path(self.environment.working_path) / p).resolve()
            return DatasourceFreshnessProbeClause(path=str(p))
        columns = [ConceptRef(address=arg) for arg in args[1]]
        return DatasourceUpdateTriggerClause(trigger_type=trigger_type, columns=columns)

    @v_args(meta=True)
    def datasource(self, meta: Meta, args):
        is_root = False
        is_partial = False
        if isinstance(args[0], Token) and args[0].lower() == "root":
            is_root = True
            args = args[1:]
        if isinstance(args[0], Modifier) and args[0] == Modifier.PARTIAL:
            is_partial = True
            args = args[1:]
        name = args[0]
        columns: List[ColumnAssignment] = args[1]
        grain: Optional[Grain] = None
        address: Optional[Address] = None
        where: Optional[WhereClause] = None
        non_partial_for: Optional[WhereClause] = None
        incremental_by: List[ConceptRef] = []
        partition_by: List[ConceptRef] = []
        freshness_by: List[ConceptRef] = []
        freshness_probe: Optional[str] = None
        datasource_status: DatasourceState = DatasourceState.PUBLISHED
        for val in args[1:]:
            if isinstance(val, Address):
                address = val
            elif isinstance(val, Grain):
                grain = val
            elif isinstance(val, WholeGrainWrapper):
                non_partial_for = val.where
            elif isinstance(val, Query):
                address = Address(location=val.text, type=AddressType.QUERY)
            elif isinstance(val, File):
                address = Address(
                    location=val.path,
                    write_location=val.write_path,
                    type=val.type,
                    exists=val.exists,
                )
            elif isinstance(val, WhereClause):
                where = val
            elif isinstance(val, DatasourceState):
                datasource_status = val
            elif isinstance(val, DatasourceFreshnessProbeClause):
                freshness_probe = val.path
            elif isinstance(val, DatasourceUpdateTriggerClause):
                if val.trigger_type == DatasourceUpdateTrigger.INCREMENTAL:
                    incremental_by = val.columns
                elif val.trigger_type == DatasourceUpdateTrigger.FRESHNESS:
                    freshness_by = val.columns
            elif isinstance(val, DatasourcePartitionClause):
                partition_by = val.columns
        if not address:
            raise ValueError(
                "Malformed datasource, missing address or query declaration"
            )

        if address and (address.is_file and not address.exists):
            datasource_status = DatasourceState.UNPOPULATED
        if is_partial:
            for pc in columns:
                if Modifier.PARTIAL not in pc.modifiers:
                    pc.modifiers.append(Modifier.PARTIAL)

        datasource = Datasource(
            name=name,
            columns=columns,
            # grain will be set by default from args
            # TODO: move to factory
            grain=grain,  # type: ignore
            address=address,
            namespace=self.environment.namespace,
            where=where,
            non_partial_for=non_partial_for,
            status=datasource_status,
            incremental_by=incremental_by,
            partition_by=partition_by,
            freshness_by=freshness_by,
            freshness_probe=freshness_probe,
            is_root=is_root,
            is_partial=is_partial,
        )
        if datasource.where:
            for x in datasource.where.concept_arguments:
                if x.address not in datasource.output_concepts:
                    raise ValueError(
                        f"Datasource {name} where condition depends on concept {x.address} that does not exist on the datasource, line {meta.line}."
                    )
        if self.parse_pass == ParsePass.VALIDATION:
            self.environment.add_datasource(datasource, meta=meta)
            # if we have any foreign keys on the datasource, we can
            # at this point optimize them to properties if they do not have other usage.
            for column in columns:
                # skip partial for now
                if not grain:
                    continue
                if column.concept.address in grain.components:
                    continue
                target_c = self.environment.concepts[column.concept.address]
                if target_c.purpose != Purpose.KEY:
                    continue

                key_inputs = grain.components
                eligible = True
                for key in key_inputs:
                    # never overwrite a key with a dependency on a property
                    # for example - binding a datasource with a grain of <x>.fun should
                    # never override the grain of x to <fun>
                    if column.concept.address in (
                        self.environment.concepts[key].keys or set()
                    ):
                        eligible = False
                if not eligible:
                    continue
                keys = [self.environment.concepts[grain] for grain in key_inputs]
                # target_c.purpose = Purpose.PROPERTY
                target_c.keys = set([x.address for x in keys])
                # target_c.grain = Grain(components={x.address for x in keys})

        return datasource

    @v_args(meta=True)
    def comment(self, meta: Meta, args):
        assert len(args) == 1
        return Comment(text=args[0].value)

    def PARSE_COMMENT(self, args):
        return Comment(text=args.value.rstrip())

    @v_args(meta=True)
    def select_transform(self, meta: Meta, args) -> ConceptTransform:
        output: str = args[1]
        transformation = unwrap_transformation(args[0], self.environment)
        lookup, namespace, output, parent = parse_concept_reference(
            output, self.environment
        )

        metadata = metadata_from_meta(meta, concept_source=ConceptSource.SELECT)
        concept = arbitrary_to_concept(
            transformation,
            environment=self.environment,
            namespace=namespace,
            name=output,
            metadata=metadata,
        )
        return ConceptTransform(function=transformation, output=concept)

    @v_args(meta=True)
    def concept_nullable_modifier(self, meta: Meta, args) -> Modifier:
        return Modifier.NULLABLE

    @v_args(meta=True)
    def select_hide_modifier(self, meta: Meta, args) -> Modifier:
        return Modifier.HIDDEN

    @v_args(meta=True)
    def select_partial_modifier(self, meta: Meta, args) -> Modifier:
        return Modifier.PARTIAL

    @v_args(meta=True)
    def select_item(self, meta: Meta, args) -> Optional[SelectItem]:
        modifiers = [arg for arg in args if isinstance(arg, Modifier)]
        args = [arg for arg in args if not isinstance(arg, (Modifier, Comment))]

        if not args:
            return None
        if len(args) != 1:
            raise ParseError(
                "Malformed select statement"
                f" {args} {self.text_lookup[self.parse_address][meta.start_pos:meta.end_pos]}"
            )
        content = args[0]
        if isinstance(content, ConceptTransform):
            return SelectItem(content=content, modifiers=modifiers)
        return SelectItem(
            content=content,
            modifiers=modifiers,
        )

    def select_list(self, args):
        return [arg for arg in args if arg]

    def limit(self, args):
        return Limit(count=int(args[0].value))

    def ordering(self, args: list[str]):
        base = "asc"
        null_sort: str | None = None
        if args:
            first = args[0].lower()
            if first in {"asc", "desc"}:
                base = first
                if len(args) > 1:
                    null_sort = args[-1].lower()
            else:
                null_sort = first
        if null_sort:
            return Ordering(" ".join([base, "nulls", null_sort]))
        return Ordering(base)

    def order_list(self, args) -> List[OrderItem]:
        return [
            OrderItem(
                expr=x,
                order=y,
            )
            for x, y in zip(args[::2], args[1::2])
        ]

    def order_by(self, args):
        return OrderBy(items=args[0])

    def over_component(self, args):
        return ConceptRef(address=args[0].value.lstrip(",").strip())

    def over_list(self, args):
        return [x for x in args]

    def PUBLISH_ACTION(self, args) -> PublishAction:
        action = args.value.lower()
        if action == "publish":
            return PublishAction.PUBLISH
        elif action == "unpublish":
            return PublishAction.UNPUBLISH
        else:
            raise SyntaxError(f"Unknown publish action: {action}")

    @v_args(meta=True)
    def publish_statement(self, meta: Meta, args) -> PublishStatement:
        targets = []
        scope = ValidationScope.DATASOURCES
        publish_action = PublishAction.PUBLISH
        for arg in args:
            if isinstance(arg, str):
                targets.append(arg)
            elif isinstance(arg, PublishAction):
                publish_action = arg
            elif isinstance(arg, ValidationScope):
                scope = arg
                if arg != ValidationScope.DATASOURCES:
                    raise SyntaxError(
                        f"Publishing is only supported for Datasources, got {arg} on line {meta.line}"
                    )
        return PublishStatement(
            scope=scope,
            targets=targets,
            action=publish_action,
        )

    def create_modifier_clause(self, args):
        token = args[0]
        if token.type == "CREATE_IF_NOT_EXISTS":
            return CreateMode.CREATE_IF_NOT_EXISTS
        elif token.type == "CREATE_OR_REPLACE":
            return CreateMode.CREATE_OR_REPLACE

    @v_args(meta=True)
    def create_statement(self, meta: Meta, args) -> CreateStatement:
        targets = []
        scope = ValidationScope.DATASOURCES
        create_mode = CreateMode.CREATE
        for arg in args:
            if isinstance(arg, str):
                targets.append(arg)
            elif isinstance(arg, ValidationScope):
                scope = arg
                if arg != ValidationScope.DATASOURCES:
                    raise SyntaxError(
                        f"Creating is only supported for Datasources, got {arg} on line {meta.line}"
                    )
            elif isinstance(arg, CreateMode):
                create_mode = arg

        return CreateStatement(scope=scope, targets=targets, create_mode=create_mode)

    def VALIDATE_SCOPE(self, args) -> ValidationScope:
        base: str = args.lower()
        if not base.endswith("s"):
            base += "s"
        return ValidationScope(base)

    @v_args(meta=True)
    def validate_statement(self, meta: Meta, args) -> ValidateStatement:
        if len(args) > 1:
            scope = args[0]
            targets = args[1:]
        elif len(args) == 0:
            scope = ValidationScope.ALL
            targets = None
        else:
            scope = args[0]
            targets = None
        return ValidateStatement(
            scope=scope,
            targets=targets,
        )

    @v_args(meta=True)
    def mock_statement(self, meta: Meta, args) -> MockStatement:
        return MockStatement(scope=args[0], targets=args[1:])

    @v_args(meta=True)
    def merge_statement(self, meta: Meta, args) -> MergeStatementV2 | None:
        modifiers = []
        cargs: list[str] = []
        source_wildcard = None
        target_wildcard = None
        for arg in args:
            if isinstance(arg, Modifier):
                modifiers.append(arg)
            else:
                cargs.append(arg)
        source, target = cargs
        if source.endswith(".*"):
            if not target.endswith(".*"):
                raise ValueError("Invalid merge, source is wildcard, target is not")
            source_wildcard = source[:-2]
            target_wildcard = target[:-2]
            sources: list[Concept] = [
                v
                for k, v in self.environment.concepts.items()
                if v.namespace == source_wildcard
            ]
            targets: dict[str, Concept] = {}
            for x in sources:
                target = target_wildcard + "." + x.name
                if target in self.environment.concepts:
                    targets[x.address] = self.environment.concepts[target]
            sources = [x for x in sources if x.address in targets]
        else:
            sources = [self.environment.concepts[source]]
            targets = {sources[0].address: self.environment.concepts[target]}

        if self.parse_pass == ParsePass.VALIDATION:
            for source_c in sources:
                if isinstance(source_c, UndefinedConceptFull):
                    raise SyntaxError(
                        f"Cannot merge non-existent source concept {source_c.address} on line: {meta.line}"
                    )
            new = MergeStatementV2(
                sources=sources,
                targets=targets,
                modifiers=modifiers,
                source_wildcard=source_wildcard,
                target_wildcard=target_wildcard,
            )
            for source_c in new.sources:
                self.environment.merge_concept(
                    source_c, targets[source_c.address], modifiers
                )

            return new
        return None

    @v_args(meta=True)
    def rawsql_statement(self, meta: Meta, args) -> RawSQLStatement:
        statement = RawSQLStatement(meta=metadata_from_meta(meta), text=args[0])
        return statement

    def COPY_TYPE(self, args) -> IOType:
        return IOType(args.value)

    @v_args(meta=True)
    def copy_statement(self, meta: Meta, args) -> CopyStatement:
        return CopyStatement(
            target=args[1],
            target_type=args[0],
            meta=metadata_from_meta(meta),
            select=args[-1],
        )

    def CHART_TYPE(self, args) -> ChartType:
        return ChartType(args.value.lower())

    def chart_field_setting(self, args) -> dict:
        field_name = args[0].value.lower()
        field_map = {
            "x_axis": "x_fields",
            "y_axis": "y_fields",
            "color": "color_field",
            "size": "size_field",
            "group": "group_field",
            "trellis": "trellis_field",
            "trellis_row": "trellis_row_field",
            "geo": "geo_field",
            "annotation": "annotation_field",
        }
        fields = [str(arg) for arg in args[1:]]
        key = field_map[field_name]
        if key in ("x_fields", "y_fields"):
            return {key: fields}
        return {key: fields[0] if fields else None}

    def chart_bool_setting(self, args) -> dict:
        bool_map = {"hide_legend": "hide_legend", "show_title": "show_title"}
        return {bool_map[args[0].value.lower()]: True}

    def chart_scale_setting(self, args) -> dict:
        scale_map = {"scale_x": "scale_x", "scale_y": "scale_y"}
        return {scale_map[args[0].value.lower()]: args[1].value.lower()}

    def chart_setting(self, args) -> dict:
        return args[0]

    @v_args(meta=True)
    def chart_statement(self, meta: Meta, args) -> ChartStatement:
        chart_type = args[0]
        settings: dict = {"x_fields": [], "y_fields": []}
        select = args[-1]

        for arg in args[1:-1]:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    if k in ("x_fields", "y_fields"):
                        settings[k].extend(v)
                    else:
                        settings[k] = v

        return ChartStatement(
            config=ChartConfig(chart_type=chart_type, **settings),
            select=select,
            meta=metadata_from_meta(meta),
        )

    def resolve_import_address(self, address: str, is_stdlib: bool = False) -> str:
        if (
            isinstance(
                self.environment.config.import_resolver, FileSystemImportResolver
            )
            or is_stdlib
        ):
            with open(address, "r", encoding="utf-8") as f:
                text = f.read()
        elif isinstance(self.environment.config.import_resolver, DictImportResolver):
            lookup = address
            if lookup not in self.environment.config.import_resolver.content:
                raise ImportError(
                    f"Unable to import file {lookup}, not resolvable from provided source files."
                )
            text = self.environment.config.import_resolver.content[lookup]
        else:
            raise ImportError(
                f"Unable to import file {address}, resolver type {type(self.environment.config.import_resolver)} not supported"
            )
        return text

    def IMPORT_DOT(self, args) -> str:
        return "."

    def _resolve_import_path(
        self, raw_args: list[str]
    ) -> tuple[str, str, str, str, Path | str, bool]:
        """Parse raw import args into (alias, cache_key, input_path, target, token_lookup, is_stdlib)."""
        parent_dirs = -1
        parsed_args: list[str] = []
        for x in raw_args:
            if x == ".":
                parent_dirs += 1
            else:
                parsed_args.append(str(x))
        parent_dirs = max(parent_dirs, 0)
        if len(parsed_args) == 2:
            alias = parsed_args[-1]
            cache_key = parsed_args[-1]
        else:
            alias = self.environment.namespace
            cache_key = parsed_args[0]
        input_path = parsed_args[0]

        path = input_path.split(".")
        is_stdlib = path[0] == "std"
        if is_stdlib:
            target = join(STDLIB_ROOT, *path) + ".preql"
            token_lookup: Path | str = Path(target)
        elif isinstance(
            self.environment.config.import_resolver, FileSystemImportResolver
        ):
            troot = Path(self.environment.working_path)
            for _ in range(parent_dirs):
                troot = troot.parent
            target = join(troot, *path) + ".preql"
            token_lookup = Path(target)
        elif isinstance(self.environment.config.import_resolver, DictImportResolver):
            target = ".".join(path)
            token_lookup = target
        else:
            raise NotImplementedError
        return alias, cache_key, input_path, target, token_lookup, is_stdlib

    def process_import_summary(
        self,
        raw_path: str,
        alias: str | None,
        is_stdlib: bool,
        parent_dirs: int = 0,
        is_self: bool = False,
        concepts: list[str] | None = None,
    ) -> ImportStatement | None:
        if is_self:
            return None
        input_path = raw_path
        effective_alias = alias if alias is not None else self.environment.namespace
        cache_key = effective_alias if alias is not None else input_path
        path = input_path.split(".")
        if is_stdlib:
            target = join(STDLIB_ROOT, *path) + ".preql"
            token_lookup: Path | str = Path(target)
        elif isinstance(
            self.environment.config.import_resolver, FileSystemImportResolver
        ):
            troot = Path(self.environment.working_path)
            for _ in range(parent_dirs):
                troot = troot.parent
            target = join(troot, *path) + ".preql"
            token_lookup = Path(target)
        elif isinstance(self.environment.config.import_resolver, DictImportResolver):
            target = ".".join(path)
            token_lookup = target
        else:
            raise NotImplementedError
        statement = self._process_import(
            alias=effective_alias,
            input_path=input_path,
            target=target,
            token_lookup=token_lookup,
            cache_key=cache_key,
            is_stdlib=is_stdlib,
            concepts=concepts,
        )
        return statement

    def process_datasource_summary(
        self,
        *,
        name: str,
        is_root: bool,
        is_partial: bool,
        columns_text: str,
        grain_text: str | None,
        complete_text: str | None,
        address_text: str | None,
        query_text: str | None,
        file_text: str | None,
        where_text: str | None,
        update_text: str | None,
        partition_text: str | None,
        status_text: str | None,
    ) -> Datasource | None:
        parsed_columns = self._parse_simple_column_assignment_list_text(columns_text)
        if parsed_columns is None:
            parsed_columns = self._parse_fragment_text(
                "column_assignment_list", columns_text
            )
        if not isinstance(parsed_columns, list):
            return None

        args: list[Any] = []
        if is_root:
            args.append(Token("DATASOURCE_ROOT", "root"))
        if is_partial:
            args.append(Modifier.PARTIAL)
        args.extend([name, parsed_columns])

        clause_rules: list[
            tuple[str, str | None, Callable[[str], object | None] | None]
        ] = [
            ("grain_clause", grain_text, self._parse_simple_grain_clause_text),
            ("whole_grain_clause", complete_text, None),
            ("address", address_text, self._parse_simple_address_clause_text),
            ("query", query_text, None),
            ("file", file_text, None),
            ("where", where_text, None),
            ("datasource_update_trigger_clause", update_text, None),
            ("datasource_partition_clause", partition_text, None),
            (
                "datasource_status_clause",
                status_text,
                self._parse_simple_datasource_status_clause_text,
            ),
        ]
        for rule, clause_text, simple_parser in clause_rules:
            if clause_text:
                parsed_clause = simple_parser(clause_text) if simple_parser else None
                args.append(
                    parsed_clause
                    if parsed_clause is not None
                    else self._parse_fragment_text(rule, clause_text)
                )
        original_parse_pass = self.parse_pass
        try:
            self.parse_pass = ParsePass.VALIDATION
            return self.datasource(Meta(), args)
        finally:
            self.parse_pass = original_parse_pass

    def import_statement(self, args: list[str]) -> ImportStatement:
        alias, cache_key, input_path, target, token_lookup, is_stdlib = (
            self._resolve_import_path(args)
        )
        return self._process_import(
            alias=alias,
            input_path=input_path,
            target=target,
            token_lookup=token_lookup,
            cache_key=cache_key,
            is_stdlib=is_stdlib,
        )

    def import_concepts(self, args) -> list[str]:
        return [str(a) for a in args]

    def selective_import_statement(self, args) -> ImportStatement:
        # concepts_list is a list[str] returned by import_concepts
        concepts: list[str] = next(a for a in args if isinstance(a, list))
        alias, cache_key, input_path, target, token_lookup, is_stdlib = (
            self._resolve_import_path([a for a in args if not isinstance(a, list)])
        )
        return self._process_import(
            alias=alias,
            input_path=input_path,
            target=target,
            token_lookup=token_lookup,
            cache_key=cache_key,
            is_stdlib=is_stdlib,
            concepts=concepts,
        )

    def self_import_statement(self, args: list[str]) -> ImportStatement:
        alias = args[-1]
        is_dict_resolver = isinstance(
            self.environment.config.import_resolver, DictImportResolver
        )

        if is_dict_resolver:
            # For DictImportResolver, self is at path '.'
            input_path = "."
            path = Path(".")
        elif self.environment.env_file_path is not None:
            env_file_path = self.environment.env_file_path
            if isinstance(env_file_path, str):
                env_file_path = Path(env_file_path)
            input_path = str(env_file_path.stem)
            path = env_file_path
        else:
            raise ImportError(
                "Cannot use 'self import' without a file path context. "
                "This typically means the environment was not loaded from a file."
            )

        # Defer self-import until after parsing is complete
        self._pending_self_imports.append(alias)
        return ImportStatement(
            alias=alias,
            input_path=input_path,
            path=path,
            is_self=True,
        )

    def _process_import(
        self,
        alias: str,
        input_path: str,
        target: str,
        token_lookup: Path | str,
        cache_key: str,
        is_stdlib: bool = False,
        is_self: bool = False,
        concepts: list[str] | None = None,
    ) -> ImportStatement:
        start = datetime.now()
        is_file_resolver = isinstance(
            self.environment.config.import_resolver, FileSystemImportResolver
        )
        key_path = self.import_keys + [cache_key]
        cache_lookup = "-".join(key_path)

        if len(key_path) > self.max_parse_depth:
            return ImportStatement(
                alias=alias, input_path=input_path, path=Path(target), is_self=is_self
            )

        if token_lookup in self.tokens:
            perf_logger.debug(f"\tTokens cached for {token_lookup}")
            raw_tokens = self.tokens[token_lookup]
            text = self.text_lookup[token_lookup]
        else:
            perf_logger.debug(f"\tTokens not cached for {token_lookup}, resolving")
            text = self.resolve_import_address(target, is_stdlib)
            self.text_lookup[token_lookup] = text

            raw_tokens = None
            if not rust_hybrid_enabled():
                try:
                    raw_tokens = _get_parser().parse(text, start="start")
                except Exception as e:
                    raise ImportError(
                        f"Unable to import '{target}', parsing error: {e}"
                    ) from e
                self.tokens[token_lookup] = raw_tokens

        if cache_lookup in self.parsed:
            perf_logger.debug(f"\tEnvironment cached for {token_lookup}")
            nparser = self.parsed[cache_lookup]
            new_env = nparser.environment
            if nparser.parse_pass != ParsePass.VALIDATION:
                second_pass_start = datetime.now()
                nparser.run_second_parse_pass()
                second_pass_end = datetime.now()
                perf_logger.debug(
                    f"{second_pass_end - second_pass_start} seconds | Import {alias} key ({cache_key}) second pass took {second_pass_end - second_pass_start} to parse, {len(new_env.concepts)} concepts"
                )
        else:
            perf_logger.debug(f"\tParsing new for {token_lookup}")
            root = None
            if "." in str(token_lookup):
                root = str(token_lookup).rsplit(".", 1)[0]
            try:
                new_env = Environment(
                    working_path=dirname(target),
                    env_file_path=token_lookup,
                    config=self.environment.config.copy_for_root(root=root),
                    parameters=self.environment.parameters,
                )
                new_env.concepts.fail_on_missing = False
                self.parsed[self.parse_address] = self
                nparser = ParseToObjects(
                    environment=new_env,
                    parse_address=cache_lookup,
                    token_address=token_lookup,
                    parsed=self.parsed,
                    tokens=self.tokens,
                    text_lookup=self.text_lookup,
                    import_keys=self.import_keys + [cache_key],
                    parse_config=self.parse_config,
                    max_parse_depth=self.max_parse_depth,
                )
                nparser.set_text(text)
                if rust_hybrid_enabled():
                    _parse_text_hybrid_with_parser(
                        text,
                        environment=new_env,
                        hybrid_parser=nparser,
                        root=Path(dirname(target)),
                        parse_config=self.parse_config,
                        run_compare=False,
                    )
                    nparser.parse_pass = ParsePass.VALIDATION
                else:
                    assert raw_tokens is not None
                    nparser.transform(raw_tokens)
                self.parsed[cache_lookup] = nparser
            except Exception as e:
                raise ImportError(
                    f"Unable to import '{target}', parsing error: {e}"
                ) from e

        parsed_path = Path(input_path)
        imps = ImportStatement(
            alias=alias,
            input_path=input_path,
            path=parsed_path,
            is_self=is_self,
            concepts=concepts,
        )

        self.environment.add_import(
            alias,
            new_env,
            Import(
                alias=alias,
                path=parsed_path,
                input_path=Path(target) if is_file_resolver else None,
                concepts=concepts,
            ),
        )
        end = datetime.now()
        perf_logger.debug(
            f"{end - start} seconds | Import {alias} key ({cache_key}) took  to parse, {len(new_env.concepts)} concepts"
        )
        return imps

    @v_args(meta=True)
    def show_category(self, meta: Meta, args) -> ShowCategory:
        return ShowCategory(args[0])

    @v_args(meta=True)
    def show_statement(self, meta: Meta, args) -> ShowStatement | None:
        if self.parse_pass != ParsePass.VALIDATION:
            return None
        return ShowStatement(content=args[0])

    @v_args(meta=True)
    def persist_partition_clause(self, meta: Meta, args) -> DatasourcePartitionClause:
        return DatasourcePartitionClause([ConceptRef(address=a) for a in args[0]])

    @v_args(meta=True)
    def PERSIST_MODE(self, args) -> PersistMode:
        base = args.value.lower()
        if base == "persist":
            return PersistMode.OVERWRITE
        return PersistMode(base)

    @v_args(meta=True)
    def auto_persist(self, meta: Meta, args) -> PersistStatement | None:
        if self.parse_pass != ParsePass.VALIDATION:
            return None
        persist_mode = args[0]
        target_name = args[1]
        where = args[2] if len(args) > 2 else None

        if target_name not in self.environment.datasources:
            raise SyntaxError(
                f"Auto persist target datasource {target_name} does not exist in environment on line {meta.line}. Have {list(self.environment.datasources.keys())}"
            )
        target = self.environment.datasources[target_name]
        select: SelectStatement = target.create_update_statement(
            self.environment, where, line_no=meta.line
        )
        return PersistStatement(
            select=select,
            datasource=target,
            persist_mode=persist_mode,
            partition_by=target.incremental_by,
            meta=metadata_from_meta(meta),
        )

    @v_args(meta=True)
    def full_persist(self, meta: Meta, args) -> PersistStatement | None:
        if self.parse_pass != ParsePass.VALIDATION:
            return None
        partition_clause = DatasourcePartitionClause([])
        labels = [x for x in args if isinstance(x, str)]
        for x in args:
            if isinstance(x, DatasourcePartitionClause):
                partition_clause = x
        if len(labels) == 2:
            identifier = labels[0]
            address = labels[1]
        else:
            identifier = labels[0]
            address = None
        target: Datasource | None = self.environment.datasources.get(identifier)

        if not address and not target:
            raise SyntaxError(
                f'Append statement without concrete table address on line {meta.line} attempts to insert into datasource "{identifier}" that cannot be found in the environment. Add a physical address to create a new datasource, or check the name.'
            )
        elif target:
            address = target.safe_address

        assert address is not None

        modes = [x for x in args if isinstance(x, PersistMode)]
        mode = modes[0] if modes else PersistMode.OVERWRITE
        select: SelectStatement = [x for x in args if isinstance(x, SelectStatement)][0]

        if mode == PersistMode.APPEND:
            if target is None:
                raise SyntaxError(
                    f"Cannot append to non-existent datasource {identifier} on line {meta.line}."
                )
            new_datasource: Datasource = target
            if not new_datasource.partition_by == partition_clause.columns:
                raise SyntaxError(
                    f"Cannot append to datasource {identifier} with different partitioning scheme then insert on line {meta.line}. Datasource partitioning: {new_datasource.partition_by}, insert partitioning: {partition_clause.columns if partition_clause else '[]'}"
                )
            if len(partition_clause.columns) > 1:
                raise NotImplementedError(
                    "Incremental partition overwrites by more than 1 column are not yet supported."
                )
            for x in partition_clause.columns:
                concept = self.environment.concepts[x.address]
                if concept.output_datatype not in SUPPORTED_INCREMENTAL_TYPES:
                    raise SyntaxError(
                        f"Cannot incremental persist on concept {concept.address} of type {concept.output_datatype} on line {meta.line}."
                    )
        elif target:
            new_datasource = target
        else:
            new_datasource = select.to_datasource(
                namespace=(
                    self.environment.namespace
                    if self.environment.namespace
                    else DEFAULT_NAMESPACE
                ),
                name=identifier,
                address=Address(location=address),
                grain=select.grain,
                environment=self.environment,
            )
        return PersistStatement(
            select=select,
            datasource=new_datasource,
            persist_mode=mode,
            partition_by=partition_clause.columns if partition_clause else [],
            meta=metadata_from_meta(meta),
        )

    @v_args(meta=True)
    def persist_statement(self, meta: Meta, args) -> PersistStatement:
        return args[0]

    @v_args(meta=True)
    def align_item(self, meta: Meta, args) -> AlignItem:
        return AlignItem(
            alias=args[0],
            namespace=self.environment.namespace,
            concepts=[self.environment.concepts[arg].reference for arg in args[1:]],
        )

    @v_args(meta=True)
    def align_clause(self, meta: Meta, args) -> AlignClause:
        return AlignClause(items=args)

    @v_args(meta=True)
    def derive_item(self, meta: Meta, args) -> DeriveItem:
        return DeriveItem(
            expr=args[0], name=args[1], namespace=self.environment.namespace
        )

    @v_args(meta=True)
    def derive_clause(self, meta: Meta, args) -> DeriveClause:

        return DeriveClause(items=args)

    @v_args(meta=True)
    def multi_select_statement(self, meta: Meta, args) -> MultiSelectStatement:

        selects: list[SelectStatement] = []
        align: AlignClause | None = None
        limit: int | None = None
        order_by: OrderBy | None = None
        where: WhereClause | None = None
        having: HavingClause | None = None
        derive: DeriveClause | None = None
        for arg in args:
            atype = type(arg)
            if atype is SelectStatement:
                selects.append(arg)
            elif atype is Limit:
                limit = arg.count
            elif atype is OrderBy:
                order_by = arg
            elif atype is WhereClause:
                where = arg
            elif atype is HavingClause:
                having = arg
            elif atype is AlignClause:
                align = arg
            elif atype is DeriveClause:
                derive = arg

        assert align
        assert align is not None

        derived_concepts = []
        new_selects = [x.as_lineage(self.environment) for x in selects]
        lineage = MultiSelectLineage(
            selects=new_selects,
            align=align,
            derive=derive,
            namespace=self.environment.namespace,
            where_clause=where,
            having_clause=having,
            limit=limit,
            hidden_components=set(y for x in new_selects for y in x.hidden_components),
        )
        for x in align.items:
            concept = align_item_to_concept(
                x,
                align,
                selects,
                where=where,
                having=having,
                limit=limit,
                environment=self.environment,
            )
            derived_concepts.append(concept)
            self.environment.add_concept(concept, meta=meta)
        if derive:
            for derived in derive.items:
                derivation = derived.expr
                name = derived.name
                if not isinstance(derivation, (Function, Comparison, WindowItem)):
                    raise SyntaxError(
                        f"Invalid derive expression {derivation} in {meta.line}, must be a function or conditional"
                    )
                concept = derive_item_to_concept(
                    derivation, name, lineage, self.environment.namespace
                )
                derived_concepts.append(concept)
                self.environment.add_concept(concept, meta=meta)
        multi = MultiSelectStatement(
            selects=selects,
            align=align,
            namespace=self.environment.namespace,
            where_clause=where,
            order_by=order_by,
            limit=limit,
            meta=metadata_from_meta(meta),
            derived_concepts=derived_concepts,
            derive=derive,
        )
        return multi

    @v_args(meta=True)
    def from_clause(self, meta: Meta, args) -> FromClause:
        sources = [str(arg) for arg in args]
        return FromClause(sources=sources)

    @v_args(meta=True)
    def select_statement(self, meta: Meta, args) -> SelectStatement:
        select_items: List[SelectItem] | None = None
        limit: int | None = None
        order_by: OrderBy | None = None
        from_clause: FromClause | None = None
        where = None
        having = None
        for arg in args:
            atype = type(arg)
            if atype is list:
                select_items = arg
            elif atype is Limit:
                limit = arg.count
            elif atype is OrderBy:
                order_by = arg
            elif atype is FromClause:
                from_clause = arg
            elif atype is WhereClause:
                if where is not None:
                    raise ParseError(
                        "Multiple where clauses defined are not supported!"
                    )
                where = arg
            elif atype is HavingClause:
                having = arg
        if not select_items:
            raise ParseError("Malformed select, missing select items")
        pre_keys = set(self.environment.concepts.keys())
        base = SelectStatement.from_inputs(
            environment=self.environment,
            selection=select_items,
            order_by=order_by,
            where_clause=where,
            having_clause=having,
            limit=limit,
            eligible_datasources=from_clause.sources if from_clause else None,
            meta=metadata_from_meta(meta),
        )
        if (
            self.parse_pass == ParsePass.INITIAL
            and self.parse_config.strict_name_shadow_enforcement
        ):
            intersection = base.locally_derived.intersection(pre_keys)
            if intersection:
                for x in intersection:
                    if str(base.local_concepts[x].lineage) == str(
                        self.environment.concepts[x].lineage
                    ):
                        local = base.local_concepts[x]
                        friendly_name = (
                            local.name
                            if local.namespace == DEFAULT_NAMESPACE
                            else local.namespace
                        )
                        raise NameShadowError(
                            f"Select statement {base} creates a new concept '{friendly_name}' with identical definition as the existing concept '{friendly_name}'. Replace {base.local_concepts[x].lineage} with a direct reference to {friendly_name}."
                        )
                else:
                    raise NameShadowError(
                        f"Select statement {base} creates new named concepts from calculations {list(intersection)} with identical name(s) to existing concept(s). Use new unique names for these."
                    )
        return base

    @v_args(meta=True)
    def address(self, meta: Meta, args):
        return args[0]

    @v_args(meta=True)
    def query(self, meta: Meta, args):
        return Query(text=args[0])

    @v_args(meta=True)
    def file(self, meta: Meta, args):
        write_path: str | None
        if len(args) == 2:
            read_path, write_path = args
        else:
            read_path = args[0]
            write_path = None
        exists = True

        def process_path(ipath: str) -> tuple[str, str, bool]:
            # Cloud storage URLs should be used as-is without path resolution
            cloud_prefixes = ("gcs://", "gs://", "s3://", "https://", "http://")
            is_cloud = ipath.startswith(cloud_prefixes)

            if is_cloud:
                base = ipath
                suffix = "." + ipath.rsplit(".", 1)[-1] if "." in ipath else ""
            else:
                path = Path(ipath)
                # if it's a relative path, look it up relative to current parsing directory
                if path.is_relative_to("."):
                    path = Path(self.environment.working_path) / path
                base = str(path.resolve().absolute())
                suffix = path.suffix
            exists = False if not is_cloud and not Path(base).exists() else True
            return base, suffix, exists

        read_base, suffix, exists = process_path(read_path)
        write_base, write_suffix, _ = (
            process_path(write_path) if write_path else (None, None, False)
        )

        if suffix == ".sql":
            return File(
                path=read_base,
                write_path=write_base,
                type=AddressType.SQL,
                exists=exists,
            )
        elif suffix == ".py":
            return File(
                path=read_base,
                write_path=write_base,
                type=AddressType.PYTHON_SCRIPT,
                exists=exists,
            )
        elif suffix == ".csv":
            return File(
                path=read_base,
                write_path=write_base,
                type=AddressType.CSV,
                exists=exists,
            )
        elif suffix == ".tsv":
            return File(
                path=read_base,
                write_path=write_base,
                type=AddressType.TSV,
                exists=exists,
            )
        elif suffix == ".parquet":
            return File(
                path=read_base,
                write_path=write_base,
                type=AddressType.PARQUET,
                exists=exists,
            )
        else:
            raise ParseError(
                f"Unsupported file type {suffix} for path {read_path} on line {meta.line}"
            )

    def where(self, args):
        root = args[0]
        root = expr_to_boolean(root, self.function_factory)
        return WhereClause(conditional=root)

    def having(self, args):
        root = args[0]
        if not isinstance(root, (Comparison, Conditional, Parenthetical)):
            if arg_to_datatype(root) == DataType.BOOL:
                root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
            else:
                root = Comparison(
                    left=root,
                    right=MagicConstants.NULL,
                    operator=ComparisonOperator.IS_NOT,
                )
        return HavingClause(conditional=root)

    @v_args(meta=True)
    def function_binding_list(self, meta: Meta, args) -> list[ArgBinding]:
        return args

    @v_args(meta=True)
    def function_binding_type(self, meta: Meta, args) -> FunctionBindingType:
        return FunctionBindingType(type=args[0])

    @v_args(meta=True)
    def function_binding_default(self, meta: Meta, args):
        return args[1]

    @v_args(meta=True)
    def function_binding_item(self, meta: Meta, args) -> ArgBinding:
        default = None
        type = None
        for arg in args[1:]:
            if isinstance(arg, FunctionBindingType):
                type = arg.type
            else:
                default = arg
        return ArgBinding(
            name=args[0], datatype=type or DataType.UNKNOWN, default=default
        )

    @v_args(meta=True)
    def raw_function(self, meta: Meta, args) -> FunctionDeclaration:
        identity = args[0]
        function_arguments: list[ArgBinding] = args[1]
        output = args[2]

        self.environment.functions[identity] = CustomFunctionFactory(
            function=output,
            namespace=self.environment.namespace,
            function_arguments=function_arguments,
            name=identity,
        )
        return FunctionDeclaration(name=identity, args=function_arguments, expr=output)

    @v_args(meta=True)
    def table_function(self, meta: Meta, args) -> FunctionDeclaration:
        identity = args[0]
        idx = 1
        function_arguments: list[ArgBinding] = []
        if idx < len(args) and isinstance(args[idx], list):
            function_arguments = args[idx]
            idx += 1
        content = args[idx]
        if isinstance(content, Concept):
            content = content.reference
        idx += 1
        where = None
        order_by: list[OrderItem] = []
        limit = None
        for arg in args[idx:]:
            if isinstance(arg, WhereClause):
                where = arg
            elif isinstance(arg, list):
                order_by = arg
            elif isinstance(arg, int):
                limit = arg
        subselect = SubselectItem(
            content=content,
            where=where,
            order_by=order_by,
            limit=limit,
        )
        self.environment.functions[identity] = CustomFunctionFactory(
            function=subselect,
            namespace=self.environment.namespace,
            function_arguments=function_arguments,
            name=identity,
        )
        return FunctionDeclaration(
            name=identity, args=function_arguments, expr=subselect
        )

    def custom_function(self, args) -> FunctionCallWrapper:
        name = args[0]
        call_args = args[1:]
        remapped = FunctionCallWrapper(
            content=self.environment.functions[name](*call_args),
            name=name,
            args=call_args,
        )

        return remapped

    @v_args(meta=True)
    def function(self, meta: Meta, args) -> Function:
        return args[0]

    @v_args(meta=True)
    def type_drop_clause(self, meta: Meta, args) -> DropOn:
        return DropOn([FunctionType(x) for x in args])

    @v_args(meta=True)
    def type_add_clause(self, meta: Meta, args) -> AddOn:
        return AddOn([FunctionType(x) for x in args])

    @v_args(meta=True)
    def type_declaration(self, meta: Meta, args) -> TypeDeclaration:
        key = args[0]
        datatype: list[DataType] = [x for x in args[1:] if isinstance(x, DataType)]
        if len(datatype) == 1:
            final_datatype: list[DataType] | DataType = datatype[0]
        else:
            final_datatype = datatype
        add_on = None
        drop_on = None
        for x in args[1:]:
            if isinstance(x, AddOn):
                add_on = x
            elif isinstance(x, DropOn):
                drop_on = x
        new = CustomType(
            name=key,
            type=final_datatype,
            drop_on=drop_on.functions if drop_on else [],
            add_on=add_on.functions if add_on else [],
        )
        self.environment.data_types[key] = new
        return TypeDeclaration(type=new)

    def int_lit(self, args):
        return int("".join(args))

    def bool_lit(self, args):
        return args[0].capitalize() == "True"

    def null_lit(self, args):
        return NULL_VALUE

    def float_lit(self, args):
        return float(args[0])

    def array_lit(self, args):
        return list_to_wrapper(args)

    def tuple_lit(self, args):
        return tuple_to_wrapper(args)

    def string_lit(self, args) -> str:
        if not args:
            return ""

        return args[0]

    @v_args(meta=True)
    def struct_lit(self, meta, args):
        return self.function_factory.create_function(
            args, operator=FunctionType.STRUCT, meta=meta
        )

    def map_lit(self, args):
        parsed = dict(zip(args[::2], args[1::2]))
        wrapped = dict_to_map_wrapper(parsed)
        return wrapped

    def literal(self, args):
        return args[0]

    def product_operator(self, args) -> Function | Any:
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            new_result = None
            op = args[i]
            right = args[i + 1]
            if op == "*":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.MULTIPLY
                )
            elif op == "**":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.POWER
                )
            elif op == "/":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.DIVIDE
                )
            elif op == "%":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.MOD
                )
            else:
                raise ValueError(f"Unknown operator: {op}")
            result = new_result
        return new_result

    def PLUS_OR_MINUS(self, args) -> str:
        return args.value

    def MULTIPLY_DIVIDE_PERCENT(self, args) -> str:
        return args.value

    @v_args(meta=True)
    def sum_operator(self, meta: Meta, args) -> Function | Any:
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            new_result = None
            op = args[i].lower()
            right = args[i + 1]
            if op == "+":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.ADD, meta=meta
                )
            elif op == "-":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.SUBTRACT, meta=meta
                )
            elif op == "||":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.CONCAT, meta=meta
                )
            elif op == "like":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.LIKE, meta=meta
                )
            elif op == "ilike":
                new_result = self.function_factory.create_function(
                    [result, right], operator=FunctionType.ILIKE, meta=meta
                )
            else:
                raise ValueError(f"Unknown operator: {op}")
            result = new_result
        return result

    def comparison(self, args) -> Comparison:
        if len(args) == 1:
            return args[0]
        left = args[0]
        right = args[2]
        if args[1] in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
            return SubselectComparison(
                left=left,
                right=right,
                operator=args[1],
            )
        # Validate that literal values compared against enum-typed concepts are valid
        for concept_side, value_side in ((left, right), (right, left)):
            if isinstance(concept_side, ConceptRef) and isinstance(
                concept_side.datatype, EnumType
            ):
                if (
                    isinstance(value_side, (str, int))
                    and value_side not in concept_side.datatype.values
                ):
                    raise InvalidSyntaxException(
                        f"Value {value_side!r} is not a valid member of enum {concept_side.datatype} for '{concept_side.address}'"
                    )
        return Comparison(left=left, right=right, operator=args[1])

    def between_comparison(self, args) -> Conditional:
        left_bound = args[1]
        right_bound = args[2]
        return Conditional(
            left=Comparison(
                left=args[0], right=left_bound, operator=ComparisonOperator.GTE
            ),
            right=Comparison(
                left=args[0], right=right_bound, operator=ComparisonOperator.LTE
            ),
            operator=BooleanOperator.AND,
        )

    @v_args(meta=True)
    def subselect_comparison(self, meta: Meta, args) -> SubselectComparison:
        right = args[2]

        while isinstance(right, Parenthetical) and isinstance(
            right.content,
            (
                Concept,
                Function,
                FilterItem,
                WindowItem,
                AggregateWrapper,
                ListWrapper,
                TupleWrapper,
            ),
        ):
            right = right.content
        if isinstance(right, (Function, FilterItem, WindowItem, AggregateWrapper)):
            right_concept = arbitrary_to_concept(right, environment=self.environment)
            self.environment.add_concept(right_concept, meta=meta)
            right = right_concept.reference
        return SubselectComparison(
            left=args[0],
            right=right,
            operator=args[1],
        )

    def expr_tuple(self, args):
        datatypes = set([arg_to_datatype(x) for x in args])
        if len(datatypes) != 1:
            raise ParseError("Tuple must have same type for all elements")
        return TupleWrapper(val=tuple(args), type=datatypes.pop())

    def parenthetical(self, args):
        return Parenthetical(content=args[0])

    @v_args(meta=True)
    def condition_parenthetical(self, meta, args):
        if len(args) == 2:
            return Comparison(
                left=Parenthetical(content=args[1]),
                right=False,
                operator=ComparisonOperator.EQ,
            )
        return Parenthetical(content=args[0])

    def conditional(self, args):
        def munch_args(args):
            while args:
                if len(args) == 1:
                    return args[0]
                else:
                    return Conditional(
                        left=args[0], operator=args[1], right=munch_args(args[2:])
                    )

        return munch_args(args)

    def window_order(self, args):
        return WindowOrder(args[0])

    def window_order_by(self, args):
        # flatten tree
        return args[0]

    def window(self, args):

        return Window(count=args[1].value, window_order=args[0])

    def WINDOW_TYPE_LEGACY(self, args):
        return WindowType(args.strip())

    def WINDOW_TYPE_SQL(self, args):
        # Parse function name from pattern like "row_number(" or "lag ("
        name = args.strip().rstrip("(").strip()
        return WindowType(name)

    def window_item_over(self, args):
        return WindowItemOver(contents=args[0])

    def window_item_order(self, args):
        return WindowItemOrder(contents=args[0])

    def window_sql_partition(self, args):
        return WindowItemOver(contents=args[0])

    def window_sql_order(self, args):
        return WindowItemOrder(contents=args[0])

    def window_sql_over(self, args):
        over = []
        order = []
        for item in args:
            if isinstance(item, WindowItemOver):
                over = item.contents
            elif isinstance(item, WindowItemOrder):
                order = item.contents
        return {"over": over, "order": order}

    def logical_operator(self, args):
        return BooleanOperator(args[0].value.lower())

    def DATE_PART(self, args):
        return DatePart(args.value)

    def window_item(self, args) -> WindowItem | AggregateWrapper:
        # Pass through - the actual parsing happens in window_item_legacy or window_item_sql
        item: WindowItem = args[0]
        # Optimization: window functions without ORDER BY are equivalent to aggregates
        # e.g., sum(x) over (partition by a) == sum(x) by a
        if not item.order_by and item.type in WINDOW_TO_AGGREGATE_MAP:
            func = self.function_factory.create_function(
                [item.content], WINDOW_TO_AGGREGATE_MAP[item.type]
            )
            return AggregateWrapper(function=func, by=list(item.over))
        return item

    @v_args(meta=True)
    def window_item_legacy(self, meta: Meta, args) -> WindowItem:
        type: WindowType = args[0]
        order_by = []
        over = []
        index = None
        concept: Concept | None = None
        for item in args:
            if isinstance(item, int):
                index = item
            elif isinstance(item, WindowItemOrder):
                order_by = item.contents
            elif isinstance(item, WindowItemOver):
                over = item.contents
            elif isinstance(item, str):
                concept = self.environment.concepts[item]
            elif isinstance(item, ConceptRef):
                concept = self.environment.concepts[item.address]
            elif isinstance(item, WindowType):
                type = item
            else:
                concept = arbitrary_to_concept(item, environment=self.environment)
                self.environment.add_concept(concept, meta=meta)
        if not concept:
            raise ParseError(
                f"Window statements must be on fields, not constants - error in: `{self.text_lookup[self.parse_address][meta.start_pos:meta.end_pos]}`"
            )
        return WindowItem(
            type=type,
            content=concept.reference,
            over=over,
            order_by=order_by,
            index=index,
        )

    @v_args(meta=True)
    def window_item_sql(self, meta: Meta, args) -> WindowItem:
        type: WindowType = args[0]
        order_by: list = []
        over: list = []
        index: int | None = None
        concept: Concept | None = None

        for item in args:
            if isinstance(item, int):
                index = item
            elif isinstance(item, dict):
                # From window_sql_over
                over = item.get("over", [])
                order_by = item.get("order", [])
            elif isinstance(item, str):
                concept = self.environment.concepts[item]
            elif isinstance(item, ConceptRef):
                concept = self.environment.concepts[item.address]
            elif isinstance(item, WindowType):
                type = item
            else:
                concept = arbitrary_to_concept(item, environment=self.environment)
                self.environment.add_concept(concept, meta=meta)

        if not concept:
            # Provide helpful error for common mistakes like row_number()
            text = self.text_lookup[self.parse_address][meta.start_pos : meta.end_pos]
            if "(" in text and ")" in text:
                func_name = type.value
                raise ParseError(
                    f"Window function `{func_name}()` requires a field argument. "
                    f"Did you mean `{func_name}(your_field)`? Error in: `{text}`"
                )
            raise ParseError(
                f"Window statements must be on fields, not constants - error in: `{text}`"
            )

        return WindowItem(
            type=type,
            content=concept.reference,
            over=over,
            order_by=order_by,
            index=index,
        )

    def filter_item(self, args) -> FilterItem:
        where: WhereClause
        expr, raw = args
        if isinstance(raw, WhereClause):
            where = raw
        else:
            where = WhereClause(conditional=expr_to_boolean(raw, self.function_factory))
        if isinstance(expr, str):
            expr = self.environment.concepts[expr].reference
        return FilterItem(content=expr, where=where)

    # BEGIN FUNCTIONS
    @v_args(meta=True)
    def expr_reference(self, meta, args) -> Concept:
        return self.environment.concepts.__getitem__(args[0], meta.line)

    def expr(self, args):
        if len(args) > 1:
            raise ParseError("Expression should have one child only.")
        return args[0]

    def aggregate_over(self, args):
        return args[0]

    def aggregate_all(self, args):
        return [
            ConceptRef(
                address=f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
                datatype=DataType.INTEGER,
            )
        ]

    def aggregate_functions(self, args):
        if len(args) == 2:
            return AggregateWrapper(function=args[0], by=args[1])
        return AggregateWrapper(function=args[0])

    @v_args(meta=True)
    def index_access(self, meta, args):
        args = process_function_args(args, meta=meta, environment=self.environment)
        base = args[0]
        if base.datatype == DataType.MAP or isinstance(base.datatype, MapType):
            return self.function_factory.create_function(
                args, FunctionType.MAP_ACCESS, meta
            )
        return self.function_factory.create_function(
            args, FunctionType.INDEX_ACCESS, meta
        )

    @v_args(meta=True)
    def map_key_access(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.MAP_ACCESS, meta
        )

    @v_args(meta=True)
    def attr_access(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.ATTR_ACCESS, meta
        )

    @v_args(meta=True)
    def chained_access(self, meta, args):
        # First arg is the base access expression, rest are chained access operations
        args = process_function_args(args, meta=meta, environment=self.environment)
        base = args[0]
        for accessor in args[1:]:
            if isinstance(accessor, int):
                # Index access
                base = self.function_factory.create_function(
                    [base, accessor], FunctionType.INDEX_ACCESS, meta
                )
            else:
                # String key access (map) or attribute access
                # If base is a map, use MAP_ACCESS; otherwise use ATTR_ACCESS
                if hasattr(base, "datatype") and (
                    base.datatype == DataType.MAP or isinstance(base.datatype, MapType)
                ):
                    base = self.function_factory.create_function(
                        [base, accessor], FunctionType.MAP_ACCESS, meta
                    )
                else:
                    base = self.function_factory.create_function(
                        [base, accessor], FunctionType.ATTR_ACCESS, meta
                    )
        return base

    @v_args(meta=True)
    def fcoalesce(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.COALESCE, meta)

    @v_args(meta=True)
    def fgreatest(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.GREATEST, meta)

    @v_args(meta=True)
    def fleast(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LEAST, meta)

    @v_args(meta=True)
    def fnullif(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.NULLIF, meta)

    @v_args(meta=True)
    def frecurse_edge(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.RECURSE_EDGE, meta
        )

    @v_args(meta=True)
    def unnest(self, meta, args):

        return self.function_factory.create_function(args, FunctionType.UNNEST, meta)

    @v_args(meta=True)
    def subselect(self, meta: Meta, args) -> SubselectItem:
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
        return SubselectItem(
            content=content,
            where=where,
            order_by=order_by,
            limit=limit,
        )

    def subselect_where(self, args):
        root = args[0]
        root = expr_to_boolean(root, self.function_factory)
        return WhereClause(conditional=root)

    def subselect_order(self, args) -> list[OrderItem]:
        return args[0]

    def subselect_limit(self, args) -> int:
        return int(args[0])

    @v_args(meta=True)
    def count(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.COUNT, meta)

    @v_args(meta=True)
    def fgroup(self, meta, args):
        if len(args) == 2:
            fargs = [args[0]] + list(args[1])
        else:
            fargs = [args[0]]
        return self.function_factory.create_function(fargs, FunctionType.GROUP, meta)

    @v_args(meta=True)
    def fabs(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.ABS, meta)

    @v_args(meta=True)
    def count_distinct(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.COUNT_DISTINCT, meta
        )

    @v_args(meta=True)
    def sum(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.SUM, meta)

    @v_args(meta=True)
    def array_agg(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.ARRAY_AGG, meta)

    @v_args(meta=True)
    def any(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.ANY, meta)

    @v_args(meta=True)
    def bool_and(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.BOOL_AND, meta)

    @v_args(meta=True)
    def bool_or(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.BOOL_OR, meta)

    @v_args(meta=True)
    def avg(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.AVG, meta)

    @v_args(meta=True)
    def max(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.MAX, meta)

    @v_args(meta=True)
    def min(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.MIN, meta)

    @v_args(meta=True)
    def len(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LENGTH, meta)

    @v_args(meta=True)
    def fsplit(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.SPLIT, meta)

    @v_args(meta=True)
    def concat(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.CONCAT, meta)

    @v_args(meta=True)
    def union(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.UNION, meta)

    @v_args(meta=True)
    def like(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LIKE, meta)

    @v_args(meta=True)
    def alt_like(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LIKE, meta)

    @v_args(meta=True)
    def ilike(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.ILIKE, meta)

    @v_args(meta=True)
    def upper(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.UPPER, meta)

    @v_args(meta=True)
    def fstrpos(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.STRPOS, meta)

    @v_args(meta=True)
    def freplace(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.REPLACE, meta)

    @v_args(meta=True)
    def fcontains(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.CONTAINS, meta)

    @v_args(meta=True)
    def ftrim(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.TRIM, meta)

    @v_args(meta=True)
    def fltrim(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LTRIM, meta)

    @v_args(meta=True)
    def frtrim(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.RTRIM, meta)

    @v_args(meta=True)
    def fhash(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.HASH, meta)

    @v_args(meta=True)
    def fhex(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.HEX, meta)

    @v_args(meta=True)
    def fsubstring(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.SUBSTRING, meta)

    @v_args(meta=True)
    def flower(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LOWER, meta)

    @v_args(meta=True)
    def fregexp_contains(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.REGEXP_CONTAINS, meta
        )

    @v_args(meta=True)
    def fregexp_extract(self, meta, args):
        if len(args) == 2:
            # this is a magic value to represent the default behavior
            args.append(-1)
        return self.function_factory.create_function(
            args, FunctionType.REGEXP_EXTRACT, meta
        )

    @v_args(meta=True)
    def fregexp_replace(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.REGEXP_REPLACE, meta
        )

    # date functions
    @v_args(meta=True)
    def fdate(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATE, meta)

    @v_args(meta=True)
    def fdate_trunc(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.DATE_TRUNCATE, meta
        )

    @v_args(meta=True)
    def fdate_part(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATE_PART, meta)

    @v_args(meta=True)
    def fdate_add(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATE_ADD, meta)

    @v_args(meta=True)
    def fdate_sub(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATE_SUB, meta)

    @v_args(meta=True)
    def fdate_diff(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATE_DIFF, meta)

    @v_args(meta=True)
    def fdatetime(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DATETIME, meta)

    @v_args(meta=True)
    def ftimestamp(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.TIMESTAMP, meta)

    @v_args(meta=True)
    def fsecond(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.SECOND, meta)

    @v_args(meta=True)
    def fminute(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.MINUTE, meta)

    @v_args(meta=True)
    def fhour(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.HOUR, meta)

    @v_args(meta=True)
    def fday(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DAY, meta)

    @v_args(meta=True)
    def fday_name(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.DAY_NAME, meta)

    @v_args(meta=True)
    def fday_of_week(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.DAY_OF_WEEK, meta
        )

    @v_args(meta=True)
    def fweek(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.WEEK, meta)

    @v_args(meta=True)
    def fmonth(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.MONTH, meta)

    @v_args(meta=True)
    def fmonth_name(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.MONTH_NAME, meta
        )

    @v_args(meta=True)
    def fformat_time(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.FORMAT_TIME, meta
        )

    @v_args(meta=True)
    def fparse_time(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.PARSE_TIME, meta
        )

    @v_args(meta=True)
    def fquarter(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.QUARTER, meta)

    @v_args(meta=True)
    def fyear(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.YEAR, meta)

    def internal_fcast(self, meta, args) -> Function:
        args = process_function_args(args, meta=meta, environment=self.environment)

        # Destructure for readability
        value, dtype = args[0], args[1]
        processed: Any
        if isinstance(value, str):
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
                    raise SyntaxError(f"Invalid cast type {dtype}")

            # Determine function type and arguments
            if isinstance(dtype, TraitDataType):
                return self.function_factory.create_function(
                    [processed, dtype], FunctionType.TYPED_CONSTANT, meta
                )

            return self.function_factory.create_function(
                [processed], FunctionType.CONSTANT, meta
            )

        return self.function_factory.create_function(args, FunctionType.CAST, meta)

    @v_args(meta=True)
    def fdate_spine(self, meta, args) -> Function:
        return self.function_factory.create_function(
            args, FunctionType.DATE_SPINE, meta
        )

    @v_args(meta=True)
    def fgeo_from_text(self, meta, args) -> Function:
        return self.function_factory.create_function(
            args, FunctionType.GEO_FROM_TEXT, meta
        )

    @v_args(meta=True)
    def fgeo_point(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.GEO_POINT, meta)

    @v_args(meta=True)
    def fgeo_distance(self, meta, args) -> Function:
        return self.function_factory.create_function(
            args, FunctionType.GEO_DISTANCE, meta
        )

    @v_args(meta=True)
    def fgeo_x(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.GEO_X, meta)

    @v_args(meta=True)
    def fgeo_y(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.GEO_Y, meta)

    @v_args(meta=True)
    def fgeo_centroid(self, meta, args) -> Function:
        return self.function_factory.create_function(
            args, FunctionType.GEO_CENTROID, meta
        )

    @v_args(meta=True)
    def fgeo_transform(self, meta, args) -> Function:
        return self.function_factory.create_function(
            args, FunctionType.GEO_TRANSFORM, meta
        )

    # utility functions
    @v_args(meta=True)
    def fcast(self, meta, args) -> Function:
        return self.internal_fcast(meta, args)

    # math functions
    @v_args(meta=True)
    def fadd(self, meta, args) -> Function:

        return self.function_factory.create_function(args, FunctionType.ADD, meta)

    @v_args(meta=True)
    def fsub(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.SUBTRACT, meta)

    @v_args(meta=True)
    def fmul(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.MULTIPLY, meta)

    @v_args(meta=True)
    def fdiv(self, meta: Meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.DIVIDE, meta)

    @v_args(meta=True)
    def fmod(self, meta: Meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.MOD, meta)

    @v_args(meta=True)
    def fsqrt(self, meta: Meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.SQRT, meta)

    @v_args(meta=True)
    def frandom(self, meta: Meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.RANDOM, meta)

    @v_args(meta=True)
    def fround(self, meta, args) -> Function:
        if len(args) == 1:
            args.append(0)
        return self.function_factory.create_function(args, FunctionType.ROUND, meta)

    @v_args(meta=True)
    def flog(self, meta, args) -> Function:
        if len(args) == 1:
            args.append(10)
        return self.function_factory.create_function(args, FunctionType.LOG, meta)

    @v_args(meta=True)
    def ffloor(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.FLOOR, meta)

    @v_args(meta=True)
    def fceil(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.CEIL, meta)

    @v_args(meta=True)
    def fcase(self, meta, args: List[Union[CaseWhen, CaseElse]]) -> Function:
        return self.function_factory.create_function(args, FunctionType.CASE, meta)

    @v_args(meta=True)
    def fcase_when(self, meta, args) -> CaseWhen:
        args = process_function_args(args, meta=meta, environment=self.environment)
        root = expr_to_boolean(args[0], self.function_factory)
        return CaseWhen(comparison=root, expr=args[1])

    @v_args(meta=True)
    def fcase_else(self, meta, args) -> CaseElse:
        args = process_function_args(args, meta=meta, environment=self.environment)
        return CaseElse(expr=args[0])

    @v_args(meta=True)
    def fcase_simple_when(self, meta, args) -> CaseSimpleWhen:
        args = process_function_args(args, meta=meta, environment=self.environment)
        # Return (value_expr, result_expr) tuple; comparison will be built in fcase_simple
        return CaseSimpleWhen(value_expr=args[0], expr=args[1])

    @v_args(meta=True)
    def fcase_simple(self, meta, args) -> Function:
        args = process_function_args(args, meta=meta, environment=self.environment)
        switch_expr = args[0]
        case_args: List[Union[CaseWhen, CaseSimpleWhen, CaseElse]] = []
        for arg in args[1:]:
            if isinstance(arg, CaseSimpleWhen):
                case_args.append(arg)
            elif isinstance(arg, CaseElse):
                case_args.append(arg)

        return self.function_factory.create_function(
            [switch_expr] + case_args, FunctionType.SIMPLE_CASE, meta
        )

    @v_args(meta=True)
    def fcurrent_date(self, meta, args):
        return CurrentDate([])

    @v_args(meta=True)
    def fcurrent_datetime(self, meta, args):
        return self.function_factory.create_function(
            args=[], operator=FunctionType.CURRENT_DATETIME, meta=meta
        )

    @v_args(meta=True)
    def fcurrent_timestamp(self, meta, args):
        return self.function_factory.create_function(
            args=[], operator=FunctionType.CURRENT_TIMESTAMP, meta=meta
        )

    @v_args(meta=True)
    def fnot(self, meta, args):
        if arg_to_datatype(args[0]) == DataType.BOOL:
            return Comparison(
                left=self.function_factory.create_function(
                    [args[0], False], FunctionType.COALESCE, meta
                ),
                operator=ComparisonOperator.EQ,
                right=False,
            )
        return self.function_factory.create_function(args, FunctionType.IS_NULL, meta)

    @v_args(meta=True)
    def fbool(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.BOOL, meta)

    @v_args(meta=True)
    def fmap_keys(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.MAP_KEYS, meta)

    @v_args(meta=True)
    def fmap_values(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.MAP_VALUES, meta
        )

    @v_args(meta=True)
    def farray_sum(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.ARRAY_SUM, meta)

    @v_args(meta=True)
    def fgenerate_array(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.GENERATE_ARRAY, meta
        )

    @v_args(meta=True)
    def farray_distinct(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.ARRAY_DISTINCT, meta
        )

    @v_args(meta=True)
    def farray_to_string(self, meta, args):
        return self.function_factory.create_function(
            args, FunctionType.ARRAY_TO_STRING, meta
        )

    @v_args(meta=True)
    def farray_sort(self, meta, args):
        if len(args) == 1:
            # this is a magic value to represent the default behavior
            args = [args[0], Ordering.ASCENDING]
        return self.function_factory.create_function(
            args, FunctionType.ARRAY_SORT, meta
        )

    @v_args(meta=True)
    def transform_lambda(self, meta, args):
        return self.environment.functions[args[0]]

    @v_args(meta=True)
    def farray_transform(self, meta, args) -> Function:
        factory: CustomFunctionFactory = args[1]
        if not len(factory.function_arguments) == 1:
            raise InvalidSyntaxException(
                "Array transform function must have exactly one argument;"
            )
        array_type = arg_to_datatype(args[0])
        if not isinstance(array_type, ArrayType):
            raise InvalidSyntaxException(
                f"Array transform function must be applied to an array, not {array_type}"
            )
        return self.function_factory.create_function(
            [
                args[0],
                factory.function_arguments[0],
                factory(
                    ArgBinding(
                        name=factory.function_arguments[0].name,
                        datatype=array_type.value_data_type,
                    )
                ),
            ],
            FunctionType.ARRAY_TRANSFORM,
            meta,
        )

    @v_args(meta=True)
    def farray_filter(self, meta, args) -> Function:
        factory: CustomFunctionFactory = args[1]
        if not len(factory.function_arguments) == 1:
            raise InvalidSyntaxException(
                "Array filter function must have exactly one argument;"
            )
        array_type = arg_to_datatype(args[0])
        if not isinstance(array_type, ArrayType):
            raise InvalidSyntaxException(
                f"Array filter function must be applied to an array, not {array_type}"
            )
        return self.function_factory.create_function(
            [
                args[0],
                factory.function_arguments[0],
                factory(
                    ArgBinding(
                        name=factory.function_arguments[0].name,
                        datatype=array_type.value_data_type,
                    )
                ),
            ],
            FunctionType.ARRAY_FILTER,
            meta,
        )


def unpack_visit_error(e: VisitError, text: str | None = None):
    """This is required to get exceptions from imports, which would
    raise nested VisitErrors"""
    if isinstance(e.orig_exc, VisitError):
        unpack_visit_error(e.orig_exc, text)
    elif isinstance(e.orig_exc, (UndefinedConceptException, ImportError)):
        raise e.orig_exc
    elif isinstance(e.orig_exc, InvalidSyntaxException):
        raise e.orig_exc
    elif isinstance(e.orig_exc, TypeError):
        if isinstance(e.obj, Tree):
            if text:
                extract = text[e.obj.meta.start_pos - 5 : e.obj.meta.end_pos + 5]
                raise TypeError(
                    str(e.orig_exc)
                    + " Raised when parsing rule: "
                    + str(e.rule)
                    + f' Line: {e.obj.meta.line} "...{extract}..."'
                )
            raise TypeError(
                str(e.orig_exc) + " in " + str(e.rule) + f" Line: {e.obj.meta.line}"
            ).with_traceback(e.orig_exc.__traceback__)
    elif isinstance(e.orig_exc, SyntaxError):
        if isinstance(e.obj, Tree):
            if text:
                extract = text[e.obj.meta.start_pos - 5 : e.obj.meta.end_pos + 5]
                raise InvalidSyntaxException(
                    str(e.orig_exc)
                    + " Raised when parsing rule: "
                    + str(e.rule)
                    + f' Line: {e.obj.meta.line} "...{extract}..."'
                )
            InvalidSyntaxException(
                str(e.orig_exc) + " in " + str(e.rule) + f" Line: {e.obj.meta.line}"
            )
        raise InvalidSyntaxException(str(e.orig_exc)).with_traceback(
            e.orig_exc.__traceback__
        )
    raise e.orig_exc


def parse_text_raw(text: str, environment: Optional[Environment] = None):
    _get_parser().parse(text, start="start")


def _parse_fragment_lark(
    start_rule: str,
    text: str,
    environment: Environment,
    parse_config: Parsing | None = None,
):
    parser = ParseToObjects(
        environment=environment, import_keys=["root"], parse_config=parse_config
    )
    parser.set_text(text)
    original_fail_on_missing = environment.concepts.fail_on_missing
    try:
        parser.prepare_parse()
        if start_rule == "datasource":
            parser.parse_pass = ParsePass.VALIDATION
        return parser.transform(_get_parser().parse(text, start=start_rule))
    finally:
        environment.concepts.fail_on_missing = original_fail_on_missing


def _parse_fragment_with_parser(
    start_rule: str,
    text: str,
    parser: ParseToObjects,
):
    return parser._parse_fragment_text(start_rule, text)


ERROR_CODES: dict[int, str] = {
    # 100 code are SQL compatability errors
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    # 200 codes relate to required explicit syntax (we could loosen these?)
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
}

DEFAULT_ERROR_SPAN: int = 30


def inject_context_maker(pos: int, text: str, span: int = 40) -> str:
    """Returns a pretty string pinpointing the error in the text,
    with span amount of context characters around it.

    Note:
        The parser doesn't hold a copy of the text it has to parse,
        so you have to provide it again
    """

    start = max(pos - span, 0)
    end = pos + span
    if not isinstance(text, bytes):

        before = text[start:pos].rsplit("\n", 1)[-1]
        after = text[pos:end].split("\n", 1)[0]
        rcap = ""
        # if it goes beyond the end of text, no ...
        # if it terminates on a space, no need for ...
        if after and not after[-1].isspace() and not (end > len(text)):
            rcap = "..."
        lcap = ""
        if start > 0 and not before[0].isspace():
            lcap = "..."
        lpad = " "
        rpad = " "
        if before.endswith(" "):
            lpad = ""
        if after.startswith(" "):
            rpad = ""
        return f"{lcap}{before}{lpad}???{rpad}{after}{rcap}"
    else:
        before = text[start:pos].rsplit(b"\n", 1)[-1]
        after = text[pos:end].split(b"\n", 1)[0]
        return (before + b" ??? " + after).decode("ascii", "backslashreplace")


def _parse_text_lark(
    text: str,
    environment: Optional[Environment] = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
    run_compare: bool = True,
) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | ValidateStatement
        | None
    ],
]:
    def _create_syntax_error(code: int, pos: int, text: str) -> InvalidSyntaxException:
        """Helper to create standardized syntax error with context."""
        return InvalidSyntaxException(
            f"Syntax [{code}]: "
            + ERROR_CODES[code]
            + "\nLocation:\n"
            + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
        )

    def _create_generic_syntax_error(
        message: str, pos: int, text: str
    ) -> InvalidSyntaxException:
        """Helper to create generic syntax error with context."""
        return InvalidSyntaxException(
            message
            + "\nLocation:\n"
            + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
        )

    def _handle_unexpected_token(e: UnexpectedToken, text: str) -> None:
        """Handle UnexpectedToken errors to make friendlier error messages."""
        pos = e.pos_in_stream or 0
        if e.interactive_parser.lexer_thread.state:
            last_token = e.interactive_parser.lexer_thread.state.last_token
        else:
            last_token = None

        # Handle FROM token error
        parsed_tokens = (
            [x.value for x in e.token_history if x] if e.token_history else []
        )

        if parsed_tokens == ["FROM"]:
            raise _create_syntax_error(101, pos, text)
        # check if they are missing a semicolon
        if last_token and e.token.type == "$END":
            try:

                e.interactive_parser.feed_token(Token("_TERMINATOR", ";"))
                state = e.interactive_parser.lexer_thread.state
                if state and state.last_token:
                    new_pos = state.last_token.end_pos or pos
                else:
                    new_pos = pos
                raise _create_syntax_error(202, new_pos, text)
            except UnexpectedToken:
                pass
        # check if they forgot an as
        try:
            e.interactive_parser.feed_token(Token("AS", "AS"))
            state = e.interactive_parser.lexer_thread.state
            if state and state.last_token:
                new_pos = state.last_token.end_pos or pos
            else:
                new_pos = pos
            e.interactive_parser.feed_token(Token("IDENTIFIER", e.token.value))
            raise _create_syntax_error(201, new_pos, text)
        except UnexpectedToken:
            pass

        # Default UnexpectedToken handling
        raise _create_generic_syntax_error(str(e), pos, text)

    environment = environment or (
        Environment(working_path=root) if root else Environment()
    )
    parser = ParseToObjects(
        environment=environment, import_keys=["root"], parse_config=parse_config
    )
    start = datetime.now()

    try:
        parser.set_text(text)
        # disable fail on missing to allow for circular dependencies
        parser.prepare_parse()
        parser.transform(_get_parser().parse(text, start="start"))
        # this will reset fail on missing
        pass_two = parser.run_second_parse_pass()
        output = [v for v in pass_two if v]
        environment.concepts.fail_on_missing = True
        if run_compare and parser_compare_enabled():
            assert_public_parse_matches_rust(text, output)
        end = datetime.now()
        perf_logger.debug(
            f"Parse time: {end - start} for {len(text)} characters, {len(output)} objects"
        )
    except VisitError as e:
        unpack_visit_error(e, text)
        # this will never be reached
        raise e
    except UnexpectedToken as e:
        _handle_unexpected_token(e, text)
    except (UnexpectedCharacters, UnexpectedEOF, UnexpectedInput) as e:
        raise _create_generic_syntax_error(str(e), e.pos_in_stream or 0, text)
    except (ValidationError, TypeError) as e:
        raise InvalidSyntaxException(str(e))

    return environment, output


def _extract_trailing_comments(
    text: str, blocks: list[RustStatementBlock]
) -> list[str | None]:
    """For each block, collect # comments that follow its ; terminator before a blank line."""
    result: list[str | None] = []
    search_from = 0
    for block in blocks:
        pos = text.find(block.text, search_from)
        if pos < 0:
            result.append(None)
            continue
        semi_pos = text.find(";", pos + len(block.text))
        if semi_pos < 0:
            result.append(None)
            continue
        search_from = semi_pos + 1
        comments: list[str] = []
        for line in text[search_from:].splitlines():
            stripped = line.strip()
            if not stripped:
                break
            if stripped.startswith("#"):
                comment = stripped[1:].rstrip()
                if comment:
                    comments.append(comment)
            elif stripped.startswith("--"):
                comment = stripped[2:].rstrip()
                if comment:
                    comments.append(comment)
            else:
                break
        result.append("\n".join(comments) if comments else None)
    return result


def _check_hybrid_trailing_content(text: str, blocks: list[RustStatementBlock]) -> None:
    """Raise InvalidSyntaxException if non-whitespace/comment content follows the last block."""
    last_semi = text.rfind(";")
    if not blocks:
        remainder = text
    elif last_semi == -1:
        remainder = text
    else:
        remainder = text[last_semi + 1 :]
    # Strip line comments (both # and --)
    stripped = "\n".join(
        line.split("#")[0].split("--")[0] for line in remainder.splitlines()
    ).strip()
    if stripped:
        raise InvalidSyntaxException(f"Syntax [202]: {ERROR_CODES[202]}")


def _parse_text_hybrid_with_parser(
    text: str,
    environment: Environment,
    hybrid_parser: ParseToObjects,
    root: Path | None = None,
    parse_config: Parsing | None = None,
    run_compare: bool = True,
) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | ValidateStatement
        | None
    ],
]:
    blocks = parse_statement_blocks(text)
    _check_hybrid_trailing_content(text, blocks)
    output: list[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | ValidateStatement
        | None
    ] = []

    def parse_fragment(start_rule: str, fragment_text: str):
        return _parse_fragment_with_parser(
            start_rule,
            fragment_text,
            hybrid_parser,
        )

    parse_fragment.parser_state = hybrid_parser  # type: ignore[attr-defined]

    def process_import(summary):
        return hybrid_parser.process_import_summary(
            raw_path=summary.raw_path,
            alias=summary.alias,
            is_stdlib=summary.is_stdlib,
            parent_dirs=summary.parent_dirs,
            is_self=summary.is_self,
            concepts=summary.concepts,
        )

    def process_datasource(summary):
        return hybrid_parser.process_datasource_summary(
            name=summary.name,
            is_root=summary.is_root,
            is_partial=summary.is_partial,
            columns_text=summary.columns_text,
            grain_text=summary.grain_text,
            complete_text=summary.complete_text,
            address_text=summary.address_text,
            query_text=summary.query_text,
            file_text=summary.file_text,
            where_text=summary.where_text,
            update_text=summary.update_text,
            partition_text=summary.partition_text,
            status_text=summary.status_text,
        )

    def parse_nested_statement(nested_text: str):
        nested_kind = None
        nested_blocks = parse_statement_blocks(nested_text)
        if len(nested_blocks) == 1:
            nested_kind = nested_blocks[0].kind
        direct_nested = build_direct_statement(
            nested_text,
            environment,
            parse_nested_statement=parse_nested_statement,
            parse_fragment=parse_fragment,
            process_import=process_import,
            process_datasource=process_datasource,
            parse_config=parse_config,
            block_kind=nested_kind,
        )
        if direct_nested is not None:
            return direct_nested
        return _parse_text_lark(
            nested_text,
            environment=environment,
            root=root,
            parse_config=parse_config,
            run_compare=False,
        )[1][-1]

    trailing_comments = _extract_trailing_comments(text, blocks)
    for block, block_trailing in zip(blocks, trailing_comments):
        direct = build_direct_statement(
            block.text,
            environment,
            parse_nested_statement=parse_nested_statement,
            parse_fragment=parse_fragment,
            process_import=process_import,
            process_datasource=process_datasource,
            parse_config=parse_config,
            block_kind=block.kind,
        )
        if direct is not None:
            if block_trailing and isinstance(
                direct, (ConceptDeclarationStatement, ConceptDerivationStatement)
            ):
                if not direct.concept.metadata.description:
                    direct.concept.metadata.description = block_trailing
            output.append(
                cast(
                    Datasource
                    | ImportStatement
                    | SelectStatement
                    | PersistStatement
                    | ShowStatement
                    | RawSQLStatement
                    | ValidateStatement,
                    direct,
                )
            )
            continue
        _, parsed = _parse_text_lark(
            block.text + "\n;",
            environment=environment,
            root=root,
            parse_config=parse_config,
            run_compare=False,
        )
        output.extend(parsed)

    # Rehydrate concepts with UNKNOWN datatypes (e.g. inline select aliases
    # that reference other inline aliases defined earlier in the same SELECT).
    passed = False
    passes = 0
    while not passed:
        new_passed = True
        for x, y in environment.concepts.items():
            if y.datatype == DataType.UNKNOWN and y.lineage:
                environment.concepts[x] = rehydrate_concept_lineage(
                    y, environment, hybrid_parser.function_factory
                )
                new_passed = False
        passes += 1
        if passes > MAX_PARSE_DEPTH:
            break
        passed = new_passed

    if run_compare and parser_compare_enabled():
        assert_public_parse_matches_rust(text, [v for v in output if v])
    return environment, output


def _parse_text_hybrid(
    text: str,
    environment: Optional[Environment] = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | ValidateStatement
        | None
    ],
]:
    environment = environment or (
        Environment(working_path=root) if root else Environment()
    )
    hybrid_parser = ParseToObjects(
        environment=environment, import_keys=["root"], parse_config=parse_config
    )
    return _parse_text_hybrid_with_parser(
        text,
        environment=environment,
        hybrid_parser=hybrid_parser,
        root=root,
        parse_config=parse_config,
        run_compare=True,
    )


def parse_text(
    text: str,
    environment: Optional[Environment] = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | ValidateStatement
        | None
    ],
]:
    if rust_hybrid_enabled():
        return _parse_text_hybrid(
            text, environment=environment, root=root, parse_config=parse_config
        )
    return _parse_text_lark(
        text, environment=environment, root=root, parse_config=parse_config
    )
