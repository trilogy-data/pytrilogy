from os.path import dirname, join
from typing import List, Optional, Tuple, Union
from re import IGNORECASE
from lark import Lark, Transformer, v_args
from lark.exceptions import (
    UnexpectedCharacters,
    UnexpectedEOF,
    UnexpectedInput,
    UnexpectedToken,
    VisitError,
)
from lark.tree import Meta
from pydantic import ValidationError
from preql.core.internal import INTERNAL_NAMESPACE, ALL_ROWS_CONCEPT
from preql.constants import (
    DEFAULT_NAMESPACE,
    NULL_VALUE,
    VIRTUAL_CONCEPT_PREFIX,
)
from preql.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    InfiniteFunctionArgs,
    FunctionClass,
    Modifier,
    Ordering,
    Purpose,
    WindowOrder,
    WindowType,
    DatePart,
    ShowCategory,
)
from preql.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from preql.core.functions import (
    Count,
    CountDistinct,
    Group,
    Max,
    Min,
    Split,
    IndexAccess,
    AttrAccess,
    Abs,
    Unnest,
    Coalesce,
    function_args_to_output_purpose,
    CurrentDate,
    CurrentDatetime,
    IsNull,
    SubString,
    StrPos,
)
from preql.core.models import (
    Address,
    AlignClause,
    AlignItem,
    AggregateWrapper,
    CaseElse,
    CaseWhen,
    ColumnAssignment,
    Comment,
    Comparison,
    Concept,
    ConceptTransform,
    Conditional,
    Datasource,
    MergeStatement,
    Environment,
    FilterItem,
    Function,
    Grain,
    ImportStatement,
    Limit,
    Metadata,
    MultiSelectStatement,
    OrderBy,
    OrderItem,
    Parenthetical,
    PersistStatement,
    Query,
    SelectStatement,
    SelectItem,
    WhereClause,
    Window,
    WindowItem,
    WindowItemOrder,
    WindowItemOver,
    RawColumnExpr,
    arg_to_datatype,
    ListWrapper,
    MapType,
    ShowStatement,
    DataType,
    StructType,
    ListType,
    ConceptDeclarationStatement,
    ConceptDerivation,
    RowsetDerivationStatement,
    LooseConceptList,
)
from preql.parsing.exceptions import ParseError
from preql.utility import string_to_hash
from preql.parsing.common import (
    agg_wrapper_to_concept,
    window_item_to_concept,
    function_to_concept,
    filter_item_to_concept,
    constant_to_concept,
)

CONSTANT_TYPES = (int, float, str, bool, ListWrapper)

grammar = r"""
    !start: ( block | show_statement | comment )*
    block: statement _TERMINATOR comment?
    ?statement: concept
    | datasource
    | function
    | multi_select_statement
    | select_statement
    | persist_statement
    | rowset_derivation_statement
    | import_statement
    | merge_statement
    
    _TERMINATOR:  ";"i /\s*/
    
    comment:   /#.*(\n|$)/ |  /\/\/.*\n/  
    
    // property display_name string
    concept_declaration: PURPOSE IDENTIFIER data_type metadata?
    //customer_id.property first_name STRING;
    //<customer_id,country>.property local_alias STRING
    concept_property_declaration: PROPERTY (prop_ident | IDENTIFIER) data_type metadata?
    //metric post_length <- len(post_text);
    concept_derivation:  (PURPOSE | AUTO | PROPERTY ) IDENTIFIER "<" "-" expr

    rowset_derivation_statement: ("rowset"i IDENTIFIER "<" "-" (multi_select_statement | select_statement)) | ("with"i IDENTIFIER "as"i (multi_select_statement | select_statement))
    
    constant_derivation: CONST IDENTIFIER "<" "-" literal
    
    concept:  (concept_declaration | concept_derivation | concept_property_declaration | constant_derivation)
    
    //concept property
    prop_ident: "<" (IDENTIFIER ",")* IDENTIFIER ","? ">" "." IDENTIFIER

    // datasource concepts
    datasource: "datasource" IDENTIFIER  "("  column_assignment_list ")"  grain_clause? (address | query)
    
    grain_clause: "grain" "(" column_list ")"
    
    address: "address" ADDRESS
    
    query: "query" MULTILINE_STRING
    
    concept_assignment: IDENTIFIER | (MODIFIER "[" concept_assignment "]" ) | (SHORTHAND_MODIFIER concept_assignment  )
    
    column_assignment: ((IDENTIFIER | raw_column_assignment | _static_functions ) ":" concept_assignment) 

    raw_column_assignment: "raw" "(" MULTILINE_STRING ")"
    
    column_assignment_list : (column_assignment "," )* column_assignment ","?
    
    column_list : (IDENTIFIER "," )* IDENTIFIER ","?
    
    import_statement: "import" (IDENTIFIER ".") * IDENTIFIER "as" IDENTIFIER

    // persist_statement
    persist_statement: "persist"i IDENTIFIER "into"i IDENTIFIER "from"i select_statement grain_clause?

    // select statement
    select_statement: "select"i select_list  where? comment* order_by? comment* limit? comment*

    // multiple_selects
    multi_select_statement: select_statement ("merge" select_statement)+ "align"i align_clause  where? comment* order_by? comment* limit? comment*


    align_item: IDENTIFIER ":" IDENTIFIER ("," IDENTIFIER)* ","?

    align_clause: align_item ("," align_item)*  ","?

    // merge statemment

    merge_statement: "merge" IDENTIFIER ("," IDENTIFIER)* ","? comment*

    // FUNCTION blocks
    function: raw_function
    function_binding_item: IDENTIFIER data_type
    function_binding_list: (function_binding_item ",")* function_binding_item ","?
    raw_function: "def" "rawsql"  IDENTIFIER  "("  function_binding_list ")" "-" ">" data_type "as"i MULTILINE_STRING

    
    // user_id where state = Mexico
    filter_item: "filter"i IDENTIFIER where

    // rank/lag/lead
    WINDOW_TYPE: ("row_number"i|"rank"i|"lag"i|"lead"i | "sum"i)  /[\s]+/
    
    window_item: WINDOW_TYPE (IDENTIFIER | select_transform | comment+ ) window_item_over? window_item_order?
    
    window_item_over: ("OVER"i over_list)
    
    window_item_order: ("ORDER"i? "BY"i order_list)
    
    select_hide_modifier: "--"
    select_item: select_hide_modifier? (IDENTIFIER | select_transform | comment+ ) | ("~" select_item)
    
    select_list:  ( select_item "," )* select_item ","?
    
    //  count(post_id) -> post_count
    _assignment: ("-" ">") | "as"
    select_transform : expr _assignment IDENTIFIER metadata?
    
    metadata: "metadata" "(" IDENTIFIER "=" _string_lit ")"
    
    limit: "LIMIT"i /[0-9]+/
    
    !window_order: ("TOP"i | "BOTTOM"i)
    
    window: window_order /[0-9]+/
    
    window_order_by: "BY"i column_list
    
    order_list: (expr ORDERING "," )* expr ORDERING ","?
    
    over_list: (IDENTIFIER "," )* IDENTIFIER ","?
    
    ORDERING: ("ASC"i | "DESC"i)
    
    order_by: "ORDER"i "BY"i order_list
    
    //WHERE STATEMENT
    
    LOGICAL_OPERATOR: "AND"i | "OR"i
    
    conditional: expr LOGICAL_OPERATOR (conditional | expr)
    
    where: "WHERE"i (expr | conditional)
    
    expr_reference: IDENTIFIER

    !array_comparison: ( ("NOT"i "IN"i) | "IN"i)

    COMPARISON_OPERATOR: (/is[\s]+not/ | "is" |"=" | ">" | "<" | ">=" | "<=" | "!="  )
    
    comparison: (expr COMPARISON_OPERATOR expr) | (expr array_comparison expr_tuple) 
    
    expr_tuple: "("  (expr ",")* expr ","?  ")"

    //unnesting is a function
    unnest: "UNNEST"i "(" expr ")"
    //indexing into an expression is a function
    index_access: expr "[" int_lit "]"
    attr_access: expr  "[" _string_lit "]"

    parenthetical: "(" (conditional | expr) ")"
    
    expr: window_item | filter_item | fgroup | aggregate_functions | unnest | _string_functions | _math_functions | _generic_functions | _constant_functions| _date_functions |  literal |  expr_reference  | index_access | attr_access | comparison | parenthetical
    
    // functions
    
    //math TODO: add syntactic sugar
    fadd: ("add"i "(" expr "," expr ")" ) | ( expr "+" expr )
    fsub: ("subtract"i "(" expr "," expr ")" ) | ( expr "-" expr )
    fmul: ("multiply"i "(" expr "," expr ")" ) | ( expr "*" expr )
    fdiv: ( "divide"i "(" expr "," expr ")") | ( expr "/" expr )
    fmod: ( "mod"i "(" expr "," expr ")") | ( expr "%" expr )
    fround: "round"i "(" expr "," expr ")"
    fabs: "abs"i "(" expr ")"
        
    _math_functions: fadd | fsub | fmul | fdiv | fround | fmod | fabs
    
    //generic
    fcast: "cast"i "(" expr "AS"i data_type ")"
    concat: ("concat"i "(" (expr ",")* expr ")") | (expr "||" expr)
    fcoalesce: "coalesce"i "(" (expr ",")* expr ")"
    fcase_when: "WHEN"i (expr | conditional) "THEN"i expr
    fcase_else: "ELSE"i expr
    fcase: "CASE"i (fcase_when)* (fcase_else)? "END"i
    len: "len"i "(" expr ")"
    fnot: "NOT"i expr

    _generic_functions: fcast | concat | fcoalesce | fcase | len | fnot

    //constant
    fcurrent_date: "current_date"i "(" ")"
    fcurrent_datetime: "current_datetime"i "(" ")"

    _constant_functions: fcurrent_date | fcurrent_datetime
    
    //string
    like: "like"i "(" expr "," _string_lit ")"
    ilike: "ilike"i "(" expr "," _string_lit ")"
    alt_like: expr "like"i expr
    upper: "upper"i "(" expr ")"
    lower: "lower"i "(" expr ")"    
    fsplit: "split"i "(" expr "," _string_lit ")"
    fstrpos: "strpos"i "(" expr "," expr ")"
    fsubstring: "substring"i "(" expr "," expr "," expr ")"
    
    _string_functions: like | ilike | upper | lower | fsplit | fstrpos | fsubstring | alt_like
    
    // special aggregate
    fgroup: "group"i "(" expr ")" aggregate_over?
    //aggregates
    count: "count"i "(" expr ")"
    count_distinct: "count_distinct"i "(" expr ")"
    sum: "sum"i "(" expr ")"
    avg: "avg"i "(" expr ")"
    max: "max"i "(" expr ")"
    min: "min"i "(" expr ")"
    
    //aggregates can force a grain
    aggregate_all: "*"
    aggregate_over: ("BY"i (aggregate_all | over_list))
    aggregate_functions: (count | count_distinct | sum | avg | max | min) aggregate_over?

    // date functions
    fdate: "date"i "(" expr ")"
    fdatetime: "datetime"i "(" expr ")"
    ftimestamp: "timestamp"i "(" expr ")"
    
    fsecond: "second"i "(" expr ")"
    fminute: "minute"i "(" expr ")"
    fhour: "hour"i "(" expr ")"
    fday: "day"i "(" expr ")"
    fday_of_week: "day_of_week"i "(" expr ")"
    fweek: "week"i "(" expr ")"
    fmonth: "month"i "(" expr ")"
    fquarter: "quarter"i  "(" expr ")"
    fyear: "year"i "(" expr ")"
    
    DATE_PART: "DAY"i | "WEEK"i | "MONTH"i | "QUARTER"i | "YEAR"i | "MINUTE"i | "HOUR"i | "SECOND"i
    fdate_trunc: "date_trunc"i "(" expr "," DATE_PART ")"
    fdate_part: "date_part"i "(" expr "," DATE_PART ")"
    fdate_add: "date_add"i "(" expr "," DATE_PART "," int_lit ")"
    fdate_diff: "date_diff"i "(" expr "," expr "," DATE_PART ")"
    
    _date_functions: fdate | fdate_add | fdate_diff | fdatetime | ftimestamp | fsecond | fminute | fhour | fday | fday_of_week | fweek | fmonth | fquarter | fyear | fdate_part | fdate_trunc
    
    _static_functions: _string_functions | _math_functions | _generic_functions | _constant_functions| _date_functions

    // base language constructs
    IDENTIFIER: /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-]*/
    ADDRESS: /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-\*]*/ | /`[a-zA-Z_][a-zA-Z0-9_\\-\\.\-\*]*`/

    MULTILINE_STRING: /\'{3}(.*?)\'{3}/s
    
    DOUBLE_STRING_CHARS: /(?:(?!\${)([^"\\]|\\.))+/+ // any character except "
    SINGLE_STRING_CHARS: /(?:(?!\${)([^'\\]|\\.))+/+ // any character except '
    _single_quote: "'" ( SINGLE_STRING_CHARS )* "'" 
    _double_quote: "\"" ( DOUBLE_STRING_CHARS )* "\"" 
    _string_lit: _single_quote | _double_quote

    MINUS: "-"

    int_lit: MINUS? /[0-9]+/
    
    float_lit: /[0-9]*\.[0-9]+/

    array_lit: "[" (literal ",")* literal ","? "]"()
    
    !bool_lit: "True"i | "False"i

    !null_lit: "null"i
    
    literal: _string_lit | int_lit | float_lit | bool_lit | null_lit | array_lit

    MODIFIER: "Optional"i | "Partial"i

    SHORTHAND_MODIFIER: "~"

    struct_type: "struct" "<" ((data_type | IDENTIFIER) ",")* (data_type | IDENTIFIER) ","? ">" 

    list_type: "list" "<" data_type ">"


    !data_type: "string"i | "number"i | "numeric"i | "map"i | "list"i | "array"i | "any"i | "int"i | "bigint" | "date"i | "datetime"i | "timestamp"i | "float"i | "bool"i | struct_type | list_type
    
    PURPOSE:  "key"i | "metric"i | "const"i | "constant"i
    PROPERTY: "property"i
    CONST: "const"i | "constant"i
    AUTO: "AUTO"i 

    // meta functions
    CONCEPTS: "CONCEPTS"i
    DATASOURCES: "DATASOURCES"i

    show_category: CONCEPTS | DATASOURCES

    show_statement: "show"i ( show_category | select_statement | persist_statement) _TERMINATOR

    %import common.WS_INLINE -> _WHITESPACE
    %import common.WS
    %ignore WS
"""  # noqa: E501

PARSER = Lark(
    grammar, start="start", propagate_positions=True, g_regex_flags=IGNORECASE
)


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


def unwrap_transformation(
    input: Union[
        FilterItem,
        WindowItem,
        Concept,
        Function,
        AggregateWrapper,
        int,
        str,
        float,
        bool,
    ]
) -> Function | FilterItem | WindowItem | AggregateWrapper:
    if isinstance(input, Function):
        return input
    elif isinstance(input, AggregateWrapper):
        return input
    elif isinstance(input, Concept):
        return Function(
            operator=FunctionType.ALIAS,
            output_datatype=input.datatype,
            output_purpose=input.purpose,
            arguments=[input],
        )
    elif isinstance(input, FilterItem):
        return input
    elif isinstance(input, WindowItem):
        return input
    elif isinstance(input, Parenthetical):
        return unwrap_transformation(input.content)
    else:
        return Function(
            operator=FunctionType.CONSTANT,
            output_datatype=arg_to_datatype(input),
            output_purpose=Purpose.CONSTANT,
            arguments=[input],
        )


class ParseToObjects(Transformer):
    def __init__(
        self,
        visit_tokens,
        text,
        environment: Environment,
        parse_address: str | None = None,
        parsed: dict | None = None,
    ):
        Transformer.__init__(self, visit_tokens)
        self.text = text
        self.environment: Environment = environment
        self.imported: set[str] = set()
        self.parse_address = parse_address or "root"
        self.parsed: dict[str, ParseToObjects] = parsed if parsed else {}
        # we do a second pass to pick up circular dependencies
        # after initial parsing
        self.pass_count = 1

    def hydrate_missing(self):
        self.pass_count = 2
        for k, v in self.parsed.items():
            if v.pass_count == 2:
                continue
            v.hydrate_missing()
        self.environment.concepts.fail_on_missing = True
        reparsed = self.transform(PARSER.parse(self.text))
        self.environment.concepts.undefined = {}
        return reparsed

    def process_function_args(
        self, args, meta: Meta, concept_arguments: Optional[LooseConceptList] = None
    ):
        final: List[Concept | Function] = []
        for arg in args:
            # if a function has an anonymous function argument
            # create an implicit concept
            while isinstance(arg, Parenthetical):
                arg = arg.content
            if isinstance(arg, Function):
                # if it's not an aggregate function, we can skip the virtual concepts
                # to simplify anonymous function handling
                if arg.operator not in FunctionClass.AGGREGATE_FUNCTIONS.value:
                    final.append(arg)
                    continue
                id_hash = string_to_hash(str(arg))
                concept = function_to_concept(
                    arg,
                    name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                    namespace=self.environment.namespace,
                )
                # to satisfy mypy, concept will always have metadata
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            elif isinstance(arg, FilterItem):
                id_hash = string_to_hash(str(arg))
                concept = filter_item_to_concept(
                    arg,
                    name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                    namespace=self.environment.namespace,
                )
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            elif isinstance(arg, WindowItem):
                id_hash = string_to_hash(str(arg))
                concept = window_item_to_concept(
                    arg,
                    namespace=self.environment.namespace,
                    name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                )
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            elif isinstance(arg, AggregateWrapper):
                id_hash = string_to_hash(str(arg))
                concept = agg_wrapper_to_concept(
                    arg,
                    namespace=self.environment.namespace,
                    name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                )
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            # we don't need virtual types for most constants
            elif isinstance(arg, (ListWrapper)):
                id_hash = string_to_hash(str(arg))
                concept = constant_to_concept(
                    arg,
                    name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                    namespace=self.environment.namespace,
                )
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            else:
                final.append(arg)
        return final

    def start(self, args):
        return args

    def block(self, args):
        output = args[0]
        if isinstance(output, ConceptDeclarationStatement):
            if len(args) > 1 and isinstance(args[1], Comment):
                output.concept.metadata.description = (
                    output.concept.metadata.description
                    or args[1].text.split("#")[1].strip()
                )

        return args[0]

    def metadata(self, args):
        pairs = {key: val for key, val in zip(args[::2], args[1::2])}
        return Metadata(**pairs)

    def IDENTIFIER(self, args) -> str:
        return args.value

    def ADDRESS(self, args) -> str:
        return args.value

    def STRING_CHARS(self, args) -> str:
        return args.value

    def SINGLE_STRING_CHARS(self, args) -> str:
        return args.value

    def DOUBLE_STRING_CHARS(self, args) -> str:
        return args.value

    def MINUS(self, args) -> str:
        return "-"

    @v_args(meta=True)
    def struct_type(self, meta: Meta, args) -> StructType:
        final: list[DataType | MapType | ListType | StructType | Concept] = []
        for arg in args:
            if not isinstance(arg, (DataType, ListType, StructType)):
                new = self.environment.concepts.__getitem__(  # type: ignore
                    key=arg, line_no=meta.line
                )
                final.append(new)
            else:
                final.append(arg)
        return StructType(fields=final)

    def list_type(self, args) -> ListType:
        return ListType(type=args[0])

    def data_type(self, args) -> DataType | ListType | StructType:
        resolved = args[0]
        if isinstance(resolved, StructType):
            return resolved
        elif isinstance(resolved, ListType):
            return resolved
        return DataType(args[0].lower())

    def array_comparison(self, args) -> ComparisonOperator:
        return ComparisonOperator([x.value.lower() for x in args])

    def COMPARISON_OPERATOR(self, args) -> ComparisonOperator:
        return ComparisonOperator(args)

    def LOGICAL_OPERATOR(self, args) -> BooleanOperator:
        return BooleanOperator(args.lower())

    def concept_assignment(self, args):
        return args

    @v_args(meta=True)
    def column_assignment(self, meta: Meta, args):
        # TODO -> deal with conceptual modifiers
        modifiers = []
        concept = args[1]
        # recursively collect modifiers
        while len(concept) > 1:
            modifiers.append(concept[0])
            concept = concept[1]
        resolved = self.environment.concepts.__getitem__(  # type: ignore
            key=concept[0], line_no=meta.line
        )
        return ColumnAssignment(alias=args[0], modifiers=modifiers, concept=resolved)

    def _TERMINATOR(self, args):
        return None

    def MODIFIER(self, args) -> Modifier:
        return Modifier(args.value)

    def SHORTHAND_MODIFIER(self, args) -> Modifier:
        return Modifier(args.value)

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

    @v_args(meta=True)
    def prop_ident(self, meta: Meta, args) -> Tuple[List[Concept], str]:
        return [self.environment.concepts[grain] for grain in args[:-1]], args[-1]

    @v_args(meta=True)
    def concept_property_declaration(self, meta: Meta, args) -> Concept:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None

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
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            grain=Grain(components=parents),
            namespace=namespace,
            keys=parents,
        )
        self.environment.add_concept(concept, meta)
        return concept

    @v_args(meta=True)
    def concept_declaration(self, meta: Meta, args) -> ConceptDeclarationStatement:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        lookup, namespace, name, parent = parse_concept_reference(
            name, self.environment
        )
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            namespace=namespace,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
        self.environment.add_concept(concept, meta=meta)
        return ConceptDeclarationStatement(concept=concept)

    @v_args(meta=True)
    def concept_derivation(self, meta: Meta, args) -> ConceptDerivation:

        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        purpose = args[0]
        if purpose == Purpose.AUTO:
            purpose = None
        name = args[1]
        lookup, namespace, name, parent_concept = parse_concept_reference(
            name, self.environment, purpose
        )
        source_value = args[2]
        # we need to strip off every parenthetical to see what is being assigned.
        while isinstance(source_value, Parenthetical):
            source_value = source_value.content

        if isinstance(source_value, FilterItem):
            concept = filter_item_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                purpose=purpose,
                metadata=metadata,
            )

            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivation(concept=concept)
        elif isinstance(source_value, WindowItem):

            concept = window_item_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                purpose=purpose,
                metadata=metadata,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivation(concept=concept)
        elif isinstance(source_value, AggregateWrapper):
            concept = agg_wrapper_to_concept(
                source_value,
                namespace=namespace,
                name=name,
                metadata=metadata,
                purpose=purpose,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivation(concept=concept)
        elif isinstance(source_value, CONSTANT_TYPES):
            concept = constant_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                purpose=purpose,
                metadata=metadata,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivation(concept=concept)

        elif isinstance(source_value, Function):
            function: Function = source_value

            concept = function_to_concept(
                function,
                name=name,
                namespace=namespace,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivation(concept=concept)

        raise SyntaxError(
            f"Received invalid type {type(args[2])} {args[2]} as input to select"
            " transform"
        )

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
        for new_concept in output.derived_concepts:
            if new_concept.metadata:
                new_concept.metadata.line_number = meta.line
            self.environment.add_concept(new_concept)

        return output

    @v_args(meta=True)
    def constant_derivation(self, meta: Meta, args) -> Concept:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        constant: Union[str, float, int, bool] = args[2]
        lookup, namespace, name, parent = parse_concept_reference(
            name, self.environment
        )
        concept = Concept(
            name=name,
            datatype=arg_to_datatype(constant),
            purpose=Purpose.CONSTANT,
            metadata=metadata,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                output_datatype=arg_to_datatype(constant),
                output_purpose=Purpose.CONSTANT,
                arguments=[constant],
            ),
            grain=Grain(components=[]),
            namespace=namespace,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
        self.environment.add_concept(concept, meta)
        return concept

    @v_args(meta=True)
    def concept(self, meta: Meta, args) -> ConceptDeclarationStatement:

        if isinstance(args[0], Concept):
            concept: Concept = args[0]
        else:
            concept = args[0].concept
        if concept.metadata:
            concept.metadata.line_number = meta.line
        return ConceptDeclarationStatement(concept=concept)

    def column_assignment_list(self, args):
        return args

    def column_list(self, args) -> List:
        return args

    def grain_clause(self, args) -> Grain:
        #            namespace=self.environment.namespace,
        return Grain(components=[self.environment.concepts[a] for a in args[0]])

    def raw_column_assignment(self, args):
        return RawColumnExpr(text=args[0][3:-3])

    @v_args(meta=True)
    def datasource(self, meta: Meta, args):
        name = args[0]
        columns: List[ColumnAssignment] = args[1]
        grain: Optional[Grain] = None
        address: Optional[Address] = None
        for val in args[1:]:
            if isinstance(val, Address):
                address = val
            elif isinstance(val, Grain):
                grain = val
            elif isinstance(val, Query):
                address = Address(location=f"({val.text})")
        if not address:
            raise ValueError(
                "Malformed datasource, missing address or query declaration"
            )
        datasource = Datasource(
            identifier=name,
            columns=columns,
            # grain will be set by default from args
            # TODO: move to factory
            grain=grain,  # type: ignore
            address=address,
            namespace=self.environment.namespace,
        )
        for column in columns:
            column.concept = column.concept.with_grain(datasource.grain)
        self.environment.datasources[datasource.identifier] = datasource
        return datasource

    @v_args(meta=True)
    def comment(self, meta: Meta, args):
        assert len(args) == 1
        return Comment(text=args[0].value)

    @v_args(meta=True)
    def select_transform(self, meta, args) -> ConceptTransform:

        output: str = args[1]
        function = unwrap_transformation(args[0])
        lookup, namespace, output, parent = parse_concept_reference(
            output, self.environment
        )

        if isinstance(function, AggregateWrapper):
            concept = agg_wrapper_to_concept(function, namespace=namespace, name=output)
        elif isinstance(function, WindowItem):
            concept = window_item_to_concept(function, namespace=namespace, name=output)
        elif isinstance(function, FilterItem):
            concept = filter_item_to_concept(function, namespace=namespace, name=output)
        elif isinstance(function, CONSTANT_TYPES):
            concept = constant_to_concept(function, namespace=namespace, name=output)
        elif isinstance(function, Function):
            concept = function_to_concept(function, namespace=namespace, name=output)
        else:
            if function.output_purpose == Purpose.PROPERTY:
                pkeys = [x for x in function.arguments if isinstance(x, Concept)]
                grain = Grain(components=pkeys)
                keys = tuple(grain.components_copy)
            else:
                grain = None
                keys = None
            concept = Concept(
                name=output,
                datatype=function.output_datatype,
                purpose=function.output_purpose,
                lineage=function,
                namespace=namespace,
                grain=Grain(components=[]) if not grain else grain,
                keys=keys,
            )
        if concept.metadata:
            concept.metadata.line_number = meta.line
        self.environment.add_concept(concept, meta=meta)
        return ConceptTransform(function=function, output=concept)

    @v_args(meta=True)
    def select_hide_modifier(self, meta: Meta, args) -> Modifier:
        return Modifier.HIDDEN

    @v_args(meta=True)
    def select_item(self, meta: Meta, args) -> Optional[SelectItem]:
        modifiers = [arg for arg in args if isinstance(arg, Modifier)]
        args = [arg for arg in args if not isinstance(arg, (Modifier, Comment))]

        if not args:
            return None
        if len(args) != 1:
            raise ParseError(
                "Malformed select statement"
                f" {args} {self.text[meta.start_pos:meta.end_pos]}"
            )
        content = args[0]
        if isinstance(content, ConceptTransform):
            return SelectItem(content=content, modifiers=modifiers)
        return SelectItem(
            content=self.environment.concepts.__getitem__(content, meta.line),
            modifiers=modifiers,
        )

    def select_list(self, args):
        return [arg for arg in args if arg]

    def limit(self, args):
        return Limit(count=int(args[0].value))

    def ORDERING(self, args):
        return Ordering(args.lower())

    def order_list(self, args):
        return [OrderItem(expr=x, order=y) for x, y in zip(args[::2], args[1::2])]

    def order_by(self, args):
        return OrderBy(items=args[0])

    def over_list(self, args):
        return [self.environment.concepts[x] for x in args]

    @v_args(meta=True)
    def merge_statement(self, meta: Meta, args) -> MergeStatement:

        parsed = [self.environment.concepts[x] for x in args]
        datatypes = {x.datatype for x in parsed}
        if not len(datatypes) == 1:
            raise SyntaxError(
                f"Cannot merge concepts with different datatypes {datatypes}"
                f"line: {meta.line} concepts: {[x.address for x in parsed]}"
            )
        merge = MergeStatement(concepts=parsed, datatype=datatypes.pop())
        new = merge.merge_concept
        self.environment.add_concept(new, meta=meta)
        return merge

    def import_statement(self, args: list[str]):
        alias = args[-1]
        path = args[0].split(".")

        target = join(self.environment.working_path, *path) + ".preql"
        self.imported.add(target)
        if target in self.parsed:
            nparser = self.parsed[target]
        else:
            try:
                with open(target, "r", encoding="utf-8") as f:
                    text = f.read()
                nparser = ParseToObjects(
                    visit_tokens=True,
                    text=text,
                    environment=Environment(
                        working_path=dirname(target),
                        # namespace=alias,
                    ),
                    parse_address=target,
                    parsed={**self.parsed, **{self.parse_address: self}},
                )
                nparser.transform(PARSER.parse(text))
                self.parsed[target] = nparser
            except Exception as e:
                raise ImportError(
                    f"Unable to import file {dirname(target)}, parsing error: {e}"
                )

        for key, concept in nparser.environment.concepts.items():
            # self.environment.concepts[f"{alias}.{key}"] = concept.with_namespace(new_namespace)
            self.environment.add_concept(concept.with_namespace(alias))

        for key, datasource in nparser.environment.datasources.items():
            self.environment.add_datasource(datasource.with_namespace(alias))
            # self.environment.datasources[f"{alias}.{key}"] = datasource.with_namespace(new_namespace)

        self.environment.imports[alias] = ImportStatement(alias=alias, path=args[0])
        return None

    @v_args(meta=True)
    def show_category(self, meta: Meta, args) -> ShowCategory:
        return ShowCategory(args[0])

    @v_args(meta=True)
    def show_statement(self, meta: Meta, args) -> ShowStatement:
        return ShowStatement(content=args[0])

    @v_args(meta=True)
    def persist_statement(self, meta: Meta, args) -> PersistStatement:
        identifier: str = args[0]
        address: str = args[1]
        select: SelectStatement = args[2]
        if len(args) > 3:
            grain: Grain | None = args[3]
        else:
            grain = None
        new_datasource = select.to_datasource(
            namespace=(
                self.environment.namespace
                if self.environment.namespace
                else DEFAULT_NAMESPACE
            ),
            identifier=identifier,
            address=Address(location=address),
            grain=grain,
        )
        return PersistStatement(select=select, datasource=new_datasource)

    @v_args(meta=True)
    def align_item(self, meta: Meta, args) -> AlignItem:
        return AlignItem(
            alias=args[0],
            namespace=self.environment.namespace,
            concepts=[self.environment.concepts[arg] for arg in args[1:]],
        )

    @v_args(meta=True)
    def align_clause(self, meta: Meta, args) -> AlignClause:
        return AlignClause(items=args)

    @v_args(meta=True)
    def multi_select_statement(self, meta: Meta, args) -> MultiSelectStatement:
        selects = []
        align: AlignClause | None = None
        limit: int | None = None
        order_by: OrderBy | None = None
        where: WhereClause | None = None
        for arg in args:
            if isinstance(arg, SelectStatement):
                selects.append(arg)
            elif isinstance(arg, Limit):
                limit = arg.count
            elif isinstance(arg, OrderBy):
                order_by = arg
            elif isinstance(arg, WhereClause):
                where = arg
            elif isinstance(arg, AlignClause):
                align = arg

        assert align
        assert align is not None
        multi = MultiSelectStatement(
            selects=selects,
            align=align,
            namespace=self.environment.namespace,
            where_clause=where,
            order_by=order_by,
            limit=limit,
        )
        for concept in multi.derived_concepts:
            self.environment.add_concept(concept, meta=meta)
        return multi

    @v_args(meta=True)
    def select_statement(self, meta: Meta, args) -> SelectStatement:
        select_items = None
        limit = None
        order_by = None
        where = None
        for arg in args:
            if isinstance(arg, List):
                select_items = arg
            elif isinstance(arg, Limit):
                limit = arg.count
            elif isinstance(arg, OrderBy):
                order_by = arg
            elif isinstance(arg, WhereClause):
                where = arg
        if not select_items:
            raise ValueError("Malformed select, missing select items")
        output = SelectStatement(
            selection=select_items, where_clause=where, limit=limit, order_by=order_by
        )
        for item in select_items:
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if isinstance(item.content, ConceptTransform):
                new_concept = item.content.output.with_select_grain(output.grain)
                self.environment.add_concept(new_concept, meta=meta)
                item.content.output = new_concept
        if order_by:
            for item in order_by.items:
                if (
                    isinstance(item.expr, Concept)
                    and item.expr.purpose == Purpose.METRIC
                ):
                    item.expr = item.expr.with_grain(output.grain)
        return output

    @v_args(meta=True)
    def address(self, meta: Meta, args):
        return Address(location=args[0])

    @v_args(meta=True)
    def query(self, meta: Meta, args):
        return Query(text=args[0][3:-3])

    def where(self, args):
        root = args[0]
        if not isinstance(root, (Comparison, Conditional, Parenthetical)):
            root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        return WhereClause(conditional=root)

    @v_args(meta=True)
    def function_binding_list(self, meta: Meta, args) -> Concept:
        return args

    @v_args(meta=True)
    def function_binding_item(self, meta: Meta, args) -> Concept:
        return args

    @v_args(meta=True)
    def raw_function(self, meta: Meta, args) -> Function:
        print(args)
        identity = args[0]
        fargs = args[1]
        output = args[2]
        item = Function(
            operator=FunctionType.SUM,
            arguments=[x[1] for x in fargs],
            output_datatype=output,
            output_purpose=Purpose.PROPERTY,
            arg_count=len(fargs) + 1,
        )
        self.environment.functions[identity] = item
        return item

    @v_args(meta=True)
    def function(self, meta: Meta, args) -> Function:
        return args[0]

    def int_lit(self, args):
        return int("".join(args))

    def bool_lit(self, args):
        return args[0].capitalize() == "True"

    def null_lit(self, args):
        return NULL_VALUE

    def float_lit(self, args):
        return float(args[0])

    def array_lit(self, args):
        types = [arg_to_datatype(arg) for arg in args]
        assert len(set(types)) == 1
        return ListWrapper(args, type=types[0])

    def literal(self, args):
        return args[0]

    def comparison(self, args) -> Comparison:
        return Comparison(left=args[0], right=args[2], operator=args[1])

    def expr_tuple(self, args):
        return Parenthetical(content=args)

    def parenthetical(self, args):
        return Parenthetical(content=args[0])

    def conditional(self, args):
        return Conditional(left=args[0], right=args[2], operator=args[1])

    def window_order(self, args):
        return WindowOrder(args[0])

    def window_order_by(self, args):
        # flatten tree
        return args[0]

    def window(self, args):
        return Window(count=args[1].value, window_order=args[0])

    def WINDOW_TYPE(self, args):
        return WindowType(args.strip())

    def window_item_over(self, args):
        return WindowItemOver(contents=args[0])

    def window_item_order(self, args):
        return WindowItemOrder(contents=args[0])

    def window_item(self, args) -> WindowItem:
        type = args[0]
        order_by = []
        over = []
        for item in args[2:]:
            if isinstance(item, WindowItemOrder):
                order_by = item.contents
            elif isinstance(item, WindowItemOver):
                over = item.contents
        concept = self.environment.concepts[args[1]]
        return WindowItem(type=type, content=concept, over=over, order_by=order_by)

    def filter_item(self, args) -> FilterItem:
        where: WhereClause
        string_concept, where = args
        concept = self.environment.concepts[string_concept]
        return FilterItem(content=concept, where=where)

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
        return [self.environment.concepts[f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"]]

    def aggregate_functions(self, args):
        if len(args) == 2:
            return AggregateWrapper(function=args[0], by=args[1])
        return AggregateWrapper(function=args[0])

    @v_args(meta=True)
    def index_access(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return IndexAccess(args)

    @v_args(meta=True)
    def attr_access(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return AttrAccess(args)

    @v_args(meta=True)
    def fcoalesce(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Coalesce(args)

    @v_args(meta=True)
    def unnest(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Unnest(args)

    @v_args(meta=True)
    def count(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Count(args)

    @v_args(meta=True)
    def fgroup(self, meta, args):
        if len(args) == 2:
            args = self.process_function_args([args[0]] + args[1], meta=meta)
        else:
            args = self.process_function_args([args[0]], meta=meta)
        return Group(args)

    @v_args(meta=True)
    def fabs(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Abs(args)

    @v_args(meta=True)
    def count_distinct(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return CountDistinct(args)

    @v_args(meta=True)
    def sum(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.SUM,
            arguments=args,
            output_datatype=args[0].datatype,
            output_purpose=Purpose.METRIC,
            arg_count=1,
        )

    @v_args(meta=True)
    def avg(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        arg = args[0]

        return Function(
            operator=FunctionType.AVG,
            arguments=args,
            output_datatype=arg.datatype,
            output_purpose=Purpose.METRIC,
            valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
            arg_count=1,
        )

    @v_args(meta=True)
    def max(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Max(args)

    @v_args(meta=True)
    def min(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Min(args)

    @v_args(meta=True)
    def len(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.LENGTH,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING, DataType.ARRAY, DataType.MAP},
            # output_grain=args[0].grain,
        )

    @v_args(meta=True)
    def fsplit(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Split(args)

    @v_args(meta=True)
    def concat(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.CONCAT,
            arguments=args,
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=99,
            # output_grain=args[0].grain,
        )

    @v_args(meta=True)
    def like(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.LIKE,
            arguments=args,
            output_datatype=DataType.BOOL,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=2,
        )

    @v_args(meta=True)
    def alt_like(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.LIKE,
            arguments=args,
            output_datatype=DataType.BOOL,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=2,
        )

    @v_args(meta=True)
    def ilike(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.ILIKE,
            arguments=args,
            output_datatype=DataType.BOOL,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=2,
        )

    @v_args(meta=True)
    def upper(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.UPPER,
            arguments=args,
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=1,
        )

    @v_args(meta=True)
    def fstrpos(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return StrPos(args)

    @v_args(meta=True)
    def fsubstring(self, meta, args):
        args = self.process_function_args(
            args,
            meta=meta,
        )
        return SubString(args)

    @v_args(meta=True)
    def lower(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.LOWER,
            arguments=args,
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=1,
        )

    # date functions
    @v_args(meta=True)
    def fdate(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DATE,
            arguments=args,
            output_datatype=DataType.DATE,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            arg_count=1,
        )

    def DATE_PART(self, args):
        return DatePart(args.value)

    @v_args(meta=True)
    def fdate_trunc(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DATE_TRUNCATE,
            arguments=args,
            output_datatype=DataType.DATE,
            output_purpose=Purpose.PROPERTY,
            valid_inputs=[
                {
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.DATETIME,
                    DataType.STRING,
                },
                {DataType.DATE_PART},
            ],
            arg_count=2,
        )

    @v_args(meta=True)
    def fdate_part(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DATE_PART,
            arguments=args,
            output_datatype=DataType.DATE,
            output_purpose=Purpose.PROPERTY,
            valid_inputs=[
                {
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.DATETIME,
                    DataType.STRING,
                },
                {DataType.DATE_PART},
            ],
            arg_count=2,
        )

    @v_args(meta=True)
    def fdate_add(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DATE_ADD,
            arguments=args,
            output_datatype=DataType.DATE,
            output_purpose=Purpose.PROPERTY,
            valid_inputs=[
                {
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.DATETIME,
                    DataType.STRING,
                },
                {DataType.DATE_PART},
                {DataType.INTEGER},
            ],
            arg_count=3,
        )

    @v_args(meta=True)
    def fdate_diff(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        purpose = function_args_to_output_purpose(args)
        return Function(
            operator=FunctionType.DATE_DIFF,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=purpose,
            valid_inputs=[
                {
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.DATETIME,
                },
                {
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.DATETIME,
                },
                {DataType.DATE_PART},
            ],
            arg_count=3,
        )

    @v_args(meta=True)
    def fdatetime(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DATETIME,
            arguments=args,
            output_datatype=DataType.DATETIME,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            arg_count=1,
        )

    @v_args(meta=True)
    def ftimestamp(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.TIMESTAMP,
            arguments=args,
            output_datatype=DataType.TIMESTAMP,
            output_purpose=Purpose.PROPERTY,
            valid_inputs=[{DataType.TIMESTAMP, DataType.STRING}],
            arg_count=1,
        )

    @v_args(meta=True)
    def fsecond(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.SECOND,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fminute(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.MINUTE,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fhour(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.HOUR,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fday(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DAY,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fday_of_week(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DAY_OF_WEEK,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fweek(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.WEEK,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fmonth(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.MONTH,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fquarter(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.QUARTER,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    @v_args(meta=True)
    def fyear(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.YEAR,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=1,
        )

    # utility functions
    @v_args(meta=True)
    def fcast(self, meta, args) -> Function:
        args = self.process_function_args(args, meta=meta)
        output_datatype = args[1]
        return Function(
            operator=FunctionType.CAST,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            valid_inputs={
                DataType.INTEGER,
                DataType.STRING,
                DataType.FLOAT,
                DataType.NUMBER,
            },
            arg_count=2,
        )

    # math functions
    @v_args(meta=True)
    def fadd(self, meta, args) -> Function:
        args = self.process_function_args(args, meta=meta)
        output_datatype = arg_to_datatype(args[0])
        # TODO: check for valid transforms?
        return Function(
            operator=FunctionType.ADD,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            # valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=2,
        )

    @v_args(meta=True)
    def fsub(self, meta, args) -> Function:
        args = self.process_function_args(args, meta=meta)
        output_datatype = arg_to_datatype(args[0])
        return Function(
            operator=FunctionType.SUBTRACT,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            # valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=2,
        )

    @v_args(meta=True)
    def fmul(self, meta, args) -> Function:
        args = self.process_function_args(args, meta=meta)
        output_datatype = arg_to_datatype(args[0])
        return Function(
            operator=FunctionType.MULTIPLY,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            # valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=2,
        )

    @v_args(meta=True)
    def fdiv(self, meta: Meta, args):
        output_datatype = arg_to_datatype(args[0])
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.DIVIDE,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            # valid_inputs={DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME},
            arg_count=2,
        )

    @v_args(meta=True)
    def fmod(self, meta: Meta, args):
        output_datatype = arg_to_datatype(args[0])
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.MOD,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            valid_inputs=[
                {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
                {DataType.INTEGER},
            ],
            arg_count=2,
        )

    @v_args(meta=True)
    def fround(self, meta, args) -> Function:
        args = self.process_function_args(args, meta=meta)
        output_datatype = arg_to_datatype(args[0])
        return Function(
            operator=FunctionType.ROUND,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=function_args_to_output_purpose(args),
            valid_inputs=[
                {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
                {DataType.INTEGER},
            ],
            arg_count=2,
        )

    def fcase(self, args: List[Union[CaseWhen, CaseElse]]):
        datatypes = set()
        for arg in args:
            output_datatype = arg_to_datatype(arg.expr)
            datatypes.add(output_datatype)
        if not len(datatypes) == 1:
            raise SyntaxError(
                f"All case expressions must have the same output datatype, got {datatypes}"
            )
        return Function(
            operator=FunctionType.CASE,
            arguments=args,
            output_datatype=datatypes.pop(),
            output_purpose=Purpose.PROPERTY,
            # valid_inputs=[{DataType.INTEGER, DataType.FLOAT, DataType.NUMBER}, {DataType.INTEGER}],
            arg_count=InfiniteFunctionArgs,
        )

    @v_args(meta=True)
    def fcase_when(self, meta, args) -> CaseWhen:
        args = self.process_function_args(args, meta=meta)
        return CaseWhen(comparison=args[0], expr=args[1])

    @v_args(meta=True)
    def fcase_else(self, meta, args) -> CaseElse:
        args = self.process_function_args(args, meta=meta)
        return CaseElse(expr=args[0])

    @v_args(meta=True)
    def fcurrent_date(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return CurrentDate(args)

    @v_args(meta=True)
    def fcurrent_datetime(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return CurrentDatetime(args)

    @v_args(meta=True)
    def fnot(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return IsNull(args)


def unpack_visit_error(e: VisitError):
    """This is required to get exceptions from imports, which would
    raise nested VisitErrors"""
    if isinstance(e.orig_exc, VisitError):
        unpack_visit_error(e.orig_exc)
    elif isinstance(e.orig_exc, (UndefinedConceptException, ImportError)):
        raise e.orig_exc
    elif isinstance(e.orig_exc, (ValidationError, TypeError)):
        raise InvalidSyntaxException(str(e.orig_exc))
    raise e


def parse_text(text: str, environment: Optional[Environment] = None) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | None
    ],
]:
    environment = environment or Environment(datasources={})
    parser = ParseToObjects(visit_tokens=True, text=text, environment=environment)

    try:
        parser.transform(PARSER.parse(text))
        # handle circular dependencies
        pass_two = parser.hydrate_missing()
        output = [v for v in pass_two if v]
    except VisitError as e:
        unpack_visit_error(e)
        # this will never be reached
        raise e
    except (
        UnexpectedCharacters,
        UnexpectedEOF,
        UnexpectedInput,
        UnexpectedToken,
        ValidationError,
        TypeError,
    ) as e:
        raise InvalidSyntaxException(str(e))

    return environment, output
