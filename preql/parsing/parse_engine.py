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

from preql.constants import DEFAULT_NAMESPACE, NULL_VALUE
from preql.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    DataType,
    FunctionType,
    InfiniteFunctionArgs,
    Modifier,
    Ordering,
    Purpose,
    WindowOrder,
    WindowType,
    DatePart,
)
from preql.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from preql.core.functions import (
    Count,
    CountDistinct,
    Max,
    Min,
    Split,
    IndexAccess,
    Abs,
    Unnest,
    Coalesce,
    function_args_to_output_purpose,
    CurrentDate,
    CurrentDatetime,
)
from preql.core.models import (
    Address,
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
    Environment,
    FilterItem,
    Function,
    Grain,
    Import,
    Limit,
    Metadata,
    OrderBy,
    OrderItem,
    Parenthetical,
    Persist,
    Query,
    Select,
    SelectItem,
    WhereClause,
    Window,
    WindowItem,
    WindowItemOrder,
    WindowItemOver,
    RawColumnExpr,
    arg_to_datatype,
    ListWrapper,
)
from preql.parsing.exceptions import ParseError
from preql.utility import string_to_hash

grammar = r"""
    !start: ( block | show |comment )*
    block: statement _TERMINATOR comment?
    ?statement: concept
    | datasource
    | select
    | persist
    | import_statement
    
    _TERMINATOR:  ";"i /\s*/
    
    comment:   /#.*(\n|$)/ |  /\/\/.*\n/  
    
    // property display_name string
    concept_declaration: PURPOSE IDENTIFIER TYPE metadata?
    //customer_id.property first_name STRING;
    concept_property_declaration: PROPERTY IDENTIFIER TYPE metadata?
    //metric post_length <- len(post_text);
    concept_derivation:  (PURPOSE | PROPERTY | CONST | AUTO ) IDENTIFIER "<" "-" expr
    
    constant_derivation: CONST IDENTIFIER "<" "-" literal
    
    concept:  concept_declaration | concept_derivation | concept_property_declaration | constant_derivation
    
    // datasource concepts
    datasource: "datasource" IDENTIFIER  "("  column_assignment_list ")"  grain_clause? (address | query)
    
    grain_clause: "grain" "(" column_list ")"
    
    address: "address" IDENTIFIER
    
    query: "query" MULTILINE_STRING
    
    concept_assignment: IDENTIFIER | (MODIFIER "[" concept_assignment "]" )
    
    column_assignment: ((IDENTIFIER | raw_column_assignment ) ":" concept_assignment) 

    raw_column_assignment: "raw" "(" MULTILINE_STRING ")"
    
    column_assignment_list : (column_assignment "," )* column_assignment ","?
    
    column_list : (IDENTIFIER "," )* IDENTIFIER ","?
    
    import_statement: "import" (IDENTIFIER ".") * IDENTIFIER "as" IDENTIFIER

    // persist_statement
    persist: "persist"i IDENTIFIER "into"i IDENTIFIER "from"i select grain_clause?

    // select statement
    select: "select"i select_list  where? comment* order_by? comment* limit? comment*
    
    // user_id where state = Mexico
    filter_item: "filter"i IDENTIFIER where

    // rank/lag/lead
    WINDOW_TYPE: ("rank"i|"lag"i|"lead"i)  /[\s]+/
    
    window_item: WINDOW_TYPE (IDENTIFIER | select_transform | comment+ ) window_item_over? window_item_order?
    
    window_item_over: ("OVER"i over_list)
    
    window_item_order: ("ORDER"i? "BY"i order_list)
    
    select_item: (IDENTIFIER | select_transform | comment+ ) | ("~" select_item)
    
    select_list:  ( select_item "," )* select_item ","?
    
    //  count(post_id) -> post_count
    select_transform : expr "-" ">" IDENTIFIER metadata?
    
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

    COMPARISON_OPERATOR: (/is[\s]+not/ | "is" |"=" | ">" | "<" | ">=" | "<" | "!="  )
    
    comparison: (expr COMPARISON_OPERATOR expr) | (expr array_comparison expr_tuple) 
    
    expr_tuple: "("  (expr ",")* expr ","?  ")"

    //unnesting is a function
    unnest: "UNNEST"i "(" expr ")"
    //indexing into an expression is a function
    index_access: expr "[" int_lit "]"

    parenthetical: "(" (conditional | expr) ")"
    
    expr: window_item | filter_item |  aggregate_functions | unnest | _string_functions | _math_functions | _generic_functions | _constant_functions| _date_functions | comparison | literal |  expr_reference  | index_access | parenthetical
    
    // functions
    
    //math TODO: add syntactic sugar
    fadd: ("add"i "(" expr "," expr ")" ) | ( expr "+" expr )
    fsub: ("subtract"i "(" expr "," expr ")" ) | ( expr "-" expr )
    fmul: ("multiply"i "(" expr "," expr ")" ) | ( expr "*" expr )
    fdiv: ( "divide"i "(" expr "," expr ")") | ( expr "/" expr )
    fround: "round"i "(" expr "," expr ")"
    fabs: "abs"i "(" expr ")"
    
    _math_functions: fadd | fsub | fmul | fdiv | fround
    
    //generic
    fcast: "cast"i "(" expr "AS"i TYPE ")"
    concat: "concat"i "(" (expr ",")* expr ")"
    fcoalesce: "coalesce"i "(" (expr ",")* expr ")"
    fcase_when: "WHEN"i comparison "THEN"i expr
    fcase_else: "ELSE"i expr
    fcase: "CASE"i (fcase_when)* (fcase_else)? "END"i
    len: "len"i "(" expr ")"

    _generic_functions: fcast | concat | fcoalesce | fcase | len 

    //constant
    fcurrent_date: "current_date"i "(" ")"
    fcurrent_datetime: "current_datetime"i "(" ")"

    _constant_functions: fcurrent_date | fcurrent_datetime
    
    //string
    like: "like"i "(" expr "," _string_lit ")"
    ilike: "ilike"i "(" expr "," _string_lit ")"
    upper: "upper"i "(" expr ")"
    lower: "lower"i "(" expr ")"    
    fsplit: "split"i "(" expr "," _string_lit ")"
    
    _string_functions: like | ilike | upper | lower | fsplit
    
    //aggregates
    count: "count"i "(" expr ")"
    count_distinct: "count_distinct"i "(" expr ")"
    sum: "sum"i "(" expr ")"
    avg: "avg"i "(" expr ")"
    max: "max"i "(" expr ")"
    min: "min"i "(" expr ")"
    
    //aggregates can force a grain
    aggregate_over: ("BY"i over_list)
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
    
    DATE_PART: "DAY"i | "WEEK"i | "MONTH"i | "QUARTER"i | "YEAR"i
    fdate_trunc: "date_trunc"i "(" expr "," DATE_PART ")"
    fdate_part: "date_part"i "(" expr ")"
    
    _date_functions: fdate | fdatetime | ftimestamp | fsecond | fminute | fhour | fday | fday_of_week | fweek | fmonth | fquarter | fyear | fdate_part | fdate_trunc
    
    // base language constructs
    IDENTIFIER : /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-]*/
    
    MULTILINE_STRING: /\'{3}(.*?)\'{3}/s
    
    DOUBLE_STRING_CHARS: /(?:(?!\${)([^"\\]|\\.))+/+ // any character except "
    SINGLE_STRING_CHARS: /(?:(?!\${)([^'\\]|\\.))+/+ // any character except '
    _single_quote: "'" ( SINGLE_STRING_CHARS )* "'" 
    _double_quote: "\"" ( DOUBLE_STRING_CHARS )* "\"" 
    _string_lit: _single_quote | _double_quote
    
    int_lit: /[0-9]+/
    
    float_lit: /[0-9]+\.[0-9]+/

    array_lit: "[" (literal ",")* literal ","? "]"
    
    !bool_lit: "True"i | "False"i

    !null_lit: "null"i
    
    literal: _string_lit | int_lit | float_lit | bool_lit | null_lit | array_lit

    MODIFIER: "Optional"i | "Partial"i
    
    TYPE: "string"i | "number"i | "map"i | "list"i | "any"i | "int"i | "date"i | "datetime"i | "timestamp"i | "float"i | "bool"i 
    
    PURPOSE:  "key"i | "metric"i
    PROPERTY: "property"i
    CONST: "const"i | "constant"i
    AUTO: "AUTO"i 

    // meta functions
    SHOW: "show"i
    CONCEPTS: "CONCEPTS"i
    DATASOURCES: "DATASOURCES"i

    show: SHOW (CONCEPTS | DATASOURCES)

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
    input: Union[Function, AggregateWrapper, int, str, float, bool]
) -> Function:
    if isinstance(input, Function):
        return input
    elif isinstance(input, AggregateWrapper):
        return input.function
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

    def process_function_args(self, args, meta: Meta):
        final = []
        for arg in args:
            # if a function has an anonymous function argument
            # create an implicit concept
            if isinstance(arg, Function):
                id_hash = string_to_hash(str(arg))
                concept = Concept(
                    name=f"_anon_function_input_{id_hash}",
                    datatype=arg.output_datatype,
                    purpose=arg.output_purpose,
                    lineage=arg,
                    namespace=DEFAULT_NAMESPACE,
                    grain=None,
                    keys=None,
                )
                # to satisfy mypy, concept will always have metadata
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            elif isinstance(arg, FilterItem):
                id_hash = string_to_hash(str(arg))
                concept = Concept(
                    name=f"_anon_function_input_{id_hash}",
                    datatype=arg.content.datatype,
                    purpose=arg.content.purpose,
                    lineage=arg,
                    # filters are implicitly at the grain of the base item
                    grain=Grain(components=[arg.output]),
                    namespace=DEFAULT_NAMESPACE,
                )
                if concept.metadata:
                    concept.metadata.line_number = meta.line
                self.environment.add_concept(concept, meta=meta)
                final.append(concept)
            elif isinstance(arg, AggregateWrapper):
                parent = arg
                aggfunction = arg.function
                id_hash = string_to_hash(str(arg))
                concept = Concept(
                    name=f"_anon_function_input_{id_hash}",
                    datatype=aggfunction.output_datatype,
                    purpose=aggfunction.output_purpose,
                    lineage=aggfunction,
                    grain=Grain(components=parent.by)
                    if parent.by
                    else aggfunction.output_grain,
                    namespace=DEFAULT_NAMESPACE,
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
        if isinstance(output, Concept):
            if len(args) > 1 and isinstance(args[1], Comment):
                output.metadata.description = (
                    output.metadata.description or args[1].text.split("#")[1].strip()
                )
        return args[0]

    def metadata(self, args):
        pairs = {key: val for key, val in zip(args[::2], args[1::2])}
        return Metadata(**pairs)

    def IDENTIFIER(self, args) -> str:
        return args.value

    def STRING_CHARS(self, args) -> str:
        return args.value

    def SINGLE_STRING_CHARS(self, args) -> str:
        return args.value

    def DOUBLE_STRING_CHARS(self, args) -> str:
        return args.value

    def TYPE(self, args) -> DataType:
        return DataType(args.lower())

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
        return ColumnAssignment(
            alias=args[0],
            modifiers=modifiers,
            concept=self.environment.concepts.__getitem__(  # type: ignore
                key=concept[0], line_no=meta.line
            ),
        )

    def _TERMINATOR(self, args):
        return None

    def MODIFIER(self, args) -> Modifier:
        return Modifier(args.value)

    def PURPOSE(self, args) -> Purpose:
        return Purpose(args.value)

    def PROPERTY(self, args):
        return Purpose.PROPERTY

    @v_args(meta=True)
    def concept_property_declaration(self, meta: Meta, args) -> Concept:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        if "." not in args[1]:
            raise ParseError(
                f"Property declaration {args[1]} must be fully qualified with a parent key"
            )
        grain, name = args[1].rsplit(".", 1)
        parent = self.environment.concepts[grain]
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            grain=Grain(components=[self.environment.concepts[grain]]),
            namespace=parent.namespace,
            keys=[parent],
        )
        self.environment.add_concept(concept, meta)
        return concept

    @v_args(meta=True)
    def concept_declaration(self, meta: Meta, args) -> Concept:
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
        return concept

    @v_args(meta=True)
    def concept_derivation(self, meta: Meta, args) -> Concept:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        purpose = args[0]
        if purpose == "auto":
            purpose = None
        name = args[1]

        lookup, namespace, name, parent_concept = parse_concept_reference(
            name, self.environment, purpose
        )
        if isinstance(args[2], FilterItem):
            filter_item: FilterItem = args[2]
            concept = Concept(
                name=name,
                datatype=filter_item.content.datatype,
                purpose=purpose
                if purpose
                else filter_item.content.purpose,  # filter_item.content.purpose,
                metadata=metadata,
                lineage=filter_item,
                # filters are implicitly at the grain of the base item
                # 2023-08-18 - asked why, commented out
                # grain=Grain(components=[filter_item.output]),
                namespace=namespace,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return concept
        elif isinstance(args[2], WindowItem):
            window_item: WindowItem = args[2]
            if purpose == Purpose.PROPERTY:
                keys = [window_item.content]
            else:
                keys = []
            concept = Concept(
                name=name,
                datatype=window_item.content.datatype,
                purpose=purpose if purpose else window_item.content.purpose,
                metadata=metadata,
                lineage=window_item,
                # windows are implicitly at the grain of the group by + the original content
                grain=Grain(components=window_item.over + [window_item.content.output]),
                namespace=namespace,
                keys=keys,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return concept
        elif isinstance(args[2], AggregateWrapper):
            parent: AggregateWrapper = args[2]
            aggfunction: Function = parent.function
            concept = Concept(
                name=name,
                datatype=aggfunction.output_datatype,
                purpose=aggfunction.output_purpose,
                metadata=metadata,
                lineage=aggfunction,
                grain=Grain(components=parent.by)
                if parent.by
                else aggfunction.output_grain,
                namespace=namespace,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return concept
        elif isinstance(args[2], (int, float, str, bool, list)):
            const_function: Function = Function(
                operator=FunctionType.CONSTANT,
                output_datatype=arg_to_datatype(args[2]),
                output_purpose=Purpose.CONSTANT,
                arguments=[args[2]],
            )
            concept = Concept(
                name=name,
                datatype=const_function.output_datatype,
                purpose=Purpose.CONSTANT,
                metadata=metadata,
                lineage=const_function,
                grain=const_function.output_grain,
                namespace=namespace,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return concept

        elif isinstance(args[2], Function):
            function: Function = args[2]
            # if purpose != function.output_purpose:
            #     raise SyntaxError(f'Invalid output purpose assigned {purpose}')
            concept = Concept(
                name=name,
                datatype=function.output_datatype,
                purpose=purpose if purpose else function.output_purpose,
                metadata=metadata,
                lineage=function,
                grain=function.output_grain,
                namespace=namespace,
                keys=[self.environment.concepts[parent_concept]]
                if parent_concept
                else None,
            )
            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return concept
        raise SyntaxError(
            f"Received invalid type {type(args[2])} {args[2]} as input to select"
            " transform"
        )

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
    def concept(self, meta: Meta, args) -> Concept:
        concept: Concept = args[0]
        if concept.metadata:
            concept.metadata.line_number = meta.line
        return args[0]

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
        function = unwrap_transformation(args[0])
        output: str = args[1]

        lookup, namespace, output, parent = parse_concept_reference(
            output, self.environment
        )
        # keys are used to pass through derivations

        if function.output_purpose == Purpose.PROPERTY:
            keys = [x for x in function.arguments if isinstance(x, Concept)]
            grain = Grain(components=keys)
        else:
            grain = None
            keys = None
        concept = Concept(
            name=output,
            datatype=function.output_datatype,
            purpose=function.output_purpose,
            lineage=function,
            namespace=namespace,
            grain=grain,
            keys=keys,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
        self.environment.add_concept(concept, meta=meta)
        return ConceptTransform(function=function, output=concept)

    @v_args(meta=True)
    def select_item(self, meta: Meta, args) -> Optional[SelectItem]:
        args = [arg for arg in args if not isinstance(arg, Comment)]
        if not args:
            return None
        if len(args) != 1:
            raise ParseError(
                "Malformed select statement"
                f" {args} {self.text[meta.start_pos:meta.end_pos]}"
            )
        content = args[0]
        if isinstance(content, ConceptTransform):
            return SelectItem(content=content)
        return SelectItem(
            content=self.environment.concepts.__getitem__(content, meta.line)
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
                        namespace=alias,
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
            self.environment.concepts[f"{alias}.{key}"] = concept
        for key, datasource in nparser.environment.datasources.items():
            self.environment.datasources[f"{alias}.{key}"] = datasource
        self.environment.imports[alias] = Import(alias=alias, path=args[0])
        return None

    @v_args(meta=True)
    def show(self, meta: Meta, args) -> Select:
        raise NotImplementedError("TODO: let users query current model values")
        output = Select(
            selection=SelectItem(), where_clause=None, limit=None, order_by=None
        )
        return output

    @v_args(meta=True)
    def persist(self, meta: Meta, args) -> Persist:
        identifier: str = args[0]
        address: str = args[1]
        select: Select = args[2]
        if len(args) > 3:
            grain: Grain | None = args[3]
        else:
            grain = None
        columns = [
            # TODO: replace hardcoded replacement here
            ColumnAssignment(alias=c.address.replace(".", "_"), concept=c)
            for c in select.output_components
        ]
        new_datasource = Datasource(
            identifier=identifier,
            address=address,
            grain=grain or select.grain,
            columns=columns,
            namespace=self.environment.namespace,
        )
        for column in columns:
            column.concept = column.concept.with_grain(new_datasource.grain)
        # self.environment.add_datasource(new_datasource)
        return Persist(select=select, datasource=new_datasource)

    @v_args(meta=True)
    def select(self, meta: Meta, args) -> Select:
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
        output = Select(
            selection=select_items, where_clause=where, limit=limit, order_by=order_by
        )
        for item in select_items:
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if isinstance(item.content, ConceptTransform):
                new_concept = item.content.output.with_grain(output.grain)
                self.environment.concepts[new_concept.address] = new_concept
                item.content.output = new_concept
            # elif isinstance(item.content, Concept):
            #     # new_concept = item.content.with_grain(output.grain)
            #     item.content = new_concept
            # elif isinstance(item.content, WindowItem):
            #     new_concept = item.content.output.with_grain(output.grain)
            #     item.content.output = new_concept
            # else:
            #     raise ValueError

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
        return WhereClause(conditional=args[0])

    def int_lit(self, args):
        return int(args[0])

    def bool_lit(self, args):
        return args[0].capitalize() == "True"

    def null_lit(self, args):
        return NULL_VALUE

    def float_lit(self, args):
        return float(args[0])

    def array_lit(self, args):
        return ListWrapper(args)

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

    def aggregate_functions(self, args):
        if len(args) == 2:
            return AggregateWrapper(function=args[0], by=args[1])
        return AggregateWrapper(function=args[0])

    @v_args(meta=True)
    def index_access(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return IndexAccess(args)

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
            arg_count=1
            # output_grain=Grain(components=arguments),
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
            arg_count=1
            # output_grain=Grain(components=arguments),
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
            arg_count=99
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
            arg_count=2
            # output_grain=Grain(components=args),
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
            arg_count=2
            # output_grain=Grain(components=args),
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
            arg_count=1
            # output_grain=Grain(components=args),
        )

    @v_args(meta=True)
    def lower(self, meta, args):
        args = self.process_function_args(args, meta=meta)
        return Function(
            operator=FunctionType.LOWER,
            arguments=args,
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            arg_count=1
            # output_grain=Grain(components=args),
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


def unpack_visit_error(e: VisitError):
    """This is required to get exceptions from imports, which would
    raise nested VisitErrors"""
    if isinstance(e.orig_exc, VisitError):
        unpack_visit_error(e.orig_exc)
    if isinstance(e.orig_exc, (UndefinedConceptException, TypeError)):
        raise e.orig_exc
    if isinstance(e.orig_exc, ImportError):
        raise e.orig_exc
    elif isinstance(e.orig_exc, ValidationError):
        raise InvalidSyntaxException(str(e.orig_exc))
    raise e


def parse_text(
    text: str, environment: Optional[Environment] = None
) -> Tuple[Environment, List[Datasource | Import | Select | Persist | None]]:
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
    ) as e:
        raise InvalidSyntaxException(str(e))

    return environment, output
