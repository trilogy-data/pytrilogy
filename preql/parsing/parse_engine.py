from os.path import join, dirname
from typing import Tuple, List, Optional

from lark import Lark, Transformer, v_args
from lark.tree import Meta

from preql.core.enums import (
    Purpose,
    DataType,
    Modifier,
    Ordering,
    FunctionType,
    ComparisonOperator,
    LogicalOperator,
    WindowOrder,
)
from preql.core.models import (
    WhereClause,
    Comparison,
    Conditional,
    Comment,
    Datasource,
    Concept,
    ColumnAssignment,
    Select,
    Address,
    Grain,
    SelectItem,
    ConceptTransform,
    Function,
    OrderItem,
    Environment,
    Limit,
    OrderBy,
    Metadata,
    Window,
    WindowItem,
)
from preql.parsing.exceptions import ParseError

grammar = r"""
    !start: ((statement _TERMINATOR) | comment )*
    ?statement: concept
    | datasource
    | select
    | import_statement
    
    _TERMINATOR :  ";"i /\s*/
    
    comment :   /#.*(\n|$)/ |  /\/\/.*\n/  
    
    // property display_name string
    concept_declaration: PURPOSE IDENTIFIER TYPE metadata?
    //customer_id.property first_name STRING;
    concept_property_declaration: PROPERTY IDENTIFIER TYPE metadata?
    //metric post_length <- len(post_text);
    concept_derivation:  (PURPOSE | PROPERTY) IDENTIFIER "<" "-" expr
    concept :  concept_declaration | concept_derivation | concept_property_declaration
    
    // datasource concepts
    datasource : "datasource" IDENTIFIER  "("  column_assignment_list ")"  grain_clause? "address" address 
    
    grain_clause: "grain" "(" column_list ")"
    
    address: IDENTIFIER
    
    concept_assignment: IDENTIFIER | (MODIFIER "[" concept_assignment "]" )
    
    column_assignment : (IDENTIFIER ":" concept_assignment) 
    
    column_assignment_list : (column_assignment "," )* column_assignment ","?
    
    column_list : (IDENTIFIER "," )* IDENTIFIER ","?
    
    import_statement : "import" (IDENTIFIER ".") * IDENTIFIER "as" IDENTIFIER
    
    // select statement
    select : "select"i select_list  where? comment* order_by? comment* limit? comment*
    
    // top 5 user_id
    window_item: "rank" (IDENTIFIER | select_transform | comment+ )  ("BY"i order_list)?
    
    select_item : (IDENTIFIER | select_transform | comment+ ) 
    
    select_list :  ( select_item "," )* select_item ","?
    
    //  count(post_id) -> post_count
    select_transform : expr "-" ">" IDENTIFIER metadata?
    
    metadata : "metadata" "(" IDENTIFIER "=" _string_lit ")"
    
    limit: "LIMIT"i /[0-9]+/
    
    !window_order: ("TOP"i | "BOTTOM"i)
    
    window: window_order /[0-9]+/
    
    window_order_by: "BY"i column_list
    
    order_list : (expr ORDERING "," )* expr ORDERING ","?
    
    ORDERING: ("ASC"i | "DESC"i)
    
    order_by: "ORDER"i "BY"i order_list
    
    //WHERE STATEMENT
    
    LOGICAL_OPERATOR: "AND"i | "OR"i
    
    conditional: expr LOGICAL_OPERATOR (conditional | expr)
    
    where: "WHERE"i (expr | conditional+ )
    
    expr_reference: IDENTIFIER
    
    COMPARISON_OPERATOR: ("=" | ">" | "<" | ">=" | "<" | "!=" )
    comparison: expr COMPARISON_OPERATOR expr
    
    expr: count | avg | sum | len | like | concat | comparison | literal | window_item | expr_reference
    
    // functions
    
    count: "count" "(" expr ")"
    sum: "sum" "(" expr ")"
    avg: "avg" "(" expr ")"
    len: "len" "(" expr ")"
    like: "like"i "(" expr "," _string_lit ")"
    concat: "concat"i "(" (expr ",")* expr ")"
    
    // base language constructs
    IDENTIFIER : /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-]*/
    
    STRING_CHARS: /(?:(?!\${)([^"\\]|\\.))+/+ // any character except '"" 
    
    _string_lit: "\"" ( STRING_CHARS )* "\"" 
    
    int_lit: /[0-9]+/
    
    float_lit: /[0-9]+\.[0-9]+/
    
    bool_lit: "True" | "False"
    
    literal: _string_lit | int_lit | float_lit | bool_lit

    MODIFIER: "Optional"i | "Partial"i
    
    TYPE : "string" | "number" | "bool" | "map" | "list" | "any" | "int" | "date" | "datetime" | "timestamp" | "float"
    
    PURPOSE:  "key" | "metric"
    PROPERTY: "property"
    


    %import common.WS_INLINE -> _WHITESPACE
    %import common.WS
    %ignore WS
"""

PARSER = Lark(grammar, start="start", propagate_positions=True)


class ParseToObjects(Transformer):
    def __init__(self, visit_tokens, text, environment: Environment):
        Transformer.__init__(self, visit_tokens)
        self.text = text
        self.environment = environment

    def start(self, args):
        return args

    def metadata(self, args):
        return Metadata()

    def IDENTIFIER(self, args) -> str:
        return args.value

    def STRING_CHARS(self, args) -> str:
        return args.value

    def BOOLEAN_LIT(self, args) -> bool:
        return bool(args)

    def TYPE(self, args) -> DataType:
        return DataType(args)

    def COMPARISON_OPERATOR(self, args) -> ComparisonOperator:
        return ComparisonOperator(args)

    def LOGICAL_OPERATOR(self, args) -> LogicalOperator:
        return LogicalOperator(args.lower())

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
            concept=self.environment.concepts[concept[0]],
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
        grain, name = args[1].rsplit(".", 1)
        existing = self.environment.concepts.get(name)
        if existing:
            raise ParseError(
                f"Concept {name} on line {meta.line} is a duplicate declaration"
            )
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            grain=Grain(components=[self.environment.concepts[grain]]),
            namespace=self.environment.namespace,
        )
        self.environment.concepts[name] = concept
        return args

    @v_args(meta=True)
    def concept_declaration(self, meta: Meta, args) -> Concept:

        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        existing = self.environment.concepts.get(name)
        if existing:
            raise ParseError(
                f"Concept {name} on line {meta.line} is a duplicate declaration"
            )
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            namespace=self.environment.namespace,
        )
        self.environment.concepts[name] = concept
        return args

    @v_args(meta=True)
    def concept_derivation(self, meta: Meta, args) -> Concept:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        existing = self.environment.concepts.get(name)
        if existing:
            raise ParseError(
                f"Concept {name} on line {meta.line} is a duplicate declaration"
            )
        if isinstance(args[2], WindowItem):
            window_item: WindowItem = args[2]
            concept = Concept(
                name=name,
                datatype=window_item.content.datatype,
                purpose=args[0],
                metadata=metadata,
                lineage=window_item,
                # grain=function.output_grain,
                namespace=self.environment.namespace,
            )
            self.environment.concepts[name] = concept
            return args
        else:
            function: Function = args[2]
            concept = Concept(
                name=name,
                datatype=function.output_datatype,
                purpose=args[0],
                metadata=metadata,
                lineage=function,
                # grain=function.output_grain,
                namespace=self.environment.namespace,
            )
            self.environment.concepts[name] = concept
            return args

    @v_args(meta=True)
    def concept(self, meta: Meta, args) -> Concept:
        return args[0]

    def column_assignment_list(self, args):
        return args

    def column_list(self, args) -> List:
        return args

    def grain_clause(self, args) -> Grain:
        return Grain(components=[self.environment.concepts[a] for a in args[0]])

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
        if not address:
            raise ValueError("Malformed datasource, missing address declaration")

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
        function: Function = args[0]
        output: str = args[1]
        existing = self.environment.concepts.get(output)
        if existing:
            raise ParseError(
                f"Assignment to concept {output} on line {meta.line} is a duplicate declaration for this concept"
            )
        concept = Concept(
            name=output,
            datatype=function.output_datatype,
            purpose=function.output_purpose,
            lineage=function,
            namespace=self.environment.namespace,
        )
        # We don't assign it here because we'll do this later when we know the grain
        self.environment.concepts[output] = concept
        return ConceptTransform(function=function, output=concept)

    @v_args(meta=True)
    def select_item(self, meta: Meta, args) -> Optional[SelectItem]:

        args = [arg for arg in args if not isinstance(arg, Comment)]
        if not args:
            return None
        if len(args) != 1:
            raise ParseError(
                f"Malformed select statement {args} {self.text[meta.start_pos:meta.end_pos]}"
            )
        content = args[0]
        if isinstance(content, ConceptTransform):
            return SelectItem(content=content)
        return SelectItem(content=self.environment.concepts[content])

    def select_list(self, args):
        return [arg for arg in args if arg]

    def limit(self, args):
        return Limit(count=args[0].value)

    def ORDERING(self, args):
        return Ordering(args)

    def order_list(self, args):
        return [OrderItem(x, y) for x, y in zip(args[::2], args[1::2])]

    def order_by(self, args):
        return OrderBy(items=args[0])

    def import_statement(self, args):
        alias = args[-1]
        path = args[0].split(".")

        target = join(self.environment.working_path, *path) + ".preql"
        with open(target, "r", encoding="utf-8") as f:
            text = f.read()
            nparser = ParseToObjects(
                visit_tokens=True,
                text=text,
                environment=Environment(
                    {}, {}, working_path=dirname(target), namespace=alias
                ),
            )
            nparser.transform(PARSER.parse(text))

            for key, concept in nparser.environment.concepts.items():
                self.environment.concepts[f"{alias}.{key}"] = concept.with_namespace(
                    alias
                )
            for key, datasource in nparser.environment.datasources.items():
                self.environment.datasources[
                    f"{alias}.{key}"
                ] = datasource.with_namespace(alias)
        return None

    @v_args(meta=True)
    def select(self, meta: Meta, args):

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
                item.content.output = new_concept
            elif isinstance(item.content, Concept):
                new_concept = item.content.with_grain(output.grain)
                item.content = new_concept
            elif isinstance(item.content, WindowItem):
                new_concept = item.content.output.with_grain(output.grain)
                item.content.output = new_concept
            else:
                raise ValueError
            self.environment.concepts[new_concept.name] = new_concept
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

    def where(self, args):
        return WhereClause(conditional=args[0])

    def int_lit(self, args):
        return int(args[0])

    def bool_lit(self, args):
        return bool(args[0])

    def float_lit(self, args):
        return float(args[0])

    def literal(self, args):
        return args[0]

    def comparison(self, args):
        return Comparison(args[0], args[2], args[1])

    def conditional(self, args):
        return Conditional(args[0], args[2], args[1])

    def window_order(self, args):
        return WindowOrder(args[0])

    def window_order_by(self, args):
        # flatten tree
        return args[0]

    def window(self, args):
        return Window(count=args[1].value, window_order=args[0])

    def window_item(self, args):
        if len(args) > 1:
            sort_concepts = args[1]
        else:
            sort_concepts = []
        concept = self.environment.concepts[args[0]]
        # sort_concepts_mapped = [self.environment.concepts[x].with_grain(concept.grain) for x in sort_concepts]
        return WindowItem(content=concept, order_by=sort_concepts)

    # BEGIN FUNCTIONS
    def expr_reference(self, args) -> Concept:
        return self.environment.concepts[args[0]]

    def expr(self, args):
        if len(args) > 1:
            raise ParseError("Expression should have one child only.")
        return args[0]

    def count(self, args):
        return Function(
            operator=FunctionType.COUNT,
            arguments=args,
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
        )

    def sum(self, arguments):
        if not len(arguments) == 1:
            raise ParseError("Too many arguments to sum")
        return Function(
            operator=FunctionType.SUM,
            arguments=arguments,
            output_datatype=arguments[0].datatype,
            output_purpose=Purpose.METRIC,
            # output_grain=Grain(components=arguments),
        )

    def avg(self, arguments):
        if not len(arguments) == 1:
            raise ParseError("Too many arguments to avg")
        return Function(
            operator=FunctionType.AVG,
            arguments=arguments,
            output_datatype=arguments[0].datatype,
            output_purpose=Purpose.METRIC,
            # output_grain=Grain(components=arguments),
        )

    def len(self, args):
        if not len(args) == 1:
            raise ParseError("Too many arguments to len")
        return Function(
            operator=FunctionType.LENGTH,
            arguments=args,
            output_datatype=args[0].datatype,
            output_purpose=Purpose.METRIC,
            valid_inputs={DataType.STRING, DataType.ARRAY},
            # output_grain=args[0].grain,
        )

    def concat(self, args):
        return Function(
            operator=FunctionType.CONCAT,
            arguments=args,
            output_datatype=args[0].datatype,
            output_purpose=Purpose.KEY,
            valid_inputs={DataType.STRING},
            # output_grain=args[0].grain,
        )

    def like(self, args):
        if not len(args) == 2:
            raise ParseError("The Like function expects two arguments")
        return Function(
            operator=FunctionType.LIKE,
            arguments=args,
            output_datatype=DataType.BOOL,
            output_purpose=Purpose.PROPERTY,
            valid_inputs={DataType.STRING},
            # output_grain=Grain(components=args),
        )


def parse_text(
    text: str, environment: Optional[Environment] = None, print_flag: bool = False
) -> Tuple[Environment, List]:
    environment = environment or Environment(datasources={})
    parser = ParseToObjects(visit_tokens=True, text=text, environment=environment)
    output = [v for v in parser.transform(PARSER.parse(text)) if v]
    return environment, output
