from lark import Lark, Transformer, v_args
from lark.tree import Meta
from typing import Dict, Tuple, List, Optional
from preql.core.models import WhereClause, Comparison, Conditional, Comment, Datasource, Concept, ColumnAssignment, Select, Address, Grain, SelectItem, ConceptTransform, Function, OrderItem, Environment, Limit, OrderBy
from preql.core.enums import Purpose, DataType, Modifier, Ordering, FunctionType, Boolean, ComparisonOperator, LogicalOperator
from preql.parsing.exceptions import ParseError


grammar = r"""
    !start: ((statement ";") | comment )*
    ?statement: concept
    | datasource
    | select
    | import_statement
    
    comment :   /#.*(\n|$)/ |  /\/\/.*\n/  
    
    // property display_name string
    concept_declaration: PURPOSE IDENTIFIER TYPE metadata?
    //metric post_length <- len(post_text);
    concept_derivation:  PURPOSE IDENTIFIER "<" "-" expr
    concept :  concept_declaration | concept_derivation
    
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
    select : "select"i select_list comment* where? order_by? comment* limit? comment*
    
    select_item : (IDENTIFIER | select_transform | comment )
    
    select_list :  ( select_item "," )* select_item ","?
    
    // 
    select_transform : expr "-" ">" IDENTIFIER metadata?
    
    metadata : "metadata" "(" IDENTIFIER "=" _string_lit ")"
    
    limit: "LIMIT"i /[0-9]+/
    
    order_list : (IDENTIFIER ORDERING "," )* IDENTIFIER ORDERING ","?
    
    ORDERING: ("ASC"i | "DESC"i)
    
    order_by: "ORDER"i "BY"i order_list
    
    //WHERE STATEMENT
    
    LOGICAL_OPERATOR: "AND"i | "OR"i
    
    conditional: expr LOGICAL_OPERATOR (conditional | expr)
    
    where: "WHERE" conditional+ 
    
    expr_reference: IDENTIFIER
    
    expr: count | avg | sum | len | comparison | literal | expr_reference
    
    
    COMPARISON_OPERATOR: ("=" | ">" | "<" | ">=" | "<" | "!=" )
    comparison: expr COMPARISON_OPERATOR expr
    
    // functions
    
    count: "count" "(" expr ")"
    sum: "sum" "(" expr ")"
    avg: "avg" "(" expr ")"
    len: "len" "(" expr ")"
    
    // base language constructs
    IDENTIFIER : /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-]*/
    
    STRING_CHARS: /(?:(?!\${)([^"\\]|\\.))+/+ // any character except '"" 
    
    _string_lit: "\"" ( STRING_CHARS )* "\"" 
    
    _int_lit: /[0-9]+/
    
    _float_lit: /[0-9]+\.[0-9]+/
    
    literal: _string_lit | _int_lit | _float_lit

    MODIFIER: "Optional"i | "Partial"i
    
    TYPE : "string" | "number" | "bool" | "map" | "list" | "any" | "int" | "date" | "datetime" | "timestamp"
    
    PURPOSE: "property" | "key" | "metric"

    %import common.WS_INLINE -> _WHITESPACE
    %import common.WS
    %ignore WS
"""


PARSER = Lark(grammar, start="start", propagate_positions=True)


class ParseToObjects(Transformer):
    def __init__(self, visit_tokens, text, environment:Environment)  :
        Transformer.__init__(self, visit_tokens)
        self.text = text
        self.environment = environment

    def start(self, args):
        return args

    def IDENTIFIER(self, args)->str:
        return args.value

    def STRING_CHARS(self, args)->str:
        return args.value

    def TYPE(self, args)->DataType:
        return DataType(args)

    def COMPARISON_OPERATOR(self, args) -> ComparisonOperator:
        return ComparisonOperator(args)

    def LOGICAL_OPERATOR(self, args) -> LogicalOperator:
        return LogicalOperator(args.lower())

    def concept_assignment(self, args):
        return args

    @v_args(meta=True)
    def column_assignment(self, meta: Meta, args):
        #TODO -> deal with conceptual modifiers
        modifiers = []
        concept = args[1]
        # recursively collect modifiers
        while len(concept)>1:
            modifiers.append(concept[0])
            concept = concept[1]
        return ColumnAssignment(alias=args[0],
                                modifiers = modifiers,
                                concept =self.environment.concepts[concept[0]])

    def MODIFIER(self, args)->Modifier:
        return Modifier(args.value)

    def PURPOSE(self, args)->Purpose:
        return Purpose(args.value)

    @v_args(meta=True)
    def concept_declaration(self, meta:Meta, args)->Concept:

        if len(args) >3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        existing = self.environment.concepts.get(name)
        if existing:
            raise ParseError(f'Concept {name} on line {meta.line} is a duplicate declaration' )
        concept = Concept(name=name,
                          datatype = args[2],
                          purpose=args[0],
                          metadata=metadata
                          )
        self.environment.concepts[name] = concept
        return args

    @v_args(meta=True)
    def concept_derivation(self,meta:Meta, args)->Concept:
        if len(args) >3:
            metadata = args[3]
        else:
            metadata = None
        name = args[1]
        existing = self.environment.concepts.get(name)
        function = args[2]
        if existing:
            raise ParseError(f'Concept {name} on line {meta.line} is a duplicate declaration' )
        concept = Concept(name=name,
                          datatype = function.output_datatype,
                          purpose=args[0],
                          metadata=metadata,
                          lineage = function
                          )
        self.environment.concepts[name] = concept
        return args

    @v_args(meta=True)
    def concept(self, meta: Meta, args)->Concept:
        return args[0]

    def column_assignment_list(self, args):
        return args

    def column_list(self, args):
        if len(args)>1:
            raise ParseError('Invalid column list, should have one child')
        return args[0]

    def grain_clause(self, args)->Grain:
        return Grain([self.environment.concepts[a] for a in args])

    @v_args(meta=True)
    def datasource(self, meta: Meta, args):

        name = args[0]
        columns=args[1]
        grain:Optional[Grain] = None
        address:Optional[Address] = None
        for val in args[1:]:
            if isinstance(val, Address):
                address = val
            elif isinstance(object, Grain):
                grain = val
        datasource = Datasource(identifier=name, columns=columns,
                                grain=grain, address = address)
        self.environment.datasources[datasource.identifier]=datasource
        return datasource

    @v_args(meta=True)
    def comment(self, meta: Meta, args):
        assert len(args) ==1
        return Comment(text=args[0].value)

    @v_args(meta=True)
    def select_transform(self, meta,  args)->ConceptTransform:
        function:Function = args[0]
        output:str = args[1]
        existing = self.environment.concepts.get(output)
        if existing:
            raise ParseError(f'Assignment {output} on line {meta.line} is a duplicate concept declaration' )
        concept = Concept(name = output,
                datatype = function.output_datatype,
                purpose = function.output_purpose,
                          lineage = function)
        self.environment.concepts[output] = concept
        return ConceptTransform(function=function, output=concept)

    @v_args(meta=True)
    def select_item(self, meta: Meta, args)->SelectItem:
        # basic select
        if len(args) != 1:
            raise ParseError(f"Malformed select statement {args}")
        content = args[0]
        if isinstance(args[0], ConceptTransform):
            return SelectItem(content=content)
        return SelectItem(content=self.environment.concepts[content])


    def select_list(self, args):
        return args

    def limit (self, args):
        return Limit(value=args[0].value)

    def ORDERING(self, args):
        return Ordering(args)

    def order_list(self, args):
        return [OrderItem(x, y) for x, y in zip(args[::2], args[1::2])]

    def order_by(self, args):
        return OrderBy(items=args[0])

    def import_statement(self, args):
        alias = args[-1]
        path = args[0].split('.')
        from os.path import join, dirname
        from os import getcwd
        target = join(getcwd(), *path) +'.preql'
        with open(target, 'r', encoding='utf-8') as f:
            text = f.read()
            nparser = ParseToObjects(visit_tokens=True, text=text, environment=Environment({}, {}))
            nparser.transform(
                PARSER.parse(text)
            )

            for key, concept in nparser.environment.concepts.items():
                self.environment.concepts[f'{alias}.{key}'] = concept
            for key, datasource in nparser.environment.datasources.items():
                self.environment.datasources[f'{alias}.{key}'] = datasource
        return None


    @v_args(meta=True)
    def select(self, meta: Meta, args):

        select_items = None
        limit =None
        order_by = None
        where = None
        for arg in args:
            if isinstance(arg, List):
                select_items = arg
            elif isinstance(arg, Limit):
                limit = arg.value
            elif isinstance(arg, OrderBy):
                order_by = arg
            elif isinstance(arg, WhereClause):
                where = arg
        return Select(selection = select_items, where_clause=where, limit=limit, order_by = order_by)

    @v_args(meta=True)
    def address(self, meta:Meta, args):
        return Address(location=args[0])

    def where(self, args):
        return WhereClause(conditional=args[0])

    def literal(self, args):
        return args[0]

    def comparison(self, args):
        return Comparison(args[0], args[2], args[1])

    def conditional(self, args):
        return Conditional(args[0], args[2], args[1])



    #BEGIN FUNCTIONS
    def expr_reference(self, args)->Concept:
        return self.environment.concepts[args[0]]


    def expr(self, args):
        if len(args)>1:
            raise ParseError('Expression should have one child only.')
        return args[0]

    def count(self, args):
        '''    operator: str
    arguments:List[Concept]
    output:Concept'''
        return Function(operator=FunctionType.COUNT, arguments=[self.environment.concepts[i] for i in args], output_datatype=DataType.INTEGER,
                        output_purpose = Purpose.METRIC)

    def sum(self, args):
        arguments = [self.environment.concepts[i] for i in args]
        if not len(arguments) == 1:
            raise ParseError("Too many arguments to sum")
        return Function(operator=FunctionType.SUM, arguments=arguments, output_datatype=arguments[0].datatype,
                        output_purpose = Purpose.METRIC)


    def avg(self, args):
        arguments = [self.environment.concepts[i] for i in args]
        if not len(arguments) == 1:
            raise ParseError("Too many arguments to avg")
        return Function(operator=FunctionType.AVG, arguments=arguments, output_datatype=arguments[0].datatype,
                        output_purpose = Purpose.METRIC)

    def len(self, args):
        arguments = [self.environment.concepts[i] for i in args]
        if not len(arguments) == 1:
            raise ParseError("Too many arguments to len")
        return Function(operator=FunctionType.LENGTH, arguments=arguments, output_datatype=arguments[0].datatype,
                        output_purpose = Purpose.METRIC)

def parse_text(text:str, environment:Optional[Environment]=None, print_flag: bool = False)->Tuple[Environment, List]:
    environment = environment or Environment(concepts = {}, datasources = {})
    parser = ParseToObjects(visit_tokens=True, text=text, environment = environment)
    output = parser.transform(
        PARSER.parse(text)
    )
    return environment, output

