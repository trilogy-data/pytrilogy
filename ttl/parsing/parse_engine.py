from lark import Lark, Transformer, v_args
from lark.tree import Meta
from typing import Dict, Tuple, List, Optional
from ttl.core.models import Comment, Datasource, Concept, ColumnAssignment, Select, Address, Grain, SelectItem, ConceptTransform, Function, OrderItem, Environment, Limit, OrderBy
from ttl.core.enums import Purpose, DataType, Modifier, Ordering
from ttl.parsing.exceptions import ParseError


grammar = r"""
    !start: ((statement ";") | comment )*
    ?statement: concept
    | datasource
    | select

    
    concept : "concept" IDENTIFIER TYPE ":" PURPOSE metadata?
    
    datasource : "datasource" IDENTIFIER  "("  column_assignment_list ")"  grain_clause? "address" address 
    
    grain_clause: "grain" "(" column_list ")"
    
    address: IDENTIFIER
    
    select : "select"i select_list comment* order_by? comment* limit? comment*
    
    comment :   /#.*(\n|$)/ |  /\/\/.*\n/  
    
    concept_declaration: IDENTIFIER | (MODIFIER "[" concept_declaration "]" )
    
    column_assignment : (IDENTIFIER ":" concept_declaration) 
    
    column_assignment_list : (column_assignment "," )* column_assignment ","?
    
    column_list : (IDENTIFIER "," )* IDENTIFIER ","?
    
    select_item : (IDENTIFIER | select_transform | comment )
    
    select_list :  ( select_item "," )* select_item ","?
    
    // 
    select_transform : expr "-" ">" IDENTIFIER metadata?
    
    metadata : "metadata" "(" IDENTIFIER "=" _string_lit ")"
    
    limit: "LIMIT"i /[0-9]+/
    
    order_list : (IDENTIFIER ORDERING "," )* IDENTIFIER ORDERING ","?
    
    ORDERING: ("ASC"i | "DESC"i)
    
    order_by: "ORDER"i "BY"i order_list
    
    expr: count | IDENTIFIER
    
    // functions
    
    count: "count" "(" expr ")"
    
    // base language constructs
    IDENTIFIER : /[a-zA-Z_][a-zA-Z0-9_\\-\\.\-]*/
    
    STRING_CHARS: /(?:(?!\${)([^"\\]|\\.))+/+ // any character except '"" 
    
    _string_lit: "\"" ( STRING_CHARS )* "\""

    MODIFIER: "Optional"i | "Partial"i
    
    TYPE : "string" | "number" | "bool" | "map" | "list" | "any" | "int" 
    
    PURPOSE: "property" | "key" | "metric"

    %import common.WS_INLINE -> _WHITESPACE
    %import common.WS
    %ignore WS
"""


PARSER = Lark(grammar, start="start", propagate_positions=True)


class ParseToObjects(Transformer):
    def __init__(self, visit_tokens, text):
        Transformer.__init__(self, visit_tokens)
        self.text = text
        self.concepts:Dict[str, Concept] = {}
        self.datasources:Dict[str, Datasource] = {}

    def start(self, args):
        return args

    def IDENTIFIER(self, args)->str:
        return args.value

    def STRING_CHARS(self, args)->str:
        return args.value

    def TYPE(self, args)->DataType:
        return DataType(args)

    @v_args(meta=True)
    def column_assignment(self, meta: Meta, args):
        #TODO -> deal with conceptual modifiers
        return ColumnAssignment(alias=args[0],
                                concept =self.concepts[args[1][0]])

    def MODIFIER(self, args)->Modifier:
        return Modifier(args.value)

    def concept_declaration(self, args):
        # TODO implement
        return args


    @v_args(meta=True)
    def concept(self, meta: Meta, args)->Concept:
        if len(args) >3:
            metadata = args[3]
        else:
            metadata = None
        name = args[0]
        existing = self.concepts.get(name)
        if existing:
            raise ParseError(f'Concept {name} on line {meta.line} is a duplicate declaration' )
        concept = Concept(name=name,
                          datatype = args[1],
                          purpose=Purpose(args[2]),
                          metadata=metadata
                          )
        self.concepts[name] = concept
        return concept

    def column_assignment_list(self, args):
        return args

    def column_list(self, args):
        if len(args)>1:
            raise ParseError('Invalid column list, should have one child')
        return args[0]

    def grain_clause(self, args)->Grain:
        return Grain([self.concepts[a] for a in args])

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
        self.datasources[datasource.identifier]=datasource
        return datasource

    @v_args(meta=True)
    def comment(self, meta: Meta, args):
        assert len(args) ==1
        return Comment(text=args[0].value)

    @v_args(meta=True)
    def select_transform(self, meta,  args)->ConceptTransform:
        function:Function = args[0]
        output:str = args[1]
        existing = self.concepts.get(output)
        if existing:
            raise ParseError(f'Assignment {output} on line {meta.line} is a duplicate concept declaration' )
        concept = Concept(name = output,
                datatype = function.output_datatype,
                purpose = function.output_purpose,
                          lineage = function)
        self.concepts[output] = concept
        return ConceptTransform(function=function, output=concept)

    @v_args(meta=True)
    def select_item(self, meta: Meta, args)->SelectItem:
        # basic select
        if len(args) != 1:
            raise ParseError(f"Malformed select statement {args}")
        content = args[0]
        if isinstance(args[0], ConceptTransform):
            return SelectItem(content=content)
        return SelectItem(content=self.concepts[content])


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

    @v_args(meta=True)
    def select(self, meta: Meta, args):

        select_items = None
        limit =None
        order_by = None
        for arg in args:
            if isinstance(arg, List):
                select_items = arg
            elif isinstance(arg, Limit):
                limit = arg.value
            elif isinstance(arg, OrderBy):
                order_by = arg
        print(order_by)
        return Select(selection = select_items, limit=limit, order_by = order_by)

    @v_args(meta=True)
    def address(self, meta:Meta, args):
        return Address(location=args[0])


    #BEGIN FUNCTIONS
    def expr(self, args):
        if len(args)>1:
            raise ParseError('Expression should have one child only.')
        return args[0]

    def count(self, args):
        '''    operator: str
    arguments:List[Concept]
    output:Concept'''
        return Function(operator='count', arguments=[self.concepts[i] for i in args], output_datatype=DataType.INTEGER,
                        output_purpose = Purpose.METRIC)

def parse_text(text:str, print_flag: bool = False)->Tuple[Environment, List]:
    parser = ParseToObjects(visit_tokens=True, text=text)
    output = parser.transform(
        PARSER.parse(text)
    )
    return Environment(concepts=parser.concepts, datasources=parser.datasources), output

