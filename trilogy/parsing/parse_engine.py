from dataclasses import dataclass
from datetime import date, datetime
from os.path import dirname, join
from pathlib import Path
from re import IGNORECASE
from typing import List, Optional, Tuple, Union

from lark import Lark, ParseTree, Transformer, Tree, v_args
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
    DEFAULT_NAMESPACE,
    NULL_VALUE,
    MagicConstants,
)
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    FunctionType,
    IOType,
    Modifier,
    Ordering,
    Purpose,
    ShowCategory,
    WindowOrder,
    WindowType,
)
from trilogy.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from trilogy.core.functions import (
    CurrentDate,
    CurrentDatetime,
    FunctionFactory,
)
from trilogy.core.internal import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    CaseElse,
    CaseWhen,
    Comment,
    Comparison,
    Concept,
    Conditional,
    FilterItem,
    Function,
    Grain,
    HavingClause,
    Metadata,
    OrderBy,
    OrderItem,
    Parenthetical,
    SubselectComparison,
    WhereClause,
    Window,
    WindowItem,
    WindowItemOrder,
    WindowItemOver,
)
from trilogy.core.models.core import (
    DataType,
    ListType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TupleWrapper,
    arg_to_datatype,
    dict_to_map_wrapper,
    list_to_wrapper,
    tuple_to_wrapper,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    Query,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment, EnvironmentConceptDict, Import
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ConceptTransform,
    CopyStatement,
    ImportStatement,
    Limit,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    ShowStatement,
)
from trilogy.parsing.common import (
    agg_wrapper_to_concept,
    align_item_to_concept,
    arbitrary_to_concept,
    constant_to_concept,
    filter_item_to_concept,
    function_to_concept,
    process_function_args,
    rowset_to_concepts,
    window_item_to_concept,
)
from trilogy.parsing.exceptions import ParseError

CONSTANT_TYPES = (int, float, str, bool, list, ListWrapper, MapWrapper)

SELF_LABEL = "root"


@dataclass
class WholeGrainWrapper:
    where: WhereClause


with open(join(dirname(__file__), "trilogy.lark"), "r") as f:
    PARSER = Lark(
        f.read(),
        start="start",
        propagate_positions=True,
        g_regex_flags=IGNORECASE,
        parser="lalr",
        cache=True,
    )


def gen_cache_lookup(path: str, alias: str, parent: str) -> str:
    return path + alias + parent


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
) -> Union[Comparison, SubselectComparison, Conditional]:
    if not isinstance(root, (Comparison, SubselectComparison, Conditional)):
        if arg_to_datatype(root) == DataType.BOOL:
            root = Comparison(left=root, right=True, operator=ComparisonOperator.EQ)
        else:
            root = Comparison(
                left=root,
                right=MagicConstants.NULL,
                operator=ComparisonOperator.IS_NOT,
            )
    return root


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
    ],
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
        environment: Environment,
        parse_address: str | None = None,
        token_address: Path | None = None,
        parsed: dict[str, "ParseToObjects"] | None = None,
        tokens: dict[Path | str, ParseTree] | None = None,
        text_lookup: dict[Path | str, str] | None = None,
    ):
        Transformer.__init__(self, True)
        self.environment: Environment = environment
        self.parse_address: str = parse_address or SELF_LABEL
        self.token_address: Path | str = token_address or SELF_LABEL
        self.parsed: dict[str, ParseToObjects] = parsed if parsed is not None else {}
        self.tokens: dict[Path | str, ParseTree] = tokens if tokens is not None else {}
        self.text_lookup: dict[Path | str, str] = (
            text_lookup if text_lookup is not None else {}
        )
        # we do a second pass to pick up circular dependencies
        # after initial parsing
        self.pass_count = 1
        self.function_factory = FunctionFactory(self.environment)

    def set_text(self, text: str):
        self.text_lookup[self.token_address] = text

    def transform(self, tree: Tree):
        results = super().transform(tree)
        self.tokens[self.token_address] = tree
        return results

    def prepare_parse(self):
        self.pass_count = 1
        self.environment.concepts.fail_on_missing = False
        for _, v in self.parsed.items():
            v.prepare_parse()

    def hydrate_missing(self):
        self.pass_count = 2
        for k, v in self.parsed.items():
            if v.pass_count == 2:
                continue
            v.hydrate_missing()
        reparsed = self.transform(self.tokens[self.token_address])
        self.environment.concepts.undefined = {}
        return reparsed

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
        if isinstance(output, ImportStatement):
            if len(args) > 1 and isinstance(args[1], Comment):
                comment = args[1].text.split("#")[1].strip()
                namespace = output.alias
                for _, v in self.environment.concepts.items():
                    if v.namespace == namespace:
                        if v.metadata.description:
                            v.metadata.description = (
                                f"{comment}: {v.metadata.description}"
                            )
                        else:
                            v.metadata.description = comment

        return args[0]

    def metadata(self, args):
        pairs = {key: val for key, val in zip(args[::2], args[1::2])}
        return Metadata(**pairs)

    def IDENTIFIER(self, args) -> str:
        return args.value

    def WILDCARD_IDENTIFIER(self, args) -> str:
        return args.value

    def QUOTED_IDENTIFIER(self, args) -> str:
        return args.value[1:-1]

    @v_args(meta=True)
    def concept_lit(self, meta: Meta, args) -> Concept:
        address = args[0]
        return self.environment.concepts.__getitem__(address, meta.line)

    def ADDRESS(self, args) -> Address:
        return Address(location=args.value, quoted=False)

    def QUOTED_ADDRESS(self, args) -> Address:
        return Address(location=args.value[1:-1], quoted=True)

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
        final: list[
            DataType | MapType | ListType | NumericType | StructType | Concept
        ] = []
        for arg in args:
            new = self.environment.concepts.__getitem__(  # type: ignore
                key=arg, line_no=meta.line
            )
            final.append(new)

        return StructType(
            fields=final,
            fields_map={x.name: x for x in final if isinstance(x, Concept)},
        )

    def list_type(self, args) -> ListType:
        return ListType(type=args[0])

    def numeric_type(self, args) -> NumericType:
        return NumericType(precision=args[0], scale=args[1])

    def map_type(self, args) -> MapType:
        return MapType(key_type=args[0], value_type=args[1])

    def data_type(
        self, args
    ) -> DataType | ListType | StructType | MapType | NumericType:
        resolved = args[0]
        if isinstance(resolved, StructType):
            return resolved
        elif isinstance(resolved, ListType):
            return resolved
        elif isinstance(resolved, NumericType):
            return resolved
        elif isinstance(resolved, MapType):
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
        alias = args[0]
        concept_list = args[1]
        # recursively collect modifiers
        if len(concept_list) > 1:
            modifiers += concept_list[:-1]
        concept = concept_list[-1]
        resolved = self.environment.concepts.__getitem__(  # type: ignore
            key=concept, line_no=meta.line, file=self.token_address
        )
        return ColumnAssignment(alias=alias, modifiers=modifiers, concept=resolved)

    def _TERMINATOR(self, args):
        return None

    def _static_functions(self, args):
        return args[0]

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
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            grain=Grain(components={x.address for x in parents}),
            namespace=namespace,
            keys=set([x.address for x in parents]),
            modifiers=modifiers,
        )
        self.environment.add_concept(concept, meta)
        return concept

    @v_args(meta=True)
    def concept_declaration(self, meta: Meta, args) -> ConceptDeclarationStatement:
        metadata = Metadata()
        modifiers = []
        for arg in args:
            if isinstance(arg, Metadata):
                metadata = arg
            if isinstance(arg, Modifier):
                modifiers.append(arg)
        name = args[1]
        _, namespace, name, _ = parse_concept_reference(name, self.environment)
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=args[0],
            metadata=metadata,
            namespace=namespace,
            modifiers=modifiers,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line
        self.environment.add_concept(concept, meta=meta, force=True)
        return ConceptDeclarationStatement(concept=concept)

    @v_args(meta=True)
    def concept_derivation(self, meta: Meta, args) -> ConceptDerivationStatement:
        if len(args) > 3:
            metadata = args[3]
        else:
            metadata = None
        purpose = args[0]
        if purpose == Purpose.AUTO:
            purpose = None
        raw_name = args[1]
        # abc.def.property pattern
        if isinstance(raw_name, str):
            lookup, namespace, name, parent_concept = parse_concept_reference(
                raw_name, self.environment, purpose
            )
        # <abc.def,zef.gf>.property pattern
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

        if isinstance(
            source_value, (FilterItem, WindowItem, AggregateWrapper, Function)
        ):
            concept = arbitrary_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                environment=self.environment,
                purpose=purpose,
                metadata=metadata,
            )

            if concept.metadata:
                concept.metadata.line_number = meta.line
            self.environment.add_concept(concept, meta=meta)
            return ConceptDerivationStatement(concept=concept)

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
            return ConceptDerivationStatement(concept=concept)

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
        for new_concept in rowset_to_concepts(output, self.environment):
            if new_concept.metadata:
                new_concept.metadata.line_number = meta.line
            # output.select.local_concepts[new_concept.address] = new_concept
            self.environment.add_concept(new_concept)
        return output

    @v_args(meta=True)
    def constant_derivation(self, meta: Meta, args) -> Concept:
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
            metadata=metadata,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                output_datatype=arg_to_datatype(constant),
                output_purpose=Purpose.CONSTANT,
                arguments=[constant],
            ),
            grain=Grain(components=set()),
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
        return Grain(
            components=set([self.environment.concepts[a].address for a in args[0]])
        )

    def whole_grain_clause(self, args) -> WholeGrainWrapper:
        return WholeGrainWrapper(where=args[0])

    def MULTILINE_STRING(self, args) -> str:
        return args[3:-3]

    def raw_column_assignment(self, args):
        return RawColumnExpr(text=args[1])

    @v_args(meta=True)
    def datasource(self, meta: Meta, args):
        name = args[0]
        columns: List[ColumnAssignment] = args[1]
        grain: Optional[Grain] = None
        address: Optional[Address] = None
        where: Optional[WhereClause] = None
        non_partial_for: Optional[WhereClause] = None
        for val in args[1:]:
            if isinstance(val, Address):
                address = val
            elif isinstance(val, Grain):
                grain = val
            elif isinstance(val, WholeGrainWrapper):
                non_partial_for = val.where
            elif isinstance(val, Query):
                address = Address(location=f"({val.text})", is_query=True)
            elif isinstance(val, WhereClause):
                where = val
        if not address:
            raise ValueError(
                "Malformed datasource, missing address or query declaration"
            )
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
        )
        for column in columns:
            column.concept = column.concept.with_grain(datasource.grain)
        if datasource.where:
            for x in datasource.where.concept_arguments:
                if x.address not in datasource.output_concepts:
                    raise ValueError(
                        f"Datasource {name} where condition depends on concept {x.address} that does not exist on the datasource, line {meta.line}."
                    )
        self.environment.add_datasource(datasource, meta=meta)
        return datasource

    @v_args(meta=True)
    def comment(self, meta: Meta, args):
        assert len(args) == 1
        return Comment(text=args[0].value)

    def PARSE_COMMENT(self, args):
        return Comment(text=args.value)

    @v_args(meta=True)
    def select_transform(self, meta: Meta, args) -> ConceptTransform:
        output: str = args[1]
        transformation = unwrap_transformation(args[0])
        lookup, namespace, output, parent = parse_concept_reference(
            output, self.environment
        )

        metadata = Metadata(line_number=meta.line, concept_source=ConceptSource.SELECT)

        if isinstance(transformation, AggregateWrapper):
            concept = agg_wrapper_to_concept(
                transformation, namespace=namespace, name=output, metadata=metadata
            )
        elif isinstance(transformation, WindowItem):
            concept = window_item_to_concept(
                transformation, namespace=namespace, name=output, metadata=metadata
            )
        elif isinstance(transformation, FilterItem):
            concept = filter_item_to_concept(
                transformation, namespace=namespace, name=output, metadata=metadata
            )
        elif isinstance(transformation, CONSTANT_TYPES):
            concept = constant_to_concept(
                transformation, namespace=namespace, name=output, metadata=metadata
            )
        elif isinstance(transformation, Function):
            concept = function_to_concept(
                transformation,
                namespace=namespace,
                name=output,
                metadata=metadata,
                environment=self.environment,
            )
        else:
            raise SyntaxError("Invalid transformation")

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
        base = args[0].lower()
        if len(args) > 1:
            null_sort = args[-1]
            return Ordering(" ".join([base, "nulls", null_sort.lower()]))
        return Ordering(base)

    def order_list(self, args):
        def handle_order_item(x, namespace: str):
            if not isinstance(x, Concept):
                x = arbitrary_to_concept(
                    x, namespace=namespace, environment=self.environment
                )
            return x

        return [
            OrderItem(
                expr=handle_order_item(
                    x,
                    self.environment.namespace,
                ),
                order=y,
            )
            for x, y in zip(args[::2], args[1::2])
        ]

    def order_by(self, args):
        return OrderBy(items=args[0])

    def over_list(self, args):
        return [x for x in args]

    @v_args(meta=True)
    def merge_statement(self, meta: Meta, args) -> MergeStatementV2:
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
            sources = [
                v
                for k, v in self.environment.concepts.items()
                if v.namespace == source_wildcard
            ]
            targets = {}
            for x in sources:
                target = target_wildcard + "." + x.name
                if target in self.environment.concepts:
                    targets[x.address] = self.environment.concepts[target]
            sources = [x for x in sources if x.address in targets]
        else:
            sources = [self.environment.concepts[source]]
            targets = {sources[0].address: self.environment.concepts[target]}
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

    @v_args(meta=True)
    def rawsql_statement(self, meta: Meta, args) -> RawSQLStatement:
        statement = RawSQLStatement(meta=Metadata(line_number=meta.line), text=args[0])
        return statement

    def COPY_TYPE(self, args) -> IOType:
        return IOType(args.value)

    @v_args(meta=True)
    def copy_statement(self, meta: Meta, args) -> CopyStatement:
        return CopyStatement(
            target=args[1],
            target_type=args[0],
            meta=Metadata(line_number=meta.line),
            select=args[-1],
        )

    def resolve_import_address(self, address) -> str:
        with open(address, "r", encoding="utf-8") as f:
            text = f.read()
        return text

    def import_statement(self, args: list[str]) -> ImportStatement:
        if len(args) == 2:
            alias = args[-1]
        else:
            alias = self.environment.namespace
        path = args[0].split(".")

        target = join(self.environment.working_path, *path) + ".preql"

        # tokens + text are cached by path
        token_lookup = Path(target)

        # cache lookups by the target, the alias, and the file we're importing it from
        cache_lookup = gen_cache_lookup(
            path=target, alias=alias, parent=str(self.token_address)
        )
        if token_lookup in self.tokens:
            raw_tokens = self.tokens[token_lookup]
            text = self.text_lookup[token_lookup]
        else:
            text = self.resolve_import_address(target)
            self.text_lookup[token_lookup] = text

            raw_tokens = PARSER.parse(text)
            self.tokens[token_lookup] = raw_tokens

        if cache_lookup in self.parsed:
            nparser = self.parsed[cache_lookup]
            new_env = nparser.environment
        else:
            try:
                new_env = Environment(
                    working_path=dirname(target),
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
                )
                nparser.transform(raw_tokens)
                self.parsed[cache_lookup] = nparser
            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target}, parsing error: {e}"
                ) from e

        imps = ImportStatement(alias=alias, path=Path(args[0]))
        self.environment.add_import(
            alias, new_env, Import(alias=alias, path=Path(args[0]))
        )
        return imps

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
            name=identifier,
            address=Address(location=address),
            grain=grain,
        )
        return PersistStatement(
            select=select,
            datasource=new_datasource,
            meta=Metadata(line_number=meta.line),
        )

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

        selects: list[SelectStatement] = []
        align: AlignClause | None = None
        limit: int | None = None
        order_by: OrderBy | None = None
        where: WhereClause | None = None
        having: HavingClause | None = None
        for arg in args:
            if isinstance(arg, SelectStatement):
                selects.append(arg)
            elif isinstance(arg, Limit):
                limit = arg.count
            elif isinstance(arg, OrderBy):
                order_by = arg
            elif isinstance(arg, WhereClause):
                where = arg
            elif isinstance(arg, HavingClause):
                having = arg
            elif isinstance(arg, AlignClause):
                align = arg

        assert align
        assert align is not None
        base_local: EnvironmentConceptDict = selects[0].local_concepts
        for select in selects[1:]:
            for k, v in select.local_concepts.items():
                base_local[k] = v
        derived_concepts = []
        for x in align.items:
            concept = align_item_to_concept(
                x,
                align,
                selects,
                local_concepts=base_local,
                where=where,
                having=having,
                limit=limit,
                environment=self.environment,
            )
            derived_concepts.append(concept)
            self.environment.add_concept(concept, meta=meta)
            base_local[concept.address] = concept
        multi = MultiSelectStatement(
            selects=selects,
            align=align,
            namespace=self.environment.namespace,
            where_clause=where,
            order_by=order_by,
            limit=limit,
            meta=Metadata(line_number=meta.line),
            local_concepts=base_local,
            derived_concepts=derived_concepts,
        )
        return multi

    @v_args(meta=True)
    def select_statement(self, meta: Meta, args) -> SelectStatement:
        select_items: List[SelectItem] | None = None
        limit = None
        order_by = None
        where = None
        having = None
        for arg in args:
            if isinstance(arg, List):
                select_items = arg
            elif isinstance(arg, Limit):
                limit = arg.count
            elif isinstance(arg, OrderBy):
                order_by = arg
            elif isinstance(arg, WhereClause) and not isinstance(arg, HavingClause):
                where = arg
            elif isinstance(arg, HavingClause):
                having = arg
        if not select_items:
            raise ValueError("Malformed select, missing select items")

        return SelectStatement.from_inputs(
            environment=self.environment,
            selection=select_items,
            order_by=order_by,
            where_clause=where,
            having_clause=having,
            limit=limit,
            meta=Metadata(line_number=meta.line),
        )

    @v_args(meta=True)
    def address(self, meta: Meta, args):
        return args[0]

    @v_args(meta=True)
    def query(self, meta: Meta, args):
        return Query(text=args[0])

    def where(self, args):
        root = args[0]
        root = expr_to_boolean(root)
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
    def function_binding_list(self, meta: Meta, args) -> Concept:
        return args

    @v_args(meta=True)
    def function_binding_item(self, meta: Meta, args) -> Concept:
        return args

    @v_args(meta=True)
    def raw_function(self, meta: Meta, args) -> Function:
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

    def comparison(self, args) -> Comparison:
        if args[1] == ComparisonOperator.IN:
            raise SyntaxError
        if isinstance(args[0], AggregateWrapper):
            left = arbitrary_to_concept(
                args[0],
                environment=self.environment,
            )
            self.environment.add_concept(left)
        else:
            left = args[0]
        if isinstance(args[2], AggregateWrapper):
            right = arbitrary_to_concept(
                args[2],
                environment=self.environment,
            )
            self.environment.add_concept(right)
        else:
            right = args[2]
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
            right = arbitrary_to_concept(right, environment=self.environment)
            self.environment.add_concept(right, meta=meta)
        return SubselectComparison(
            left=args[0],
            right=right,
            operator=args[1],
        )

    def expr_tuple(self, args):
        return TupleWrapper(content=tuple(args))

    def parenthetical(self, args):
        return Parenthetical(content=args[0])

    def condition_parenthetical(self, args):
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

    def WINDOW_TYPE(self, args):
        return WindowType(args.strip())

    def window_item_over(self, args):
        return WindowItemOver(contents=args[0])

    def window_item_order(self, args):
        return WindowItemOrder(contents=args[0])

    def logical_operator(self, args):
        return BooleanOperator(args[0].value.lower())

    def DATE_PART(self, args):
        return DatePart(args.value)

    @v_args(meta=True)
    def window_item(self, meta, args) -> WindowItem:
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
            elif isinstance(item, Concept):
                concept = item
            elif isinstance(item, WindowType):
                type = item
            else:
                concept = arbitrary_to_concept(item, environment=self.environment)
                self.environment.add_concept(concept, meta=meta)
        assert concept
        return WindowItem(
            type=type, content=concept, over=over, order_by=order_by, index=index
        )

    def filter_item(self, args) -> FilterItem:
        where: WhereClause
        string_concept, raw = args
        if isinstance(raw, WhereClause):
            where = raw
        else:
            where = WhereClause(conditional=raw)
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
    def fcoalesce(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.COALESCE, meta)

    @v_args(meta=True)
    def unnest(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.UNNEST, meta)

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
        return self.function_factory.create_function(args, FunctionType.LIKE, meta)

    @v_args(meta=True)
    def upper(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.UPPER, meta)

    @v_args(meta=True)
    def fstrpos(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.STRPOS, meta)

    @v_args(meta=True)
    def fsubstring(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.SUBSTRING, meta)

    @v_args(meta=True)
    def lower(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.LOWER, meta)

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
    def fquarter(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.QUARTER, meta)

    @v_args(meta=True)
    def fyear(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.YEAR, meta)

    # utility functions
    @v_args(meta=True)
    def fcast(self, meta, args) -> Function:
        # if it's casting a constant, we'll process that directly
        args = process_function_args(args, meta=meta, environment=self.environment)
        if isinstance(args[0], str):
            processed: date | datetime | int | float | bool | str
            if args[1] == DataType.DATE:
                processed = date.fromisoformat(args[0])
            elif args[1] == DataType.DATETIME:
                processed = datetime.fromisoformat(args[0])
            elif args[1] == DataType.TIMESTAMP:
                processed = datetime.fromisoformat(args[0])
            elif args[1] == DataType.INTEGER:
                processed = int(args[0])
            elif args[1] == DataType.FLOAT:
                processed = float(args[0])
            elif args[1] == DataType.BOOL:
                processed = args[0].capitalize() == "True"
            elif args[1] == DataType.STRING:
                processed = args[0]
            else:
                raise SyntaxError(f"Invalid cast type {args[1]}")
            return self.function_factory.create_function(
                [processed], FunctionType.CONSTANT, meta
            )
        return self.function_factory.create_function(args, FunctionType.CAST, meta)

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
    def fround(self, meta, args) -> Function:
        return self.function_factory.create_function(args, FunctionType.ROUND, meta)

    @v_args(meta=True)
    def fcase(self, meta, args: List[Union[CaseWhen, CaseElse]]) -> Function:
        return self.function_factory.create_function(args, FunctionType.CASE, meta)

    @v_args(meta=True)
    def fcase_when(self, meta, args) -> CaseWhen:
        args = process_function_args(args, meta=meta, environment=self.environment)
        root = expr_to_boolean(args[0])
        return CaseWhen(comparison=root, expr=args[1])

    @v_args(meta=True)
    def fcase_else(self, meta, args) -> CaseElse:
        args = process_function_args(args, meta=meta, environment=self.environment)
        return CaseElse(expr=args[0])

    @v_args(meta=True)
    def fcurrent_date(self, meta, args):
        return CurrentDate([])

    @v_args(meta=True)
    def fcurrent_datetime(self, meta, args):
        return CurrentDatetime([])

    @v_args(meta=True)
    def fnot(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.IS_NULL, meta)

    @v_args(meta=True)
    def fbool(self, meta, args):
        return self.function_factory.create_function(args, FunctionType.BOOL, meta)


def unpack_visit_error(e: VisitError):
    """This is required to get exceptions from imports, which would
    raise nested VisitErrors"""
    if isinstance(e.orig_exc, VisitError):
        unpack_visit_error(e.orig_exc)
    elif isinstance(e.orig_exc, (UndefinedConceptException, ImportError)):
        raise e.orig_exc
    elif isinstance(e.orig_exc, (SyntaxError, TypeError)):
        if isinstance(e.obj, Tree):
            raise InvalidSyntaxException(
                str(e.orig_exc) + " in " + str(e.rule) + f" Line: {e.obj.meta.line}"
            )
        raise InvalidSyntaxException(str(e.orig_exc)).with_traceback(
            e.orig_exc.__traceback__
        )
    raise e.orig_exc


def parse_text_raw(text: str, environment: Optional[Environment] = None):
    PARSER.parse(text)


def parse_text(text: str, environment: Optional[Environment] = None) -> Tuple[
    Environment,
    List[
        Datasource
        | ImportStatement
        | SelectStatement
        | PersistStatement
        | ShowStatement
        | RawSQLStatement
        | None
    ],
]:
    environment = environment or Environment()
    parser = ParseToObjects(environment=environment)

    try:
        parser.set_text(text)
        # disable fail on missing to allow for circular dependencies
        parser.prepare_parse()
        parser.transform(PARSER.parse(text))
        # this will reset fail on missing
        pass_two = parser.hydrate_missing()
        output = [v for v in pass_two if v]
        environment.concepts.fail_on_missing = True
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
