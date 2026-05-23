from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from functools import singledispatchmethod
from typing import Any

from jinja2 import Template

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX, MagicConstants
from trilogy.core.enums import (
    ConceptSource,
    DatasourceState,
    DatePart,
    FunctionType,
    Modifier,
    PersistMode,
    Purpose,
    ShowCategory,
    ValidationScope,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    Between,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comment,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    DeriveClause,
    DeriveItem,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    NavigationWindowItem,
    NumberingWindowItem,
    OrderBy,
    Ordering,
    OrderItem,
    Parenthetical,
    SubselectComparison,
    WhereClause,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    ListWrapper,
    MapWrapper,
    NumericType,
    TraitDataType,
    TupleWrapper,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    Query,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment, Import
from trilogy.core.statements.author import (
    ArgBinding,
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ConceptTransform,
    CopyStatement,
    FunctionDeclaration,
    ImportStatement,
    KeyMergeStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    PropertiesDeclarationStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    ShowStatement,
    TypeDeclaration,
    ValidateStatement,
)
from trilogy.parsing.pretty import Break, DocPart
from trilogy.parsing.pretty import render as pretty_render

QUERY_TEMPLATE = Template("""{% if where %}where
{{ where }}
{% endif %}select{%- for select in select_columns %}
{{ select }},{% endfor %}{% if having %}
having
{{ having }}
{% endif %}{%- if order_by %}
order by{% for order in order_by %}
{{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{%- if limit is not none %}
limit {{ limit }}{% endif %}
;""")


@dataclass
class IndentationContext:
    """Tracks indentation state during rendering"""

    depth: int = 0
    indent_string: str = "    "  # 4 spaces by default

    @property
    def current_indent(self) -> str:
        return self.indent_string * self.depth

    def increase_depth(self, extra_levels: int = 1) -> "IndentationContext":
        return IndentationContext(
            depth=self.depth + extra_levels, indent_string=self.indent_string
        )


def safe_address(address: str) -> str:
    if "." not in address:
        address = f"{DEFAULT_NAMESPACE}.{address}"
    return address


DEFAULT_MAX_LINE_LENGTH = 100


def _purpose_keyword(purpose: Purpose) -> str:
    """Grammar keyword for a Purpose — ``unique property`` not ``unique_property``."""
    if purpose == Purpose.UNIQUE_PROPERTY:
        return "unique property"
    return purpose.value


class Renderer:

    def __init__(
        self,
        environment: Environment | None = None,
        indent_string: str = "    ",
        max_line_length: int = DEFAULT_MAX_LINE_LENGTH,
    ):
        self.environment = environment
        self.indent_context = IndentationContext(indent_string=indent_string)
        self.max_line_length = max_line_length
        # Stack of rowset-name unmangling prefixes; concept names within a
        # rowset SELECT carry an ``_{rowset}_`` prefix that must be stripped
        # so reparsing doesn't double-prefix.
        self._rowset_prefix_stack: list[str] = []

    @contextmanager
    def _rowset_scope(self, rowset_name: str):
        prefix = f"_{rowset_name}_"
        self._rowset_prefix_stack.append(prefix)
        try:
            yield
        finally:
            self._rowset_prefix_stack.pop()

    def _unmangle_rowset_name(self, name: str) -> str:
        for prefix in reversed(self._rowset_prefix_stack):
            if name.startswith(prefix):
                return name[len(prefix) :]
        return name

    @contextmanager
    def indented(self, levels: int = 1):
        """Context manager for temporarily increasing indentation"""
        old_context = self.indent_context
        self.indent_context = self.indent_context.increase_depth(levels)
        try:
            yield
        finally:
            self.indent_context = old_context

    def _available_width(self, extra_prefix: str = "") -> int:
        return (
            self.max_line_length
            - len(self.indent_context.current_indent)
            - len(extra_prefix)
        )

    def _pretty(self, parts: list[DocPart], *, extra_prefix: str = "") -> str:
        """Render a Doc with the current indent column as the starting col."""
        col = len(self.indent_context.current_indent) + len(extra_prefix)
        return pretty_render(
            parts,
            width=self.max_line_length,
            col=col,
            indent_unit=self.indent_context.indent_string,
        )

    def _render_call(
        self,
        name: str,
        args: list[str],
        *,
        priority: int = 5,
        extra_prefix: str = "",
    ) -> str:
        """Render ``name(arg1, arg2, ...)`` with one-per-line wrap as a fallback.

        All inter-arg breaks (and the prefix/suffix breaks) share ``priority``
        so they activate together when the flat form doesn't fit.
        """
        if not args:
            return f"{name}()"
        parts: list[DocPart] = [f"{name}(", Break(priority, indent=1, flat="")]
        for i, a in enumerate(args):
            parts.append(a)
            if i < len(args) - 1:
                parts.append(",")
                parts.append(Break(priority, indent=1, flat=" "))
        parts.append(Break(priority, indent=0, flat=""))
        parts.append(")")
        return self._pretty(parts, extra_prefix=extra_prefix)

    def _render_window_tail(self, head: str, over, order_by) -> str:
        """Append ``over (partition by ... order by ...)`` to a window call.

        Breaks (in priority order): the ``over`` boundary (10), the
        partition/order boundary (9), inter-column commas (7).
        """
        if not over and not order_by:
            return head
        over_strs = [self.to_string(c) for c in over]
        order_strs = [self.to_string(c) for c in order_by]
        clause_parts: list[DocPart] = ["("]
        if over_strs:
            clause_parts.append("partition by ")
            for i, s in enumerate(over_strs):
                clause_parts.append(s)
                if i < len(over_strs) - 1:
                    clause_parts.append(",")
                    clause_parts.append(Break(priority=7, indent=2, flat=" "))
        if order_strs:
            if over_strs:
                clause_parts.append(Break(priority=9, indent=1, flat=" "))
            clause_parts.append("order by ")
            for i, s in enumerate(order_strs):
                clause_parts.append(s)
                if i < len(order_strs) - 1:
                    clause_parts.append(",")
                    clause_parts.append(Break(priority=7, indent=2, flat=" "))
        clause_parts.append(")")
        parts: list[DocPart] = [head, Break(priority=10, indent=1, flat=" "), "over "]
        parts.extend(clause_parts)
        return self._pretty(parts)

    def _flatten_boolean(self, cond: "Conditional") -> tuple[list[Any], str]:
        op = cond.operator.value
        parts: list[Any] = []

        def walk(c):
            if isinstance(c, Conditional) and c.operator.value == op:
                walk(c.left)
                walk(c.right)
            else:
                parts.append(c)

        walk(cond)
        return parts, op

    def _render_boolean(self, cond, extra_prefix: str = "") -> str:
        """Render a boolean tree with optional line breaking.

        Lines in the returned string carry NO base indent — the caller is
        expected to prefix it via ``indent_lines`` (or place it at column 0).
        Continuation lines and paren-interior lines carry ``extra_prefix``
        and further inner indent so they align under the same base column.
        """
        # Strip outer parens; the WhereClause path strips already, but nested
        # callers may pass a Parenthetical here.
        while isinstance(cond, Parenthetical):
            cond = cond.content

        if not isinstance(cond, Conditional):
            return self._render_expression(cond, extra_prefix)

        parts, op = self._flatten_boolean(cond)
        # Try single-line first
        rendered_singles = [self.to_string(p) for p in parts]
        single = f" {op} ".join(rendered_singles)
        if "\n" not in single and len(single) <= self._available_width(extra_prefix):
            return single

        rendered: list[str] = []
        for p in parts:
            rendered.append(self._render_expression(p, extra_prefix))

        lines = [rendered[0]]
        for r in rendered[1:]:
            lines.append(f"{op} {r}")
        return ("\n" + extra_prefix).join(lines)

    def _render_expression(self, expr, extra_prefix: str = "") -> str:
        """Render an expression, breaking inside parens if needed."""
        if isinstance(expr, Parenthetical):
            inner_prefix = extra_prefix + self.indent_context.indent_string
            content = expr.content
            if isinstance(content, (Conditional, Parenthetical)):
                inner = self._render_boolean(content, inner_prefix)
            else:
                inner = self.to_string(content)
            single = f"({inner})"
            if "\n" not in inner and len(single) <= self._available_width(extra_prefix):
                return single
            return f"(\n{inner_prefix}{inner}\n{extra_prefix})"
        return self.to_string(expr)

    def indent_lines(self, text: str, extra_levels: int = 0) -> str:
        """Apply current indentation to all lines in text"""
        if not text:
            return text

        indent = self.indent_context.indent_string * (
            self.indent_context.depth + extra_levels
        )
        lines = text.split("\n")
        indented_lines = []

        for line in lines:
            if line.strip():  # Only indent non-empty lines
                indented_lines.append(indent + line)
            else:
                indented_lines.append(line)  # Keep empty lines as-is

        return "\n".join(indented_lines)

    # Statement types that pack tightly when adjacent to the same type.
    # Anything else (or a type switch) gets a blank line between statements.
    _TIGHT_TYPES: tuple = (
        ImportStatement,
        ConceptDeclarationStatement,
        ConceptDerivationStatement,
        PropertiesDeclarationStatement,
        TypeDeclaration,
        MergeStatementV2,
        KeyMergeStatement,
        FunctionDeclaration,
    )

    def _statement_separator(self, prev, curr) -> str:
        if prev is None:
            return ""
        # A comment binds tightly to the statement that follows it.
        if isinstance(prev, Comment):
            return "\n"
        # A comment introducing a new section gets a blank line before it.
        if isinstance(curr, Comment):
            return "\n\n"
        # Same tight type: pack with a single newline.
        if (
            isinstance(prev, self._TIGHT_TYPES)
            and isinstance(curr, self._TIGHT_TYPES)
            and type(prev) is type(curr)
        ):
            return "\n"
        return "\n\n"

    def render_statement_string(self, list_of_statements: list[Any]) -> str:
        out: list[str] = []
        prev = None
        for stmt in list_of_statements:
            sep = self._statement_separator(prev, stmt)
            if sep:
                out.append(sep)
            out.append(self.to_string(stmt))
            prev = stmt
        return "".join(out)

    @singledispatchmethod
    def to_string(self, arg):
        raise NotImplementedError("Cannot render type {}".format(type(arg)))

    @to_string.register
    def _(self, arg: Environment):
        constants: list[Concept] = []
        keys: list[Concept] = []
        properties = defaultdict(list)
        metrics = []
        # first, keys
        for concept in arg.concepts.values():
            if "__preql_internal" in concept.address:
                continue

            # don't render anything that came from an import
            if concept.namespace in arg.imports:
                continue
            if (
                concept.metadata
                and concept.metadata.concept_source == ConceptSource.AUTO_DERIVED
            ):
                continue
            elif not concept.lineage and concept.purpose == Purpose.CONSTANT:
                constants.append(concept)
            elif not concept.lineage and concept.purpose == Purpose.KEY:
                keys.append(concept)

            elif not concept.lineage and concept.purpose == Purpose.PROPERTY:
                if concept.keys:
                    # avoid duplicate declarations
                    # but we need better composite key support
                    for key in sorted(list(concept.keys))[:1]:
                        properties[key].append(concept)
                else:
                    keys.append(concept)
            else:
                metrics.append(concept)

        rendered_concepts = [
            self.to_string(ConceptDeclarationStatement(concept=c)) for c in constants
        ]
        for key_concept in keys:
            rendered_concepts.append(
                self.to_string(ConceptDeclarationStatement(concept=key_concept))
            )
            key_props = properties.get(key_concept.address, [])
            grouped: dict[frozenset, list[Concept]] = defaultdict(list)
            for prop in key_props:
                grouped[frozenset(prop.keys or [])].append(prop)
            for group_props in grouped.values():
                if len(group_props) > 1:
                    rendered_concepts.append(
                        self.to_string(
                            PropertiesDeclarationStatement(concepts=group_props)
                        )
                    )
                else:
                    rendered_concepts.append(
                        self.to_string(
                            ConceptDeclarationStatement(concept=group_props[0])
                        )
                    )
        rendered_concepts += [
            self.to_string(ConceptDeclarationStatement(concept=c)) for c in metrics
        ]

        rendered_datasources = [
            # extra padding between datasources
            # todo: make this more generic
            self.to_string(datasource) + "\n"
            for datasource in arg.datasources.values()
            if datasource.namespace == DEFAULT_NAMESPACE
        ]
        rendered_imports = []
        for _, imports in arg.imports.items():
            for import_statement in imports:
                rendered_imports.append(self.to_string(import_statement))
        components = []
        if rendered_imports:
            components.append(rendered_imports)
        if rendered_concepts:
            components.append(rendered_concepts)
        if rendered_datasources:
            components.append(rendered_datasources)

        final = "\n\n".join("\n".join(x) for x in components)
        return final

    @to_string.register
    def _(self, arg: TypeDeclaration):
        return f"type {arg.type.name} {self.to_string(arg.type.type)};"

    @to_string.register
    def _(self, arg: ArgBinding):
        if arg.default:
            return f"{arg.name}={self.to_string(arg.default)}"
        return f"{arg.name}"

    @to_string.register
    def _(self, arg: FunctionDeclaration):
        args = ", ".join([self.to_string(x) for x in arg.args])
        output = f"def {arg.name}({args}) -> {self.to_string(arg.expr)};"
        if arg.meta and arg.meta.description:
            lines = "\n#".join(arg.meta.description.split("\n"))
            output += f" #{lines}"
        return output

    @to_string.register
    def _(self, arg: Datasource):
        # When the datasource itself is `partial`, the parser stamps PARTIAL on
        # every column. Only render `~` on columns that were partial at the
        # source level — the rest inherit it implicitly from the keyword.
        explicit_partial = (
            arg.column_level_partial_addresses if arg.is_partial else None
        )
        with self.indented():
            assignments = ",\n".join(
                self.indent_lines(self._render_column_assignment(c, explicit_partial))
                for c in arg.columns
            )
            # Trailing comma matches Trilogy convention and lets diffs stay
            # one-line-per-binding when columns are added or removed.
            if assignments:
                assignments += ","

        if arg.non_partial_for:
            non_partial = f"\ncomplete where {self.to_string(arg.non_partial_for)}"
        else:
            non_partial = ""
        modifiers = []
        if arg.is_root:
            modifiers.append("root")
        if arg.is_partial:
            modifiers.append("partial")
        entry = " ".join(modifiers + ["datasource"])
        base = f"""{entry} {arg.name} (
{assignments}
)
{self.to_string(arg.grain) if arg.grain.components else ''}{non_partial}
{self.to_string(arg.address)}"""
        if arg.where:
            where_text = self.to_string(arg.where)
            if "\n" in where_text:
                first, rest = where_text.split("\n", 1)
                rest = "\n".join(
                    f"    {line}" if line else line for line in rest.split("\n")
                )
                where_text = f"{first}\n{rest}"
            base += f"\nwhere {where_text}"

        if arg.incremental_by:
            base += f"\nincremental by {','.join(self.to_string(x) for x in arg.incremental_by)}"

        if arg.partition_by:
            base += f"\npartition by {','.join(self.to_string(x) for x in arg.partition_by)}"
        if arg.freshness_by:
            base += f"\nfreshness by {','.join(self.to_string(x) for x in arg.freshness_by)}"
        if arg.freshness_probe:
            base += f"\nfreshness by `{arg.freshness_probe}`"
        if arg.refresh_script:
            base += f"\nrefresh `{arg.refresh_script}`"
        # UNPOPULATED is auto-derived by the parser from file non-existence,
        # not a token in the grammar — only emit states the parser accepts.
        if arg.status == DatasourceState.UNPUBLISHED:
            base += f"\nstate {arg.status.value.lower()}"

        base += ";"
        return base

    @to_string.register
    def _(self, arg: "Grain"):
        final = []
        for comp in arg.components:
            if comp.startswith(DEFAULT_NAMESPACE):
                final.append(comp.split(".", 1)[1])
            else:
                final.append(comp)
        final = sorted(final)
        components = ", ".join(final)
        return f"grain ({components})"

    @to_string.register
    def _(self, arg: "Query"):
        return f"""query {arg.text}"""

    @to_string.register
    def _(self, arg: RowsetDerivationStatement):
        with self._rowset_scope(arg.name):
            select_str = self.to_string(arg.select)
        return f"""rowset {arg.name} <- {select_str}"""

    @to_string.register
    def _(self, arg: "CaseWhen"):
        return (
            f"""when {self.to_string(arg.comparison)} then {self.to_string(arg.expr)}"""
        )

    @to_string.register
    def _(self, arg: "CaseSimpleWhen"):
        return f""" when {self.to_string(arg.value_expr)} then {self.to_string(arg.expr)}"""

    @to_string.register
    def _(self, arg: "CaseElse"):
        return f"""else {self.to_string(arg.expr)}"""

    @to_string.register
    def _(self, arg: "FunctionCallWrapper"):
        args = [self.to_string(c) for c in arg.args]
        arg_string = ", ".join(args)
        return f"""@{arg.name}({arg_string})"""

    @to_string.register
    def _(self, arg: "Parenthetical"):
        return f"""({self.to_string(arg.content)})"""

    @to_string.register
    def _(self, arg: DataType):
        return arg.value

    @to_string.register
    def _(self, arg: "NumericType"):
        return f"""numeric({arg.precision},{arg.scale})"""

    @to_string.register
    def _(self, arg: EnumType):
        members = ", ".join(self.to_string(v) for v in arg.values)
        return f"enum<{self.to_string(arg.type)}>[{members}]"

    @to_string.register
    def _(self, arg: TraitDataType):
        traits = "::".join([x for x in arg.traits])
        return f"{self.to_string(arg.data_type)}::{traits}"

    @to_string.register
    def _(self, arg: ListWrapper):
        return "[" + ", ".join([self.to_string(x) for x in arg]) + "]"

    @to_string.register
    def _(self, arg: TupleWrapper):
        return "(" + ", ".join([self.to_string(x) for x in arg]) + ")"

    @to_string.register
    def _(self, arg: MapWrapper):
        def process_key_value(key, value):
            return f"{self.to_string(key)}: {self.to_string(value)}"

        return (
            "{"
            + ", ".join([process_key_value(key, value) for key, value in arg.items()])
            + "}"
        )

    @to_string.register
    def _(self, arg: DatePart):
        return arg.value

    @to_string.register
    def _(self, arg: "Address"):
        if arg.is_query:
            if arg.location.startswith("("):
                return f"query '''{arg.location[1:-1]}'''"
            return f"query '''{arg.location}'''"
        elif arg.is_file:
            if arg.additional_locations:
                paths = ", ".join(
                    f"`{p}`" for p in (arg.location, *arg.additional_locations)
                )
                return f"file [{paths}]"
            if arg.write_location:
                return f"file `{arg.location}`:`{arg.write_location}`"
            return f"file `{arg.location}`"
        return f"address {arg.location}"

    @to_string.register
    def _(self, arg: "RawSQLStatement"):
        return f"raw_sql('''{arg.text}''');"

    @to_string.register
    def _(self, arg: "MagicConstants"):
        if arg == MagicConstants.NULL:
            return "null"
        return arg.value

    def _render_column_assignment(
        self,
        arg: "ColumnAssignment",
        explicit_partial: set[str] | None = None,
    ) -> str:
        """Render a ColumnAssignment, optionally filtering implicit PARTIAL.

        When called from a Datasource render, ``explicit_partial`` carries
        the set of column concept addresses that were ``~``-marked at parse
        time; columns that got PARTIAL stamped on by a ``partial datasource``
        keyword fall outside that set and should not re-emit ``~``.
        """
        mods: list[Modifier] = list(arg.modifiers)
        if explicit_partial is not None and Modifier.PARTIAL in mods:
            if arg.concept.address not in explicit_partial:
                mods = [m for m in mods if m != Modifier.PARTIAL]
        if mods:
            modifiers = "".join(self.to_string(m) for m in sorted(mods))
        else:
            modifiers = ""

        concept_str = self.to_string(arg.concept)
        if isinstance(arg.alias, str):
            alias_str = arg.alias
        else:
            alias_str = self.to_string(arg.alias)

        if alias_str == concept_str and not modifiers:
            return alias_str
        return f"{alias_str}: {modifiers}{concept_str}"

    @to_string.register
    def _(self, arg: "ColumnAssignment"):
        return self._render_column_assignment(arg)

    @to_string.register
    def _(self, arg: "RawColumnExpr"):
        return f"raw('''{arg.text}''')"

    @to_string.register
    def _(self, arg: "ConceptDeclarationStatement"):
        concept = arg.concept
        if concept.metadata and concept.metadata.description:
            base_description = concept.metadata.description
        else:
            base_description = None
        if concept.namespace and concept.namespace != DEFAULT_NAMESPACE:
            namespace = f"{concept.namespace}."
        else:
            namespace = ""
        # Grammar accepts ``unique property``, not ``unique_property``.
        purpose_kw = _purpose_keyword(concept.purpose)
        is_propertyish = concept.purpose in (Purpose.PROPERTY, Purpose.UNIQUE_PROPERTY)
        # Build the ``<a, b>.`` or ``a.`` key prefix shared by typed and
        # derived property declarations. We only emit the prefix when the
        # implied namespace on reparse matches the concept's own namespace
        # — single-key form takes its namespace from the key's parent, and
        # multi-key form takes it from the environment default.
        key_prefix = ""
        env_default_ns = (
            self.environment.namespace
            if self.environment and self.environment.namespace
            else DEFAULT_NAMESPACE
        )
        concept_ns = concept.namespace or DEFAULT_NAMESPACE
        if is_propertyish and concept.keys:
            key_namespaces: set[str] = set()
            for addr in concept.keys:
                safe = safe_address(addr)
                key_ns = safe.rsplit(".", 1)[0]
                key_namespaces.add(key_ns)
            sorted_keys = sorted(
                self.to_string(ConceptRef(address=safe_address(x)))
                for x in concept.keys
            )
            if len(concept.keys) == 1:
                # Single-key form: parser sets namespace = key's namespace.
                if next(iter(key_namespaces)) == concept_ns:
                    key_prefix = f"{sorted_keys[0]}."
            else:
                # Multi-key form: parser sets namespace = shared key ns when
                # all keys share one, otherwise the env default.
                if len(key_namespaces) == 1:
                    inferred_ns = next(iter(key_namespaces))
                else:
                    inferred_ns = env_default_ns
                if inferred_ns == concept_ns:
                    key_prefix = f"<{', '.join(sorted_keys)}>."
        # For derived concepts whose namespace is an import alias rather
        # than a real concept (e.g. ``import unified_sales as sales``),
        # emitting ``sales.X`` would make the parser treat ``sales`` as a
        # parent concept. Drop the prefix and let re-inference place the
        # concept back in the right namespace via its lineage.
        ns_for_emit = namespace
        if (
            ns_for_emit
            and concept.lineage
            and not key_prefix
            and self.environment
            and concept_ns in self.environment.imports
        ):
            ns_for_emit = ""
        if not concept.lineage:
            if key_prefix:
                output = f"{purpose_kw} {key_prefix}{concept.name} {self.to_string(concept.datatype)};"
            else:
                output = f"{purpose_kw} {namespace}{concept.name} {self.to_string(concept.datatype)};"
        else:
            # `auto` lets the parser re-infer purpose and keys from the
            # lineage — far more readable than an explicit multi-key
            # `<k1, k2, ...>.name` prefix. Single-key `key.name` form
            # stays since it's already concise.
            use_auto = key_prefix.startswith("<")
            if use_auto:
                output = f"auto {ns_for_emit}{concept.name} <- {self.to_string(concept.lineage)};"
            elif key_prefix:
                output = f"{purpose_kw} {key_prefix}{concept.name} <- {self.to_string(concept.lineage)};"
            else:
                output = f"{purpose_kw} {ns_for_emit}{concept.name} <- {self.to_string(concept.lineage)};"
        if base_description:
            lines = "\n#".join(base_description.split("\n"))
            output += f" #{lines}"
        return output

    @to_string.register
    def _(self, arg: PropertiesDeclarationStatement):
        concepts = arg.concepts
        keys_set = concepts[0].keys or set()
        if len(keys_set) == 1:
            key_str = self.to_string(
                ConceptRef(address=safe_address(list(keys_set)[0]))
            )
        else:
            # Parser takes the property block's namespace from the first
            # listed key's namespace. Put a key matching the concepts'
            # actual namespace first so reparse lands in the same place;
            # break ties alphabetically for stable diffs.
            target_ns = concepts[0].namespace
            keys = list(keys_set)
            keys.sort(
                key=lambda x: (
                    safe_address(x).rsplit(".", 1)[0] != target_ns,
                    safe_address(x),
                )
            )
            key_str = (
                "<"
                + ", ".join(
                    self.to_string(ConceptRef(address=safe_address(x))) for x in keys
                )
                + ">"
            )
        namespace = concepts[0].namespace
        namespace_prefix = (
            f"{namespace}." if namespace and namespace != DEFAULT_NAMESPACE else ""
        )
        with self.indented():
            prop_lines = []
            for c in concepts:
                nullable = "?" if Modifier.NULLABLE in c.modifiers else ""
                line = (
                    f"{namespace_prefix}{c.name} "
                    f"{self.to_string(c.datatype)}{nullable},"
                )
                # Descriptions ride as a trailing comment — the grammar captures
                # PARSE_COMMENT inside inline_property_list, so this round-trips.
                if c.metadata and c.metadata.description:
                    desc = " ".join(c.metadata.description.splitlines())
                    line += f" #{desc}"
                prop_lines.append(self.indent_lines(line))
        return "properties {} (\n{}\n);".format(key_str, "\n".join(prop_lines))

    @to_string.register
    def _(self, arg: ArrayType):
        return f"list<{self.to_string(arg.value_data_type)}>"

    @to_string.register
    def _(self, arg: DataType):
        return arg.value

    @to_string.register
    def _(self, arg: date):
        return f"'{arg.isoformat()}'::date"

    @to_string.register
    def _(self, arg: datetime):
        return f"'{arg.isoformat()}'::datetime"

    @to_string.register
    def _(self, arg: ConceptDerivationStatement):
        # this is identical rendering;
        return self.to_string(ConceptDeclarationStatement(concept=arg.concept))

    @to_string.register
    def _(self, arg: PersistStatement):
        if arg.persist_mode == PersistMode.APPEND:
            keyword = "append"
        else:
            keyword = "overwrite"
        if arg.partition_by:
            partition_by = (
                f"by {', '.join(self.to_string(x) for x in arg.partition_by)}"
            )
            return f"{keyword} {arg.identifier} into {arg.address.location} {partition_by} from {self.to_string(arg.select)}"
        return f"{keyword} {arg.identifier} into {arg.address.location} from {self.to_string(arg.select)}"

    @to_string.register
    def _(self, arg: SelectItem):
        prefixes = []
        if Modifier.HIDDEN in arg.modifiers:
            prefixes.append("--")
        if Modifier.PARTIAL in arg.modifiers:
            prefixes.append("~")
        final = "".join(prefixes)
        return f"{final}{self.to_string(arg.content)}"

    @to_string.register
    def _(self, arg: ShowStatement):
        content = arg.content
        if isinstance(content, ShowCategory):
            return f"show {content.value};"
        body = self.to_string(content)
        if body.endswith(";"):
            body = body[:-1]
        return f"show {body};"

    @to_string.register
    def _(self, arg: ValidateStatement):
        targets = ",".join(arg.targets) if arg.targets else "*"
        if arg.scope.value == ValidationScope.ALL:
            return "validate all;"
        return f"validate {arg.scope.value} {targets};"

    @to_string.register
    def _(self, arg: SelectStatement):
        with self.indented():
            select_columns = [
                self.indent_lines(self.to_string(c)) for c in arg.selection
            ]
            where_clause = None
            if arg.where_clause:
                where_clause = self.indent_lines(self.to_string(arg.where_clause))
            having_clause = None
            if arg.having_clause:
                having_clause = self.indent_lines(self.to_string(arg.having_clause))
            order_by = None
            if arg.order_by:
                order_by = [
                    self.indent_lines(self.to_string(c)) for c in arg.order_by.items
                ]

        return QUERY_TEMPLATE.render(
            select_columns=select_columns,
            where=where_clause,
            having=having_clause,
            order_by=order_by,
            limit=arg.limit,
        )

    @to_string.register
    def _(self, arg: MultiSelectStatement):
        # Each select gets its own indentation
        select_parts = []
        for select in arg.selects:
            select_parts.append(
                self.to_string(select)[:-2]
            )  # Remove the trailing ";\n"

        base = "\nmerge\n".join(select_parts)
        base += self.to_string(arg.align)
        if arg.derive:
            base += self.to_string(arg.derive)
        if arg.where_clause:
            base += f"\nwhere\n{self.to_string(arg.where_clause)}"
        if arg.order_by:
            base += f"\norder by\n{self.to_string(arg.order_by)}"
        if arg.limit:
            base += f"\nlimit {arg.limit}"
        base += "\n;"
        return base

    @to_string.register
    def _(self, arg: DeriveClause):
        with self.indented():
            items = [self.indent_lines(self.to_string(i)) for i in arg.items]
        return "\nderive\n" + ",\n".join(items)

    @to_string.register
    def _(self, arg: DeriveItem):
        return f"{self.to_string(arg.expr)} -> {self._unmangle_rowset_name(arg.name)}"

    @to_string.register
    def _(self, arg: CopyStatement):
        return f"copy into {arg.target_type.value} '{arg.target}' from {self.to_string(arg.select)}"

    @to_string.register
    def _(self, arg: AlignClause):
        # Grammar separates items with `and`, not commas.
        joined = "\nand ".join(self.to_string(c) for c in arg.items)
        with self.indented():
            joined = self.indent_lines(joined)
        return "\nalign\n" + joined

    @to_string.register
    def _(self, arg: AlignItem):
        prefix = "--" if arg.hidden else ""
        concepts = ", ".join(self.to_string(c) for c in arg.concepts)
        return f"{prefix}{arg.alias}: {concepts}"

    @to_string.register
    def _(self, arg: OrderBy):
        with self.indented():
            order_items = [self.indent_lines(self.to_string(c)) for c in arg.items]
        return ",\n".join(order_items)

    @to_string.register
    def _(self, arg: Ordering):
        return arg.value

    @to_string.register
    def _(self, arg: "WhereClause"):
        base = self._render_boolean(arg.conditional)
        # Strip outer parens only when we still have a single-line result —
        # multi-line breaks already unwrap the outer Parenthetical layer.
        if "\n" not in base and base.startswith("(") and base.endswith(")"):
            return base[1:-1]
        return base

    @to_string.register
    def _(self, arg: "Conditional"):
        return self._render_boolean(arg)

    @to_string.register
    def _(self, arg: "SubselectComparison"):
        return f"{self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)}"

    @to_string.register
    def _(self, arg: "Comparison"):
        return f"{self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)}"

    @to_string.register
    def _(self, arg: "Between"):
        return f"{self.to_string(arg.left)} between {self.to_string(arg.low)} and {self.to_string(arg.high)}"

    @to_string.register
    def _(self, arg: "Comment"):
        lines = "\n#".join(arg.text.split("\n"))
        return f"{lines}"

    @to_string.register
    def _(self, arg: "NumberingWindowItem"):
        args = [self.to_string(a) for a in arg.arguments]
        head = self._render_call(arg.type.value, args)
        return self._render_window_tail(head, arg.over, arg.order_by)

    @to_string.register
    def _(self, arg: "NavigationWindowItem"):
        call_args = [self.to_string(arg.content)]
        if arg.offset is not None:
            call_args.append(str(arg.offset))
        head = self._render_call(arg.type.value, call_args)
        return self._render_window_tail(head, arg.over, arg.order_by)

    @to_string.register
    def _(self, arg: "FilterItem"):
        content = self.to_string(arg.content)
        # Plain identifiers can sit naked on the LHS of ``?``; anything else
        # must be parenthesized so the grammar's ``_filter_alt`` rule binds it.
        if not isinstance(arg.content, (ConceptRef, Concept)):
            if not (content.startswith("(") and content.endswith(")")):
                content = f"({content})"
        where = self.to_string(arg.where)
        return self._pretty(
            [content, Break(priority=8, indent=1, flat=" "), f"? {where}"]
        )

    @to_string.register
    def _(self, arg: "ConceptRef"):
        if arg.address == "__preql_internal.all_rows":
            return "*"
        if arg.name.startswith(VIRTUAL_CONCEPT_PREFIX) and self.environment:
            return self.to_string(self.environment.concepts[arg.address])

        ns, base = arg.address.rsplit(".", 1)
        base = self._unmangle_rowset_name(base)
        if ns == DEFAULT_NAMESPACE:
            return base
        return f"{ns}.{base}"

    @to_string.register
    def _(self, arg: "ImportStatement"):
        if arg.is_self:
            return f"self import as {arg.alias};"
        path: str = str(arg.path).replace("\\", ".")
        path = path.replace("/", ".")
        if path.endswith(".preql"):
            path = path.rsplit(".", 1)[0]
        if path.startswith("."):
            path = path[1:]
        prefix = "." * arg.leading_dots
        if arg.alias == DEFAULT_NAMESPACE or not arg.alias:
            return f"import {prefix}{path};"
        return f"import {prefix}{path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "Import"):
        path: str = str(arg.path).replace("\\", ".")
        path = path.replace("/", ".")
        if path.endswith(".preql"):
            path = path.rsplit(".", 1)[0]
        if path.startswith("."):
            path = path[1:]
        if arg.alias == DEFAULT_NAMESPACE or not arg.alias:
            return f"import {path};"
        return f"import {path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "Concept"):
        if arg.name.startswith(VIRTUAL_CONCEPT_PREFIX):
            return self.to_string(arg.lineage)
        name = self._unmangle_rowset_name(arg.name)
        if arg.namespace == DEFAULT_NAMESPACE:
            return name
        return f"{arg.namespace}.{name}"

    @to_string.register
    def _(self, arg: "ConceptTransform"):
        return f"{self.to_string(arg.function)} as {self._unmangle_rowset_name(arg.output.name)}"

    @to_string.register
    def _(self, arg: "Function"):
        args = [self.to_string(c) for c in arg.arguments]

        if arg.operator == FunctionType.SUBTRACT:
            return " - ".join(args)
        if arg.operator == FunctionType.ADD:
            return " + ".join(args)
        if arg.operator == FunctionType.MULTIPLY:
            return " * ".join(args)
        if arg.operator == FunctionType.DIVIDE:
            return " / ".join(args)
        if arg.operator == FunctionType.MOD:
            return f"{args[0]} % {args[1]}"
        if arg.operator == FunctionType.PARENTHETICAL:
            return f"({args[0]})"
        if arg.operator == FunctionType.GROUP:
            arg_string = ", ".join(args[1:])
            if len(args) == 1:
                return f"group({args[0]})"
            return f"group({args[0]}) by {arg_string}"

        if arg.operator == FunctionType.CONSTANT:
            return f"{', '.join(args)}"
        if arg.operator == FunctionType.CAST:
            return f"{self.to_string(arg.arguments[0])}::{self.to_string(arg.arguments[1])}"
        if arg.operator == FunctionType.INDEX_ACCESS:
            return f"{self.to_string(arg.arguments[0])}[{self.to_string(arg.arguments[1])}]"
        if arg.operator == FunctionType.SIMPLE_CASE:
            switch_expr = self.to_string(arg.arguments[0])
            with self.indented():
                when_strs = [self.to_string(arg.arguments[0])]
                for case_arg in arg.arguments[1:]:
                    if isinstance(case_arg, CaseSimpleWhen):
                        # Extract the right side of the comparison (the value)
                        val = self.to_string(case_arg.value_expr)
                        result = self.to_string(case_arg.expr)
                        when_strs.append(self.indent_lines(f"when {val} then {result}"))
                    elif isinstance(case_arg, CaseElse):
                        when_strs.append(self.indent_lines(self.to_string(case_arg)))
            inputs = "\n".join(when_strs)
            return (
                f"case {switch_expr}\n{inputs}\n{self.indent_context.current_indent}end"
            )
        if arg.operator == FunctionType.CASE:
            with self.indented():
                indented_args = [
                    self.indent_lines(self.to_string(a)) for a in arg.arguments
                ]
            inputs = "\n".join(indented_args)
            return f"case\n{inputs}\n{self.indent_context.current_indent}end"

        if arg.operator == FunctionType.STRUCT:
            # zip arguments to pairs
            input_pairs = zip(arg.arguments[0::2], arg.arguments[1::2])
            with self.indented():
                pair_strings = []
                for k, v in input_pairs:
                    pair_line = f"{self.to_string(k)}-> {v}"
                    pair_strings.append(self.indent_lines(pair_line))
            inputs = ",\n".join(pair_strings)
            return f"struct(\n{inputs}\n{self.indent_context.current_indent})"
        if arg.operator == FunctionType.ALIAS:
            return f"{self.to_string(arg.arguments[0])}"
        return self._render_call(arg.operator.value, args)

    @to_string.register
    def _(self, arg: "OrderItem"):
        return f"{self.to_string(arg.expr)} {arg.order.value}"

    @to_string.register
    def _(self, arg: AggregateWrapper):
        func_str = self.to_string(arg.function)
        grouping = arg.grouping.value
        if not arg.by and grouping == "standard":
            return func_str
        if grouping == "grouping_sets":
            sets = []
            for grouping_set in arg.grouping_sets:
                sets.append(f"({', '.join([self.to_string(x) for x in grouping_set])})")
            tail = "by grouping sets " + ", ".join(sets)
        elif not arg.by:
            # Empty form: ``BY ROLLUP()`` rolls up over the select's own grain.
            # Grammar only allows the empty parens for ROLLUP, but we mirror
            # whichever grouping keyword the AST carries.
            kw = {"rollup": "rollup", "cube": "cube"}.get(grouping, "")
            tail = f"by {kw}()" if kw else "by ()"
        else:
            kw = {"rollup": "by rollup ", "cube": "by cube "}.get(grouping, "by ")
            by = ", ".join([self.to_string(x) for x in arg.by])
            tail = f"{kw}{by}"
        # The ``by`` boundary is the most natural break; give it higher
        # priority than function-internal arg breaks (which use 5).
        return self._pretty([func_str, Break(priority=10, indent=1, flat=" "), tail])

    @to_string.register
    def _(self, arg: MergeStatementV2):
        if len(arg.sources) == 1:
            return f"merge {self.to_string(arg.sources[0])} into {''.join([self.to_string(modifier) for modifier in arg.modifiers])}{self.to_string(arg.targets[arg.sources[0].address])};"
        return f"merge {arg.source_wildcard}.* into {''.join([self.to_string(modifier) for modifier in arg.modifiers])}{arg.target_wildcard}.*;"

    @to_string.register
    def _(self, arg: KeyMergeStatement):
        keys = ", ".join(sorted(list(arg.keys)))
        return f"merge property <{keys}> from {arg.target.address};"

    @to_string.register
    def _(self, arg: Modifier):
        if arg == Modifier.PARTIAL:
            return "~"
        elif arg == Modifier.HIDDEN:
            return "--"
        elif arg == Modifier.NULLABLE:
            return "?"
        return arg.value

    @to_string.register
    def _(self, arg: int):
        return f"{arg}"

    @to_string.register
    def _(self, arg: str):
        return f"'{arg}'"

    @to_string.register
    def _(self, arg: float):
        return f"{arg}"

    @to_string.register
    def _(self, arg: bool):
        return f"{arg}"

    @to_string.register
    def _(self, arg: list):
        base = ", ".join([self.to_string(x) for x in arg])
        return f"[{base}]"


def render_query(query: "SelectStatement") -> str:
    return Renderer().to_string(query)


def render_environment(environment: "Environment") -> str:
    return Renderer().to_string(environment)
