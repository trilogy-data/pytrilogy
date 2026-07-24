"""Build-time HAVING normalization.

A HAVING clause may reference values that are not projected outputs. The
resolution rules (mirrored by parse-time validation in
``trilogy/parsing/v2/select_finalize.py``, which reports the genuinely
invalid cases with line positions):

* an aggregate whose value is one-per-grain-row (bare, or ``by`` a grain
  coarser than / equal to the select grain) is promoted to a hidden output and
  computed at the select grain — broadcasting it never changes the grain;
* a named aggregate/window/scalar concept reference is likewise materialized
  as a hidden plain output;
* a dimension finer than / outside the select grain — or a *finer* off-grain
  aggregate, which promotion must skip because it would fan the output out —
  becomes a post-aggregation semijoin on the grain key
  (``key in (filter key where <predicate>)``).

This runs at build time, on ``SelectLineage``, from
``Factory._build_select_lineage`` — so it applies uniformly to top-level
selects, rowset bodies, and multiselect arms, without mutating the authored
statement or the environment: minted concepts land in the (cloned) lineage's
``local_concepts``, which ``materialize_for_select`` exposes to discovery.
The pass is pure and deterministic — a lineage may be rebuilt several times
per query (rowset bodies, multiselect arms) and must normalize identically
each time.

The expression-level helpers here are shared with the parse layer, which
still performs alias substitution and user-facing validation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from dataclasses import replace as dc_replace
from typing import Any, cast

from trilogy.core.enums import (
    AggregateGroupingMode,
    BooleanOperator,
    ComparisonOperator,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    HavingClause,
    NavigationWindowItem,
    NumberingWindowItem,
    OrderItem,
    Parenthetical,
    ReferenceReplacements,
    RowsetItem,
    SelectLineage,
    SubselectComparison,
    SubselectItem,
    UndefinedConcept,
    UndefinedConceptFull,
    WhereClause,
)
from trilogy.core.models.core import ListWrapper, MapWrapper, TupleWrapper
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import _row_grain_arguments

_GROUPING_FNS = (FunctionType.GROUPING, FunctionType.GROUPING_ID)


def _strip_local_namespace(address: str) -> str:
    return address.removeprefix("local.")


def _macro_inner_aggregate(node: Any) -> Any | None:
    """If ``node`` is a ``def``-macro invocation whose body is a single aggregate
    (``@rollup_sales()`` -> ``sum(...) by rollup ...``), return that inner
    aggregate node, else ``None``. Lets HAVING/SELECT aggregate matching see
    through the ``FunctionCallWrapper`` produced by macro expansion."""
    if isinstance(node, FunctionCallWrapper):
        inner = node.content
        if isinstance(inner, AggregateWrapper) or (
            isinstance(inner, Function)
            and inner.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return inner
    return None


def _concept_address(c: Any, rename: Mapping[str, str] | None = None) -> str:
    if isinstance(c, (AggregateWrapper, Function)):
        nested = _render_aggregate(c)
        if nested:
            return nested
        # A composite (non-aggregate) argument like `a || b` embeds concept refs
        # that `rename` must reach: after the parse layer rewrites a HAVING
        # copy's inner ref to the SELECT alias while the SELECT copy keeps the
        # source name, the two aggregate signatures only compare equal once the
        # rename is pushed through the whole expression. Both compared sides run
        # this, so the rendered key need only be self-consistent.
        if isinstance(c, Function) and rename:
            replacements: ReferenceReplacements = [
                (src, ConceptRef(address=tgt)) for src, tgt in rename.items()
            ]
            return str(c.with_reference_replacement(replacements))
    addr = c.address if isinstance(c, (ConceptRef, Concept)) else str(c)
    return rename.get(addr, addr) if rename is not None else addr


def _aggregate_full_signature(
    node: Any,
    rename: Mapping[str, str] | None = None,
) -> tuple[Any, tuple[str, ...], tuple[str, ...]] | None:
    """Signature (operator, args, by) for matching aggregates across SELECT/HAVING.

    ``rename`` maps pure-rename SELECT sources to their alias addresses so a
    SELECT ``by rollup sales.channel`` and a HAVING ``by ... local.channel``
    (which finalize rewrites to the output grain) compare equal.

    Returns ``None`` for non-aggregate nodes.
    """
    inner = _macro_inner_aggregate(node)
    if inner is not None:
        return _aggregate_full_signature(inner, rename)
    if isinstance(node, AggregateWrapper):
        return (
            node.function.operator,
            tuple(_concept_address(a, rename) for a in node.function.arguments),
            tuple(sorted(_concept_address(c, rename) for c in node.by)),
        )
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        return (
            node.operator,
            tuple(_concept_address(a, rename) for a in node.arguments),
            (),
        )
    return None


def _render_aggregate(node: Any) -> str:
    sig = _aggregate_full_signature(node)
    if sig is None:
        return ""
    op, args, by = sig
    op_name = (op.value if hasattr(op, "value") else str(op)).lower()
    rendered = f"{op_name}({', '.join(args)})"
    if by:
        rendered += f" by {', '.join(by)}"
    return rendered


def _collect_condition_aggregates(node: Any) -> list[Any]:
    """Walk a WHERE/HAVING conditional tree and return outer aggregate nodes.

    Recognizes both ``AggregateWrapper`` and bare ``Function`` calls whose operator
    is an aggregate function. Does not descend into the inner argument expression
    of an aggregate — nested aggregates are matched by the outer signature.
    """
    found: list[Any] = []
    if isinstance(node, AggregateWrapper):
        found.append(node)
        return found
    if _macro_inner_aggregate(node) is not None:
        # `@rollup_agg()` macro wrapping a single aggregate — match as a whole.
        found.append(node)
        return found
    if isinstance(node, FunctionCallWrapper):
        found.extend(_collect_condition_aggregates(node.content))
        for macro_arg in node.args:
            found.extend(_collect_condition_aggregates(macro_arg))
        return found
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        found.append(node)
        return found
    if isinstance(node, Comparison) or isinstance(node, Conditional):
        found.extend(_collect_condition_aggregates(node.left))
        found.extend(_collect_condition_aggregates(node.right))
    elif isinstance(node, Parenthetical):
        found.extend(_collect_condition_aggregates(node.content))
    elif isinstance(node, Between):
        found.extend(_collect_condition_aggregates(node.left))
        found.extend(_collect_condition_aggregates(node.low))
        found.extend(_collect_condition_aggregates(node.high))
    elif isinstance(node, Function):
        for arg in node.arguments:
            found.extend(_collect_condition_aggregates(arg))
    return found


def _aggregate_node_has_nested_aggregate(node: Any) -> bool:
    """True if the aggregate node's argument expression contains another aggregate.

    Nested aggregates (``avg(sum(x) by ...)``) cannot be rendered as inline SQL
    and must be materialized via a SELECT alias.
    """
    if isinstance(node, AggregateWrapper):
        args: list[Any] = list(node.function.arguments)
    elif isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        args = list(node.arguments)
    else:
        return False
    stack: list[Any] = list(args)
    while stack:
        cur = stack.pop()
        if _aggregate_full_signature(cur) is not None:
            return True
        if isinstance(cur, Parenthetical):
            stack.append(cur.content)
        elif isinstance(cur, Function):
            stack.extend(cur.arguments)
    return False


def _substitute_condition_tree(node: Any, match_leaf: Any) -> Any:
    """Walk a HAVING conditional tree, replacing any node for which
    ``match_leaf(node)`` returns a ``ConceptRef`` with that reference. A
    ``None`` return leaves the node to the structural recursion. Matched
    nodes are leaves — we do not descend into their argument expressions."""
    replacement = match_leaf(node)
    if replacement is not None:
        return replacement
    if isinstance(node, Comparison):
        # Preserve the concrete class (e.g. SubselectComparison) — a plain
        # Comparison rebuild would strip a membership's existence semantics,
        # leaving the HAVING set-concept to render as a bare column.
        return type(node)(
            left=_substitute_condition_tree(node.left, match_leaf),
            right=_substitute_condition_tree(node.right, match_leaf),
            operator=node.operator,
        )
    if isinstance(node, Conditional):
        return Conditional(
            left=_substitute_condition_tree(node.left, match_leaf),
            right=_substitute_condition_tree(node.right, match_leaf),
            operator=node.operator,
        )
    if isinstance(node, Parenthetical):
        return Parenthetical(
            content=_substitute_condition_tree(node.content, match_leaf)
        )
    if isinstance(node, Between):
        return Between(
            left=_substitute_condition_tree(node.left, match_leaf),
            low=_substitute_condition_tree(node.low, match_leaf),
            high=_substitute_condition_tree(node.high, match_leaf),
        )
    if isinstance(node, Function):
        new_args = [_substitute_condition_tree(a, match_leaf) for a in node.arguments]
        if all(a is b for a, b in zip(new_args, node.arguments)):
            return node
        replacement_fn = Function.__new__(Function)
        replacement_fn.__dict__.update(node.__dict__)
        replacement_fn.arguments = new_args
        return replacement_fn
    # A window's inner order-by/operand may itself be an aggregate that matches a
    # SELECT alias (`rank(k) over (order by sum(x) desc)` alongside `sum(x) as t`).
    # Descend so it's rewritten to the materialized column — otherwise the renderer
    # re-derives the anonymous inline aggregate against a CTE that only has the
    # projected one, emitting an INVALID_REFERENCE_BUG sentinel.
    if isinstance(node, (NumberingWindowItem, NavigationWindowItem)):
        new_order = [
            OrderItem(
                expr=_substitute_condition_tree(o.expr, match_leaf), order=o.order
            )
            for o in node.order_by
        ]
        new_over = [_substitute_condition_tree(o, match_leaf) for o in node.over]
        order_changed = any(
            a.expr is not b.expr for a, b in zip(new_order, node.order_by)
        )
        over_changed = any(a is not b for a, b in zip(new_over, node.over))
        if isinstance(node, NumberingWindowItem):
            new_args = [
                _substitute_condition_tree(a, match_leaf) for a in node.arguments
            ]
            if not (
                order_changed
                or over_changed
                or any(a is not b for a, b in zip(new_args, node.arguments))
            ):
                return node
            return NumberingWindowItem(
                type=node.type,
                arguments=new_args,
                order_by=new_order,
                over=new_over,
            )
        new_content = _substitute_condition_tree(node.content, match_leaf)
        if not (order_changed or over_changed or new_content is not node.content):
            return node
        return NavigationWindowItem(
            type=node.type,
            content=new_content,
            order_by=new_order,
            over=new_over,
            offset=node.offset,
        )
    return node


def _substitute_having_aggregates(
    node: Any,
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef],
    rename: Mapping[str, str] | None = None,
) -> Any:
    """Rewrite a HAVING conditional tree, replacing matched aggregates with
    ``ConceptRef`` to the SELECT alias."""

    def match(n: Any) -> ConceptRef | None:
        sig = _aggregate_full_signature(n, rename)
        return sig_to_ref.get(sig) if sig is not None else None

    return _substitute_condition_tree(node, match)


def _child_exprs(node: Any) -> Iterator[Any]:
    """Direct child expressions of a composite ``Expr`` node — the value-bearing
    sub-expressions an aggregate could be nested in. Covers every non-leaf member
    of the ``Expr`` union (author.py); leaves (``ConceptRef``, scalars) yield
    nothing. Filter/window/subselect carry their value in ``content``/``arguments``
    only — their ``where``/``order_by`` are conditions, not part of the projected
    value, so they are intentionally not descended here.

    Keep in sync with the ``Expr`` union; ``test_aggregate_wrapper_collection``'s
    exhaustiveness guard fails if a new composite type is added without coverage.
    """
    if isinstance(node, (Comparison, Conditional)):
        yield node.left
        yield node.right
    elif isinstance(node, Between):
        yield node.left
        yield node.low
        yield node.high
    elif isinstance(node, Parenthetical):
        yield node.content
    elif isinstance(node, CaseWhen):
        yield node.comparison
        yield node.expr
    elif isinstance(node, CaseElse):
        yield node.expr
    elif isinstance(node, AggregateWrapper):
        yield from node.function.arguments
    elif isinstance(node, Function):
        yield from node.arguments
    elif isinstance(node, FunctionCallWrapper):
        # `def` macro expansion (`@rollup_agg(x)`): body in `content`, call args.
        yield node.content
        yield from node.args
    elif isinstance(node, FilterItem) or isinstance(node, NavigationWindowItem):
        yield node.content
    elif isinstance(node, NumberingWindowItem):
        yield from node.arguments
    elif isinstance(node, SubselectItem):
        yield node.content
    elif isinstance(node, (TupleWrapper, ListWrapper)):
        yield from node
    elif isinstance(node, MapWrapper):
        yield from node.keys()
        yield from node.values()


def _expression_contains_window(node: Any) -> bool:
    if isinstance(node, (NavigationWindowItem, NumberingWindowItem)):
        return True
    return any(_expression_contains_window(child) for child in _child_exprs(node))


def _window_key_addresses(node: Any) -> set[str]:
    if isinstance(node, (NavigationWindowItem, NumberingWindowItem)):
        refs = {
            expr.address
            for expr in [
                *node.over,
                *(item.expr for item in node.order_by),
            ]
            if isinstance(expr, ConceptRef)
        }
        if isinstance(node, NavigationWindowItem):
            refs |= _window_key_addresses(node.content)
        else:
            for argument in node.arguments:
                refs |= _window_key_addresses(argument)
        return refs
    return {
        address
        for child in _child_exprs(node)
        for address in _window_key_addresses(child)
    }


def _collect_aggregate_wrappers(
    node: Any, predicate: Callable[[Any], bool]
) -> list[AggregateWrapper]:
    """Every ``AggregateWrapper`` matching ``predicate`` anywhere in an expression
    tree — so a target agg nested inside ``coalesce(...)``, a ``case``, a ``def``
    macro, a window, a filter, etc. is still found. A matching node is returned
    without descending into it (an aggregate won't nest a same-class aggregate)."""
    if predicate(node):
        return [cast(AggregateWrapper, node)]
    found: list[AggregateWrapper] = []
    for child in _child_exprs(node):
        found += _collect_aggregate_wrappers(child, predicate)
    return found


def _is_grouping_aggregate(node: Any) -> bool:
    return (
        isinstance(node, AggregateWrapper) and node.function.operator in _GROUPING_FNS
    )


def _is_standard_grouping_aggregate(node: Any) -> bool:
    return (
        _is_grouping_aggregate(node) and node.grouping == AggregateGroupingMode.STANDARD
    )


def _collect_standard_grouping_wrappers(node: Any) -> list[AggregateWrapper]:
    """All STANDARD-mode ``grouping()``/``grouping_id()`` wrappers anywhere in an
    expression tree (e.g. nested inside a ``case`` that derives a rollup level)."""
    return _collect_aggregate_wrappers(node, _is_standard_grouping_aggregate)


def _is_window_item(node: Any) -> bool:
    return isinstance(node, (NavigationWindowItem, NumberingWindowItem))


def _bare_row_refs(node: Any) -> list[ConceptRef]:
    """Concept references of an expression reachable without crossing an
    aggregate or window boundary — the references that must exist as row-level
    columns of the post-aggregation result for the expression to evaluate.

    A ref *inside* an aggregate/window is not one: the computed value is
    materialized (or validated) separately and its operands are collapsed away
    by the grouping. This is the distinction between a raw concept being an
    operand of a selected expression and it being available at the output
    grain — ``count(x ? dim is not null) as c`` consumes ``dim`` but does not
    carry it through the group CTE, so a bare HAVING reference to ``dim`` still
    needs the semijoin rewrite (q72)."""
    if isinstance(node, ConceptRef):
        return [node]
    if isinstance(node, (AggregateWrapper, NavigationWindowItem, NumberingWindowItem)):
        return []
    if isinstance(node, Function):
        if (
            node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            or node.operator in _GROUPING_FNS
        ):
            return []
        out: list[ConceptRef] = []
        for arg in node.arguments:
            out += _bare_row_refs(arg)
        return out
    if isinstance(node, FunctionCallWrapper):
        return _bare_row_refs(node.content)
    if isinstance(node, Parenthetical):
        return _bare_row_refs(node.content)
    if isinstance(node, SubselectComparison):
        # The existence RHS is sourced as an independent subselect; only the
        # row side must resolve at the output grain.
        return _bare_row_refs(node.left)
    if isinstance(node, (Comparison, Conditional)):
        return _bare_row_refs(node.left) + _bare_row_refs(node.right)
    if isinstance(node, Between):
        return (
            _bare_row_refs(node.left)
            + _bare_row_refs(node.low)
            + _bare_row_refs(node.high)
        )
    if isinstance(node, CaseWhen):
        return _bare_row_refs(node.comparison) + _bare_row_refs(node.expr)
    if isinstance(node, CaseElse):
        return _bare_row_refs(node.expr)
    if isinstance(node, FilterItem):
        return _bare_row_refs(node.content) + _bare_row_refs(node.where.conditional)
    return []


def _promotion_grows_grain(
    concept: Concept,
    base_targets: list[Any],
    base_components: set[str],
    environment: Environment,
) -> bool:
    """True if adding ``concept`` as a SELECT output would enlarge the select
    grain. A bare/coarser aggregate broadcasts one value per grain row (grain
    unchanged); a *finer* off-grain aggregate (``agg(x) by k`` where ``k`` is
    finer than the grain) enters the grain as itself, which would fan the output
    out — so it must be semijoined, not promoted."""
    combined = Grain.from_concepts(
        base_targets + [concept],
        where_clause=None,
        environment=environment,
        local_concepts={concept.address: concept},
    )
    # Resolve the candidate's own address through the seeded map; every other
    # target is a concrete Concept or resolves via the environment, so the only
    # possible new component beyond ``base_components`` is the candidate itself.
    return bool(combined.components - base_components)


class _ChainedConcepts:
    """Address lookup over a select's local concepts, falling back to the
    environment — the build-time analogue of the parse layer's merged
    pending/committed view."""

    def __init__(self, local: Mapping[str, Concept], environment: Environment):
        self._local = local
        self._environment = environment

    def get(self, address: str) -> Concept | None:
        found = self._local.get(address)
        if found is not None:
            return found
        return self._environment.concepts.get(address)


def _is_single_row_rowset_scalar(
    named: Concept,
    concepts: _ChainedConcepts | Mapping[str, Concept] | None = None,
    _seen: set[str] | None = None,
) -> bool:
    """True for a rowset output whose source select is grainless — it produces
    exactly one row and so broadcasts like a scalar (``rowset r <- select
    sum(x)/sum(y) as v``). Such an output is mis-tagged ``MULTI_ROW`` granularity
    (and ``ROWSET`` derivation, not ``AGGREGATE``), so the plain scalar-promotion
    gate misses it and the HAVING predicate gets misrouted into the finer-dim
    grain-key semijoin — which silently drops rollup subtotal rows (NULL-key
    collision) and doesn't even filter correctly on a plain grouped select.
    Restricted to a single (non-union) ``SelectLineage``: a union of grainless
    arms yields one row *per arm*, not a single broadcast row.

    Also true for a BASIC concept derived only from single-row rowset scalars
    (plus literals) — e.g. ``auto avg <- ov.total / ov.count``. Combining scalars
    stays a single broadcast row, but the wrapper is ``derivation=BASIC`` (not a
    ``RowsetItem``), so without recursing the wrapper the HAVING predicate is
    again misrouted into the semijoin and rollup subtotals vanish. This is the
    natural HAVING form (`having sum(x) > my_scalar`), so it must be covered.
    Requires ``concepts`` to resolve the BASIC args (they arrive as ``ConceptRef``)."""
    lineage = named.lineage
    if isinstance(lineage, RowsetItem):
        select = lineage.rowset.select
        return isinstance(select, SelectLineage) and not select.grain.components
    if concepts is not None and named.derivation == Derivation.BASIC:
        args = named.concept_arguments
        if not args:
            return False
        if _seen is None:
            _seen = set()
        if named.address in _seen:
            return False
        _seen.add(named.address)
        resolved = [concepts.get(a.address) for a in args]
        return all(
            r is not None and _is_single_row_rowset_scalar(r, concepts, _seen)
            for r in resolved
        )
    return False


@dataclass
class _HavingNormalization:
    """Working state for one select's HAVING normalization. Copies of the
    lineage's collections are mutated here and committed via ``dc_replace``
    only when something changed — the input lineage may be a shared
    environment object (a rowset body) and must never be mutated."""

    environment: Environment
    base: SelectLineage
    selection: list[ConceptRef]
    local_concepts: dict[str, Concept]
    hidden: set[str]
    conditional: Any
    changed: bool = False

    @property
    def line_no(self) -> int | None:
        return self.base.meta.line_number if self.base.meta else None

    def resolve(self, address: str) -> Concept | None:
        found = self.local_concepts.get(address)
        if found is not None:
            return found
        return self.environment.concepts.get(address)

    def is_unresolved(self, address: str) -> bool:
        resolved = self.resolve(address)
        return resolved is None or isinstance(
            resolved, (UndefinedConcept, UndefinedConceptFull)
        )

    def rename_map(self) -> dict[str, str]:
        """Pure-rename outputs (`X as Y`) as `source_address -> alias_address`,
        recovered from local concept ALIAS lineages — mirrors the parse layer's
        ``_alias_rename_map`` so signature matching survives the rewrite of
        HAVING refs to output aliases."""
        rename: dict[str, str] = {}
        for ref in self.selection:
            concept = self.local_concepts.get(ref.address)
            if concept is None:
                continue
            lineage = concept.lineage
            if (
                isinstance(lineage, Function)
                and lineage.operator == FunctionType.ALIAS
                and len(lineage.arguments) == 1
                and isinstance(lineage.arguments[0], ConceptRef)
            ):
                rename[lineage.arguments[0].address] = ref.address
        return rename

    def derived_lineages(self) -> Iterator[tuple[str, Any]]:
        """(address, lineage) of transform-derived selection items. Rowset-output
        locals are materialized boundaries, not select-scope derivations, and are
        excluded — matching the parse layer's ConceptTransform-only view."""
        for ref in self.selection:
            concept = self.local_concepts.get(ref.address)
            if concept is None or concept.lineage is None:
                continue
            if isinstance(concept.lineage, RowsetItem):
                continue
            yield ref.address, concept.lineage

    def row_arguments(self) -> list[ConceptRef]:
        return list(HavingClause(conditional=self.conditional).row_arguments)

    def add_hidden_output(self, concept: Concept) -> None:
        self.selection.append(concept.reference)
        self.hidden.add(concept.address)
        self.local_concepts[concept.address] = concept
        self.changed = True

    def base_grain_inputs(self) -> tuple[list[Any], set[str]]:
        """Targets and grain components of the explicit selection, computed the
        way hidden-promotion safety was judged at authoring time (no WHERE
        enrichment)."""
        targets: list[Any] = list(self.selection)
        components = Grain.from_concepts(
            targets,
            where_clause=None,
            environment=self.environment,
            local_concepts=self.local_concepts,
        ).components
        return targets, components

    def promote_aggregates(self) -> None:
        """Mint a hidden output for each HAVING aggregate that is not already a
        SELECT output, and point the HAVING reference at it.

        A HAVING aggregate filters the post-aggregation rows, so it must be a
        materialized column. Promoting a *bare or coarser* aggregate is
        grain-safe: it broadcasts one value per grain row, so the select grain
        (and therefore the grain at which every projection aggregate computes)
        is unchanged. A *finer* off-grain aggregate would instead enter the
        grain as itself and fan the output out, so it is skipped here and left
        for the semijoin rewrite in ``rewrite_finer_dims_to_membership``."""
        from trilogy.parsing.common import arbitrary_to_concept

        rename = self.rename_map()
        existing: set[tuple[Any, tuple[str, ...], tuple[str, ...]]] = set()
        for _, lineage in self.derived_lineages():
            sig = _aggregate_full_signature(lineage, rename)
            if sig is not None:
                existing.add(sig)
        base_targets: list[Any] | None = None
        base_components: set[str] = set()
        sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef] = {}
        for node in _collect_condition_aggregates(self.conditional):
            # Macros (`@rollup()`) and grouping indicators are materialized by
            # the parse layer (they need rollup-spec colocation); only mint
            # plain/off-grain/nested aggregate expressions here.
            if isinstance(node, FunctionCallWrapper) or _is_standard_grouping_aggregate(
                node
            ):
                continue
            sig = _aggregate_full_signature(node, rename)
            if sig is None or sig in existing or sig in sig_to_ref:
                continue
            concept = arbitrary_to_concept(node, self.environment)
            if base_targets is None:
                base_targets, base_components = self.base_grain_inputs()
            if _promotion_grows_grain(
                concept, base_targets, base_components, self.environment
            ):
                continue
            adjusted = concept.set_select_grain(
                self.base.grain,
                self.environment,
                # under a select-level grouping spec, bare aggregates stay
                # un-pinned so the build factory can stamp the spec onto them
                pin_bare_aggregates=self.base.grouping is None,
            )
            self.add_hidden_output(adjusted)
            sig_to_ref[sig] = adjusted.reference
        if sig_to_ref:
            self.conditional = _substitute_having_aggregates(
                self.conditional, sig_to_ref, rename
            )
            self.changed = True

    def promote_named_refs(self) -> None:
        """A HAVING reference to a *named* aggregate concept (`auto m <- sum(x)
        by k`, or a `@macro()` aggregate used directly in HAVING) or to a scalar
        (a single-row rowset output like a whole-table `avg(...)`) is the same
        case as an inline aggregate: it must be materialized and broadcast at
        the select grain. It arrives as a plain ConceptRef (not an aggregate
        node) so ``promote_aggregates`` misses it — add it as a hidden plain
        output here. Grain-safe: a metric never contributes to grain and a
        single-row value is constant across every row. A reference *finer* than
        the grain is left for the semijoin rewrite. ``is_aggregate`` is False
        for a macro-wrapped aggregate, so key off ``derivation`` too (and
        SINGLE_ROW granularity for scalars)."""
        output_addrs = {r.address for r in self.selection}
        concepts = _ChainedConcepts(self.local_concepts, self.environment)
        for ref in self.row_arguments():
            if ref.address in output_addrs:
                continue
            named = self.resolve(ref.address)
            if named is None or isinstance(
                named, (UndefinedConcept, UndefinedConceptFull)
            ):
                continue
            grain_determined = (
                named.is_aggregate
                or named.derivation in (Derivation.AGGREGATE, Derivation.WINDOW)
                or named.granularity == Granularity.SINGLE_ROW
                or _is_single_row_rowset_scalar(named, concepts)
            )
            if not grain_determined:
                continue
            self.add_hidden_output(named)
            output_addrs.add(named.address)

    def alias_source_addresses(self) -> set[str]:
        sources: set[str] = set()
        for _, lineage in self.derived_lineages():
            for arg in _row_grain_arguments(lineage):
                sources.add(arg.address)
        return sources

    def row_grain_allowed_addresses(self) -> set[str]:
        """Addresses available as row-level references of the post-aggregation
        result: the outputs themselves plus operands of *scalar* select
        derivations (which stay at the output grain). Operands that appear only
        inside a selected aggregate or window are excluded — the grouping
        collapses them. Stricter than ``alias_source_addresses``, which
        intentionally exposes aggregate operands so an inline HAVING aggregate
        can match its projected twin."""
        allowed = {r.address for r in self.selection}
        for _, lineage in self.derived_lineages():
            for ref in _bare_row_refs(lineage):
                allowed.add(ref.address)
        return allowed

    def stable_grain_keys(self) -> list[str]:
        """Grain keys usable as the semijoin anchor. A window-derived grain key
        is not stable pre-aggregation — substitute the window's own partition/
        order keys instead."""
        grain_keys = sorted(self.base.grain.components)
        stable: list[str] = []
        window_keys: set[str] = set()
        for address in grain_keys:
            concept = self.resolve(address)
            lineage = concept.lineage if concept is not None else None
            if isinstance(lineage, (NavigationWindowItem, NumberingWindowItem)):
                window_keys |= _window_key_addresses(lineage)
                continue
            if lineage is not None and _expression_contains_window(lineage):
                window_keys |= _window_key_addresses(lineage)
                continue
            stable.append(address)
        stable.extend(sorted(window_keys - set(stable)))
        return stable if stable else grain_keys

    def build_grain_key_membership(
        self, leaf: Comparison | Between, grain_keys: list[str]
    ) -> SubselectComparison:
        """Turn a HAVING predicate on a non-output dimension into a semijoin
        against the select grain key(s): the row-wise composite ``(k1, k2) in
        (filter k1 where <leaf>, filter k2 where <leaf>)``. The per-key
        filtered sets share the same predicate and are sourced together, so
        they stay correlated (the surviving key *combinations*, not the cross
        product). Always the ROW_TUPLE form — even for a single-key grain — so
        rendering is null-safe row identity matching; a scalar ``key IN
        (select ...)`` deletes any group whose key is NULL (q11/q59).

        The select's WHERE clause is AND-ed into the filter so the existence
        set is evaluated within the same pre-aggregation universe the aggregate
        saw — a group only survives if it has a matching row *that also passed
        WHERE*."""
        from trilogy.parsing.common import arbitrary_to_concept, row_tuple_function

        if not grain_keys:
            raise InvalidSyntaxException(
                f"HAVING filters on a dimension outside the SELECT projection, but "
                f"the select has no grain key to anchor a post-aggregation semijoin "
                f"(line {self.line_no}). Move the filter to WHERE to filter before "
                f"aggregation."
            )
        predicate: Any = leaf
        if self.base.where_clause is not None:
            predicate = Conditional(
                left=leaf,
                right=self.base.where_clause.conditional,
                operator=BooleanOperator.AND,
            )
        key_refs: list[ConceptRef] = []
        filtered_refs: list[ConceptRef] = []
        for addr in grain_keys:
            key_concept = self.resolve(addr)
            if key_concept is None or isinstance(
                key_concept, (UndefinedConcept, UndefinedConceptFull)
            ):
                raise InvalidSyntaxException(
                    f"HAVING filters on a dimension outside the SELECT projection "
                    f"but the select grain key '{addr}' could not be resolved to "
                    f"build the semijoin (line {self.line_no}). Move the filter to "
                    f"WHERE instead."
                )
            key_ref = key_concept.reference
            filtered = arbitrary_to_concept(
                FilterItem(content=key_ref, where=WhereClause(conditional=predicate)),
                self.environment,
            )
            self.local_concepts[filtered.address] = filtered
            key_refs.append(key_ref)
            filtered_refs.append(filtered.reference)
        left = row_tuple_function(list(key_refs))
        right = row_tuple_function(list(filtered_refs))
        return SubselectComparison(
            left=left, right=right, operator=ComparisonOperator.IN
        )

    def rewrite_finer_dims_to_membership(self, allowed_addresses: set[str]) -> None:
        """Rewrite each HAVING predicate on a dimension outside the select grain
        into a post-aggregation semijoin ``key in (filter key where <predicate>)``.

        HAVING filters *after* aggregation, so a finer-grain predicate must never
        change the value of a select-grain aggregate — it may only decide which
        select-grain rows survive (those that have at least one matching finer
        row). Filtering the select grain *key* against the keys that satisfy the
        predicate is exactly that, and — because it filters whole groups rather
        than rows within a group — it is invariant to predicate pushdown. The
        membership reuses the existing existence machinery, which sources the
        filtered set as an independent subquery that never contaminates the
        aggregate.

        A leaf needs the rewrite under either rule:

        - any of its *bare* row references (outside aggregates/windows) is not
          materialized at the output grain (``row_grain_allowed_addresses``). The
          broader ``allowed_addresses`` cannot decide this: it includes aggregate
          operands, but ``sum(x ? dim = 1) as c`` consumes ``dim`` without
          carrying it through the group CTE, so a bare HAVING ``dim`` would
          otherwise slip through unrewritten and either get pushed
          pre-aggregation (silent wrong result) or reach rendering with no
          source (INVALID_REFERENCE_BUG, q72);
        - any reference at all (including inside a finer off-grain aggregate
          that ``promote_aggregates`` deliberately skipped) is outside
          ``allowed_addresses`` — the pre-existing rule.
        """
        grain_keys = self.stable_grain_keys()
        row_grain_allowed = self.row_grain_allowed_addresses()
        built_membership = False

        def needs_membership(leaf: Any) -> bool:
            # SubselectComparison subclasses Comparison: an existing `x in <set>`
            # whose *row* side x is a non-output dimension is wrapped too (its
            # row_arguments exclude the existence RHS, so only the row side
            # counts).
            if not isinstance(leaf, (Comparison, Between)):
                return False
            extra = [
                r for r in leaf.row_arguments if r.address not in allowed_addresses
            ]
            extra += [
                r for r in _bare_row_refs(leaf) if r.address not in row_grain_allowed
            ]
            if not extra:
                return False
            # A genuinely undefined reference is reported by the final ref
            # check; don't wrap it.
            return not any(self.is_unresolved(r.address) for r in extra)

        def transform(node: Any) -> Any:
            nonlocal built_membership
            if isinstance(node, Conditional):
                new_left = transform(node.left)
                new_right = transform(node.right)
                if new_left is node.left and new_right is node.right:
                    return node
                return Conditional(
                    left=new_left, right=new_right, operator=node.operator
                )
            if isinstance(node, Parenthetical):
                new_content = transform(node.content)
                if new_content is node.content:
                    return node
                return Parenthetical(content=new_content)
            if needs_membership(node):
                built_membership = True
                return self.build_grain_key_membership(node, grain_keys)
            return node

        new_conditional = transform(self.conditional)
        if new_conditional is not self.conditional:
            self.conditional = new_conditional
            self.changed = True

        # The membership left side is the full select-grain key tuple, but a
        # grain key need not be a projected output (e.g. a window's `order by`/
        # `partition by` key that's pulled into the grain but not selected).
        # Such a key is absent from the final CTE, so the rendered left tuple
        # resolves to INVALID_REFERENCE_BUG. Materialize any unprojected grain
        # key as a hidden output so it survives to the final CTE for the
        # semijoin to reference.
        if built_membership:
            output_addrs = {r.address for r in self.selection}
            for addr in grain_keys:
                if addr in output_addrs:
                    continue
                key_concept = self.resolve(addr)
                if key_concept is None or isinstance(
                    key_concept, (UndefinedConcept, UndefinedConceptFull)
                ):
                    continue
                self.add_hidden_output(key_concept)
                output_addrs.add(key_concept.address)

    def validate_remaining_refs(self, allowed_addresses: set[str]) -> None:
        """After promotion and the semijoin rewrite, the only row references
        left should be SELECT outputs and grain keys. Anything else is either
        undefined (normally caught at parse; backstop here) or a reference the
        rewrite could not route — report it rather than let it reach planning
        with no backing column."""
        grain = set(self.base.grain.components)
        allowed_for_having = (
            allowed_addresses | grain | {r.address for r in self.selection}
        )
        row_grain_allowed = self.row_grain_allowed_addresses() | grain
        flagged = [
            ref.address
            for ref in self.row_arguments()
            if ref.address not in allowed_for_having
        ]
        flagged += [
            ref.address
            for ref in _bare_row_refs(self.conditional)
            if ref.address not in row_grain_allowed
        ]
        undefined_refs: list[str] = []
        unhandled_refs: list[str] = []
        for address in flagged:
            if self.is_unresolved(address):
                if address not in undefined_refs:
                    undefined_refs.append(address)
            elif address not in unhandled_refs:
                unhandled_refs.append(address)
        if undefined_refs:
            refs = ", ".join(f"'{a}'" for a in undefined_refs)
            verb = "is" if len(undefined_refs) == 1 else "are"
            raise InvalidSyntaxException(
                f"HAVING references {refs}, which {verb} not defined (line "
                f"{self.line_no}). Check for a typo or import the relevant concept."
            )
        if unhandled_refs:
            refs = ", ".join(f"'{a}'" for a in unhandled_refs)
            raise InvalidSyntaxException(
                f"HAVING references {refs} outside the SELECT projection and could "
                f"not be resolved as a post-aggregation filter (line {self.line_no})."
                f" Move the filter to WHERE to filter before aggregation."
            )


def normalize_select_having(
    base: SelectLineage, environment: Environment
) -> SelectLineage:
    """Resolve every non-output HAVING reference before the lineage is built:
    grain-safe aggregates and named metrics/scalars become hidden outputs;
    finer dimensions become grain-key semijoins. Returns ``base`` untouched
    when there is nothing to do; otherwise a modified copy — the input may be
    a shared environment object (rowset body) and is never mutated."""
    if base.having_clause is None:
        return base
    state = _HavingNormalization(
        environment=environment,
        base=base,
        selection=list(base.selection),
        local_concepts=dict(base.local_concepts),
        hidden=set(base.hidden_components),
        conditional=base.having_clause.conditional,
    )
    state.promote_aggregates()
    state.promote_named_refs()
    allowed_addresses = {
        r.address for r in state.selection
    } | state.alias_source_addresses()
    state.rewrite_finer_dims_to_membership(allowed_addresses)
    state.validate_remaining_refs(allowed_addresses)
    if not state.changed:
        return base
    return dc_replace(
        base,
        selection=state.selection,
        local_concepts=state.local_concepts,
        hidden_components=state.hidden,
        having_clause=HavingClause(conditional=state.conditional),
    )
