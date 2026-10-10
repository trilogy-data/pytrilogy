import dataclasses
from typing import cast

from trilogy.core.enums import (
    BooleanOperator,
    Derivation,
    FunctionType,
    SourceType,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConceptArgs,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildRowsetItem,
)
from trilogy.core.models.execute import (
    CTE,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
)
from trilogy.utility import unique

# Derivations whose rows cannot be re-scoped: a window, unnest or recursive
# output changes meaning when its CTE is folded into, or filtered by, another.
SENSITIVE_DERIVATIONS = frozenset(
    {Derivation.WINDOW, Derivation.UNNEST, Derivation.RECURSIVE}
)

# a CTE of these sources reshapes its rows; it is not a plain projection
ROW_RESHAPING_SOURCE_TYPES = (
    SourceType.GROUP,
    SourceType.WINDOW,
    SourceType.UNNEST,
    SourceType.RECURSIVE,
    SourceType.SUBSELECT,
    SourceType.UNION,
)


def propagate_existence_sources(
    branch: CTE, consumer: CTE | UnionCTE, addresses: set[str]
) -> bool:
    """Give ``branch`` the source entries a pushed-down condition needs for its
    existence arguments, copied from whichever map the consumer resolved them
    through, and the dependencies that back them. True when anything moved."""
    changed = False
    for address in addresses:
        if address in branch.source_map or address in branch.existence_source_map:
            continue
        if address in consumer.source_map:
            origin = list(consumer.source_map[address])
            branch.source_map[address] = origin
        elif address in consumer.existence_source_map:
            origin = list(consumer.existence_source_map[address])
            branch.existence_source_map[address] = origin
        else:
            continue
        for source in consumer.dependency_nodes():
            if source.name in origin:
                branch.add_dependency(source)
        changed = True
    return changed


def existence_linked(cte: CTE, parent: CTE) -> bool:
    """True when an existence subselect on either CTE reads from the other.
    Merging the pair would make the exists() reference the CTE it renders in,
    or a name that no longer exists."""
    return any(
        parent.name in (sources or []) for sources in cte.existence_source_map.values()
    ) or any(
        cte.name in (sources or []) for sources in parent.existence_source_map.values()
    )


def carry_child_state(parent: CTE, cte: CTE) -> None:
    """Move the child's state onto the parent it is being merged into.

    Nullability: an under-reported nullable set lets SimplifyNullSafeJoins
    falsely prove a key non-null and downgrade IS NOT DISTINCT FROM to `=`,
    dropping NULL-keyed groups. Existence references: an `IN (<set>)` resolves
    its set columns through existence_source_map, and dropping those entries
    strands the membership and lets the feeder CTE be pruned as unreferenced.
    Partiality: a dropped partial mark lets UpgradeJoinOnGuards read a proof
    on a key the merged CTE binds partially (a region domain's span on the
    solid stream) as forcing it present, and INNER-narrow the join that pads
    it. DISTINCT counts: the child's aggregates now render over the parent's
    rows, the stream that repeats their counted keys. LIMIT is the last
    logical operation of a SELECT, so the child's limit and ORDER BY apply
    unchanged to the merged CTE."""
    parent.distinct_counts = parent.distinct_counts | cte.distinct_counts
    nullable_addresses = {c.address for c in parent.nullable_concepts}
    for column in cte.nullable_concepts:
        if column.address not in nullable_addresses:
            parent.nullable_concepts.append(column)
    partial_addresses = {c.address for c in parent.partial_concepts}
    for column in cte.partial_concepts:
        if column.address not in partial_addresses:
            parent.partial_concepts.append(column)
    for address, sources in cte.existence_source_map.items():
        if address not in parent.existence_source_map:
            parent.existence_source_map[address] = sources
    if cte.limit is not None:
        parent.limit = cte.limit
        parent.order_by = cte.order_by


def output_addresses(cte: CTE | UnionCTE) -> set[str]:
    return {c.address for c in cte.output_columns}


def cte_source_keys(cte: CTE | UnionCTE) -> set[str]:
    return {cte.name, cte.safe_identifier}


def seed_ctes(cte: CTE | UnionCTE) -> list[CTE | UnionCTE]:
    """The CTE supplying the FROM clause: the LEFT side of the chain's first
    join, resolved in priority order:
      - ``joins[0].left_cte`` (explicit).
      - The ``joinkey_pair.cte``\\s of the first join, which by construction
        supply its left values and so name the FROM directly. This must beat
        the parent scan: a parent consumed only through an existence subselect
        never reaches the join chain, and seeding from it makes the real FROM
        table's columns look right-only, so a WHERE proof on a shared join key
        would falsely promote the join.
      - A ``parent_cte`` that is not consumed as any join's right side,
        skipping existence-only parents.
      - A ``joinkey_pair.cte`` on any later join, covering a chain whose left
        is an inlined CTE with no ``parent_cte`` and no explicit ``left_cte``."""
    if not isinstance(cte, CTE) or not cte.joins:
        return []
    first = cte.joins[0]
    if not isinstance(first, Join):
        return []
    if first.left_cte is not None:
        return [first.left_cte]
    right_names = {j.right_cte.name for j in cte.joins if isinstance(j, Join)}
    first_pair_seeds: list[CTE | UnionCTE] = []
    seen_pair_names: set[str] = set()
    for pair in first.joinkey_pairs or []:
        if (
            isinstance(pair, CTEConceptPair)
            and pair.cte.name not in right_names
            and pair.cte.name not in seen_pair_names
        ):
            seen_pair_names.add(pair.cte.name)
            first_pair_seeds.append(pair.cte)
    if first_pair_seeds:
        return first_pair_seeds
    existence = {s for vals in cte.existence_source_map.values() for s in vals}
    for parent in cte.dependency_nodes(include_inlined=True):
        if (
            isinstance(parent, (CTE, UnionCTE))
            and parent.name not in right_names
            and not (cte_source_keys(parent) & existence)
        ):
            return [parent]
    for j in cte.joins:
        if not isinstance(j, Join):
            continue
        for pair in j.joinkey_pairs or []:
            if isinstance(pair, CTEConceptPair) and pair.cte.name not in right_names:
                return [pair.cte]
    return []


def accumulated_left_ctes(cte: CTE | UnionCTE, idx: int) -> list[CTE | UnionCTE]:
    """What join ``idx`` reads on its left (left-deep): its explicit left, the
    FROM (``seed_ctes``) and every prior join's right side."""
    if not isinstance(cte, CTE):
        return []
    join = cte.joins[idx] if idx < len(cte.joins) else None
    left: list[CTE | UnionCTE] = []
    if isinstance(join, Join) and join.left_cte is not None:
        left.append(join.left_cte)
    left.extend(seed_ctes(cte))
    left.extend(prior.right_cte for prior in cte.joins[:idx] if isinstance(prior, Join))
    return unique(left, "name")


def zero_filled_reads(cte: CTE, condition: BoolExpr | None) -> set[str]:
    """COUNTs ``condition`` reads that ``cte`` renders coalesced to 0
    (``CTE.zero_fills_count``): a null-rejecting atom over one is satisfied by
    a padded row, so it is not a non-null proof."""
    if not isinstance(condition, BuildConceptArgs):
        return set()
    return cte.zero_filled_counts(condition.row_arguments)


def join_padded_ctes(cte: CTE) -> list[tuple[Join, list[CTE | UnionCTE]]]:
    """Each of ``cte``'s joins with the sides it NULL-pads: the right of a
    LEFT/FULL, and everything on its left (plus its joinkey sources) of a
    RIGHT/FULL."""
    out: list[tuple[Join, list[CTE | UnionCTE]]] = []
    for idx, join in enumerate(cte.joins or []):
        if not isinstance(join, Join):
            continue
        padded: list[CTE | UnionCTE] = []
        if join.jointype in PADS_RIGHT_JOIN_TYPES:
            padded.append(join.right_cte)
        if join.jointype in PADS_LEFT_JOIN_TYPES:
            padded.extend(accumulated_left_ctes(cte, idx))
            padded.extend(pair.cte for pair in join.joinkey_pairs or [])
        out.append((join, padded))
    return out


def null_padded_nodes(cte: CTE) -> list[CTE | UnionCTE]:
    """The sides ``cte``'s own outer joins NULL-pad (``join_padded_ctes``)."""
    return [node for _, padded in join_padded_ctes(cte) for node in padded]


def is_grouped_cte(cte: CTE) -> bool:
    return cte.group_to_grain or cte.source.source_type == SourceType.GROUP


def equivalent_addresses(concepts: list[BuildConcept]) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
    return out


def base_datasource(
    datasource: BuildDatasource | QueryDatasource,
) -> BuildDatasource | QueryDatasource | None:
    if isinstance(datasource, QueryDatasource):
        return datasource.base_datasource
    return None


def render_cte_used_map(cte: CTE | UnionCTE) -> dict[str, set[str]]:
    """Render ``cte`` against a throwaway dialect and return the per-parent map
    of addresses it actually consumed. The renderer follows alias/lineage
    chains, so it captures concepts reached via ``output_column.lineage`` that
    a shallow ``output_columns`` scan would miss."""
    from trilogy.dialect.base import BaseDialect

    renderer = BaseDialect()
    renderer.SUPPORTS_AGGREGATE_GROUPING_MODES = True
    renderer.SUPPORTS_QUALIFY = True
    renderer.render_cte(cte)
    return dict(renderer.used_map)


def condition_contains_atom(atom: object, condition: object | None) -> bool:
    if condition is None:
        return False
    if condition == atom:
        return True
    if (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return condition_contains_atom(atom, condition.left) or condition_contains_atom(
            atom, condition.right
        )
    return False


def strip_condition_atom(
    condition: BoolExpr | None,
    atom: object,
) -> BoolExpr | None:
    if condition is None or condition == atom:
        return None
    if not (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return condition
    left = strip_condition_atom(cast(BoolExpr | None, condition.left), atom)
    right = strip_condition_atom(cast(BoolExpr | None, condition.right), atom)
    if left is None:
        return right
    if right is None:
        return left
    return BuildConditional(left=left, operator=BooleanOperator.AND, right=right)


def append_condition(
    condition: BoolExpr | None,
    atom: BoolExpr,
) -> BoolExpr:
    if condition is None:
        return atom
    # Dedup on AND-atoms so re-appending a predicate the condition already
    # carries returns `condition` unchanged instead of growing `X AND X`.
    return merge_conditions_and_dedup(atom, condition)


def add_datasource_sorted(
    cte: CTE, datasource: BuildDatasource | QueryDatasource
) -> None:
    if datasource in cte.source.datasources:
        return
    cte.source.datasources = sorted(
        cte.source.datasources + [datasource],
        key=lambda x: x.identifier,
    )


def rename_reference(column: BuildConcept) -> BuildConcept | None:
    """The single column a pure rename re-labels, else None.

    Three rename shapes exist: a rowset boundary output (`with rs as select x
    ...` exposing `rs.x` over `x`), a concept alias (`select x as y`), and a
    filter (`x ? cond`) whose predicate the CTE's rows already satisfy (the
    renderer emits the content bare then; `consumed_parent_column` is what
    confirms the bare render). All render as `<content's sql> as <new name>`,
    so a CTE whose novel outputs are all renames of parent columns folds into
    the parent, which renders the rename from lineage (no source_map entry)
    against its own columns."""
    lineage = column.lineage
    if isinstance(lineage, BuildRowsetItem):
        return lineage.content
    if isinstance(lineage, BuildFilterItem) and isinstance(
        lineage.content, BuildConcept
    ):
        return lineage.content
    if isinstance(lineage, BuildFunction) and lineage.operator == FunctionType.ALIAS:
        args = lineage.concept_arguments
        if len(args) == 1:
            return args[0]
    if isinstance(lineage, BuildFunction) and lineage.operator == FunctionType.GROUP:
        # `sum(x) by k` re-labels the aggregate a parent already grouped to k.
        args = lineage.concept_arguments
        if args and args[0].derivation == Derivation.AGGREGATE:
            return args[0]
    return None


def consumed_parent_column(
    column: BuildConcept, cte: CTE, parent: CTE
) -> BuildConcept | None:
    """The parent output column `cte` actually renders `column` from, when that
    render is a bare parent-column reference (`"parent"."T"`); None otherwise.

    Pseudonym recovery may pick any exposed twin T, including a scoped-join
    canonical whose coalesce expression the lineage chain never mentions, so T
    is recovered from the actual render rather than predicted from lineage."""
    from trilogy.dialect.base import BaseDialect

    renderer = BaseDialect()
    try:
        rendered = renderer.render_concept_sql(column, cte, alias=False)
    except Exception:  # unrenderable: not a bare reference
        return None
    q = renderer.QUOTE_CHARACTER
    prefix = f"{q}{parent.safe_identifier}{q}.{q}"
    if not (rendered.startswith(prefix) and rendered.count(q) == 4):
        return None
    consumed_name = rendered[len(prefix) : -1]
    return next(
        (c for c in parent.output_columns if c.safe_address == consumed_name), None
    )


def rebind_rename_to_consumed(
    column: BuildConcept, consumed: BuildConcept
) -> BuildConcept:
    """Rebind a rename column's lineage to the exact parent column object it
    consumed, so the merged CTE always renders it through that object.

    A rename's lineage resolves against whatever bindings the CTE has when
    rendered; later phases add bindings that can flip it to a same-address
    side-variant (a raw column where the child read the coalescing canonical,
    which NULLs one-sided keys on a FULL union-join axis). Pinning the consumed
    object makes the rename and the parent's own output the same object."""
    lineage = column.lineage
    # A filter item is left alone: its consumed column may be the parent's own
    # MAX-collapsed twin of the same filter, and `CASE WHEN cond THEN max(...)`
    # nests aggregates. The merged parent renders it from its content.
    if isinstance(lineage, BuildRowsetItem) and lineage.content is not consumed:
        return dataclasses.replace(
            column, lineage=dataclasses.replace(lineage, content=consumed)
        )
    if (
        isinstance(lineage, BuildFunction)
        and lineage.operator in (FunctionType.ALIAS, FunctionType.GROUP)
        and lineage.arguments
        and lineage.arguments[0] is not consumed
    ):
        return dataclasses.replace(
            column,
            lineage=dataclasses.replace(
                lineage, arguments=[consumed, *lineage.arguments[1:]]
            ),
        )
    return column


def is_sole_consumer(
    cte: CTE,
    parent: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> bool:
    """Return True if cte is the only consumer of parent in the inverse map."""
    children = {c.name for c in inverse_map.get(parent.name, [])}
    return len(children) == 1 and cte.name in children


def repoint_consumers(
    old: CTE,
    new: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> None:
    """Redirect all consumers of old to new and update the inverse map."""
    consumers = inverse_map.get(old.name, [])
    for child in consumers:
        child.replace_dependency(old, new)
    if consumers:
        inverse_map[new.name] = inverse_map.get(new.name, []) + consumers
