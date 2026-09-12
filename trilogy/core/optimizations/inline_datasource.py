import re
from collections import defaultdict

from trilogy.constants import CONFIG
from trilogy.core.enums import JoinType
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.datasource import RawColumnExpr
from trilogy.core.models.execute import (
    CTE,
    DatasourceCTE,
    InstantiatedUnnestJoin,
    Join,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    append_condition,
    consumed_parent_column,
    is_sole_consumer,
    rebind_rename_to_consumed,
    rename_reference,
    render_cte_used_map,
)

_SQL_STRING_RE = re.compile(r"'(?:[^']|'')*'")
_SQL_TOKEN_RE = re.compile(r'"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*')

# Words a raw() expression can carry that are not column references: SQL
# syntax and type names. This is precision, not safety -- an unlisted word is
# treated as a column of this datasource and checked for a collision, so a gap
# here can only cost a CTE the fold would have been welcome to take.
_NON_COLUMN_WORDS = """
    all and any as asc at between by case cast collate cross current_date
    current_time current_timestamp desc distinct else end escape exists false
    filter first from full ilike in inner interval into is join last left like
    localtime localtimestamp natural not null nulls on or order outer over
    partition precision right rlike similar some symmetric then to true unknown
    using when where window with within zone
    bigint bit blob bool boolean bytea char date datetime decimal double float
    hugeint int int1 int2 int4 int8 integer json numeric real smallint string
    text time timestamp timestamptz tinyint uuid varbinary varchar
"""
_SQL_NON_COLUMN_WORDS = frozenset(_NON_COLUMN_WORDS.split())


def _raw_text_column_refs(text: str) -> set[str] | None:
    """Column names ``text`` reads, lowercased, or None when it carries a
    qualified ``alias.column`` whose qualifier does not survive the fold.

    A raw() binding is evaluated in its own datasource's scope, so every word
    in it that is not SQL syntax is a column of that table, declared in the
    model or not. Folding changes exactly one thing about that: the name stops
    being alone in the FROM. A literal carries no words and yields the empty
    set.
    """
    body = _SQL_STRING_RE.sub(" ", text)
    refs: set[str] = set()
    for match in _SQL_TOKEN_RE.finditer(body):
        token = match.group(0)
        after = body[match.end() :].lstrip()
        if after.startswith("("):
            # A function name resolves against the schema, not against a table.
            continue
        if after.startswith(".") or body[: match.start()].rstrip().endswith("."):
            return None
        if token.startswith('"'):
            refs.add(token[1:-1].replace('""', '"').lower())
            continue
        name = token.lower()
        if name in _SQL_NON_COLUMN_WORDS:
            continue
        refs.add(name)
    return refs


def _consumer_scope_names(cte: CTE, parent: DatasourceCTE) -> set[str]:
    """Lowercased names every source in the consumer's scope OTHER than
    ``parent`` exposes. Raw text renders unqualified, so a name in here is
    ambiguous -- or binds to the wrong table -- once ``parent`` is folded in.
    A datasource leaf contributes its physical columns as well as its CTE
    outputs, because a later pass may fold it in too."""
    names = {c.safe_address.lower() for c in cte.output_columns}
    sources: list[CTE | UnionCTE] = [
        x
        for x in [*cte.parent_ctes, *cte.inlined_parents]
        if x.safe_identifier != parent.safe_identifier
    ]
    for source in sources:
        names |= {c.safe_address.lower() for c in source.output_columns}
        if isinstance(source, DatasourceCTE):
            names |= {
                c.alias.lower()
                for c in source.datasource.columns
                if isinstance(c.alias, str)
            }
    names |= {
        join.alias.lower()
        for join in cte.joins
        if isinstance(join, InstantiatedUnnestJoin)
    }
    return names


def _raw_columns_inline_safely(
    cte: CTE, parent: DatasourceCTE, root: BuildDatasource, parent_count: int
) -> bool:
    """Verbatim raw() text is evaluated wherever it lands. As the consumer's
    sole source that is the datasource's own scope, and a consumer that never
    reads a raw-bound concept never renders the text at all. Beside another
    table the text is unqualified, so it is only sound when no other source in
    scope exposes a name the text reads (the binding's own scope guarantees the
    names are this datasource's columns; the fold is what puts them in company).
    Value is a second question: the text reads as if the datasource had a row
    wherever it lands, so it stays per-row correct only where every result row
    carries one -- the driving table of INNER/LEFT joins, or an INNER-joined
    table in a plan with no FULL/RIGHT join to manufacture rows without it."""
    if not root.has_raw_columns:
        return True
    if not cte.joins and parent_count <= 1:
        return True
    raw_text: dict[str, str] = {}
    for column in root.columns:
        if isinstance(column.alias, RawColumnExpr):
            for address in {column.concept.address} | column.concept.pseudonyms:
                raw_text[address] = column.alias.text
    consumed = render_cte_used_map(cte).get(parent.name, set()) | _join_key_demand(
        cte, parent.name
    )
    rendered = [raw_text[address] for address in consumed if address in raw_text]
    if not rendered:
        return True
    scope = _consumer_scope_names(cte, parent)
    for text in rendered:
        refs = _raw_text_column_refs(text)
        if refs is None or refs & scope:
            return False
    joins = [join for join in cte.joins if isinstance(join, Join)]
    if len(joins) != len(cte.joins) or any(
        join.jointype in (JoinType.FULL, JoinType.RIGHT_OUTER) for join in joins
    ):
        return False
    if cte.base_name == parent.name:
        return True
    return all(
        join.jointype == JoinType.INNER
        for join in joins
        if join.right_cte.name == parent.name
    )


def _can_inline_filtered_parent(
    cte: CTE,
    parent: DatasourceCTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> bool:
    if not parent.condition or not is_sole_consumer(cte, parent, inverse_map):
        return False
    return all(
        isinstance(join, Join) and join.jointype == JoinType.INNER for join in cte.joins
    )


def _rename_fold_plan(
    cte: CTE,
    parent: DatasourceCTE,
    missing: set[str],
    root_outputs: set[str],
) -> list[tuple[int, BuildConcept]] | None:
    """Plan to render consumer outputs that read a parent-scan rename (an
    address the raw datasource cannot supply) from lineage after the fold.

    Each qualifying output is a bare reference to a parent column that is
    itself a single-hop rename of a datasource column: pin the consumer's
    column to the rename's base object (see rebind_rename_to_consumed) and
    drop its source_map entry, so the merged CTE renders `<raw column> as
    <name>` exactly as the scan did. Returns None when any missing address is
    not such a rename; a derived expression needs re-derivation this fold
    cannot prove."""
    by_address: dict[str, tuple[int, BuildConcept]] = {}
    for i, col in enumerate(cte.output_columns):
        by_address.setdefault(col.address, (i, col))
    plan: list[tuple[int, BuildConcept]] = []
    for address in missing:
        # An existence subselect resolves through existence_source_map against
        # a named CTE; a lineage render can't satisfy it.
        if address in cte.existence_source_map:
            return None
        entry = by_address.get(address)
        if entry is None:
            return None
        i, col = entry
        consumed = consumed_parent_column(col, cte, parent)
        if consumed is None:
            return None
        base = rename_reference(consumed)
        if base is None or (
            base.address not in root_outputs and not (base.pseudonyms & root_outputs)
        ):
            return None
        plan.append((i, rebind_rename_to_consumed(col, base)))
    return plan


def _join_key_demand(cte: CTE, parent_name: str) -> set[str]:
    """Addresses the consumer renders from ``parent_name`` as a join key.

    Join legs resolve their column through ``CTEConceptPair.cte`` /
    ``Join.right_cte``, never through ``source_map``, so a key can be demanded
    from a parent the source_map does not attribute it to (the synthesized
    ``__preql_internal.all_rows`` broadcast constant is the common case)."""
    demand: set[str] = set()
    for join in cte.joins:
        if not isinstance(join, Join):
            continue
        for pair in join.joinkey_pairs or []:
            if pair.cte is not None and pair.cte.name == parent_name:
                demand.add(pair.left.address)
            if join.right_cte.name == parent_name:
                demand.add(pair.right.address)
    return demand


def _fold_plan(
    cte: CTE,
    parent: DatasourceCTE,
    base: BuildDatasource,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> tuple[set[str], list[tuple[int, BuildConcept]] | None]:
    """The join keys the raw scan cannot supply, and the rename-fold plan
    covering whatever else the consumer reads through this parent. An empty
    plan means nothing needs folding; None means no fold is provable.

    A merged key present as one datasource column also satisfies its pseudonym
    addresses (a fact FK covers the canonical dim key it was merged with); the
    base datasource only declares the native address, and the join resolver is
    pseudonym-aware. Gated to a single-consumer scan: inlining a scan shared by
    more than one consumer duplicates it into each, and a shared scan is
    cheaper kept as one CTE."""
    root_outputs = {x.address for x in base.output_concepts}
    if len(inverse_map.get(parent.name, [])) <= 1:
        for x in base.output_concepts:
            root_outputs |= x.pseudonyms
    join_demand = _join_key_demand(cte, parent.name) - root_outputs
    if join_demand:
        return join_demand, []
    inherited = {x for x, v in cte.source_map.items() if v and parent.name in v}
    if inherited.issubset(root_outputs):
        return set(), []
    # A source_map entry the consumer never renders from this parent is
    # metadata, not a requirement: derived concepts get attached to the scan
    # that could compute them while the consumer computes them from raw columns
    # itself. Hiding runs after this rule, so consult the rendered used-map.
    consumed = render_cte_used_map(cte).get(parent.name, set())
    missing = (inherited & consumed) - root_outputs
    if not missing:
        return set(), []
    return set(), _rename_fold_plan(cte, parent, missing, root_outputs)


class InlineDatasource(OptimizationRule):
    def __init__(self, raw_scope_only: bool = False):
        super().__init__()
        # The late phase exists only to retry scans whose raw() text needed
        # final join types to clear `_raw_columns_inline_safely`; every other
        # candidate settled in the initial phase and is left alone.
        self.raw_scope_only = raw_scope_only
        self.candidates: defaultdict[str, set[str]] = defaultdict(set)
        self.count: defaultdict[str, int] = defaultdict(int)

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            optimized = any(
                self.optimize(x, inverse_map=inverse_map)[0] for x in cte.internal_ctes
            )
            return optimized, None
        if isinstance(cte, RecursiveCTE):
            return False, None
        parents = cte.dependency_nodes()
        if not parents:
            return False, None

        self.debug(
            f"Checking {cte.name} for consolidating inline tables with {len(parents)} parents"
        )
        to_inline: list[DatasourceCTE] = []
        for parent_cte in parents:
            if isinstance(parent_cte, UnionCTE):
                continue
            if isinstance(parent_cte, RecursiveCTE):
                continue
            if not isinstance(parent_cte, DatasourceCTE):
                self.debug(
                    f"Cannot inline: parent {parent_cte.name} is not a DatasourceCTE"
                )
                continue
            if not parent_cte.is_root_datasource:
                self.debug(f"Cannot inline: parent {parent_cte.name} is not root")
                continue
            if parent_cte.dependency_nodes():
                self.debug(f"Cannot inline: parent {parent_cte.name} has parents")
                continue
            filtered_inline = _can_inline_filtered_parent(cte, parent_cte, inverse_map)
            if parent_cte.condition and not filtered_inline:
                self.debug(
                    f"Cannot inline: parent {parent_cte.name} has condition, cannot be inlined"
                )
                continue
            if parent_cte.group_to_grain:
                self.debug(f"Cannot inline: parent {parent_cte.name} is grouped")
                continue
            raw_root = parent_cte.source.base_datasource
            if not isinstance(raw_root, BuildDatasource):
                self.debug(f"Cannot inline: Parent {parent_cte.name} is not datasource")
                continue
            root: BuildDatasource = raw_root
            if self.raw_scope_only and not root.has_raw_columns:
                continue
            if not root.can_be_inlined:
                self.debug(
                    f"Cannot inline: Parent {parent_cte.name} datasource is not inlineable"
                )
                continue
            if not _raw_columns_inline_safely(cte, parent_cte, root, len(parents)):
                self.debug(
                    f"Cannot inline: Parent {parent_cte.name} has raw() columns "
                    "the consumer's joins would misattribute"
                )
                continue
            join_demand, plan = _fold_plan(cte, parent_cte, root, inverse_map)
            if join_demand:
                self.log(
                    f"Cannot inline: join keys {join_demand} read from "
                    f"{parent_cte.name} are not columns of the raw datasource"
                )
                continue
            if plan is None:
                self.log(
                    f"Cannot inline: Not all required inputs to {parent_cte.name} are found on datasource"
                )
                continue
            if not root.grain.issubset(parent_cte.grain):
                self.log(
                    f"Cannot inline: {parent_cte.name} is at wrong grain to inline ({root.grain} vs {parent_cte.grain})"
                )
                continue
            to_inline.append(parent_cte)

        # Register every candidate before inlining any, so the cutoff count
        # reflects all consumers of a raw source.
        registered = False
        for replaceable in to_inline:
            if replaceable.name not in self.candidates[cte.name]:
                self.candidates[cte.name].add(replaceable.name)
                self.count[replaceable.source.identifier] += 1
                registered = True
        if registered:
            return True, None
        optimized = False
        for replaceable in to_inline:
            if (
                self.count[replaceable.source.identifier]
                > CONFIG.optimizations.constant_inline_cutoff
            ):
                self.log(
                    f"Skipping inlining raw datasource {replaceable.source.identifier} ({replaceable.name}) due to multiple references"
                )
                continue
            replaceable_base = replaceable.source.base_datasource
            # Candidacy already established both, on the visit that registered it.
            assert isinstance(replaceable_base, BuildDatasource)
            # Recompute at apply time: candidacy was established on a prior
            # visit and other merges may have shifted this CTE's source_map.
            join_demand, plan = _fold_plan(
                cte, replaceable, replaceable_base, inverse_map
            )
            if join_demand:
                self.log(
                    f"Failed to inline {replaceable.name}: join keys {join_demand} "
                    "are not columns of the raw datasource"
                )
                continue
            if plan is None:
                self.log(
                    f"Failed to inline {replaceable.name}: rename fold no longer provable"
                )
                continue
            result = cte.inline_parent_datasource(replaceable, force_group=False)
            if result:
                for i, new_col in plan:
                    # Render the rename from lineage post-fold; a stale source_map
                    # entry would win over lineage and point at a column the raw
                    # table does not have.
                    cte.output_columns[i] = new_col
                    cte.source_map.pop(new_col.address, None)
                if replaceable.condition is not None:
                    cte.condition = append_condition(
                        cte.condition, replaceable.condition
                    )
                self.log(
                    f"Inlined parent {replaceable.name} with {replaceable.source.safe_identifier}"
                )
                optimized = True
            else:
                self.log(f"Failed to inline {replaceable.name}")
        return optimized, None
