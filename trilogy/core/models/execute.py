from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Set, Union

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    RECURSIVE_GATING_CONCEPT,
    MagicConstants,
    logger,
)
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionClass,
    FunctionType,
    JoinType,
    Modifier,
    Purpose,
    SetOperator,
    SourceType,
)
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    UnionOutputResolutionError,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildOrderBy,
    BuildParamaterizedConceptReference,
    BuildRowsetItem,
    DataType,
    LooseBuildConceptList,
    RawColumnExpr,
)
from trilogy.core.models.datasource import Address
from trilogy.core.utility import safe_quote
from trilogy.utility import string_to_hash, unique

LOGGER_PREFIX = "[MODELS_EXECUTE]"


def _datasource_column_for_concept(
    datasource: BuildDatasource,
    concept: BuildConcept,
) -> str | RawColumnExpr | BuildFunction | BuildAggregateWrapper | None:
    canonical_match = None
    for column in datasource.columns:
        if (
            column.concept == concept
            or column.concept.with_grain(concept.grain) == concept
            or concept.address in column.concept.pseudonyms
            or column.concept.address in concept.pseudonyms
        ):
            return column.alias
        if column.concept.canonical_address == concept.canonical_address:
            canonical_match = column.alias
    return canonical_match


@dataclass(frozen=True)
class SourceBinding:
    key: str
    node: "CTE | UnionCTE | None" = None
    datasource: "BuildDatasource | QueryDatasource | None" = None
    emitted: bool = True
    inlined: bool = False
    branch: bool = False


@dataclass
class CTE:
    name: str
    source: "QueryDatasource"
    output_columns: List[BuildConcept]
    source_map: Dict[str, list[str]]
    grain: BuildGrain
    base: bool = False
    group_to_grain: bool = False
    existence_source_map: Dict[str, list[str]] = field(default_factory=dict)
    parent_ctes: List[Union["CTE", "UnionCTE"]] = field(default_factory=list)
    joins: List[Union["Join", "InstantiatedUnnestJoin"]] = field(default_factory=list)
    condition: BoolExpr | None = None
    partial_concepts: List[BuildConcept] = field(default_factory=list)
    rollup_concepts: List[BuildConcept] = field(default_factory=list)
    nullable_concepts: List[BuildConcept] = field(default_factory=list)
    join_derived_concepts: List[BuildConcept] = field(default_factory=list)
    hidden_concepts: set[str] = field(default_factory=set)
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    base_name_override: Optional[Union["Address", str]] = None
    base_alias_override: Optional[str] = None
    # Datasource leaves folded into this consumer by inline-datasource.
    # Render-only: deliberately *not* in ``parent_ctes`` so optimizer rules
    # see the consumer exactly as if the datasource were scanned directly.
    inlined_parents: List["DatasourceCTE"] = field(default_factory=list)

    def __post_init__(self):
        if len(self.join_derived_concepts) > 1:
            raise NotImplementedError(
                "Multiple join derived concepts not yet supported."
            )
        self.join_derived_concepts = unique(self.join_derived_concepts, "address")
        self.output_columns = unique(self.output_columns, "address")

    @classmethod
    def from_datasource(cls, datasource: BuildDatasource) -> "DatasourceCTE":
        qds = QueryDatasource.from_datasource(datasource)
        return DatasourceCTE(
            name=datasource.name,
            source=qds,
            output_columns=qds.output_concepts,
            source_map={
                c.address: [datasource.safe_identifier] for c in qds.output_concepts
            },
            grain=datasource.grain,
            partial_concepts=datasource.partial_concepts,
            nullable_concepts=datasource.nullable_concepts,
            hidden_concepts={c.address for c in datasource.hidden_concepts},
            base_alias_override=datasource.safe_identifier,
            datasource=datasource,
        )

    @property
    def identifier(self):
        return self.name

    @property
    def safe_identifier(self):
        return self.name

    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_columns)

    @property
    def comment(self) -> str:
        base = f"Target: {str(self.grain)}. Group: {self.group_to_grain}"
        base += f" Source: {self.source.source_type}. Grains: {[str(ds.grain) for ds in self.parent_ctes]}"
        if self.parent_ctes:
            base += f" References: {', '.join([x.name for x in self.parent_ctes])}."
        if self.joins and CONFIG.comments.joins:
            base += f"\n-- Joins: {', '.join([str(x) for x in self.joins])}."
        if self.partial_concepts and CONFIG.comments.partial:
            base += (
                f"\n-- Partials: {', '.join([str(x) for x in self.partial_concepts])}."
            )
        if self.rollup_concepts:
            base += (
                f"\n-- Rollups: {', '.join([str(x) for x in self.rollup_concepts])}."
            )
        if CONFIG.comments.source_map:
            base += f"\n-- Source Map: {self.source_map}."
        base += f"\n-- Output: {', '.join([str(x) for x in self.output_columns])}."
        if self.source.input_concepts:
            base += f"\n-- Inputs: {', '.join([str(x) for x in self.source.input_concepts])}."
        if self.hidden_concepts:
            base += f"\n-- Hidden: {', '.join([str(x) for x in self.hidden_concepts])}."
        if self.nullable_concepts and CONFIG.comments.nullable:
            base += (
                f"\n-- Nullable: {', '.join([str(x) for x in self.nullable_concepts])}."
            )
        base += "\n"
        return base

    def inline_parent_datasource(
        self, parent: "CTE", force_group: bool = False
    ) -> bool:
        """Fold a single-datasource parent into this consumer.

        Structurally identical to the historical inline (so every downstream
        optimizer rule sees the consumer exactly as before — predicate
        placement, ``is_root_datasource``, grouping all unchanged): the parent
        QDS is replaced by its ``BuildDatasource`` in ``source.datasources``,
        ``base_datasource``/source maps are repointed, and the parent leaves
        ``parent_ctes``. The *only* change is representation: instead of an
        ``InlinedCTE`` record + per-join ``inline_cte`` maps + render-site
        branching, the folded leaf (a ``DatasourceCTE``) is parked on
        ``inlined_parents`` and rendered from the raw datasource in that
        consumer's scope.
        """
        if not isinstance(parent, DatasourceCTE):
            return False
        qds_being_inlined = parent.source
        ds_being_inlined = parent.datasource
        existing_aliases = {
            x.safe_identifier
            for x in self.source.datasources
            if x is not qds_being_inlined
        }
        inline_datasource = ds_being_inlined
        if ds_being_inlined.safe_identifier in existing_aliases:
            inline_datasource = replace(
                ds_being_inlined, name=parent.name, namespace=DEFAULT_NAMESPACE
            )
        self.source.datasources = sorted(
            [
                inline_datasource,
                *[
                    x
                    for x in self.source.datasources
                    if x is not qds_being_inlined
                    and x.safe_identifier != inline_datasource.safe_identifier
                ],
            ],
            key=lambda ds: ds.identifier,
        )
        if (
            self.source.base_datasource is not None
            and self.source.base_datasource.safe_identifier
            == qds_being_inlined.safe_identifier
        ):
            self.source.base_datasource = inline_datasource
        if self.base_name == parent.name:
            self.base_name_override = inline_datasource.safe_location
            self.base_alias_override = inline_datasource.safe_identifier
        for k, v in self.source_map.items():
            self.source_map[k] = [
                inline_datasource.safe_identifier if x == parent.safe_identifier else x
                for x in v
            ]
        for k, v in self.existence_source_map.items():
            self.existence_source_map[k] = [
                inline_datasource.safe_identifier if x == parent.safe_identifier else x
                for x in v
            ]
        for k in inline_datasource.output_lcl.addresses:
            if k in self.source_map and self.source_map[k]:
                continue
            self.source_map[k] = [inline_datasource.safe_identifier]
        self.parent_ctes = [
            x for x in self.parent_ctes if x.safe_identifier != parent.safe_identifier
        ]
        if force_group:
            self.group_to_grain = True
        rendered_parent = parent
        if inline_datasource is not ds_being_inlined:
            rendered_parent = replace(parent, datasource=inline_datasource)
        self.add_inlined_datasource(rendered_parent)
        return True

    def __add__(self, other: "CTE" | "UnionCTE"):
        if isinstance(other, UnionCTE):
            raise ValueError("cannot merge CTE and union CTE")
        logger.info('Merging two copies of CTE "%s"', self.name)
        if not self.grain == other.grain:
            error = (
                "Attempting to merge two ctes of different grains"
                f" {self.name} {other.name} grains {self.grain} {other.grain}| {self.group_to_grain} {other.group_to_grain}| {self.output_lcl} {other.output_lcl}"
            )
            raise ValueError(error)
        if not self.condition == other.condition:
            error = (
                "Attempting to merge two ctes with different conditions"
                f" {self.name} {other.name} conditions {self.condition} {other.condition}"
            )
            raise ValueError(error)
        mutually_hidden = set()
        for concept in self.hidden_concepts:
            if concept in other.hidden_concepts:
                mutually_hidden.add(concept)
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.parent_ctes = merge_ctes(self.parent_ctes + other.parent_ctes)
        self.inlined_parents = unique(
            self.inlined_parents + other.inlined_parents, "name"
        )

        self.source_map = {**self.source_map, **other.source_map}

        self.output_columns = unique(
            self.output_columns + other.output_columns, "address"
        )
        self.joins = unique(self.joins + other.joins, "unique_id")
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.rollup_concepts = unique(
            self.rollup_concepts + other.rollup_concepts, "address"
        )
        self.join_derived_concepts = unique(
            self.join_derived_concepts + other.join_derived_concepts, "address"
        )

        self.source.source_map = {**self.source.source_map, **other.source.source_map}
        self.source.output_concepts = unique(
            self.source.output_concepts + other.source.output_concepts, "address"
        )
        self.source.rollup_concepts = unique(
            self.source.rollup_concepts + other.source.rollup_concepts, "address"
        )
        self.nullable_concepts = unique(
            self.nullable_concepts + other.nullable_concepts, "address"
        )
        self.hidden_concepts = mutually_hidden
        self.existence_source_map = {
            **self.existence_source_map,
            **other.existence_source_map,
        }
        # copies of one logical CTE carry the same limit; keep it
        if self.limit is None:
            self.limit = other.limit

        return self

    @property
    def relevant_base_ctes(self):
        return self.parent_ctes

    @property
    def is_root_datasource(self) -> bool:
        base = self.source.base_datasource
        return isinstance(base, BuildDatasource) and base.name != CONSTANT_DATASET

    @property
    def source_address(self) -> Union["Address", str]:
        if self.base_name_override:
            return self.base_name_override
        base = self.source.base_datasource
        if isinstance(base, BuildDatasource) and base.name != CONSTANT_DATASET:
            if isinstance(base.address, Address):
                return base.address
            return base.safe_location
        elif len(self.source.datasources) == 1 and len(self.parent_ctes) == 1:
            return self.parent_ctes[0].name
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        return self.source.name

    @property
    def base_name(self) -> str:
        addr = self.source_address
        if isinstance(addr, Address):
            return addr.location
        return addr

    @property
    def quote_address(self) -> bool:
        base = self.source.base_datasource
        if isinstance(base, BuildDatasource) and base.name != CONSTANT_DATASET:
            if isinstance(base.address, Address):
                return not base.address.is_query
            return True
        if not self.source.datasources:
            return False
        # No explicit base, but datasources is non-empty — preserve historical
        # behavior of consulting the first listed datasource for quoting.
        first = self.source.datasources[0]
        if isinstance(first, BuildDatasource):
            if isinstance(first.address, Address):
                return not first.address.is_query
            return True
        return True

    @property
    def base_alias(self) -> str:
        if self.base_alias_override:
            return self.base_alias_override
        base = self.source.base_datasource
        if isinstance(base, BuildDatasource) and base.name != CONSTANT_DATASET:
            return base.identifier
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        elif self.parent_ctes:
            return self.parent_ctes[0].name
        return self.name

    def get_concept(self, address: str) -> BuildConcept | None:
        for cte in self.parent_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                if match:
                    return match

        for array in [self.source.input_concepts, self.source.output_concepts]:
            match_list = [x for x in array if x.address == address]
            if match_list:
                return match_list.pop()
        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    def outer_join_key_class(self, address: str) -> list[BuildConcept]:
        """Transitive equivalence class of ``address`` under this CTE's
        outer-join keys (LEFT/RIGHT/FULL). More than one member means no
        single side is guaranteed non-null, so the key must render null-safely
        (coalesced across members)."""
        adjacent: dict[str, set[str]] = defaultdict(set)
        for join in self.joins:
            if not isinstance(join, Join) or join.jointype not in (
                JoinType.LEFT_OUTER,
                JoinType.RIGHT_OUTER,
                JoinType.FULL,
            ):
                continue
            for pair in join.joinkey_pairs or []:
                left = pair.left.address
                right = pair.right.address
                if left == right:
                    continue
                adjacent[left].add(right)
                adjacent[right].add(left)
        if address not in adjacent:
            return []
        pending = [address]
        seen: set[str] = set()
        concepts: list[BuildConcept] = []
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            concept = self.get_concept(current)
            if concept is not None:
                concepts.append(concept)
            pending.extend(adjacent[current] - seen)
        return concepts

    def get_alias(
        self, concept: BuildConcept, source: str | None = None
    ) -> str | RawColumnExpr | BuildFunction | BuildAggregateWrapper:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
                if source and source != cte.name:
                    continue
                # A normal parent (incl. a materialized DatasourceCTE) is
                # referenced as ``name.safe_address``. Inlined datasources are
                # not in parent_ctes — handled by the fallback below.
                return concept.safe_address

        # A derived-key FULL join coalesces the canonical key across both sides;
        # the null-extendable side outputs it under a pseudonym column (da for
        # db), so render that side's own column. Walk the pseudonym closure so a
        # transitively-equivalent key resolves (e.g. an OUTER merge key whose only
        # pseudonym is a binding key that is itself a pseudonym of the rendered
        # join-key alias), and prefer a NON-hidden member: a hidden column is
        # absent from the source's SELECT, so referencing it is invalid SQL.
        for cte in self.parent_ctes:
            if source and source != cte.name:
                continue
            by_address = {col.address: col for col in cte.output_columns}
            # Pseudonym sets on chained merged keys (`a=b=c`) are pairwise, not
            # closed: a and c may relate only through an ABSENT intermediate b
            # (both list b as a pseudonym), so walk undirected pseudonym edges
            # rather than requiring direct adjacency to a present column.
            edges: dict[str, set[str]] = defaultdict(set)
            for col in cte.output_columns:
                for pseudonym in col.pseudonyms:
                    edges[col.address].add(pseudonym)
                    edges[pseudonym].add(col.address)
            for pseudonym in concept.pseudonyms:
                edges[concept.address].add(pseudonym)
                edges[pseudonym].add(concept.address)
            seen: set[str] = set()
            frontier = [concept.address]
            hidden_match: str | None = None
            while frontier:
                addr = frontier.pop()
                if addr in seen:
                    continue
                seen.add(addr)
                # sorted: several equivalent members can match, and the first
                # non-hidden one wins — unordered traversal makes the rendered
                # column vary run to run.
                frontier.extend(sorted(edges.get(addr, set()) - seen))
                match = by_address.get(addr)
                if match is None or addr == concept.address:
                    continue
                if addr not in cte.hidden_concepts:
                    return match.safe_address
                hidden_match = match.safe_address
            if hidden_match is not None:
                return hidden_match

        # An inlined datasource exposes *all* its raw columns to the consumer,
        # not just the leaf's pruned projection (e.g. ``_raw_name`` backing a
        # derived ``name``). Resolve those through the underlying datasource.
        inlined = self.inlined_parent_providing(concept)
        if inlined is not None and (source is None or source == inlined.name):
            return inlined.consumer_column(concept)

        try:
            return self.source.get_alias(concept, source=source)
        except ValueError as e:
            return f"INVALID_ALIAS: {str(e)}"

    @property
    def group_concepts(self) -> List[BuildConcept]:
        from trilogy.core.processing.condition_utility import condition_implies

        rollup_addresses = {c.address for c in self.rollup_concepts}

        def has_local_aggregate(c: BuildConcept) -> bool:
            # does the rendered column value of `c` contain an aggregate computed
            # in this CTE? A concept sourced from a parent is a plain column here,
            # so any aggregate in its lineage was already materialized upstream.
            if len(self.source_map.get(c.address, [])) > 0:
                return False
            if c.derivation == Derivation.AGGREGATE:
                return True
            # a rowset output renders as its underlying content here; mirror
            # check_is_not_in_group and recurse so a rowset aggregate is still
            # recognized as aggregate-bearing (e.g. inside a `agg / dim` ratio).
            if c.derivation == Derivation.ROWSET and isinstance(
                c.lineage, BuildRowsetItem
            ):
                return has_local_aggregate(c.lineage.content)
            if (
                c.purpose == Purpose.METRIC
                and isinstance(c.lineage, BuildFunction)
                and c.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            ):
                return True
            # the filtered content renders as the value, but when the per-row
            # CASE WHEN is emitted (not elided) the where condition becomes part
            # of the column expression too -- so a local aggregate in the
            # predicate (e.g. a HAVING `sum(x) > threshold` lowered to a
            # `_virt_filter` existence column) makes the column aggregate-bearing
            # and unfit for GROUP BY. When the CTE's own condition implies the
            # filter predicate the renderer elides the CASE to bare content
            # (base.py) -- the where aggregate is then NOT in the column and the
            # filter follows its content, so only widen for the un-elided case.
            if c.derivation == Derivation.FILTER and isinstance(
                c.lineage, BuildFilterItem
            ):
                if any(
                    has_local_aggregate(x) for x in c.lineage.content_concept_arguments
                ):
                    return True
                where_cond = c.lineage.where.conditional
                elided = self.condition is not None and (
                    self.condition == where_cond
                    or condition_implies(self.condition, where_cond)
                )
                return not elided and any(
                    has_local_aggregate(x)
                    for x in c.lineage.where.rendered_concept_arguments
                )
            if c.derivation == Derivation.BASIC and c.lineage:
                return any(
                    has_local_aggregate(x) for x in c.lineage.rendered_concept_arguments
                )
            return False

        def check_is_not_in_group(c: BuildConcept):
            if c.address in rollup_addresses:
                return True
            if len(self.source_map.get(c.address, [])) > 0:
                return False
            if c.derivation == Derivation.ROWSET:
                assert isinstance(c.lineage, BuildRowsetItem)
                return check_is_not_in_group(c.lineage.content)

            # An aligned multiselect column resolves, in this arm CTE, to the
            # underlying per-arm concept (e.g. `lc` -> `cnt1`). It inherits that
            # concept's group-ness: aligning an aggregate must NOT add it to the
            # GROUP BY (DuckDB rejects aggregates in GROUP BY); aligning a
            # dimension keeps it a group key. Derive items are computed at the
            # merge grain — never a group key here.
            if c.derivation == Derivation.MULTISELECT:
                assert isinstance(c.lineage, BuildMultiSelectLineage)
                if c.address in c.lineage.calculated_derivations:
                    return True
                try:
                    return check_is_not_in_group(c.lineage.find_source(c, self))
                except UnionOutputResolutionError:
                    return False

            if c.derivation == Derivation.CONSTANT:
                return True
            if (
                c.purpose == Purpose.CONSTANT
                and isinstance(c.lineage, BuildFunction)
                and c.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            ):
                return True

            if (
                c.derivation
                in (Derivation.BASIC, Derivation.FILTER, Derivation.GROUP_TO)
                and c.lineage
            ):
                # A GROUP_TO (`group(agg, key)`) renders as its inner aggregate
                # value, so it follows that aggregate — never a group key here.
                if (
                    isinstance(c.lineage, BuildFunction)
                    and c.lineage.operator == FunctionType.GROUP
                ):
                    return check_is_not_in_group(c.lineage.concept_arguments[0])
                if all(
                    [
                        check_is_not_in_group(x)
                        for x in c.lineage.rendered_concept_arguments
                    ]
                ):
                    return True
                # a basic expression mixing a group key with a locally-computed
                # aggregate (e.g. a ratio `a / sum(x)`) contains an aggregate and
                # cannot appear in GROUP BY
                if has_local_aggregate(c):
                    return True
                return False
            if c.purpose == Purpose.METRIC:
                return True

            return False

        return (
            unique(
                [
                    c
                    for c in self.output_columns
                    if not check_is_not_in_group(c)
                    and not (
                        c.address in self.hidden_concepts
                        and c.derivation == Derivation.MULTISELECT
                    )
                ],
                "address",
            )
            if self.group_to_grain
            else []
        )

    @property
    def render_from_clause(self) -> bool:
        if (
            all([c.derivation == Derivation.CONSTANT for c in self.output_columns])
            and not self.parent_ctes
            and not self.group_to_grain
        ):
            return False
        # if we don't need to source any concepts from anywhere
        # render without from
        # most likely to happen from inlining constants
        if not any([v for v in self.source_map.values()]):
            return False
        base = self.source.base_datasource
        if isinstance(base, BuildDatasource) and base.name == CONSTANT_DATASET:
            return False
        return True

    @property
    def sourced_concepts(self) -> List[BuildConcept]:
        return [c for c in self.output_columns if c.address in self.source_map]

    @property
    def inlined_alias_map(self) -> dict[str, str]:
        """Map inlined CTE names to the raw table aliases this consumer emits."""
        used: set[str] = {p.name for p in self.parent_ctes}
        mapping: dict[str, str] = {}
        for p in sorted(self.inlined_parents, key=lambda x: x.name):
            token = p.datasource.safe_identifier
            if token in used:
                token = p.name
            used.add(token)
            mapping[p.name] = token
        return mapping

    def resolve_render_alias(self, source: str) -> str:
        """Translate a source_map token into the SQL alias to emit."""
        return self.inlined_alias_map.get(source, source)

    def source_bindings(self, include_inlined: bool = True) -> list[SourceBinding]:
        bindings = [
            SourceBinding(
                key=parent.name,
                node=parent,
                datasource=(
                    parent.datasource
                    if isinstance(parent, DatasourceCTE)
                    else parent.source
                ),
                emitted=True,
            )
            for parent in self.parent_ctes
        ]
        if include_inlined:
            bindings.extend(
                SourceBinding(
                    key=self.source_key_for(parent),
                    node=parent,
                    datasource=parent.datasource,
                    emitted=False,
                    inlined=True,
                )
                for parent in self.inlined_parents
            )
        return bindings

    def source_key_for(
        self,
        source: str | "CTE" | "UnionCTE" | BuildDatasource | "QueryDatasource",
    ) -> str:
        if isinstance(source, str):
            return self.resolve_render_alias(source)
        if isinstance(source, DatasourceCTE) and self.renders_inline(source):
            return self.resolve_render_alias(source.name)
        if isinstance(source, (CTE, UnionCTE)):
            return source.name
        if isinstance(source, BuildDatasource):
            for binding in self.source_bindings(include_inlined=True):
                if binding.datasource is source:
                    return binding.key
                if (
                    binding.datasource is not None
                    and binding.datasource.identifier == source.identifier
                ):
                    return binding.key
            return source.safe_identifier
        for binding in self.source_bindings(include_inlined=True):
            if binding.datasource is source:
                return binding.key
        return source.safe_identifier

    def dependency_nodes(
        self,
        include_inlined: bool = False,
    ) -> list["CTE | UnionCTE"]:
        return [
            binding.node
            for binding in self.source_bindings(include_inlined=include_inlined)
            if binding.node is not None
        ]

    def add_dependency(self, parent: "CTE | UnionCTE") -> None:
        self.parent_ctes = unique(self.parent_ctes + [parent], "name")

    def add_inlined_datasource(self, parent: "DatasourceCTE") -> str:
        existing = {p.name for p in self.inlined_parents}
        if parent.name not in existing:
            self.inlined_parents.append(parent)
        return self.source_key_for(parent)

    def replace_dependency(self, old: "CTE", new: "CTE | UnionCTE") -> None:
        self.parent_ctes = [
            new if x.safe_identifier == old.safe_identifier else x
            for x in self.parent_ctes
        ]
        if isinstance(new, DatasourceCTE):
            self.inlined_parents = [
                new if x.safe_identifier == old.safe_identifier else x
                for x in self.inlined_parents
            ]
        else:
            self.inlined_parents = [
                x
                for x in self.inlined_parents
                if x.safe_identifier != old.safe_identifier
            ]
        old_keys = {old.safe_identifier, old.name}
        if isinstance(old, DatasourceCTE):
            old_keys.add(old.datasource.safe_identifier)
        new_key = self.source_key_for(new)
        for k, v in self.source_map.items():
            if isinstance(v, list):
                self.source_map[k] = [new_key if x in old_keys else x for x in v]
        for k, v in self.existence_source_map.items():
            if isinstance(v, list):
                self.existence_source_map[k] = [
                    new_key if x in old_keys else x for x in v
                ]
        if self.base_alias_override == old.safe_identifier:
            self.base_alias_override = new.safe_identifier
        if self.base_name_override == old.safe_identifier:
            self.base_name_override = new.safe_identifier
        for join in self.joins:
            if not isinstance(join, Join):
                continue
            if join.left_cte and join.left_cte.safe_identifier == old.safe_identifier:
                join.left_cte = new
            if join.joinkey_pairs:
                for pair in join.joinkey_pairs:
                    if pair.cte and pair.cte.safe_identifier == old.safe_identifier:
                        pair.cte = new
            if join.right_cte.safe_identifier == old.safe_identifier:
                join.right_cte = new

    def inlined_parent_for_source(self, source: str) -> "DatasourceCTE | None":
        for p in self.inlined_parents:
            if source in {p.name, p.datasource.safe_identifier}:
                return p
        return None

    def inlined_parent_providing(self, concept: BuildConcept) -> "DatasourceCTE | None":
        """An inlined datasource exposing ``concept`` as a raw column."""
        for p in self.inlined_parents:
            if _datasource_column_for_concept(p.datasource, concept) is not None:
                return p
        return None

    def renders_inline(self, node: "CTE | UnionCTE") -> bool:
        """Whether this consumer renders ``node`` as a folded raw datasource."""
        return self.inlined_parent_for_source(node.name) is not None

    def column_for(
        self, node: "CTE | UnionCTE", concept: BuildConcept
    ) -> str | RawColumnExpr | BuildFunction | BuildAggregateWrapper:
        """Column this consumer should read from ``node``."""
        if isinstance(node, DatasourceCTE) and self.renders_inline(node):
            return node.consumer_column(concept)
        return concept.safe_address


@dataclass
class BaseConceptPair:
    left: BuildConcept
    right: BuildConcept
    existing_datasource: Union[BuildDatasource, "QueryDatasource"]


@dataclass
class ConceptPair(BaseConceptPair):
    modifiers: List[Modifier] = field(default_factory=list)

    @property
    def is_partial(self):
        return Modifier.PARTIAL in self.modifiers

    @property
    def is_nullable(self):
        return Modifier.NULLABLE in self.modifiers


@dataclass
class CTEConceptPair(BaseConceptPair):
    cte: CTE | UnionCTE
    modifiers: List[Modifier] = field(default_factory=list)

    @property
    def is_partial(self):
        return Modifier.PARTIAL in self.modifiers

    @property
    def is_nullable(self):
        return Modifier.NULLABLE in self.modifiers


@dataclass
class InstantiatedUnnestJoin:
    object_to_unnest: BuildConcept | BuildParamaterizedConceptReference | BuildFunction
    alias: str = "unnest"


@dataclass
class UnnestJoin:
    concepts: list[BuildConcept]
    parent: BuildFunction
    alias: str = "unnest"
    rendering_required: bool = True

    def __hash__(self):
        return self.safe_identifier.__hash__()

    def __eq__(self, other):
        if type(other) is not UnnestJoin:
            return NotImplemented
        return self.safe_identifier == other.safe_identifier

    @property
    def safe_identifier(self) -> str:
        return self.alias + "".join([str(s.address) for s in self.concepts])


def raise_helpful_join_validation_error(
    concepts: List[BuildConcept],
    left_datasource: BuildDatasource | QueryDatasource | None,
    right_datasource: BuildDatasource | QueryDatasource | None,
):

    if not left_datasource or not right_datasource:
        raise InvalidSyntaxException(
            "No mutual keys found, and not two valid datasources"
        )
    left_keys = [c.address for c in left_datasource.output_concepts]
    right_keys = [c.address for c in right_datasource.output_concepts]
    match_concepts = [c.address for c in concepts]
    assert left_datasource
    assert right_datasource
    raise InvalidSyntaxException(
        "No mutual join keys found between"
        f" {left_datasource.identifier} and"
        f" {right_datasource.identifier}, left_keys {left_keys},"
        f" right_keys {right_keys},"
        f" provided join concepts {match_concepts}"
    )


@dataclass
class BaseJoin:
    right_datasource: Union[BuildDatasource, "QueryDatasource"]
    join_type: JoinType
    concepts: Optional[List[BuildConcept]] = None
    left_datasource: Optional[Union[BuildDatasource, "QueryDatasource"]] = None
    concept_pairs: list[ConceptPair] | None = None
    modifiers: List[Modifier] = field(default_factory=list)

    def __post_init__(self):
        if (
            self.left_datasource
            and self.left_datasource.identifier == self.right_datasource.identifier
        ):
            raise SyntaxError(
                f"Cannot join a dataself to itself, joining {self.left_datasource} and"
                f" {self.right_datasource}"
            )
        if self.concept_pairs or self.concepts == []:
            return

        final_concepts = []
        for concept in self.concepts or []:
            include = True
            for ds in [self.left_datasource, self.right_datasource]:
                synonyms = []
                if not ds:
                    continue
                for c in ds.output_concepts:
                    synonyms += list(c.pseudonyms)
                if (
                    concept.address not in ds.output_concepts
                    and concept.address not in synonyms
                ):
                    raise InvalidSyntaxException(
                        f"Invalid join, missing {concept} on {ds.name}, have"
                        f" {[c.address for c in ds.output_concepts]}"
                    )
            if include:
                final_concepts.append(concept)

        if not final_concepts and self.concepts:
            raise_helpful_join_validation_error(
                self.concepts,
                self.left_datasource,
                self.right_datasource,
            )

        self.concepts = final_concepts

    @property
    def unique_id(self) -> str:
        # Order-independent: SQL renderer AND-joins keys after sorting, so two
        # BaseJoins with the same pairs in different order produce identical
        # SQL. Dedupe on the sorted form so set-iteration nondeterminism in
        # upstream pair construction can't slip a duplicate past `unique()`.
        if self.concept_pairs:
            pair_keys = sorted(
                f"{p.existing_datasource.name}.{p.left}={p.right}"
                for p in self.concept_pairs
            )
            return f"{self.join_type.value} {self.right_datasource.name} on {','.join(pair_keys)}"
        return str(self)

    @property
    def input_concepts(self) -> List[BuildConcept]:
        base = []
        if self.concept_pairs:
            for pair in self.concept_pairs:
                base += [pair.left, pair.right]
        elif self.concepts:
            base += self.concepts
        return base

    def __str__(self):
        if self.concept_pairs:
            return (
                f"{self.join_type.value} {self.right_datasource.name} on"
                f" {','.join([str(k.existing_datasource.name) + '.'+ str(k.left)+'='+str(k.right) for k in self.concept_pairs])}"
            )
        return (
            f"{self.join_type.value} {self.right_datasource.name} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )


@dataclass
class QueryDatasource:
    input_concepts: List[BuildConcept]
    output_concepts: List[BuildConcept]
    datasources: List[Union[BuildDatasource, "QueryDatasource"]]
    source_map: Dict[str, Set[Union[BuildDatasource, "QueryDatasource", "UnnestJoin"]]]
    grain: BuildGrain
    joins: List[BaseJoin | UnnestJoin]
    limit: Optional[int] = None
    condition: BoolExpr | None = None
    source_type: SourceType = field(default=SourceType.SELECT)
    # Row combinator when source_type == UNION. EXCEPT/INTERSECT are
    # non-commutative set operators: their arm order is semantic, so
    # __post_init__ must not re-sort datasources for them.
    set_operator: SetOperator = field(default=SetOperator.UNION_ALL)
    partial_concepts: List[BuildConcept] = field(default_factory=list)
    rollup_concepts: List[BuildConcept] = field(default_factory=list)
    hidden_concepts: set[str] = field(default_factory=set)
    nullable_concepts: List[BuildConcept] = field(default_factory=list)
    join_derived_concepts: List[BuildConcept] = field(default_factory=list)
    force_group: bool | None = None
    existence_source_map: Dict[str, Set[Union[BuildDatasource, "QueryDatasource"]]] = (
        field(default_factory=dict)
    )
    ordering: BuildOrderBy | None = None
    # Explicit FROM-clause source. Set when this QDS represents a SELECT with a
    # single canonical base (single BuildDatasource, constant placeholder, or a
    # parent QDS being lifted up). Left as None for joins/merges/unions where
    # no single source is "the base".
    base_datasource: Optional[Union[BuildDatasource, "QueryDatasource"]] = None

    def __post_init__(self) -> None:
        if self.set_operator is SetOperator.UNION_ALL:
            self.datasources = sorted(self.datasources, key=lambda ds: ds.identifier)
        unique_pairs: set[str] = set()
        for join in self.joins:
            if not isinstance(join, BaseJoin):
                continue
            pairing = join.unique_id
            if pairing in unique_pairs:
                raise SyntaxError(f"Duplicate join {str(join)}")
            unique_pairs.add(pairing)
        self.input_concepts = unique(self.input_concepts, "address")
        self.output_concepts = unique(self.output_concepts, "address")
        # A base datasource surfaces nullable columns via ``nullable_concepts``;
        # a derived output (e.g. a CASE with no ELSE) has no column, so its
        # intrinsic nullability lives only on its own modifiers. Fold that in
        # here so join planning and null-safe rendering see it uniformly.
        intrinsic_nullable = [c for c in self.output_concepts if c.is_nullable]
        if intrinsic_nullable:
            self.nullable_concepts = unique(
                self.nullable_concepts + intrinsic_nullable, "address"
            )
        if CONFIG.validate_missing:
            all_concepts = self.input_concepts + self.output_concepts
            mapped_canonical = {
                c.canonical_address
                for c in all_concepts
                if c.address in self.source_map
            }
            mapped_pseudonyms: set[str] = set()
            for c in all_concepts:
                if c.address in self.source_map:
                    mapped_pseudonyms.update(c.pseudonyms)
            for concept in all_concepts:
                if concept.canonical_address in mapped_canonical:
                    continue
                if concept.address in self.hidden_concepts:
                    continue
                if concept.address in self.source_map:
                    continue
                if any(x in self.source_map for x in concept.pseudonyms):
                    continue
                if concept.address in mapped_pseudonyms:
                    continue
                raise SyntaxError(
                    f"Missing source map entry for {concept.address} with pseudonyms {concept.pseudonyms}, have map: {self.source_map}"
                )

    def __setattr__(self, name: str, value: object) -> None:
        # ``identifier`` is an expensive recursive property memoized in
        # ``_identifier_cache``. Optimization passes mutate QDS fields after
        # construction (datasources/condition/source_type/...), so drop the
        # cache on any field write to keep the memo correct.
        if name != "_identifier_cache":
            self.__dict__["_identifier_cache"] = None
        object.__setattr__(self, name, value)

    def __repr__(self):
        return f"{self.identifier}@<{self.grain}>"

    @classmethod
    def from_datasource(cls, datasource: BuildDatasource) -> "QueryDatasource":
        output_concepts = datasource.output_concepts
        return cls(
            input_concepts=output_concepts,
            output_concepts=output_concepts,
            datasources=[datasource],
            source_map={c.address: {datasource} for c in output_concepts},
            grain=datasource.grain,
            joins=[],
            partial_concepts=datasource.partial_concepts,
            rollup_concepts=[],
            hidden_concepts={c.address for c in datasource.hidden_concepts},
            nullable_concepts=datasource.nullable_concepts,
            base_datasource=datasource,
        )

    @property
    def effective_grain(self) -> BuildGrain:
        key_outputs = {
            concept.address
            for concept in self.output_concepts
            if concept.purpose == Purpose.KEY
            and concept.name != RECURSIVE_GATING_CONCEPT
        }
        return self.grain + BuildGrain(components=key_outputs)

    @property
    def safe_identifier(self):
        return self.identifier.replace(".", "_")

    @property
    def full_concepts(self) -> set[str]:
        return {
            c.address
            for c in self.output_concepts
            if c.address not in {z.address for z in self.partial_concepts}
        }

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return (self.identifier).__hash__()

    def __eq__(self, other):
        if type(other) is not QueryDatasource:
            return NotImplemented
        return self.identifier == other.identifier

    @property
    def name(self):
        return self.identifier

    @property
    def group_required(self) -> bool:
        if self.force_group is True:
            return True
        if self.force_group is False:
            return False
        if self.source_type:
            if self.source_type in [
                SourceType.FILTER,
            ]:
                return False
            elif self.source_type in [
                SourceType.GROUP,
            ]:
                return True
        return False

    def __add__(self, other) -> "QueryDatasource":
        # these are syntax errors to avoid being caught by current
        if not isinstance(other, QueryDatasource):
            raise SyntaxError("Can only merge two query datasources")
        if not other.grain == self.grain:
            raise SyntaxError(
                "Can only merge two query datasources with identical grain"
            )
        if not self.group_required == other.group_required:
            raise SyntaxError(
                "can only merge two datasources if the group required flag is the same"
            )
        if not self.join_derived_concepts == other.join_derived_concepts:
            raise SyntaxError(
                "can only merge two datasources if the join derived concepts are the same"
            )
        logger.debug(
            f"[Query Datasource] merging {self.name} with"
            f" {[c.address for c in self.output_concepts]} concepts and"
            f" {other.name} with {[c.address for c in other.output_concepts]} concepts"
        )

        merged_datasources: dict[str, Union[BuildDatasource, "QueryDatasource"]] = {}

        for ds in [*self.datasources, *other.datasources]:
            if ds.safe_identifier in merged_datasources:
                merged_datasources[ds.safe_identifier] = (
                    merged_datasources[ds.safe_identifier] + ds
                )
            else:
                merged_datasources[ds.safe_identifier] = ds

        final_source_map: defaultdict[
            str, Set[Union[BuildDatasource, QueryDatasource, UnnestJoin]]
        ] = defaultdict(set)
        final_existence_source_map: defaultdict[
            str, Set[Union[BuildDatasource, QueryDatasource]]
        ] = defaultdict(set)

        # add our sources
        for key in self.source_map:
            final_source_map[key] = self.source_map[key].union(
                other.source_map.get(key, set())
            )
        # add their sources
        for key in other.source_map:
            if key not in final_source_map:
                final_source_map[key] = other.source_map[key]
        for key in self.existence_source_map:
            final_existence_source_map[key] = self.existence_source_map[key].union(
                other.existence_source_map.get(key, set())
            )
        for key in other.existence_source_map:
            if key not in final_existence_source_map:
                final_existence_source_map[key] = other.existence_source_map[key]

        # if a ds was merged (to combine columns), we need to update the source map
        # to use the merged item
        for k, v in final_source_map.items():
            final_source_map[k] = set(
                merged_datasources.get(x.safe_identifier, x) for x in list(v)
            )
        for ex_key, ex_sources in final_existence_source_map.items():
            updated_existence_sources: Set[Union[BuildDatasource, QueryDatasource]] = (
                set()
            )
            for source in ex_sources:
                updated_existence_sources.add(
                    merged_datasources.get(source.safe_identifier, source)
                )
            final_existence_source_map[ex_key] = updated_existence_sources
        self_hidden: set[str] = self.hidden_concepts or set()
        other_hidden: set[str] = other.hidden_concepts or set()
        # hidden is the minimum overlapping set
        hidden = self_hidden.intersection(other_hidden)
        # Carry the base from LHS through the merge — the merged datasources
        # dict may have folded the original base into a wider entry (same
        # safe_identifier), so resolve through it.
        merged_base: Optional[Union[BuildDatasource, "QueryDatasource"]] = None
        if self.base_datasource is not None:
            merged_base = merged_datasources.get(
                self.base_datasource.safe_identifier, self.base_datasource
            )
        qds = QueryDatasource(
            input_concepts=unique(
                self.input_concepts + other.input_concepts, "address"
            ),
            output_concepts=unique(
                self.output_concepts + other.output_concepts, "address"
            ),
            source_map=final_source_map,
            existence_source_map=final_existence_source_map,
            datasources=list(merged_datasources.values()),
            grain=self.grain,
            joins=unique(self.joins + other.joins, "unique_id"),
            condition=(
                self.condition + other.condition
                if self.condition and other.condition
                else self.condition or other.condition
            ),
            source_type=self.source_type,
            partial_concepts=unique(
                self.partial_concepts + other.partial_concepts, "address"
            ),
            rollup_concepts=unique(
                self.rollup_concepts + other.rollup_concepts, "address"
            ),
            join_derived_concepts=self.join_derived_concepts,
            force_group=self.force_group,
            hidden_concepts=hidden,
            ordering=self.ordering,
            # only same-identifier QDSs merge, so limits agree; keep it
            limit=self.limit if self.limit is not None else other.limit,
            base_datasource=merged_base,
        )
        logger.debug(
            f"[Query Datasource] merged with {[c.address for c in qds.output_concepts]} concepts"
        )
        logger.debug(qds.source_map)
        return qds

    @property
    def identifier(self) -> str:
        cached = self.__dict__.get("_identifier_cache")
        if cached is not None:
            return cached
        result = self._compute_identifier()
        self.__dict__["_identifier_cache"] = result
        return result

    def _compute_identifier(self) -> str:
        if self.source_type == SourceType.UNION:
            # The arms — each addressable by their underlying base table — are
            # what make a union unique. Two unions over the same arms can be
            # merged by combining their projected columns, so don't fold the
            # outer grain (which reflects the projected column subset) into the
            # identifier. UnionCTE.condition is always None at render time
            # (consumers wrap row-level filters as CASE), so the QDS-level
            # condition is also irrelevant to identity. The operator IS
            # identity: an except() over the same arms must not merge with a
            # union() (and for EXCEPT the preserved arm order distinguishes
            # a-except-b from b-except-a).
            suffix = (
                "_unioned"
                if self.set_operator is SetOperator.UNION_ALL
                else f"_{self.set_operator.name.lower()}ed"
            )
            return "_union_".join([d.identifier for d in self.datasources]) + suffix
        filters = string_to_hash(str(self.condition)) if self.condition else ""
        grain = "_".join(
            [str(c).replace(".", "_") for c in sorted(self.grain.components)]
        )
        group = ""
        if self.group_required:
            keys = sorted(
                x.address for x in self.output_concepts if x.purpose != Purpose.METRIC
            )
            group = "_grouped_by_" + "_".join(keys)
        # Sort member identifiers: a join is commutative for identity, but
        # optimization passes reassign ``datasources`` post-construction (no
        # __post_init__ re-sort), so the same logical join can present its
        # members in two orders. Canonicalizing here — mirroring the sorted
        # grain/filter suffixes — keeps A_join_B and B_join_A one identity so
        # get_datasource_cte can find the built CTE. (UNION path above keeps
        # list order on purpose for EXCEPT arm semantics.)
        return (
            "_join_".join(sorted(d.identifier for d in self.datasources))
            + group
            + (f"_at_{grain}" if grain else "_at_abstract")
            + (f"_filtered_by_{filters}" if filters else "")
        )

    def get_alias(
        self,
        concept: BuildConcept,
        use_raw_name: bool = False,
        force_alias: bool = False,
        source: str | None = None,
    ):
        for x in self.datasources:
            # query datasources should be referenced by their alias, always
            force_alias = isinstance(x, QueryDatasource)
            #
            use_raw_name = isinstance(x, BuildDatasource) and not force_alias
            if source and x.safe_identifier != source:
                continue
            try:
                return x.get_alias(
                    concept.with_grain(self.grain),
                    use_raw_name,
                    force_alias=force_alias,
                )
            except ValueError:
                continue
        existing = [c.with_grain(self.grain) for c in self.output_concepts]
        if concept in existing:
            return concept.name

        existing_str = [str(c) for c in existing]
        datasources = [ds.identifier for ds in self.datasources]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {str(concept)} not found on {self.identifier};"
            f" have {existing_str} from {datasources}."
        )


class RecursiveCTE(CTE):

    def generate_loop_functions(
        self,
        recursive_derived: BuildConcept,
        left_recurse_concept: BuildConcept,
        right_recurse_concept: BuildConcept,
    ) -> tuple[BuildConcept, BuildConcept, BuildConcept]:

        join_gate = BuildConcept(
            name=RECURSIVE_GATING_CONCEPT,
            canonical_name=RECURSIVE_GATING_CONCEPT,
            namespace=DEFAULT_NAMESPACE,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=DataType.BOOL,
            purpose=Purpose.KEY,
            derivation=Derivation.BASIC,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=True,
                    ),
                    BuildCaseElse(expr=False),
                ],
                output_data_type=DataType.BOOL,
                output_purpose=Purpose.KEY,
            ),
        )
        bottom_join_gate = BuildConcept(
            name=f"{RECURSIVE_GATING_CONCEPT}_two",
            canonical_name=f"{RECURSIVE_GATING_CONCEPT}_two",
            namespace=DEFAULT_NAMESPACE,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=DataType.BOOL,
            purpose=Purpose.KEY,
            derivation=Derivation.BASIC,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=True,
                    ),
                    BuildCaseElse(expr=False),
                ],
                output_data_type=DataType.BOOL,
                output_purpose=Purpose.KEY,
            ),
        )
        bottom_derivation = BuildConcept(
            name=recursive_derived.name + "_bottom",
            canonical_name=recursive_derived.canonical_name + "_bottom",
            namespace=recursive_derived.namespace,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=recursive_derived.datatype,
            purpose=recursive_derived.purpose,
            derivation=Derivation.RECURSIVE,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=recursive_derived,
                    ),
                    BuildCaseElse(expr=right_recurse_concept),
                ],
                output_data_type=recursive_derived.datatype,
                output_purpose=recursive_derived.purpose,
            ),
        )
        return bottom_derivation, join_gate, bottom_join_gate

    @property
    def internal_ctes(self) -> List[CTE]:
        filtered_output = [
            x for x in self.output_columns if x.name != RECURSIVE_GATING_CONCEPT
        ]
        recursive_derived = [
            x for x in self.output_columns if x.derivation == Derivation.RECURSIVE
        ][0]
        if not isinstance(recursive_derived.lineage, BuildFunction):
            raise SyntaxError(
                "Recursive CTEs must have a function lineage, found"
                f" {recursive_derived.lineage}"
            )
        left_recurse_concept = recursive_derived.lineage.concept_arguments[0]
        right_recurse_concept = recursive_derived.lineage.concept_arguments[1]
        parent_ctes: List[CTE | UnionCTE]
        if self.parent_ctes:
            base = self.parent_ctes[0]
            loop_input_cte = base
            parent_ctes = [base]
            parent_identifier = base.identifier
        else:
            raise SyntaxError("Recursive CTEs must have a parent CTE currently")
        bottom_derivation, join_gate, bottom_join_gate = self.generate_loop_functions(
            recursive_derived, left_recurse_concept, right_recurse_concept
        )
        base_output = [*filtered_output, join_gate]
        bottom_output = []
        for x in filtered_output:
            if x.address == recursive_derived.address:
                bottom_output.append(bottom_derivation)
            else:
                bottom_output.append(x)

        bottom_output = [*bottom_output, bottom_join_gate]
        top = CTE(
            name=self.name,
            source=self.source,
            output_columns=base_output,
            source_map=self.source_map,
            grain=self.grain,
            existence_source_map=self.existence_source_map,
            parent_ctes=self.parent_ctes,
            joins=self.joins,
            condition=self.condition,
            partial_concepts=self.partial_concepts,
            rollup_concepts=self.rollup_concepts,
            hidden_concepts=self.hidden_concepts,
            nullable_concepts=self.nullable_concepts,
            join_derived_concepts=self.join_derived_concepts,
            group_to_grain=self.group_to_grain,
            order_by=self.order_by,
            limit=self.limit,
        )
        top_cte_array: list[CTE | UnionCTE] = [top]
        bottom_source_map = {
            left_recurse_concept.address: [top.identifier],
            right_recurse_concept.address: [parent_identifier],
            # recursive_derived.address: self.source_map[recursive_derived.address],
            join_gate.address: [top.identifier],
            recursive_derived.address: [top.identifier],
        }
        bottom = CTE(
            name=self.name,
            source=self.source,
            output_columns=bottom_output,
            source_map=bottom_source_map,
            grain=self.grain,
            existence_source_map=self.existence_source_map,
            parent_ctes=top_cte_array + parent_ctes,
            joins=[
                Join(
                    right_cte=loop_input_cte,
                    jointype=JoinType.INNER,
                    joinkey_pairs=[
                        CTEConceptPair(
                            left=recursive_derived,
                            right=left_recurse_concept,
                            existing_datasource=loop_input_cte.source,
                            modifiers=[],
                            cte=top,
                        )
                    ],
                    condition=BuildComparison(
                        left=join_gate, right=True, operator=ComparisonOperator.IS_NOT
                    ),
                )
            ],
            partial_concepts=self.partial_concepts,
            rollup_concepts=self.rollup_concepts,
            hidden_concepts=self.hidden_concepts,
            nullable_concepts=self.nullable_concepts,
            join_derived_concepts=self.join_derived_concepts,
            group_to_grain=self.group_to_grain,
            order_by=self.order_by,
            limit=self.limit,
        )
        return [top, bottom]


@dataclass
class DatasourceCTE(CTE):
    """A CTE whose source is a single raw ``BuildDatasource``.

    As a normal parent / WITH entry it behaves like a plain ``CTE``. When the
    inline-datasource optimization folds it into a consumer, the consumer parks
    this leaf on ``inlined_parents`` and renders its raw table directly.
    """

    datasource: BuildDatasource = field(kw_only=True)

    def consumer_column(
        self, concept: BuildConcept
    ) -> str | RawColumnExpr | BuildFunction | BuildAggregateWrapper:
        alias = _datasource_column_for_concept(self.datasource, concept)
        if alias is not None:
            return alias
        # Not a raw column. A cross-namespace `merge` collapses both sides into
        # one concept whose lineage points at the *other* namespace's column
        # (e.g. `passenger.last_name <- split(passenger.name)`), which this
        # datasource can't compute. The side-appropriate derivation survives as a
        # pseudonym output of this datasource (`rich_info.last_name <-
        # split(rich_info.full_name)`); return its lineage so the caller renders
        # the local derivation instead of failing on the absent raw column.
        for output in self.output_columns:
            if (
                output.address in concept.pseudonyms
                and isinstance(output.lineage, (BuildFunction, BuildAggregateWrapper))
                and _datasource_column_for_concept(self.datasource, output) is None
            ):
                return output.lineage
        assert alias is not None  # concept is an output of this datasource
        return alias


@dataclass
class UnionCTE:
    name: str
    source: QueryDatasource
    parent_ctes: list[CTE | UnionCTE]
    internal_ctes: list[CTE | UnionCTE]
    output_columns: List[BuildConcept]
    grain: BuildGrain
    operator: str = "UNION ALL"
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    hidden_concepts: set[str] = field(default_factory=set)
    partial_concepts: list[BuildConcept] = field(default_factory=list)
    rollup_concepts: list[BuildConcept] = field(default_factory=list)
    existence_source_map: Dict[str, list[str]] = field(default_factory=dict)

    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_columns)

    def get_alias(self, concept: BuildConcept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
                if source and source != cte.name:
                    continue
                return concept.safe_address
        return "INVALID_ALIAS"

    def get_concept(self, address: str) -> BuildConcept | None:
        for cte in self.internal_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                return match

        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    @property
    def source_map(self):
        return {x.address: [] for x in self.output_columns}

    @property
    def condition(self):
        return None

    @condition.setter
    def condition(self, value):
        raise NotImplementedError

    @property
    def inlined_alias_map(self) -> dict[str, str]:
        return {}

    def resolve_render_alias(self, source: str) -> str:
        return source

    def source_bindings(self, include_branches: bool = True) -> list[SourceBinding]:
        bindings = [
            SourceBinding(
                key=parent.name,
                node=parent,
                datasource=(
                    parent.datasource
                    if isinstance(parent, DatasourceCTE)
                    else parent.source
                ),
                emitted=True,
            )
            for parent in self.parent_ctes
        ]
        if include_branches:
            bindings.extend(
                SourceBinding(
                    key=branch.name,
                    node=branch,
                    datasource=branch.source,
                    emitted=False,
                    branch=True,
                )
                for branch in self.internal_ctes
            )
        return bindings

    def dependency_nodes(
        self,
        include_branches: bool = False,
    ) -> list[CTE | "UnionCTE"]:
        return [
            binding.node
            for binding in self.source_bindings(include_branches=include_branches)
            if binding.node is not None
        ]

    def source_key_for(
        self,
        source: str | CTE | "UnionCTE" | BuildDatasource | QueryDatasource,
    ) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, (CTE, UnionCTE)):
            return source.name
        for binding in self.source_bindings(include_branches=True):
            if binding.datasource is source:
                return binding.key
        return source.safe_identifier

    def add_dependency(self, parent: CTE | "UnionCTE") -> None:
        self.parent_ctes = unique(self.parent_ctes + [parent], "name")

    def replace_dependency(self, old: CTE, new: CTE | "UnionCTE") -> None:
        self.parent_ctes = [
            new if x.safe_identifier == old.safe_identifier else x
            for x in self.parent_ctes
        ]
        new_internal: list[CTE | UnionCTE] = []
        for branch in self.internal_ctes:
            if branch.safe_identifier == old.safe_identifier:
                new_internal.append(new)
                continue
            # An arm renders inline against its own base (``FROM <arm-source>``);
            # if that source is the CTE being replaced, repoint the arm too —
            # otherwise the arm dangles at a CTE that was merged away. The arm
            # object lives only here (it is not in the optimizer's working set),
            # so the consumer-level repoint never reaches it.
            if any(
                p.safe_identifier == old.safe_identifier for p in branch.parent_ctes
            ):
                branch.replace_dependency(old, new)
            new_internal.append(branch)
        self.internal_ctes = new_internal

    @property
    def identifier(self) -> str:
        return self.name

    @property
    def safe_identifier(self):
        return self.name

    @property
    def group_to_grain(self) -> bool:
        return False

    @property
    def group_concepts(self) -> List[BuildConcept]:
        # unions should always be on unique sets
        return []

    def __add__(self, other):
        if not isinstance(other, UnionCTE) or not other.name == self.name:
            raise SyntaxError("Cannot merge union CTEs")
        extra = [c for c in other.output_columns if c not in self.output_lcl]
        if not extra:
            return self
        self.output_columns = unique(
            self.output_columns + other.output_columns, "address"
        )
        # Merge each arm of the union so every branch projects the combined columns.
        self_by_name = {cte.name: i for i, cte in enumerate(self.internal_ctes)}
        for other_cte in other.internal_ctes:
            if other_cte.name in self_by_name:
                self.internal_ctes[self_by_name[other_cte.name]] = (
                    self.internal_ctes[self_by_name[other_cte.name]] + other_cte
                )
        return self


@dataclass
class Join:
    right_cte: CTE | UnionCTE
    jointype: JoinType
    left_cte: CTE | UnionCTE | None = None
    joinkey_pairs: List[CTEConceptPair] | None = None
    quote: str | None = None
    condition: BoolExpr | None = None
    modifiers: List[Modifier] = field(default_factory=list)
    # Set by union_dim_pushdown when LHS join keys are local to the rendering
    # CTE rather than read from a parent alias.
    left_is_local: bool = False

    @staticmethod
    def authoritative(
        consumer: "CTE | UnionCTE", node: "CTE | UnionCTE"
    ) -> "CTE | UnionCTE":
        """The consumer's own parent instance for ``node``.

        Joins keep their own node references, which are *not* the same
        objects inline moved into ``consumer.inlined_parents``. Resolve back
        to the consumer's own instance so inlined state is read from a single
        source of truth."""
        if isinstance(consumer, CTE):
            for binding in consumer.source_bindings(include_inlined=True):
                if binding.node is not None and binding.node.name == node.name:
                    return binding.node
        return node

    @staticmethod
    def _resolve_alias(consumer: "CTE | UnionCTE", node: "CTE | UnionCTE") -> str:
        if (
            isinstance(consumer, CTE)
            and isinstance(node, DatasourceCTE)
            and consumer.renders_inline(node)
        ):
            return consumer.source_key_for(node)
        if isinstance(consumer, CTE):
            return consumer.source_key_for(node.name)
        return node.name

    def name_for(self, consumer: "CTE | UnionCTE", node: "CTE | UnionCTE") -> str:
        """Alias token a consumer references ``node``'s columns by."""
        return self._resolve_alias(consumer, node)

    def reference_for(self, consumer: "CTE | UnionCTE", node: "CTE | UnionCTE") -> str:
        """FROM/JOIN source text for ``node`` as seen from ``consumer``.

        A normal CTE is referenced by name; an inlined ``DatasourceCTE``
        renders its raw table directly under the resolved alias."""
        node = self.authoritative(consumer, node)
        alias = self._resolve_alias(consumer, node)
        q = self.quote or ""
        if (
            isinstance(consumer, CTE)
            and isinstance(node, DatasourceCTE)
            and consumer.renders_inline(node)
        ):
            location = node.datasource.safe_location
            if self.quote:
                location = safe_quote(location, self.quote)
            return f"{location} as {q}{alias}{q}"
        return f"{q}{alias}{q}"

    @property
    def right_name(self) -> str:
        # Stable identity token for __str__ / unique_id / dedup only.
        return self.right_cte.name

    @property
    def unique_id(self) -> str:
        # Order-independent — see BaseJoin.unique_id for rationale.
        if self.joinkey_pairs:
            pair_keys = sorted(
                f"{k.cte.name}.{k.left.address}={k.right.address}"
                for k in self.joinkey_pairs
            )
            return (
                f"{self.jointype.value} join {self.right_name} on {','.join(pair_keys)}"
            )
        return str(self)

    def __str__(self):
        pairs = self.joinkey_pairs or []
        if self.joinkey_pairs:
            return (
                f"{self.jointype.value} join"
                f" {self.right_name} on"
                f" {','.join([k.cte.name + '.'+str(k.left.address)+'='+str(k.right.address) for k in pairs])}"
            )
        elif self.left_cte:
            return (
                f"{self.jointype.value} JOIN {self.left_cte.name} and"
                f" {self.right_name} on {','.join([str(k) for k in pairs])}"
            )
        return f"{self.jointype.value} JOIN  {self.right_name} on {','.join([str(k) for k in pairs])}"


def merge_ctes(ctes: List[CTE | UnionCTE]) -> List[CTE | UnionCTE]:
    final_ctes_dict: Dict[str, CTE | UnionCTE] = {}
    # merge CTEs
    for cte in ctes:
        if cte.name not in final_ctes_dict:
            final_ctes_dict[cte.name] = cte
        else:
            final_ctes_dict[cte.name] = final_ctes_dict[cte.name] + cte

    final_ctes = list(final_ctes_dict.values())
    return final_ctes


@dataclass
class CompiledCTE:
    name: str
    statement: str
