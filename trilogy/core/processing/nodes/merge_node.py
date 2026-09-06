from trilogy.constants import logger
from trilogy.core.enums import (
    Derivation,
    JoinType,
    Modifier,
    SourceType,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildOrderBy,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import (
    decompose_condition,
    gather_non_null_proofs,
    gather_or_groups,
)
from trilogy.core.processing.grain_utility import (
    JoinProofs,
    anti_join_preserved_grain,
    calculate_joined_pregrain,
    collect_applied_conditions,
    condition_key_grain,
    grain_satisfied_by_pregrain,
    has_condition_key_outside_grain,
    is_identity_group,
    narrow_directional_join_types,
    narrow_join_types,
    non_null_proofs,
)
from trilogy.core.processing.join_resolution import (
    compute_outer_null_status,
    get_node_joins,
    merge_partial_addresses,
    narrow_keyless_joins,
    partial_binding_sources,
    prune_outer_join_pairs,
    side_nullable,
)
from trilogy.core.processing.nodes.base_node import (
    NodeJoin,
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


def _has_applied_condition(source: QueryDatasource | BuildDatasource) -> bool:
    if isinstance(source, QueryDatasource):
        return bool(source.condition) or any(
            _has_applied_condition(parent) for parent in source.datasources
        )
    return bool(source.where)


def _key_equivalence_classes(pairs: list[tuple[str, str]]) -> list[set[str]]:
    """Union-find the join-key address pairs into connected equivalence classes,
    so a chain (`a=b`, `c=b`) yields the single class {a, b, c}."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in pairs:
        parent[find(a)] = find(b)

    classes: dict[str, set[str]] = {}
    for addr in parent:
        classes.setdefault(find(addr), set()).add(addr)
    return list(classes.values())


def deduplicate_nodes(
    merged: dict[str, QueryDatasource | BuildDatasource],
    logging_prefix: str,
    environment: BuildEnvironment,
) -> tuple[bool, dict[str, QueryDatasource | BuildDatasource], set[str]]:
    duplicates = False
    removed: set[str] = set()
    set_map: dict[str, set[str]] = {}
    all_map: dict[str, set[str]] = {}
    for k, v in merged.items():
        # A parent that hides a concept does not supply it downstream, so it
        # must not shadow another parent that exposes the same concept.
        hidden = set(v.hidden_concepts) if isinstance(v, QueryDatasource) else set()
        # a rowset's concept may live in a different environment
        exposed = [
            (
                (environment.concepts.get(x.address) or x).address,
                x in v.partial_concepts,
            )
            for x in v.output_concepts
            if x.address not in hidden
        ]
        set_map[k] = {address for address, partial in exposed if not partial}
        all_map[k] = {address for address, _ in exposed}
    for k1, v1 in set_map.items():
        found = False
        for k2, v2 in set_map.items():
            if k1 == k2:
                continue
            # k1 is redundant when k2 binds everything it exposes, its complete
            # bindings complete; a partial binding k2 lacks is a side k1 supplies.
            if (
                v1.issubset(v2)
                and all_map[k1].issubset(all_map[k2])
                and merged[k1].grain.issubset(merged[k2].grain)
                and not _has_applied_condition(merged[k2])
                and not _has_applied_condition(merged[k1])
                # a row-limited source is a proper row subset, never
                # interchangeable with a superset source
                and getattr(merged[k1], "limit", None) is None
                and getattr(merged[k2], "limit", None) is None
            ):
                og = merged[k1]
                subset_to = merged[k2]
                logger.info(
                    f"{logging_prefix}{LOGGER_PREFIX} extraneous parent node that is subset of another parent node {og.grain.issubset(subset_to.grain)} {og.grain.components} {subset_to.grain.components}"
                )
                merged = {k: v for k, v in merged.items() if k != k1}
                removed.add(k1)
                duplicates = True
                found = True
                break
        if found:
            break

    return duplicates, merged, removed


def deduplicate_nodes_and_joins(
    joins: list[NodeJoin] | None,
    merged: dict[str, QueryDatasource | BuildDatasource],
    logging_prefix: str,
    environment: BuildEnvironment,
) -> tuple[list[NodeJoin] | None, dict[str, QueryDatasource | BuildDatasource]]:
    duplicates = True
    while duplicates:
        duplicates = False
        duplicates, merged, removed = deduplicate_nodes(
            merged, logging_prefix, environment=environment
        )
        if joins is not None:
            joins = [
                j
                for j in joins
                if j.left_node.resolve().identifier not in removed
                and j.right_node.resolve().identifier not in removed
            ]
    return joins, merged


class MergeNode(StrategyNode):
    source_type = SourceType.MERGE

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment,
        whole_grain: bool = False,
        parents: list["StrategyNode"] | None = None,
        node_joins: list[NodeJoin] | None = None,
        force_join_type: JoinType | None = None,
        partial_concepts: list[BuildConcept] | None = None,
        rollup_concepts: list[BuildConcept] | None = None,
        nullable_concepts: list[BuildConcept] | None = None,
        force_group: bool | None = None,
        depth: int = 0,
        grain: BuildGrain | None = None,
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
        hidden_concepts: set[str] | None = None,
        existence_concepts: list[BuildConcept] | None = None,
        ordering: BuildOrderBy | None = None,
        preserve_parents: bool = False,
        host_stitch: bool = False,
        extent_free_spans: frozenset[str] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            rollup_concepts=rollup_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            grain=grain,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            existence_concepts=existence_concepts,
            ordering=ordering,
        )
        # Emit the joined relation row by row rather than collapsing to the
        # declared grain (a bare axis-member projection keeps its fan-out).
        self.whole_grain = whole_grain
        self.force_join_type = force_join_type
        self.node_joins: list[NodeJoin] | None = node_joins
        # A deliberately-assembled multi-side merge (coalescing axis, presence
        # probe): every parent is a distinct SIDE of a declared relation, so
        # the single-parent/duplicate collapse shortcuts must not fire; same
        # addresses across sides are different domains, not redundancy.
        self.preserve_parents = preserve_parents
        # An assembly stitch between sibling contributors: extension-family
        # ownership (per_group span routing) applies, so join inference gets a
        # host basis and preserves only the span owner. Mid-plan merges keep
        # plain domain-preserving semantics.
        self.host_stitch = host_stitch
        # `~` spans this merge must NOT extend: another group owns those
        # extension members (v4_helper/extent_ownership.py), so padding here
        # would manufacture a second copy. Captured from the environment at
        # construction so a merge built deep inside a generator inherits its
        # group's routing.
        self.extent_free_spans = (
            environment.extent_free_spans
            if extent_free_spans is None
            else extent_free_spans
        )

        final_joins: list[NodeJoin] = []
        if self.node_joins is not None:
            for join in self.node_joins:
                if join.left_node.resolve().name == join.right_node.resolve().name:
                    continue
                final_joins.append(join)
            self.node_joins = final_joins

    def translate_node_joins(self, node_joins: list[NodeJoin]) -> list[BaseJoin]:
        joins = []
        for join in node_joins:
            left = join.left_node.resolve()
            right = join.right_node.resolve()
            if left.identifier == right.identifier:
                raise SyntaxError(f"Cannot join node {left.identifier} to itself")
            # generator-authored joins carry no null-safety analysis; compute it
            # here as inferred joins do, else a nullable join key drops its NULL
            # matches through the plain equality
            modifiers = list(join.modifiers)
            if (
                Modifier.NULLABLE not in modifiers
                and join.concepts
                and all(
                    side_nullable(concept, left) and side_nullable(concept, right)
                    for concept in join.concepts
                )
            ):
                modifiers.append(Modifier.NULLABLE)
            joins.append(
                BaseJoin(
                    left_datasource=left,
                    right_datasource=right,
                    join_type=join.join_type,
                    concepts=join.concepts,
                    concept_pairs=join.concept_pairs,
                    modifiers=modifiers,
                )
            )
        return joins

    def create_full_joins(self, dataset_list: list[QueryDatasource | BuildDatasource]):
        joins = []
        seen = set()
        for left_value in dataset_list:
            for right_value in dataset_list:
                if left_value.identifier == right_value.identifier:
                    continue
                if left_value.identifier in seen and right_value.identifier in seen:
                    continue
                joins.append(
                    BaseJoin(
                        left_datasource=left_value,
                        right_datasource=right_value,
                        join_type=JoinType.FULL,
                        concepts=[],
                    )
                )
                seen.add(left_value.identifier)
                seen.add(right_value.identifier)
        return joins

    def generate_joins(
        self,
        final_datasets,
        final_joins: list[NodeJoin] | None,
        pregrain: BuildGrain,
        grain: BuildGrain,
        environment: BuildEnvironment,
    ) -> list[BaseJoin | UnnestJoin]:
        dataset_list: list[QueryDatasource | BuildDatasource] = sorted(
            final_datasets,
            key=lambda x: (-len(x.grain.components), x.identifier),
        )

        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has {len(dataset_list)} parents, starting merge"
        )
        if final_joins is None:
            if not pregrain.components:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} no grain components, doing full join"
                )
                joins = self.create_full_joins(dataset_list)
            else:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} inferring node joins to target grain {grain!s}"
                )
                # The host side carries extension rows: when this node emits
                # `~`-licensed keys, the side covering ALL of them owns every
                # extension family; a feeder exposing only the stitch key is not
                # a host even when it covers the merge grain. With no licensed
                # keys in play, grain coverage decides.
                host_grain: set[str] | None = None
                if self.host_stitch:
                    licensed = {
                        address
                        for datasource in environment.datasources.values()
                        for address in datasource.column_level_partial_addresses
                    }
                    licensed_outputs = {
                        c.address
                        for c in self.output_concepts
                        if c.address in licensed
                        and c.address not in self.extent_free_spans
                    }
                    host_grain = licensed_outputs or set(grain.components)
                # Domains this node emits: visible outputs and the grain,
                # each expanded to its declared keys (a visible dim attribute
                # demands its key's domain even when a later wrapper does the
                # grouping). A `~` key outside this set licenses no extension
                # rows here (get_join_type's fact-to-dim anchoring).
                hidden = self.hidden_concepts or set()
                demanded_domains: set[str] = set()
                for concept in self.output_concepts:
                    if concept.address in hidden:
                        continue
                    demanded_domains.add(concept.address)
                    if concept.keys:
                        demanded_domains |= set(concept.keys)
                for component in grain.components:
                    demanded_domains.add(component)
                    component_concept = environment.concepts.get(component)
                    if component_concept is not None and component_concept.keys:
                        demanded_domains |= set(component_concept.keys)
                demanded_domains -= self.extent_free_spans
                joins = get_node_joins(
                    dataset_list,
                    environment=environment,
                    host_grain=host_grain,
                    demanded_domains=demanded_domains,
                    extent_free_spans=self.extent_free_spans,
                )
        elif final_joins:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} translating provided node joins {len(final_joins)}"
            )
            joins = self.translate_node_joins(final_joins)
        else:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} Final joins is not null {final_joins} but is empty, skipping join generation"
            )
            return []
        if self.force_join_type is not None:
            for j in joins:
                if isinstance(j, BaseJoin):
                    j.join_type = self.force_join_type
        return joins

    def _join_proofs(
        self, final_datasets: list[QueryDatasource | BuildDatasource]
    ) -> JoinProofs:
        proofs: set[str] = set()
        side_proofs: set[str] = set()
        or_groups: list[list[set[str]]] = []
        if self.conditions:
            proofs = non_null_proofs(self.conditions)
            side_proofs = gather_non_null_proofs(self.conditions)
            or_groups = gather_or_groups(self.conditions)
        # A MULTISELECT align supplies explicit ``node_joins`` whose FULL is
        # intentional (each arm's rows survive even where the other arm, with
        # its own HAVING, has none), so arm-local evidence must not narrow it.
        if self.node_joins is not None:
            return JoinProofs(
                proofs=proofs, side_proofs=side_proofs, or_groups=or_groups
            )
        # A query-level filter pushed into the single branch that exposes its
        # columns still constrains the FINAL output: any output concept it
        # proves non-null must not be re-nulled by an outer join. That holds
        # only when no other branch supplies the column completely; with one
        # branch COMPLETE and another PARTIAL on it, the merge legitimately
        # spans rows outside the filter and the outer join must keep them.
        output_addresses = {c.address for c in self.output_concepts}
        branch_proofs: set[str] = set()
        complete_addresses: set[str] = set()
        partial_addresses: set[str] = set()
        for source in final_datasets:
            for condition in collect_applied_conditions(source):
                branch_proofs |= non_null_proofs(condition)
            source_partial = {c.address for c in source.partial_concepts}
            source_outputs = {c.address for c in source.output_concepts}
            complete_addresses |= source_outputs - source_partial
            partial_addresses |= source_outputs & source_partial
        branch_proofs &= output_addresses
        branch_proofs -= complete_addresses & partial_addresses
        # A branch carrying an atom of this merge's PRE-APPLIED request WHERE
        # (preexisting_conditions the merge itself does not re-render) is the
        # population: every final row must have a match there. Branch-local
        # filters (a rowset's internal WHERE) are not request atoms and keep
        # their deliberate preservation.
        filtered_ids: set[str] = set()
        if self.preexisting_conditions is not None:
            rendered = (
                list(decompose_condition(self.conditions)) if self.conditions else []
            )
            request_atoms = [
                atom
                for atom in decompose_condition(self.preexisting_conditions)
                if not any(atom == r for r in rendered)
            ]
            for source in final_datasets:
                applied = [
                    atom
                    for condition in collect_applied_conditions(source)
                    for atom in decompose_condition(condition)
                ]
                if any(any(atom == a for a in applied) for atom in request_atoms):
                    filtered_ids.add(source.identifier)
        # Authored coalescing (union/full) relations declare row intent: only
        # the provably-row-identical narrowing pass may tighten them, the same
        # registry veto `get_join_type` honors.
        coalescing = self.environment.domain_graph.outer_relation_keys() | set(
            self.environment.domain_graph.coalescing_relation_members()
        )
        return JoinProofs(
            proofs=proofs,
            branch_proofs=branch_proofs,
            side_proofs=side_proofs,
            or_groups=or_groups,
            filtered_ids=filtered_ids,
            coalescing_keys=coalescing,
        )

    def _resolve(self) -> QueryDatasource:
        parent_sources: list[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]
        merged: dict[str, QueryDatasource | BuildDatasource] = {}
        final_joins: list[NodeJoin] | None = self.node_joins
        # Two parents built under different extent ownership carry distinct
        # identifiers; when their resolved joins came out the same they are one
        # relation and fold like any other identifier match.
        by_shape: dict[tuple, str] = {}
        for source in parent_sources:
            key = source.identifier
            if isinstance(source, QueryDatasource) and key not in merged:
                key = by_shape.setdefault(source.shape, key)
            if key in merged:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} merging parent node with {source.identifier} into existing"
                )
                merged[key] = merged[key] + source
            else:
                merged[key] = source

        # drop redundant sources, unless every parent is a deliberate side of a
        # coalescing relation
        if not self.preserve_parents:
            final_joins, merged = deduplicate_nodes_and_joins(
                final_joins, merged, self.logging_prefix, self.environment
            )
        final_datasets: list[QueryDatasource | BuildDatasource] = sorted(
            merged.values(), key=lambda source: source.identifier
        )

        merge_output_addresses = {c.address for c in self.output_concepts}
        existence_addr_set = {c.address for c in self.existence_concepts}
        # Coalescing (union/full) key members. A semijoin feeder keyed on one of
        # these because its probe filters the coalesced key carries that key
        # only incidentally; the genuine union sides supply it (see below).
        coalescing_members = self.environment.domain_graph.coalescing_relation_members()
        existence_key_by_addr: dict[str, set[str]] = {
            c.address: {k for k in (c.keys or set()) if k in coalescing_members}
            for c in self.existence_concepts
        }

        def _is_existence_only(x: QueryDatasource | BuildDatasource) -> bool:
            out_addrs = {y.address for y in x.output_concepts}
            provided_existence = out_addrs & existence_addr_set
            if not provided_existence:
                return False
            # Existence-only if every concept it provides that this merge emits
            # as a row output is an existence concept. Incidental extra columns
            # must not promote it to a joined row source: it has no join key,
            # only a subselect, and would dangle in the FROM.
            #
            # A coalescing key the feeder exposes only as the KEY of its own
            # semijoin probe is likewise incidental: the feeder reaches its rows
            # through the EXISTS subselect, not a row join. A feeder exposing
            # OTHER coalescing keys is a real bridge row source and stays a
            # join candidate.
            probe_keys: set[str] = set()
            for addr in provided_existence:
                probe_keys |= existence_key_by_addr.get(addr, set())
            return all(
                a in existence_addr_set or a in probe_keys
                for a in out_addrs
                if a in merge_output_addresses
            )

        existence_final = [x for x in final_datasets if _is_existence_only(x)]
        # ``force_group is True`` means this merge exists to regroup its finer
        # parent to the output grain; returning a parent that merely covers the
        # output columns would drop that group. ``preserve_parents`` marks a
        # multi-side assembly where a covering parent is one side's domain,
        # never good enough for the unified axis. Both skip the short-circuits.
        can_drop_merge = self.force_group is not True and not self.preserve_parents
        if can_drop_merge and len(merged.keys()) == 1:
            final: QueryDatasource | BuildDatasource = next(iter(merged.values()))
            if (
                {c.address for c in final.output_concepts}
                == {c.address for c in self.output_concepts}
                and not self.conditions
                and not self.force_group
                and isinstance(final, QueryDatasource)
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has only one parent with the same"
                    " outputs as this merge node, dropping merge node "
                )
                final.ordering = self.ordering
                return final

        for dataset in final_datasets if can_drop_merge else []:
            if any(
                other.identifier != dataset.identifier and _has_applied_condition(other)
                for other in final_datasets
            ):
                continue
            output_set = {
                c.address
                for c in dataset.output_concepts
                if c.address not in [x.address for x in dataset.partial_concepts]
            }
            if (
                all(c.address in output_set for c in self.all_concepts)
                and not self.conditions
                and not self.force_group
                and isinstance(dataset, QueryDatasource)
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node not required as parent node {dataset.source_type}"
                    f" has all required output properties with partial {[c.address for c in dataset.partial_concepts]}"
                    f" and self has no conditions ({self.conditions})"
                )
                dataset.ordering = self.ordering
                return dataset

        # Grain components from non-existence sources; rebuilt via from_concepts
        # below, which drops where_clauses anyway.
        raw_pregrain_components: set[str] = set()
        for source in final_datasets:
            if all(
                x.address in self.existence_concepts for x in source.output_concepts
            ):
                logger.debug(
                    f"{self.logging_prefix}{LOGGER_PREFIX} skipping existence-only source {source.identifier}"
                )
                continue
            raw_pregrain_components.update(source.grain.components)
            logger.debug(
                f"{self.logging_prefix}{LOGGER_PREFIX} added grain {source.grain} from {source.identifier}; pregrain components now {raw_pregrain_components}"
            )

        raw_pregrain = BuildGrain.from_concepts(
            raw_pregrain_components, environment=self.environment
        )

        grain = self.grain if self.grain else raw_pregrain
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} has pre grain {raw_pregrain} and final merge node grain {grain}"
        )
        join_candidates = [x for x in final_datasets if x not in existence_final]
        join_proofs = self._join_proofs(final_datasets)
        if len(join_candidates) > 1:
            joins: list[BaseJoin | UnnestJoin] = self.generate_joins(
                join_candidates, final_joins, raw_pregrain, grain, self.environment
            )
        else:
            joins = []

        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Final join count for CTE parent count {len(join_candidates)} is {len(joins)}"
        )
        narrow_join_types(joins, join_proofs, final_datasets)
        # Per-datasource NULL-ability from the resolved join graph: orders
        # ``final_datasets`` so the preserved side wins ``resolve_concept_map``'s
        # first pass, and prunes NULL-able-side pairs from JOIN ON when a
        # preserved alternative exists. Both reduce redundant ``coalesce``.
        null_status = compute_outer_null_status(joins)
        prune_outer_join_pairs(joins, null_status)
        narrow_directional_join_types(joins, join_proofs, final_datasets)
        narrow_keyless_joins(joins)
        # FULL JOINs only: both sides may be NULL, so source_map needs every
        # input supplying the address. For LEFT/RIGHT the preserved-side
        # ordering above suffices.
        full_join_concepts = []
        for join in joins:
            if isinstance(join, BaseJoin) and join.join_type == JoinType.FULL:
                full_join_concepts += join.input_concepts
        pregrain = BuildGrain.from_concepts(
            calculate_joined_pregrain(
                join_candidates, joins, grain, self.environment
            ).components,
            environment=self.environment,
        )
        pregrain += condition_key_grain(self.conditions, self.environment)
        anti_grain = anti_join_preserved_grain(final_datasets, joins, self.conditions)
        if anti_grain is not None:
            grain = anti_grain
            pregrain = anti_grain
        logger.debug(
            f"{self.logging_prefix}{LOGGER_PREFIX} effective joined pregrain is {pregrain}"
        )
        condition_key_requires_group = has_condition_key_outside_grain(
            self.conditions, grain, self.environment
        )

        if self.force_group is True:
            # A node producing rowset outputs at a grain its parents satisfy
            # must not regroup. TVF_UNION counts too: a UNION ALL stack defines
            # its own no-dedup row semantics, so a wrapper at the stack grain
            # must never collapse duplicate rows.
            rowset_output = any(
                concept.derivation in (Derivation.ROWSET, Derivation.TVF_UNION)
                for concept in self.output_concepts
            )
            force_group = condition_key_requires_group or not (
                rowset_output
                and grain_satisfied_by_pregrain(pregrain, grain, self.environment)
            )
        elif self.whole_grain:
            force_group = False
        elif condition_key_requires_group:
            force_group = True
        elif self.force_group is False:
            force_group = not grain_satisfied_by_pregrain(
                pregrain, grain, self.environment
            )
        elif not grain_satisfied_by_pregrain(pregrain, grain, self.environment):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} no parents include full grain {grain} and pregrain {pregrain} does not match, assume must group to grain. Have {[str(d.grain) for d in final_datasets]}"
            )
            force_group = True
        else:
            force_group = None
        # A regroup is an identity when the joined rows are already unique at
        # the output grain: nothing to collapse (a row filter on keys outside
        # the grain cannot create duplicates), so render a plain projection.
        if force_group and is_identity_group(
            join_candidates,
            joins,
            BuildGrain.from_concepts(
                self.output_concepts, environment=self.environment
            ),
            self.conditions,
            self.output_concepts,
            self.rollup_concepts,
        ):
            force_group = False
        # Rows passed through from a ROLLUP/CUBE/GROUPING SETS parent are already
        # final-shape: a regroup would re-aggregate the subtotal rows away.
        if force_group and any(
            nonstandard_grouping_lineage(c) is not None for c in self.output_concepts
        ):
            force_group = False

        qd_joins: list[BaseJoin | UnnestJoin] = [*joins]

        # Preserved sides first: first-wins inside ``resolve_concept_map`` then
        # picks the non-NULL source for a shared concept. Existence-only sources
        # sort LAST: their columns are reachable only through a subselect, so a
        # row source_map entry pointing at one renders an unresolvable FROM
        # alias whenever a joined parent also supplies the concept; last keeps
        # them as the fallback provider only.
        ordered_datasets = sorted(
            final_datasets,
            key=lambda ds: (
                ds in existence_final,
                null_status.get(ds.identifier, 0),
                ds.identifier,
            ),
        )
        final_output_concepts = self.output_concepts

        source_map = resolve_concept_map(
            ordered_datasets,
            targets=final_output_concepts,
            inherited_inputs=self.input_concepts + self.existence_concepts,
            full_joins=full_join_concepts,
        )
        node_existence_source_map = resolve_existence_map(
            final_datasets, self.existence_concepts
        )
        # Scoped OUTER joins can bind different physical key addresses for one
        # merged key. A chain of joins (`a.k=b.k`, `c.k=b.k`) makes those
        # addresses one equivalence class; the merged key on any row is the
        # coalesce of every present member, so each member must render from the
        # union of all class sources (a pairwise merge leaves a 3-way class only
        # partly coalesced). Same-address keys are handled by normal
        # shared-column resolution.
        outer_pairs: list[tuple[str, str]] = [
            (pair.left.address, pair.right.address)
            for join in joins
            if isinstance(join, BaseJoin)
            and join.join_type
            in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER, JoinType.FULL)
            for pair in join.concept_pairs or []
            if pair.left.address != pair.right.address
        ]
        # A chained authored group (a=b=c) can reach this node with one pairing
        # already fused a level down, so this node's joins name only (b,c) and
        # (a) never learns c's source. Seed the classes with the authored
        # groups' co-present members.
        if outer_pairs:
            present = set(source_map.keys())
            for (
                member,
                group_mates,
            ) in self.environment.distinct_scoped_join_group_mates().items():
                if member not in present:
                    continue
                outer_pairs.extend(
                    (member, mate) for mate in group_mates if mate in present
                )
        for key_class in _key_equivalence_classes(outer_pairs):
            combined: set[BuildDatasource | QueryDatasource | UnnestJoin] = set()
            for addr in key_class:
                combined |= source_map.get(addr, set())
            if len(combined) <= 1:
                continue
            for addr in key_class:
                source_map[addr] = set(combined)
        nullable_concepts = find_nullable_concepts(
            source_map=source_map, joins=joins, datasources=final_datasets
        )
        rollup_concepts = unique(
            self.rollup_concepts
            + [
                c
                for source in final_datasets
                if isinstance(source, QueryDatasource)
                for c in source.rollup_concepts
                if c.address in {out.address for out in final_output_concepts}
            ],
            "address",
        )
        if force_group:

            grain = BuildGrain.from_concepts(
                self.output_concepts, environment=self.environment
            )
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} forcing group by to achieve grain {grain}"
            )
        joined_partials = merge_partial_addresses(
            final_datasets, qd_joins, final_output_concepts
        )
        qds = QueryDatasource(
            input_concepts=unique(self.input_concepts, "address"),
            output_concepts=final_output_concepts,
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=source_map,
            existence_source_map=node_existence_source_map,
            joins=qd_joins,
            grain=grain,
            # node-level nullables carry inferred nullability for concepts
            # COMPUTED at this node (e.g. a derived join key over a nullable
            # column) that join analysis cannot see
            nullable_concepts=[
                x
                for x in final_output_concepts
                if x.address in nullable_concepts
                or any(x.address == n.address for n in self.nullable_concepts)
            ],
            partial_concepts=unique(
                self.partial_concepts
                + [c for c in final_output_concepts if c.address in joined_partials]
                + self._extent_free_partials(final_datasets, final_output_concepts),
                "address",
            ),
            rollup_concepts=rollup_concepts,
            force_group=force_group,
            condition=self.conditions,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            extent_free_spans=self.extent_free_spans,
        )
        return qds

    def _extent_free_partials(
        self,
        sources: list[QueryDatasource | BuildDatasource],
        outputs: list[BuildConcept],
    ) -> list[BuildConcept]:
        """Span keys this merge covers only PARTIALLY.

        Declining to extend a span (docs/extent_ownership.md) means the key
        column holds just the members the facts below bound; the unmatched
        members belong to the elected owner. Marking them partial makes the
        assembly above preserve the owner's rows instead of INNER-joining them
        away."""
        if not self.extent_free_spans:
            return []
        return [
            concept
            for concept in outputs
            if concept.address in self.extent_free_spans
            and any(
                partial_binding_sources(source, concept.address) for source in sources
            )
        ]

    def copy(self) -> "MergeNode":
        return type(self)(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            force_group=self.force_group,
            grain=self.grain,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            nullable_concepts=list(self.nullable_concepts),
            hidden_concepts=set(self.hidden_concepts),
            node_joins=list(self.node_joins) if self.node_joins else None,
            force_join_type=self.force_join_type,
            existence_concepts=list(self.existence_concepts),
            ordering=self.ordering,
            preserve_parents=self.preserve_parents,
            host_stitch=self.host_stitch,
            extent_free_spans=self.extent_free_spans,
        )
