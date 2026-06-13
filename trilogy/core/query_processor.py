from collections import defaultdict
from dataclasses import replace
from math import ceil
from typing import Dict, List, Optional, Set, Tuple, Union

from trilogy.constants import CONFIG, DEFAULT_NAMESPACE, logger
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import (
    BooleanOperator,
    DatasourceState,
    FunctionType,
    JoinType,
    SourceType,
)
from trilogy.core.env_processor import generate_graph
from trilogy.core.ergonomics import generate_cte_names
from trilogy.core.exceptions import (
    UnresolvableQueryException,
)
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import (
    ConceptRef,
    Conditional,
    Function,
    MultiSelectLineage,
    SelectLineage,
    WhereClause,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildParamaterizedConceptReference,
    BuildRowsetItem,
    BuildSelectLineage,
    BuildWhereClause,
    Factory,
    _build_scoped_merge_index,
    get_canonical_pseudonyms,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    CTEConceptPair,
    DatasourceCTE,
    InstantiatedUnnestJoin,
    Join,
    QueryDatasource,
    RecursiveCTE,
    UnionCTE,
    UnnestJoin,
)
from trilogy.core.optimization import optimize_ctes
from trilogy.core.processing.concept_strategies_v3 import (
    append_existence_check,
    source_query_concepts,
)
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.concept_strategies_v4 import (
    search_concepts as search_concepts_v4,
)
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.statements.author import (
    ChartLayer,
    ChartStatement,
    ConceptDeclarationStatement,
    CopyStatement,
    MultiSelectStatement,
    PersistStatement,
    SelectStatement,
)
from trilogy.core.statements.execute import (
    MaterializedDataset,
    ProcessedChartCopyStatement,
    ProcessedChartLayer,
    ProcessedChartStatement,
    ProcessedCopyStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
)
from trilogy.hooks.base_hook import BaseHook
from trilogy.utility import unique

LOGGER_PREFIX = "[QUERY BUILD]"


def _extract_params(*concept_dicts) -> dict:
    result = {}
    for concepts in concept_dicts:
        for concept in concepts.values():
            if (
                isinstance(concept.lineage, Function)
                and concept.lineage.operator == FunctionType.CONSTANT
            ):
                result[concept.safe_address] = concept.lineage.arguments[0]
    return result


def base_join_to_join(
    base_join: BaseJoin | UnnestJoin, ctes: List[CTE | UnionCTE]
) -> Join | InstantiatedUnnestJoin:
    """This function converts joins at the datasource level
    to joins at the CTE level"""
    if isinstance(base_join, UnnestJoin):
        object_to_unnest = base_join.parent.arguments[0]
        if not isinstance(
            object_to_unnest,
            (BuildConcept | BuildParamaterizedConceptReference | BuildFunction),
        ):
            raise ValueError(f"Unnest join must be a concept; got {object_to_unnest}")
        return InstantiatedUnnestJoin(
            object_to_unnest=object_to_unnest,
            alias=base_join.alias,
        )

    def get_datasource_cte(
        datasource: BuildDatasource | QueryDatasource,
    ) -> CTE | UnionCTE:
        eligible = set()
        for cte in ctes:
            if cte.source.identifier == datasource.identifier:
                return cte
            eligible.add(cte.source.identifier)
        for cte in ctes:
            base = cte.source.base_datasource
            if base is not None:
                if base.identifier == datasource.identifier:
                    return cte
                eligible.add(base.identifier)
        raise ValueError(
            f"Could not find CTE for datasource {datasource.identifier}; have {eligible}"
        )

    if base_join.left_datasource is not None:
        left_cte = get_datasource_cte(base_join.left_datasource)
    else:
        # multiple left ctes
        left_cte = None
    right_cte = get_datasource_cte(base_join.right_datasource)
    if base_join.concept_pairs:
        final_pairs = [
            CTEConceptPair(
                left=pair.left,
                right=pair.right,
                existing_datasource=pair.existing_datasource,
                modifiers=pair.modifiers,
                cte=get_datasource_cte(pair.existing_datasource),
            )
            for pair in base_join.concept_pairs
        ]
    elif base_join.concepts and base_join.left_datasource:
        final_pairs = [
            CTEConceptPair(
                left=concept,
                right=concept,
                existing_datasource=base_join.left_datasource,
                modifiers=[],
                cte=get_datasource_cte(
                    base_join.left_datasource,
                ),
            )
            for concept in base_join.concepts
        ]
    else:
        final_pairs = []
    return Join(
        left_cte=left_cte,
        right_cte=right_cte,
        jointype=base_join.join_type,
        joinkey_pairs=final_pairs,
        modifiers=base_join.modifiers,
    )


def generate_source_map(
    query_datasource: QueryDatasource, all_new_ctes: List[CTE | UnionCTE]
) -> Tuple[Dict[str, list[str]], Dict[str, list[str]]]:
    source_map: Dict[str, list[str]] = defaultdict(list)
    # now populate anything derived in this level
    for qdk, qdv in query_datasource.source_map.items():
        unnest = [x for x in qdv if isinstance(x, UnnestJoin)]
        for _ in unnest:
            source_map[qdk] = []
        if (
            qdk not in source_map
            and len(qdv) == 1
            and isinstance(list(qdv)[0], UnnestJoin)
        ):
            source_map[qdk] = []
        basic = [x for x in qdv if isinstance(x, BuildDatasource)]
        for base in basic:
            source_map[qdk].append(base.safe_identifier)

        ctes = [x for x in qdv if isinstance(x, QueryDatasource)]
        if ctes:
            names = set([x.safe_identifier for x in ctes])
            matches = [
                cte for cte in all_new_ctes if cte.source.safe_identifier in names
            ]
            if not matches and names:
                raise SyntaxError(
                    f"Missing parent CTEs for source map; expecting {names}, have {[cte.source.safe_identifier for cte in all_new_ctes]}"
                )
            # when multiple sources exist (full join key), include partials
            multi_source = len(qdv) > 1
            for cte in matches:
                output_address = [
                    x.address
                    for x in cte.output_columns
                    if x.address not in [z.address for z in cte.partial_concepts]
                ]
                # A derived-key FULL join sources the canonical key from a side
                # that outputs it under a pseudonym column (da for the merged db);
                # accept that side so the renderer coalesces both physical columns.
                provides_pseudonym = multi_source and any(
                    qdk in x.pseudonyms for x in cte.output_columns
                )
                if (
                    qdk in output_address
                    or (multi_source and qdk in [x.address for x in cte.output_columns])
                    or provides_pseudonym
                ):
                    source_map[qdk].append(cte.safe_identifier)
            # now do a pass that accepts partials
            for cte in matches:
                if qdk not in source_map:
                    source_map[qdk] = [cte.safe_identifier]
        if qdk not in source_map:
            if not qdv:
                source_map[qdk] = []
            elif CONFIG.validate_missing:
                raise ValueError(
                    f"Missing {qdk} in {source_map}, source map {query_datasource.source_map} "
                )

    # existence lookups use a separate map
    # as they cannot be referenced in row resolution
    existence_source_map: Dict[str, list[str]] = defaultdict(list)
    for ek, ev in query_datasource.existence_source_map.items():
        ids = set([x.safe_identifier for x in ev])
        ematches = [
            cte.name for cte in all_new_ctes if cte.source.safe_identifier in ids
        ]
        existence_source_map[ek] = ematches
    return {
        k: [] if not v else list(set(v)) for k, v in source_map.items()
    }, existence_source_map


def datasource_to_query_datasource(datasource: BuildDatasource) -> QueryDatasource:
    sub_select: Dict[str, Set[Union[BuildDatasource, QueryDatasource, UnnestJoin]]] = {
        **{c.address: {datasource} for c in datasource.concepts},
    }
    concepts = [c for c in datasource.concepts]
    concepts = unique(concepts, "address")
    return QueryDatasource(
        output_concepts=concepts,
        input_concepts=concepts,
        source_map=sub_select,
        grain=datasource.grain,
        datasources=[datasource],
        joins=[],
        partial_concepts=[x.concept for x in datasource.columns if not x.is_complete],
        rollup_concepts=[],
        base_datasource=datasource,
    )


def generate_cte_name(full_name: str, name_map: dict[str, str]) -> str:
    cte_names = generate_cte_names()
    if CONFIG.human_identifiers:
        if full_name in name_map:
            return name_map[full_name]
        suffix = ""
        idx = len(name_map)
        if idx >= len(cte_names):
            int = ceil(idx / len(cte_names))
            suffix = f"_{int}"
        valid = [x for x in cte_names if x + suffix not in name_map.values()]
        lookup = valid[0]
        new_name = f"{lookup}{suffix}"
        name_map[full_name] = new_name
        return new_name
    else:
        return full_name.replace("<", "").replace(">", "").replace(",", "_")


def resolve_cte_base_name_and_alias_v2(
    name: str,
    source: QueryDatasource,
    source_map: Dict[str, list[str]],
    raw_joins: List[Join | InstantiatedUnnestJoin],
) -> Tuple[Address | str | None, str | None]:
    if not source.datasources:
        return None, None
    base = source.base_datasource
    if isinstance(base, BuildDatasource) and base.name != CONSTANT_DATASET:
        return base.address, base.safe_identifier

    joins: List[Join] = [join for join in raw_joins if isinstance(join, Join)]
    if joins and len(joins) > 0:
        candidates = [x.left_cte.name for x in joins if x.left_cte]
        for join in joins:
            if join.joinkey_pairs:
                candidates += [x.cte.name for x in join.joinkey_pairs if x.cte]
        disallowed = [x.right_cte.name for x in joins]
        try:
            cte = [y for y in candidates if y not in disallowed][0]
            return cte, cte
        except IndexError:
            raise SyntaxError(
                f"Invalid join configuration {candidates} {disallowed} for {name}",
            )
    counts: dict[str, int] = defaultdict(lambda: 0)
    output_addresses = [x.address for x in source.output_concepts]
    input_address = [x.address for x in source.input_concepts]
    for k, v in source_map.items():
        for vx in v:
            if k in output_addresses:
                counts[vx] = counts[vx] + 1

            if k in input_address:
                counts[vx] = counts[vx] + 1

            counts[vx] = counts[vx]
    if counts:
        return max(counts, key=counts.get), max(counts, key=counts.get)  # type: ignore
    return None, None


def datasource_to_cte(
    query_datasource: QueryDatasource, name_map: dict[str, str]
) -> CTE | UnionCTE:
    parents: list[CTE | UnionCTE] = []
    if query_datasource.source_type == SourceType.UNION:
        direct_parents: list[CTE | UnionCTE] = []
        for child in query_datasource.datasources:
            assert isinstance(child, QueryDatasource)
            child_cte = datasource_to_cte(child, name_map=name_map)
            direct_parents.append(child_cte)
            parents += child_cte.parent_ctes
        human_id = generate_cte_name(query_datasource.identifier, name_map)
        final = UnionCTE(
            name=human_id,
            source=query_datasource,
            parent_ctes=parents,
            internal_ctes=direct_parents,
            output_columns=[
                c.with_grain(query_datasource.grain)
                for c in query_datasource.output_concepts
            ],
            grain=direct_parents[0].grain,
            order_by=query_datasource.ordering,
            rollup_concepts=query_datasource.rollup_concepts,
        )
        return final

    # set in the single-source branch when the QDS is one raw, non-constant
    # BuildDatasource — the candidate for inline-datasource folding
    leaf_datasource: BuildDatasource | None = None

    if len(query_datasource.datasources) > 1 or any(
        [isinstance(x, QueryDatasource) for x in query_datasource.datasources]
    ):
        all_new_ctes: List[CTE | UnionCTE] = []
        for datasource in query_datasource.datasources:
            if isinstance(datasource, QueryDatasource):
                sub_datasource = datasource
            else:
                sub_datasource = datasource_to_query_datasource(datasource)

            sub_cte = datasource_to_cte(sub_datasource, name_map)
            parents.append(sub_cte)
            all_new_ctes.append(sub_cte)
        source_map, existence_map = generate_source_map(query_datasource, all_new_ctes)

    else:
        # single-source QDS — render directly from its base datasource
        source = query_datasource.base_datasource
        if source is not None:
            # constant datasets have no actual source; render without FROM
            if source.name == CONSTANT_DATASET:
                source_map = {k: [] for k in query_datasource.source_map}
                existence_map = source_map
            else:
                source_map = {
                    k: [] if not v else [source.safe_identifier]
                    for k, v in query_datasource.source_map.items()
                }
                existence_map = source_map
                if isinstance(source, BuildDatasource):
                    leaf_datasource = source
        else:
            source_map = {k: [] for k in query_datasource.source_map}
            existence_map = source_map

    human_id = generate_cte_name(query_datasource.identifier, name_map)

    final_joins = [
        base_join_to_join(join, [x for x in parents if isinstance(x, (CTE, UnionCTE))])
        for join in query_datasource.joins
    ]

    base_name, base_alias = resolve_cte_base_name_and_alias_v2(
        human_id, query_datasource, source_map, final_joins
    )
    cte_class: type[CTE] = CTE
    extra_kwargs: dict = {}

    if query_datasource.source_type == SourceType.RECURSIVE:
        cte_class = RecursiveCTE
        # extra_kwargs['left_recursive_concept'] = query_datasource.left
    elif leaf_datasource is not None:
        cte_class = DatasourceCTE
        extra_kwargs["datasource"] = leaf_datasource
    cte = cte_class(
        name=human_id,
        source=query_datasource,
        # output columns are what are selected/grouped by
        output_columns=[
            c.with_grain(query_datasource.grain)
            for c in query_datasource.output_concepts
        ],
        source_map=source_map,
        existence_source_map=existence_map,
        # related columns include all referenced columns, such as filtering
        joins=final_joins,
        grain=query_datasource.grain,
        group_to_grain=query_datasource.group_required,
        # we restrict parent_ctes to one level
        # as this set is used as the base for rendering the query
        parent_ctes=parents,
        condition=query_datasource.condition,
        partial_concepts=query_datasource.partial_concepts,
        rollup_concepts=query_datasource.rollup_concepts,
        nullable_concepts=query_datasource.nullable_concepts,
        join_derived_concepts=query_datasource.join_derived_concepts,
        hidden_concepts=query_datasource.hidden_concepts,
        base_name_override=base_name,
        base_alias_override=base_alias,
        order_by=query_datasource.ordering,
        **extra_kwargs,
    )
    if cte.grain != query_datasource.grain:
        raise ValueError("Grain was corrupted in CTE generation")
    if CONFIG.validate_missing:
        mapped_canonical = {
            c.canonical_address
            for c in cte.output_columns
            if c.address in cte.source_map
        }
        mapped_pseudonyms: set[str] = set()
        for c in cte.output_columns:
            if c.address in cte.source_map:
                mapped_pseudonyms.update(c.pseudonyms)
        for x in cte.output_columns:
            if (
                x.address not in cte.source_map
                and not any(y in cte.source_map for y in x.pseudonyms)
                and x.canonical_address not in mapped_canonical
                and x.address not in mapped_pseudonyms
            ):
                raise ValueError(
                    f"Missing {x.address} in {cte.source_map}, source map {cte.source.source_map.keys()} "
                )

    return cte


def _carry_order_by_concepts(
    build_statement: BuildSelectLineage | BuildMultiSelectLineage,
) -> None:
    """Pull `union(...)`/multiselect ORDER BY columns into the query grain so a
    single group node keeps them.

    A plain order-by concept is rendered from whichever CTE already exposes it
    (its source_map entry), so it needs no special handling. But a `union(...)` /
    multiselect output column is rendered via `find_source`, which only resolves
    at the union node itself — once an outer aggregate groups it away it is gone
    from every downstream CTE, and the renderer crashes trying to map it
    ("Could not find upstream map for multiselect ...").

    Such a column is always an alias-source of a selected transform (the author
    validation enforces order-by ⊆ outputs ∪ alias-sources), so it is 1:1 with a
    grain key — adding it to the grain (and as a hidden output) keeps the group a
    single node, rather than sourcing it as a finer optional that gets joined
    back via a (broken) enrichment merge."""
    if not isinstance(build_statement, BuildSelectLineage):
        return
    if not build_statement.order_by:
        return
    output_addresses = {c.address for c in build_statement.output_components}
    carry: dict[str, BuildConcept] = {}
    for item in build_statement.order_by.items:
        for c in item.concept_arguments:
            # Already projected (directly, or as the rowset handle the order-by
            # names) — it renders from the output, no carry needed.
            if c.address in output_addresses:
                continue
            target = _find_source_target(c)
            if target is None or target.address in output_addresses:
                continue
            carry.setdefault(target.address, target)
    if not carry:
        return
    build_statement.selection = build_statement.selection + list(carry.values())
    build_statement.hidden_components = build_statement.hidden_components | set(carry)
    build_statement.grain = build_statement.grain + BuildGrain.from_concepts(
        list(carry.values())
    )


def _find_source_target(concept: BuildConcept) -> BuildConcept | None:
    """The union column an order-by concept ultimately renders from, or None.

    A `union(...)`/multiselect output column is rendered through `find_source`,
    which resolves only at the union node — unlike a plain column it can't be
    referenced from a downstream CTE once grouped away. Walk through a rowset
    handle to the union column it wraps and return that column (the thing to
    carry into the grain); a plain concept returns None (no carry needed)."""
    lineage = concept.lineage
    if isinstance(lineage, BuildMultiSelectLineage):
        return concept
    if isinstance(lineage, BuildRowsetItem):
        return _find_source_target(lineage.content)
    return None


def _get_query_node_v4(
    build_statement: BuildSelectLineage | BuildMultiSelectLineage,
    build_environment: BuildEnvironment,
    graph: ReferenceGraph,
    conditions: BuildWhereClause | None,
    history: History,
) -> StrategyNode:
    """v4 discovery entrypoint (gated by CONFIG.use_v4_discovery).

    Mirrors the tail of `get_query_node`, but the root node comes from the v4
    planner, which returns a fully-grouped FINAL node (no `group_if_required_v2`)
    and may have promoted grain keys to hidden — so hidden_concepts are merged,
    not overwritten."""
    info = search_concepts_v4(
        mandatory_list=list(build_statement.output_components),
        # Inherit the outer resolution's build caches — chiefly `scoped_joins`,
        # the query-scoped JOIN merges. Sub-selects (rowsets, multiselect arms)
        # materialize their own build env via these caches; a fresh BuildCaches
        # would drop the merges, leaving a cross-fact rowset join unresolvable
        # (q29: `inner join catalog_sales.* = physical_sales.*` on the outer
        # select feeds the rowset's combined source).
        history=V4History(
            base_environment=history.base_environment,
            build_caches=history.build_caches,
        ),
        environment=build_environment,
        depth=0,
        g=graph,
        conditions=[conditions] if conditions else [],
    )
    ds = info.strategy_node
    if ds is None:
        error_strings = [
            f"{c.address}<{c.purpose}>{c.derivation}>"
            for c in build_statement.output_components
        ]
        raise UnresolvableQueryException(
            f"Could not resolve connections for query with output {error_strings} "
            "from current model (v4 discovery)."
        )
    if build_statement.having_clause:
        final = build_statement.having_clause.conditional
        if ds.conditions:
            final = BuildConditional(
                left=ds.conditions,
                right=build_statement.having_clause.conditional,
                operator=BooleanOperator.AND,
            )
        ds = SelectNode(
            output_concepts=build_statement.output_components,
            input_concepts=ds.usable_outputs,
            parents=[ds],
            environment=ds.environment,
            partial_concepts=ds.partial_concepts,
            conditions=final,
        )
    ds.hidden_concepts = set(ds.hidden_concepts or set()) | set(
        build_statement.hidden_components
    )
    ds.ordering = build_statement.order_by
    ds.rebuild_cache()
    requested = {
        c.address
        for c in build_statement.output_components
        if c.address not in build_statement.hidden_components
    }
    partial_requested = requested & {c.address for c in ds.partial_concepts}
    if partial_requested:
        raise UnresolvableQueryException(
            f"Query is unresolvable: no complete sources found for output concepts"
            f" {partial_requested}. These concepts could only be resolved from partial sources."
        )
    return ds


def get_query_node(
    environment: Environment,
    statement: SelectLineage | MultiSelectLineage,
    history: History | None = None,
    scoped_joins: list[tuple[str, str, JoinType]] | None = None,
) -> StrategyNode:
    if not statement.output_components:
        raise ValueError(f"Statement has no output components {statement}")
    history = history or History(base_environment=environment)
    logger.info(
        f"{LOGGER_PREFIX} building query node for {statement.output_components} grain {statement.grain}"
    )
    # Caches live on History so every sub-select (rowsets, multiselect arms)
    # in this resolution reuses the base environment's materialized concepts.
    caches = history.build_caches
    # Query-scoped JOINs are applied during the build, not by cloning the author
    # env: each Factory collapses merged-away source concepts to their canonical
    # target in `_build_concept` (and marks partial datasource bindings). Stored
    # on caches so nested sub-selects inherit the same merges.
    if scoped_joins:
        caches.scoped_joins = scoped_joins
    # INNER global `merge`s collapse concepts exactly like a scoped INNER join;
    # fold them into the same build-time mechanism so both share one path (and the
    # scoped-join discovery fixes cover merges too). `~` (LEFT/enrichment) merges
    # are NOT in environment.merges — they stay on the pseudonym path. Idempotent:
    # nested sub-selects inherit the same caches, so only absent pairs are added.
    if environment.merges:
        existing = set(caches.scoped_joins)
        caches.scoped_joins = caches.scoped_joins + [
            m for m in environment.merges if m not in existing
        ]
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(environment)
    build_cache: dict[str, BuildConcept] = caches.build_cache
    canonical_build_cache: dict[str, BuildConcept] = caches.canonical_build_cache
    base_factory = Factory(
        environment=environment,
        build_cache=build_cache,
        canonical_build_cache=canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
        scoped_joins=caches.scoped_joins,
    )
    build_statement: BuildSelectLineage | BuildMultiSelectLineage = base_factory.build(
        statement
    )

    build_environment = environment.materialize_for_select(
        build_statement.local_concepts,
        build_cache=build_cache,
        pseudonym_map=base_factory.pseudonym_map,
        grain_build_cache=base_factory.grain_build_cache,
        canonical_build_cache=canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
        scoped_joins=caches.scoped_joins,
    )

    _carry_order_by_concepts(build_statement)

    graph = generate_graph(build_environment)

    search_concepts: list[BuildConcept] = list(build_statement.output_components)
    if CONFIG.use_v4_discovery:
        return _get_query_node_v4(
            build_statement=build_statement,
            build_environment=build_environment,
            graph=graph,
            conditions=build_statement.where_clause,
            history=history,
        )

    logger.info(
        f"{LOGGER_PREFIX} getting source datasource for outputs {build_statement.output_components} grain {build_statement.grain}"
    )

    # A tautological `X IS NOT NULL` (X provably non-null given the actual join
    # tree) is dropped later by the StripRedundantNotNull optimization rule,
    # which operates on the built CTEs where join types and nullability are
    # known — pre-resolution we can't tell whether an outer join pads X.
    ods: StrategyNode = source_query_concepts(
        output_concepts=search_concepts,
        environment=build_environment,
        g=graph,
        conditions=build_statement.where_clause,
        history=history,
    )
    if not ods:
        raise ValueError(
            f"Could not find source query concepts for {[x.address for x in search_concepts]}"
        )
    ds: StrategyNode = ods
    if build_statement.having_clause:
        having = build_statement.having_clause.conditional
        # A HAVING filters the SELECT outputs, which a resolved merge/select
        # node already carries — so fold the predicate onto that node rather
        # than wrapping it in a fresh SelectNode. The wrapper adds a CTE level
        # that masks the node's join-key grain anchors and triggers a spurious
        # regroup in a downstream consumer (e.g. a rowset's outer select, q68).
        if isinstance(ds, (MergeNode, SelectNode)):
            ds.add_condition(having)
        else:
            final = having
            if ds.conditions:
                final = BuildConditional(
                    left=ds.conditions,
                    right=having,
                    operator=BooleanOperator.AND,
                )
            ds = SelectNode(
                output_concepts=build_statement.output_components,
                input_concepts=ds.usable_outputs,
                parents=[ds],
                environment=ds.environment,
                partial_concepts=ds.partial_concepts,
                conditions=final,
            )
        # Source any existence (`x in <set>`) args onto the node now carrying the
        # HAVING, mirroring the WHERE path — the predicate's subselect must read
        # from a real producer CTE, not a dangling reference. Idempotent, so the
        # fold branch (which may already carry the set) is a no-op.
        append_existence_check(
            ds, build_environment, graph, build_statement.having_clause, history
        )
    ds.hidden_concepts = build_statement.hidden_components
    ds.ordering = build_statement.order_by
    # TODO: avoid this
    ds.rebuild_cache()
    requested = {
        c.address
        for c in build_statement.output_components
        if c.address not in build_statement.hidden_components
    }
    partial_requested = requested & {c.address for c in ds.partial_concepts}
    if partial_requested:
        raise UnresolvableQueryException(
            f"Query is unresolvable: no complete sources found for output concepts"
            f" {partial_requested}. These concepts could only be resolved from partial sources."
        )
    return ds


def get_query_datasources(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    hooks: Optional[List[BaseHook]] = None,
) -> QueryDatasource:
    join_clauses = (
        statement.join_clauses if isinstance(statement, SelectStatement) else []
    )
    scoped_joins = [
        (j.source_address, j.target_address, j.join_type) for j in join_clauses
    ]
    ds = get_query_node(
        environment, statement.as_lineage(environment), scoped_joins=scoped_joins
    )

    final_qds = ds.resolve()

    if hooks:
        for hook in hooks:
            hook.process_root_strategy_node(ds)

    return final_qds


def flatten_ctes(input: CTE | UnionCTE) -> list[CTE | UnionCTE]:
    output = [input]
    for cte in input.parent_ctes:
        output += flatten_ctes(cte)
    return output


def _collect_unreachable_union_arms(
    ctes: list[CTE | UnionCTE],
) -> list[CTE | UnionCTE]:
    """A union's arms live in ``internal_ctes``, not ``parent_ctes``, so
    ``flatten_ctes`` only reaches an arm when something else references it (a
    join, a shared base). An arm reachable ONLY through its union — e.g. a rename
    projection sitting above a grouped arm — is otherwise never emitted, leaving
    the union pointing at an undefined CTE. Add exactly those, by name (a
    distinct same-named instance is already covered)."""
    reachable = {c.name for c in ctes}
    extra: list[CTE | UnionCTE] = []
    for cte in ctes:
        if not isinstance(cte, UnionCTE):
            continue
        for arm in cte.internal_ctes:
            for node in flatten_ctes(arm):
                if node.name not in reachable:
                    reachable.add(node.name)
                    extra.append(node)
    return extra


def process_auto(
    environment: Environment,
    statement: PersistStatement | SelectStatement,
    hooks: List[BaseHook] | None = None,
):
    if isinstance(statement, PersistStatement):
        return process_persist(environment, statement, hooks)
    elif isinstance(statement, SelectStatement):
        return process_query(environment, statement, hooks)
    elif isinstance(statement, ConceptDeclarationStatement):
        return None
    raise ValueError(f"Do not know how to process {type(statement)}")


def process_persist(
    environment: Environment,
    statement: PersistStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedQueryPersist:
    ds: Datasource = environment.datasources.get(
        statement.datasource.identifier, statement.datasource
    )
    original_status = ds.status
    # For partial datasources, scope the source query to the partition condition so
    # the planner treats sources with matching non_partial_for as non-partial and
    # selects them directly rather than resorting to a covering union.
    select_stmt = statement.select
    # Only inject non_partial_for for explicitly declared partial datasources.
    # Datasources created from a persist-with-WHERE already embed the condition
    # in the SELECT, so injecting again would duplicate it.
    if ds.is_partial and ds.non_partial_for:
        if select_stmt.where_clause is None:
            select_stmt = replace(select_stmt, where_clause=ds.non_partial_for)
        else:
            select_stmt = replace(
                select_stmt,
                where_clause=WhereClause(
                    conditional=Conditional(
                        left=ds.non_partial_for.conditional,
                        right=select_stmt.where_clause.conditional,
                        operator=BooleanOperator.AND,
                    )
                ),
            )
    # set to unpublished to avoid circular refs
    try:
        ds.status = DatasourceState.UNPUBLISHED
        select = process_query(
            environment=environment, statement=select_stmt, hooks=hooks
        )
    except:
        raise
    finally:
        ds.status = original_status

    # build our object to return
    arg_dict = {k: v for k, v in select.__dict__.items()}
    partition_by: list[str] = []
    partition_types: list[DataType] = []
    for addr in statement.partition_by:
        for c in statement.datasource.columns:
            if c.concept.address == addr and c.is_concrete:
                partition_by.append(c.alias)  # type: ignore
                partition_types.append(c.concept.output_datatype)
                break
    return ProcessedQueryPersist(
        **arg_dict,
        output_to=MaterializedDataset(address=statement.address),
        persist_mode=statement.persist_mode,
        partition_by=partition_by,
        datasource=statement.datasource,
        partition_types=partition_types,
    )


def process_copy(
    environment: Environment,
    statement: CopyStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedCopyStatement | ProcessedChartCopyStatement:
    if isinstance(statement.select, ChartStatement):
        chart = process_chart(
            environment=environment, statement=statement.select, hooks=hooks
        )
        return ProcessedChartCopyStatement(
            target=statement.target,
            target_type=statement.target_type,
            options=dict(statement.options),
            chart=chart,
        )
    select = process_query(
        environment=environment, statement=statement.select, hooks=hooks
    )

    # build our object to return
    arg_dict = {k: v for k, v in select.__dict__.items()}
    return ProcessedCopyStatement(
        **arg_dict,
        target=statement.target,
        target_type=statement.target_type,
        options=dict(statement.options),
    )


def _binding_safe_address(binding, environment: Environment) -> str:
    from trilogy.core.models.author import compute_safe_address

    if binding.alias is not None:
        namespace = environment.namespace or DEFAULT_NAMESPACE
        return compute_safe_address(namespace, binding.alias)
    if isinstance(binding.expr, ConceptRef):
        return binding.expr.safe_address
    raise ValueError(
        f"Chart binding for role '{binding.role}' has a computed expression"
        " without an alias"
    )


def _process_chart_layer(
    environment: Environment,
    layer: ChartLayer,
    hooks: List[BaseHook] | None,
) -> ProcessedChartLayer:
    if layer.select is None:
        raise ValueError("Chart layer is missing a resolved select statement")
    select = process_query(environment=environment, statement=layer.select, hooks=hooks)
    output_fields = {c.safe_address for c in layer.select.output_components}

    role_map: Dict[str, str] = {}
    for binding in layer.bindings:
        safe = _binding_safe_address(binding, environment)
        if safe not in output_fields:
            raise ValueError(
                f"Chart role '{binding.role}' resolves to '{safe}' which is"
                f" not in select output: {output_fields}"
            )
        role_map[binding.role] = safe

    def _single(role: str) -> str | None:
        return role_map.get(role)

    x_field = role_map.get("x_axis")
    y_field = role_map.get("y_axis")
    return ProcessedChartLayer(
        layer_type=layer.layer_type,
        query=select,
        x_fields=[x_field] if x_field else [],
        y_fields=[y_field] if y_field else [],
        color_field=_single("color"),
        size_field=_single("size"),
        group_field=_single("group"),
        x_trellis_field=_single("x_trellis"),
        y_trellis_field=_single("y_trellis"),
        geo_field=_single("geo"),
        annotation_field=_single("annotation"),
    )


def process_chart(
    environment: Environment,
    statement: ChartStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedChartStatement:
    layers = [
        _process_chart_layer(environment, layer, hooks) for layer in statement.layers
    ]
    return ProcessedChartStatement(
        layers=layers,
        placements=list(statement.placements),
        hide_legend=statement.hide_legend,
        show_title=statement.show_title,
        scale_x=statement.scale_x,
        scale_y=statement.scale_y,
    )


def process_query(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    hooks: List[BaseHook] | None = None,
    having_alias: bool = False,
) -> ProcessedQuery:
    hooks = hooks or []

    root_datasource = get_query_datasources(
        environment=environment, statement=statement, hooks=hooks
    )
    for hook in hooks:
        hook.process_root_datasource(root_datasource)
    # this should always return 1 - TODO, refactor
    root_cte = datasource_to_cte(root_datasource, environment.cte_name_map)

    for hook in hooks:
        hook.process_root_cte(root_cte)
    flattened = flatten_ctes(root_cte)
    flattened = _collect_unreachable_union_arms(flattened) + flattened
    raw_ctes: List[CTE | UnionCTE] = list(reversed(flattened))
    seen = dict()
    # we can have duplicate CTEs at this point
    # so merge them together
    for cte in raw_ctes:
        if cte.name not in seen:
            seen[cte.name] = cte
        else:
            # merge them up
            seen[cte.name] = seen[cte.name] + cte
    for cte in raw_ctes:
        cte.parent_ctes = [seen[x.name] for x in cte.parent_ctes]
    deduped_ctes: List[CTE | UnionCTE] = list(seen.values())

    root_cte.limit = statement.limit
    root_cte.hidden_concepts = statement.hidden_components

    join_clauses = (
        statement.join_clauses if isinstance(statement, SelectStatement) else []
    )
    scoped_merge_map, _ = _build_scoped_merge_index(
        [(j.source_address, j.target_address, j.join_type) for j in join_clauses]
    )
    # Canonical keys of query-scoped FULL joins — flagged so the outer-join
    # upgrade optimization never collapses an explicit FULL back to INNER.
    full_join_keys = {
        scoped_merge_map.get(addr, addr)
        for j in join_clauses
        if j.join_type is JoinType.FULL
        for addr in (j.source_address, j.target_address)
    }

    final_ctes = optimize_ctes(
        deduped_ctes,
        root_cte,
        statement,
        having_alias=having_alias,
        full_join_keys=full_join_keys,
    )

    return ProcessedQuery(
        order_by=root_cte.order_by,
        limit=statement.limit,
        output_columns=statement.output_components,
        ctes=final_ctes,
        base=root_cte,
        hidden_columns=set([x for x in statement.hidden_components]),
        local_concepts=statement.local_concepts,
        locally_derived=statement.locally_derived,
        parameters=_extract_params(environment.concepts, statement.local_concepts),
        scoped_merge_map=scoped_merge_map,
    )
