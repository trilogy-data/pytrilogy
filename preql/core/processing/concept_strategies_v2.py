from collections import defaultdict
from copy import deepcopy
from typing import List, Optional, Set

import networkx as nx

from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import (
    Concept,
    Environment,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
    FilterItem,
    SourceType,
)
from preql.core.processing.utility import PathInfo, path_to_joins
from preql.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL]"


def resolve_concept_map(inputs: List[QueryDatasource]):
    concept_map = defaultdict(set)
    for input in inputs:
        for concept in input.output_concepts:
            concept_map[concept.address].add(input)
    return concept_map


def concept_list_to_grain(
    inputs: List[Concept], parent_sources: List[QueryDatasource]
) -> Grain:
    candidates = [c for c in inputs if c.purpose == Purpose.KEY]
    for x in inputs:
        if x.purpose == Purpose.PROPERTY and not any(
            [key in candidates for key in (x.keys or [])]
        ):
            candidates.append(x)
        elif x.purpose == Purpose.METRIC:
            # metrics that were previously calculated must be included in grain
            if any([x in parent.output_concepts for parent in parent_sources]):
                candidates.append(x)

    return Grain(components=candidates)


class StrategyNode:
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        self.mandatory_concepts = mandatory_concepts
        self.optional_concepts = deepcopy(optional_concepts)
        self.environment = environment
        self.g = g
        self.whole_grain = whole_grain
        self.parents = parents or []

    @property
    def all_concepts(self):
        return unique(
            deepcopy(self.mandatory_concepts + self.optional_concepts), "address"
        )

    def __repr__(self):
        concepts = self.all_concepts
        contents = ",".join([c.address for c in concepts])
        return f"{self.__class__.__name__}<{contents}>"

    def resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        conditions = [
            c.lineage.where.conditional
            for c in self.mandatory_concepts
            if isinstance(c.lineage, FilterItem)
        ]
        conditional = conditions[0] if conditions else None
        if conditional:
            for condition in conditions[1:]:
                conditional += condition
        grain = Grain()
        output_concepts = self.all_concepts
        for source in parent_sources:
            grain += source.grain
            output_concepts += source.grain.components_copy
        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=unique(self.all_concepts, "address"),
            datasources=parent_sources,
            source_type=self.source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=[],
            grain=grain,
            condition=conditional,
        )


class WindowNode(StrategyNode):
    source_type = SourceType.WINDOW

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )


class FilterStrategyNode(StrategyNode):
    source_type = SourceType.FILTER

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def resolve(self) -> QueryDatasource:
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super().resolve()
        filtered_concepts = [
            c for c in self.all_concepts if isinstance(c.lineage, FilterItem)
        ]
        to_remove = [c.lineage.content.address for c in filtered_concepts]
        base.output_concepts = [
            c for c in base.output_concepts if c.address not in to_remove
        ]
        base.source_map = {
            key: value for key, value in base.source_map.items() if key not in to_remove
        }
        return base


class GroupNode(StrategyNode):
    source_type = SourceType.GROUP

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        # a group by node only outputs the actuall keys grouped by
        outputs = unique(self.mandatory_concepts + self.optional_concepts, "address")
        grain = concept_list_to_grain(self.all_concepts, [])
        comp_grain = Grain()
        for source in parent_sources:
            comp_grain += source.grain

        # dynamically select if we need to group
        # because sometimes, we are already at required grain
        if comp_grain == grain and set([c.address for c in outputs]) == set(
            [c.address for c in input_concepts]
        ):
            # if there is no group by, and inputs equal outputs
            # return the parent
            if len(parent_sources) == 1:
                return parent_sources[0]
            # otherwise if no group by, just treat it as a select
            source_type = SourceType.SELECT
        else:
            source_type = SourceType.GROUP
        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=outputs,
            datasources=parent_sources,
            source_type=source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=[],
            grain=grain,
        )


class OutputNode(StrategyNode):
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )


class MergeNode(StrategyNode):
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def resolve(self):
        parent_sources = [p.resolve() for p in self.parents]
        merged = {}
        for source in parent_sources:
            if source.identifier in merged:
                merged[source.identifier] = merged[source.identifier] + source
            else:
                merged[source.identifier] = source
        # early exit if we can just return the parent
        final_datasets = list(merged.values())
        if len(merged.keys()) == 1:
            final = list(merged.values())[0]
            # restrict outputs to only what should come out of this node
            final.output_concepts = self.all_concepts
            return final
        # if we have multiple candidates, see if one is good enough
        for dataset in final_datasets:
            output_set = set([c.address for c in dataset.output_concepts])
            if all([c.address in output_set for c in self.all_concepts]):
                return dataset

        grain = Grain()
        for source in final_datasets:
            grain += source.grain
        # only finally, join between them for unique values
        dataset_list = sorted(
            final_datasets, key=lambda x: -len(x.grain.components_copy)
        )
        base = dataset_list[0]
        joins = []
        all_concepts = unique(
            self.mandatory_concepts + self.optional_concepts, "address"
        )
        if dataset_list[1:]:
            for right_value in dataset_list[1:]:
                if not grain.components:
                    joins.append(
                        BaseJoin(
                            left_datasource=base,
                            right_datasource=right_value,
                            join_type=JoinType.FULL,
                            concepts=[],
                        )
                    )
                    continue
                joins.append(
                    BaseJoin(
                        left_datasource=base,
                        right_datasource=right_value,
                        join_type=JoinType.LEFT_OUTER,
                        concepts=all_concepts,
                        filter_to_mutual=True,
                    )
                )
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        outputs = unique(self.mandatory_concepts + self.optional_concepts, "address")

        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=outputs,
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=joins,
            grain=grain,
        )


class SelectNode(StrategyNode):
    """Select nodes actually fetch raw data, eitehr
    directly from a table or via joins """

    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def resolve_join(self) -> QueryDatasource:
        join_candidates: List[PathInfo] = []

        all_concepts = self.mandatory_concepts + self.optional_concepts
        all_input_concepts = [*all_concepts]
        for datasource in self.environment.datasources.values():
            all_found = True
            paths = {}
            for bitem in all_concepts:
                item = bitem.with_default_grain()
                try:
                    path = nx.shortest_path(
                        self.g,
                        source=datasource_to_node(datasource),
                        target=concept_to_node(item),
                    )
                    paths[concept_to_node(item)] = path
                except nx.exception.NodeNotFound:
                    # TODO: support Verbose logging mode configuration and reenable tehse
                    # logger.debug(f'{LOGGER_PREFIX} could not find node for {item.address}')
                    all_found = False

                    continue
                except nx.exception.NetworkXNoPath:
                    # logger.debug(f'{LOGGER_PREFIX} could not get to {item.address} from {datasource}')
                    all_found = False
                    continue
            if all_found:
                join_candidates.append({"paths": paths, "datasource": datasource})
        join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
        if not join_candidates:
            required = [c.address for c in all_input_concepts]
            raise ValueError(
                f"Could not find any way to associate required concepts {required}"
            )
        shortest: PathInfo = join_candidates[0]
        source_map = defaultdict(set)
        join_paths: List[BaseJoin] = []
        parents = []
        all_datasets: Set = set()
        all_search_concepts: Set = set()
        for key, value in shortest["paths"].items():
            datasource_nodes = [v for v in value if v.startswith("ds~")]
            concept_nodes = [v for v in value if v.startswith("c~")]
            all_datasets = all_datasets.union(set(datasource_nodes))
            all_search_concepts = all_search_concepts.union(set(concept_nodes))
            root = datasource_nodes[-1]
            source_concept = self.g.nodes[value[-1]]["concept"]
            parents.append(source_concept)
            new_joins = path_to_joins(value, g=self.g)

            join_paths += new_joins
            source_map[source_concept.address].add(self.g.nodes[root]["datasource"])
            # ensure we add in all keys required for joins as inputs
            # even if they are not selected out
            for join in new_joins:
                for jconcept in join.concepts:
                    source_map[jconcept.address].add(join.left_datasource)
                    source_map[jconcept.address].add(join.right_datasource)
                    all_input_concepts.append(jconcept)
        final_grain = Grain()
        datasources = sorted(
            [self.g.nodes[key]["datasource"] for key in all_datasets],
            key=lambda x: x.identifier,
        )
        all_outputs = []
        for datasource in datasources:
            final_grain += datasource.grain
            all_outputs += datasource.output_concepts
        output = QueryDatasource(
            output_concepts=unique(self.all_concepts, "address"),
            input_concepts=unique(all_input_concepts, "address"),
            source_map=source_map,
            grain=final_grain,
            datasources=datasources,
            joins=join_paths,
        )
        return output

    def resolve_from_raw_datasources(self) -> Optional[QueryDatasource]:
        whole_grain = True
        # TODO evaluate above
        if whole_grain:
            valid_matches = ["all"]
        else:
            valid_matches = ["all", "partial"]
        all_concepts = self.mandatory_concepts + self.optional_concepts
        # 2023-03-30 strategy is not currently used
        # but keeping it here for future use
        for strategy in valid_matches[:1]:
            for datasource in self.environment.datasources.values():
                # whole grain determines
                # if we can get a partial grain match
                # such as joining through a table with a PK to get properties
                # sometimes we need a source with all grain keys, in which case we
                # force this not to match

                # if strategy == "partial":
                #     if not datasource.grain.issubset(grain):
                #         continue
                # else:
                #     # either an exact match
                #     # or it's a key on the table
                #     if not datasource.grain == grain:
                #         continue
                all_found = True
                for raw_concept in all_concepts:
                    # look for connection to abstract grain
                    req_concept = raw_concept.with_default_grain()
                    path = []
                    try:
                        path = nx.shortest_path(
                            self.g,
                            source=datasource_to_node(datasource),
                            target=concept_to_node(req_concept),
                        )
                    except nx.NodeNotFound as e:
                        candidates = [
                            datasource_to_node(datasource),
                            concept_to_node(req_concept),
                        ]
                        for candidate in candidates:
                            try:
                                self.g.nodes[candidate]
                            except KeyError:
                                raise SyntaxError(
                                    "Could not find node for {}".format(candidate)
                                )
                        raise e
                    except nx.exception.NetworkXNoPath:
                        all_found = False
                        break
                    if (
                        len(
                            [p for p in path if self.g.nodes[p]["type"] == "datasource"]
                        )
                        != 1
                    ):
                        all_found = False
                        break
                if all_found:
                    # keep all concepts on the output, until we get to a node which requires reduction
                    return QueryDatasource(
                        input_concepts=unique(all_concepts, "address"),
                        output_concepts=unique(self.all_concepts, "address"),
                        source_map={
                            concept.address: {datasource}
                            for concept in self.all_concepts
                        },
                        datasources=[datasource],
                        grain=datasource.grain,
                        joins=[],
                    )
        return None

    def resolve(self) -> QueryDatasource:
        # if we have parent nodes, treat this as a normal select
        if self.parents:
            return super().resolve()
        # otherwise, look if there is a datasource in the graph
        raw = self.resolve_from_raw_datasources()

        if raw:
            return raw
        # if we don't have a select, see if we can join
        return self.resolve_join()


def resolve_window_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, WindowItem):
        raise ValueError
    base = [concept.lineage.content]
    if concept.lineage.over:
        base += concept.lineage.over
    if concept.lineage.order_by:
        for item in concept.lineage.order_by:
            base += [item.expr.output]
    return unique(base, "address")


def resolve_filter_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError
    base = [concept.lineage.content]
    base += concept.lineage.where.input
    return unique(base, "address")


def resolve_function_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, Function):
        raise ValueError(f"Concept {concept} is not an aggregate function")
    if concept.derivation == PurposeLineage.AGGREGATE:
        if concept.grain:
            return unique(
                concept.lineage.concept_arguments + concept.grain.components_copy,
                "address",
            )
        return concept.lineage.concept_arguments
    # TODO: handle basic lineage chains?
    return unique(concept.lineage.concept_arguments, "address")


def source_concepts(
    mandatory_concepts: List[Concept],
    optional_concepts: List[Concept],
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
) -> StrategyNode:
    g = g or generate_graph(environment)
    stack: List[StrategyNode] = []
    all_concepts = unique(mandatory_concepts + optional_concepts, "address")
    # TODO
    # Loop through all possible grains + subgrains
    # Starting with the most grain
    found_addresses = []

    # early exit
    while not all(c.address in found_addresses for c in all_concepts):
        remaining_concept = [
            c for c in all_concepts if c.address not in found_addresses
        ]
        priority = (
            [c for c in remaining_concept if c.derivation == PurposeLineage.AGGREGATE]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.WINDOW]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.FILTER]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.BASIC]
            + [c for c in remaining_concept if not c.lineage]
        )
        concept = priority[0]
        # we don't actually want to look for multiple aggregates at the same time
        # local optional should be relevant keys, but not metrics
        local_optional_staging = unique(
            [
                x
                for x in optional_concepts + mandatory_concepts
                if x.address
                != concept.address  # and #not x.derivation== PurposeLineage.AGGREGATE
            ],
            "address",
        )

        # reduce search space to actual grain
        local_optional = concept_list_to_grain(
            local_optional_staging, []
        ).components_copy
        if concept.lineage:
            if concept.derivation == PurposeLineage.WINDOW:
                parent_concepts = resolve_window_parent_concepts(concept)
                stack.append(
                    WindowNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            elif concept.derivation == PurposeLineage.FILTER:
                parent_concepts = resolve_filter_parent_concepts(concept)
                stack.append(
                    FilterStrategyNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            elif concept.derivation == PurposeLineage.AGGREGATE:
                # aggregates MUST always group to the proper grain
                # except when the
                parent_concepts = unique(
                    resolve_function_parent_concepts(concept), "address"
                )
                # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
                if len(concept.grain.components_copy) > 0:
                    local_optional = concept.grain.components_copy
                # otherwise, local optional are mandatory
                else:
                    parent_concepts += local_optional
                stack.append(
                    GroupNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[source_concepts(parent_concepts, [], environment, g)],
                    )
                )
            elif concept.derivation == PurposeLineage.BASIC:
                # directly select out a basic derivation
                parent_concepts = resolve_function_parent_concepts(concept)

                stack.append(
                    SelectNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            else:
                raise ValueError(f"Unknown lineage type {concept.derivation}")
        else:
            basic_inputs = [x for x in local_optional if x.lineage is None]
            stack.append(SelectNode([concept], basic_inputs, environment, g))
        for node in stack:
            for concept in node.all_concepts:
                found_addresses.append(concept.address)
    return MergeNode(
        mandatory_concepts, optional_concepts, environment, g, parents=stack
    )


def source_query_concepts(
    output_concepts,
    grain_components,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
):
    root = source_concepts(output_concepts, grain_components, environment, g)
    return GroupNode(output_concepts, grain_components, environment, g, parents=[root])