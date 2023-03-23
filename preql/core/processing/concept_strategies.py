from collections import defaultdict
from typing import List, Optional, Union, Set, Dict

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import (
    Concept,
    Environment,
    Datasource,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
    FilterItem
)
from preql.core.processing.utility import concept_to_inputs, PathInfo, path_to_joins
from preql.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL]"


def get_datasource_from_direct_select(
    concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    """Return a datasource a concept can be directly selected from at
    appropriate grain. Think select * from table."""
    all_concepts = concept_to_inputs(concept)
    for datasource in environment.datasources.values():
        if not datasource.grain.issubset(grain):
            continue
        all_found = True
        for req_concept in all_concepts:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.exception.NodeNotFound:
                all_found = False
                break
            except nx.exception.NetworkXNoPath:
                all_found = False
                break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
        if all_found:
            if datasource.grain.issubset(grain):
                final_grain = datasource.grain
            else:
                final_grain = grain
            logger.debug(
                f"{LOGGER_PREFIX} Got {datasource.identifier} for {concept} from direct select"
            )
            base_outputs = [concept] + final_grain.components_copy
            for item in datasource.concepts:
                if item.address in [c.address for c in grain.components_copy]:
                    base_outputs.append(item)
            return QueryDatasource(
                output_concepts=unique(base_outputs, "address"),
                input_concepts=all_concepts,
                source_map={concept.address: {datasource} for concept in all_concepts},
                datasources=[datasource],
                grain=final_grain,
                joins=[],
            )
    raise ValueError(f"No direct select for {concept} and grain {grain}")


def get_datasource_from_property_lookup(
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    whole_grain: bool = False,
) -> QueryDatasource:
    """Return a datasource that has a direct property/key relation
    If the datasource is a component of the grain, assume we can
    join it in the final query to the grain level"""
    all_concepts = concept_to_inputs(concept)
    if whole_grain:
        valid_matches = ["all"]
    else:
        valid_matches = ["all", "partial"]
    for strategy in valid_matches:
        for datasource in environment.datasources.values():
            # whole grain determines
            # if we can get a partial grain match
            # such as joining through a table with a PK to get properties
            # sometimes we need a source with all grain keys, in which case we
            # force this not to match

            if strategy == "partial":
                if not datasource.grain.issubset(grain):
                    continue
            else:
                # either an exact match
                # or it's a key on the table
                if not datasource.grain == grain:
                    continue
            all_found = True
            for req_concept in all_concepts:
                try:
                    path = nx.shortest_path(
                        g,
                        source=datasource_to_node(datasource),
                        target=concept_to_node(req_concept),
                    )
                except nx.exception.NetworkXNoPath:
                    all_found = False
                    break
                if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                    all_found = False
                    break
            if all_found:
                logger.debug(
                    f"{LOGGER_PREFIX} Can satisfy query from property lookup for {concept} using {datasource.identifier} with {strategy} grain"
                )
                outputs = [concept] + datasource.grain.components_copy

                # also pull through any grain component that might exist
                # to facilitate future joins
                final_grain_components = datasource.grain.components_copy
                final_grain_inputs = []
                for concept in grain.components_copy:
                    required = concept_to_inputs(concept)
                    if all(
                        [
                            sconcept.with_grain(datasource.grain)
                            in datasource.output_concepts
                            for sconcept in required
                        ]
                    ):
                        outputs.append(concept)
                        final_grain_components.append(concept)
                        final_grain_inputs += required
                outputs = unique(outputs, "address")
                filters: List[Concept] = []
                return QueryDatasource(
                    input_concepts=all_concepts + final_grain_inputs,
                    output_concepts=outputs,
                    source_map={
                        concept.address: {datasource} for concept in datasource.concepts
                    },
                    datasources=[datasource],
                    grain=Grain(components=final_grain_components),
                    joins=[],
                    filter_concepts=filters,
                )
    raise ValueError(f"No property lookup for {concept}")


def get_datasource_from_group_select(
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    whole_grain: bool = False,
) -> QueryDatasource:
    """Return a datasource that can be grouped to a value and grain.
    Unique values in a column, for example"""
    all_concepts = (
        concept_to_inputs(concept.with_default_grain()) + grain.components_copy
    )

    expanded_concepts = []
    for item in all_concepts:
        expanded_concepts += concept_to_inputs(item)

    for datasource in environment.datasources.values():
        all_found = True
        for req_concept in all_concepts:
            members = concept_to_inputs(req_concept)
            for sub_req_concept in members:
                target = sub_req_concept.with_grain(datasource.grain)
                try:
                    path = nx.shortest_path(
                        g,
                        source=datasource_to_node(datasource),
                        target=concept_to_node(target),
                    )
                except (nx.exception.NetworkXNoPath, nx.exception.NodeNotFound):
                    all_found = False
                    break
                if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                    all_found = False
                    break
        if all_found:
            # if the dataset that can reach every concept
            # is in fact a subset of the concept
            # assume property lookup
            # 2023-02-01 - cannot assume this
            # if datasource.grain.issubset(grain):
            #     final_grain = datasource.grain
            # else:
            #     final_grain = grain
            final_grain = grain
            logger.debug(
                f"{LOGGER_PREFIX} Got {datasource.identifier} for {concept} from grouped select"
            )
            outputs = [concept] + grain.components_copy
            filters: List[Concept] = []
            return QueryDatasource(
                input_concepts=all_concepts,
                output_concepts=outputs,
                source_map={
                    concept.address: {datasource} for concept in expanded_concepts
                },
                datasources=[datasource],
                grain=final_grain,
                joins=[],
                filter_concepts=filters,
            )
    raise ValueError(f"No grouped select for {concept}")


def get_datasource_by_joins(
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    whole_grain: bool = False,
) -> QueryDatasource:
    join_candidates: List[PathInfo] = []

    all_requirements = unique(
        concept_to_inputs(concept) + grain.components_copy, "address"
    )

    for datasource in environment.datasources.values():
        all_found = True
        paths = {}
        for item in all_requirements:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(item),
                )
                paths[concept_to_node(item)] = path
            except nx.exception.NodeNotFound:
                all_found = False
                continue
            except nx.exception.NetworkXNoPath:
                all_found = False
                continue
        if all_found:
            join_candidates.append({"paths": paths, "datasource": datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
    if not join_candidates:
        raise ValueError(f"No joins to get to {concept} and grain {grain}")
    shortest: PathInfo = join_candidates[0]
    source_map = defaultdict(set)
    join_paths: List[BaseJoin] = []
    parents = []
    all_datasets: Set = set()
    all_concepts: Set = set()
    for key, value in shortest["paths"].items():
        datasource_nodes = [v for v in value if v.startswith("ds~")]
        concept_nodes = [v for v in value if v.startswith("c~")]
        all_datasets = all_datasets.union(set(datasource_nodes))
        all_concepts = all_concepts.union(set(concept_nodes))
        root = datasource_nodes[-1]
        source_concept = g.nodes[value[-1]]["concept"]
        parents.append(source_concept)
        new_joins = path_to_joins(value, g=g)

        join_paths += new_joins
        source_map[source_concept.address].add(g.nodes[root]["datasource"])
        # ensure we add in all keys required for joins as inputs
        # even if they are not selected out
        for join in new_joins:
            for jconcept in join.concepts:
                source_map[jconcept.address].add(join.left_datasource)
                source_map[jconcept.address].add(join.right_datasource)
                all_requirements.append(jconcept)
    all_requirements = unique(all_requirements, "address")
    if whole_grain:
        outputs = grain.components_copy
        filters = [concept]
    else:
        outputs = [concept] + grain.components_copy
        filters = []
    output = QueryDatasource(
        output_concepts=outputs,
        input_concepts=all_requirements,
        source_map=source_map,
        grain=grain,
        datasources=sorted(
            [g.nodes[key]["datasource"] for key in all_datasets],
            key=lambda x: x.identifier,
        ),
        joins=join_paths,
        filter_concepts=filters,
    )
    logger.debug(f"{LOGGER_PREFIX} Got {concept} from joins")
    return output


def get_datasource_from_complex_lineage(
    concept: Concept, grain: Grain, environment, g, whole_grain: bool = False
):
    # always true if window item
    complex_lineage_flag = isinstance(concept.lineage, WindowItem)
    all_requirements = []
    all_datasets = []
    source_map = {}
    # for anything in function lineage, calculate to current grain
    if not isinstance(concept.lineage, Function):
        raise ValueError(
            "Attempting to get complex lineage from non-function declaration"
        )
    for sub_concept in concept.lineage.arguments:
        if not isinstance(sub_concept, Concept):
            continue
        # if aggregate of aggregate
        if sub_concept.derivation in (PurposeLineage.AGGREGATE, PurposeLineage.WINDOW):
            complex_lineage_flag = True
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, sub_concept.grain + grain, environment=environment, g=g
        )
        all_datasets.append(sub_datasource)
        all_requirements.append(sub_concept)
        source_map[sub_concept.address] = {sub_datasource}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}

    # for grain components, build in CTE if required
    for sub_concept in grain.components_copy:
        if sub_concept.derivation in (PurposeLineage.AGGREGATE, PurposeLineage.WINDOW):
            complex_lineage_flag = True
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, sub_concept.grain or Grain(), environment=environment, g=g
        )
        all_datasets.append(sub_datasource)
        all_requirements.append(sub_concept)
        source_map[sub_concept.address] = {sub_datasource}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}
    if complex_lineage_flag:
        logger.debug(
            f"{LOGGER_PREFIX} Complex lineage found for {concept} at grain {grain}"
        )
        qds = QueryDatasource(
            output_concepts=[concept] + grain.components_copy,
            input_concepts=all_requirements,
            source_map=source_map,
            grain=grain,
            datasources=all_datasets,
            joins=[]
            # joins=join_paths,
        )
        return qds


def get_property_group_by_without_key(
    concept: Concept, grain: Grain, environment, g, whole_grain: bool = False
):
    """If a query requires things at a property grain, but does not include the property
    key - first search for a query at target grain with all keys, then group up to the property level"""
    all_requirements = []
    all_datasets = []
    source_map = {}
    joins: List[BaseJoin] = []
    # early exit if no properties in the grain
    if not any([x.purpose == Purpose.PROPERTY for x in grain.components_copy]):
        raise ValueError("Cannot find property lookup")
    pgrain = Grain(
        components=[
            z
            if z.purpose != Purpose.PROPERTY
            else z.with_default_grain().grain_components[0]
            for z in grain.components_copy
        ]
    )
    # we require this sub datasource to match on all grain components
    sub_datasource = get_datasource_by_concept_and_grain(
        concept.with_default_grain().grain_components[0],
        grain=pgrain,
        environment=environment,
        g=g,
        whole_grain=True,
    )
    all_datasets.append(sub_datasource)
    all_requirements.append(concept)
    source_map[concept.address] = {sub_datasource}
    if isinstance(sub_datasource, QueryDatasource):
        source_map = {**source_map, **sub_datasource.source_map}
    remapped = [z for z in grain.components_copy if z.purpose == Purpose.PROPERTY]
    output_concepts = sub_datasource.output_concepts
    for remapped_property in remapped:
        # we don't need another source if we already have this
        if remapped_property in sub_datasource.output_concepts:
            continue
        remap_grain = remapped_property.with_default_grain().grain or Grain()
        keys = remap_grain.components_copy
        for key in keys:
            all_requirements.append(key)
        # this may not have all keys
        remapped_datasource = get_datasource_by_concept_and_grain(
            remapped_property,
            grain=remap_grain,
            environment=environment,
            g=g,
            whole_grain=whole_grain,
        )
        output_concepts = unique(
            output_concepts + remapped_datasource.output_concepts, "address"
        )
        all_datasets.append(remapped_datasource)
        all_requirements.append(remapped_property)
        source_map[remapped_property.address] = {remapped_datasource}
        if isinstance(remapped_datasource, QueryDatasource):
            source_map = {**source_map, **remapped_datasource.source_map}
        join_concepts = [
            k
            for k in keys
            if k in remapped_datasource.output_concepts
            and k in sub_datasource.output_concepts
        ]
        if join_concepts:
            joins.append(
                BaseJoin(
                    left_datasource=sub_datasource,
                    right_datasource=remapped_datasource,
                    join_type=JoinType.LEFT_OUTER,
                    concepts=join_concepts,
                )
            )
    qds = QueryDatasource(
        output_concepts=[concept] + grain.components_copy,
        input_concepts=all_requirements,
        source_map=source_map,
        grain=grain,
        datasources=all_datasets,
        joins=joins
        # joins=join_paths,
    )
    source_map[concept.address] = {qds}
    return qds


def get_datasource_from_window_function(
    concept: Concept, grain: Grain, environment, g, whole_grain: bool = False
):
    if not isinstance(concept.lineage, WindowItem):
        raise ValueError(
            "Attempting to use windowed derivation for non window function"
        )
    window: WindowItem = concept.lineage
    all_datasets: Dict = {}
    source_map = {}

    window_grain = [window.content.output]

    output_concepts = [concept, window.content.output] #+ grain.components_copy
    sub_concepts = unique([window.content.output] + grain.components_copy, "identifier")

    # order by statements need to be pulled in
    # but include them to optimize fetching
    for oarg in window.order_by:
        sub_concepts += [oarg.output]

    for warg in window.over:
        sub_concepts += [warg]
        # make sure to move this to the appropriate grain
        window_grain += [warg.output]
        output_concepts += [warg]

    cte_grain = Grain(components=window_grain)
    # window grouping need to be included in sources and in output
    # to support future joins

    for sub_concept in sub_concepts:
        sub_concept = sub_concept.with_grain(cte_grain)
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, cte_grain, environment=environment, g=g
        )
        if sub_datasource.identifier in all_datasets:
            all_datasets[sub_datasource.identifier] = (
                all_datasets[sub_datasource.identifier] + sub_datasource
            )
        else:
            all_datasets[sub_datasource.identifier] = sub_datasource
        # all_requirements.append(sub_concept)
        source_map[sub_concept.address] = {all_datasets[sub_datasource.identifier]}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}
    dataset_list = list(all_datasets.values())
    base = dataset_list[0]

    joins = []
    for right_value in dataset_list[1:]:
        joins.append(
            BaseJoin(
                left_datasource=base,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=[c for c in cte_grain.components_copy],
                filter_to_mutual=True,
            )
        )

    # we want to find any members of the grain that we can get from the cte grain
    # if this is a subset of the final target grain, that is fine
    # TODO: unless ALL is set
    final_grain_components = [item for item in cte_grain.components_copy if item in grain.components_copy]
    datasources = list(all_datasets.values())
    qds = QueryDatasource(
        output_concepts=output_concepts,
        input_concepts=sub_concepts,
        source_map=source_map,
        grain=Grain(components=final_grain_components),
        datasources=datasources,
        joins=joins,
    )
    if not qds.group_required:
        return qds
    # we can't group a row_number
    # so if we would group a window function
    # we must nest it one level further
    # and assume the base query MUST have the grain of all
    # component datasources

    qds2 = QueryDatasource(
        output_concepts=output_concepts,
        input_concepts=sub_concepts,
        source_map=source_map,
        # the grain HAS to be the grain of all component datasources
        grain=sum([ds.grain for ds in datasources]),
        datasources=datasources,
        joins=joins,
    )
    # first ensure that the grain of the first CTE does not require grouping

    # restrict our new one to just output the components we want

    # now create a new CTE, n which the group by will take place
    final_grain_components = [item for item in cte_grain.components_copy if item in grain.components_copy]
    final_grain = Grain(components=final_grain_components)
    final_outputs = [concept] + [x.with_grain(final_grain) for x in final_grain_components]
    # parent query data source for group by
    pqds = QueryDatasource(
        output_concepts=final_outputs,
        input_concepts=output_concepts,
        source_map={c.address: {qds2} for c in final_outputs},
        grain=final_grain,
        datasources=[qds2],
        joins=[],
    )
    return pqds


def get_datasource_for_filter(
    concept: Concept, grain: Grain, environment, g, whole_grain: bool = False
):
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError(
            "Attempting to use filter derivation for non-filtered Item"
        )
    filter: FilterItem = concept.lineage
    all_datasets: Dict = {}
    source_map = {}

    target_concept = [concept]

    required_inputs = filter.input


    cte_grain = Grain(components=[filter.content]+required_inputs)
    # window grouping need to be included in sources and in output
    # to support future joins
    input_concepts = cte_grain.components_copy
    for sub_concept in input_concepts:
        sub_concept = sub_concept.with_grain(cte_grain)
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, cte_grain, environment=environment, g=g
        )
        if sub_datasource.identifier in all_datasets:
            all_datasets[sub_datasource.identifier] = (
                all_datasets[sub_datasource.identifier] + sub_datasource
            )
        else:
            all_datasets[sub_datasource.identifier] = sub_datasource
        # all_requirements.append(sub_concept)
        source_map[sub_concept.address] = {all_datasets[sub_datasource.identifier]}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}
    dataset_list = list(all_datasets.values())
    base = dataset_list[0]

    joins = []
    for right_value in dataset_list[1:]:
        joins.append(
            BaseJoin(
                left_datasource=base,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=[c for c in cte_grain.components_copy],
                filter_to_mutual=True,
            )
        )

    # we want to find any members of the grain that we can get from the cte grain
    # if this is a subset of the final target grain, that is fine
    # TODO: unless ALL is set
    final_grain_components = [item for item in cte_grain.components_copy if item in grain.components_copy]
    datasources = list(all_datasets.values())

    # we must assume that we neeed to group
    # we must nest it one level further
    # and assume the base query MUST have the grain of all
    # component datasources
    qds = QueryDatasource(
        output_concepts=[concept, filter.content],
        input_concepts=input_concepts,
        source_map=source_map,
        grain=sum([ds.grain for ds in datasources]),
        datasources=datasources,
        joins=joins,
        condition = filter.where.conditional
    )

    # first ensure that the grain of the first CTE does not require grouping

    # restrict our new one to just output the components we want

    # now create a new CTE, n which the group by will take place
    # parent query data source for group by
    pqds = QueryDatasource(
        output_concepts=[filter.content,concept],
        input_concepts=[filter.content, concept],
        source_map={c.address: {qds} for c in [filter.content, concept]},
        grain=Grain(components= [filter.content, concept]),
        datasources=[qds],
        joins=[],
    )
    return pqds

def get_datasource_by_concept_and_grain(
    concept,
    grain: Grain,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
    whole_grain: bool = False,
) -> Union[Datasource, QueryDatasource]:
    """Determine if it's possible to get a certain concept at a certain grain.
    """
    g = g or generate_graph(environment)
    logger.debug(f"{LOGGER_PREFIX} sub search for {concept} at {grain}")
    if concept.lineage:
        if concept.derivation == PurposeLineage.WINDOW:
            logger.debug(f"{LOGGER_PREFIX} returning complex window function")
            complex = get_datasource_from_window_function(
                concept, grain, environment, g, whole_grain=whole_grain
            )
        elif concept.derivation == PurposeLineage.FILTER:
            logger.debug(f"{LOGGER_PREFIX} returning filtration")
            complex = get_datasource_for_filter(
                concept, grain, environment, g, whole_grain=whole_grain
            )
        elif concept.derivation == PurposeLineage.AGGREGATE:
            logger.debug(f"{LOGGER_PREFIX} Checking for complex function derivation")
            complex = get_datasource_from_complex_lineage(
                concept, grain, environment, g, whole_grain=whole_grain
            )
        else:
            complex = None
        if complex:
            logger.debug(f"{LOGGER_PREFIX} Returning complex lineage for {concept}")
            return complex
        logger.debug(f"{LOGGER_PREFIX} Can satisfy query with basic lineage")
    # the concept is available directly on a datasource at appropriate grain
    if concept.purpose in (Purpose.KEY, Purpose.PROPERTY):
        try:

            out = get_datasource_from_property_lookup(
                concept.with_default_grain(),
                grain,
                environment,
                g,
                whole_grain=whole_grain,
            )
            logger.debug(f"{LOGGER_PREFIX} Got {concept} from property lookup")
            return out
        except ValueError as e:
            logger.debug(f"{LOGGER_PREFIX} {str(e)}")
    # the concept is available on a datasource, but at a higher granularity
    try:
        out = get_datasource_from_group_select(
            concept, grain, environment, g, whole_grain=whole_grain
        )
        logger.debug(f"{LOGGER_PREFIX} Got {concept} from grouped select")
        return out
    except ValueError as e:
        logger.debug(f"{LOGGER_PREFIX} {str(e)}")
    # the concept and grain together can be gotten via
    # a join from a root dataset to enrichment datasets
    try:
        out = get_datasource_by_joins(
            concept, grain, environment, g, whole_grain=whole_grain
        )
        logger.debug(f"{LOGGER_PREFIX} Got {concept} from joins")
        return out
    except ValueError as e:
        logger.debug(f"{LOGGER_PREFIX} {str(e)}")

    logger.debug(f"{LOGGER_PREFIX} Full grain search exhausted, reducing grain ")
    from itertools import combinations

    for x in range(1, len(grain.components_copy)):
        for combo in combinations(grain.components_copy, x):
            ngrain = Grain(components=list(combo))
            try:
                out = get_datasource_by_joins(
                    concept.with_grain(ngrain),
                    grain,
                    environment,
                    g,
                    whole_grain=whole_grain,
                )
                logger.debug(
                    f"{LOGGER_PREFIX} Got {concept} from join to a sub portion of grain"
                )
                return out
            except ValueError as e:
                logger.debug(f"{LOGGER_PREFIX} {str(e)}")

    # if there is a property in the grain, see if we can find a datasource
    # with all the keys of the property, which we can then join to
    try:
        out = get_property_group_by_without_key(
            concept, grain, environment, g, whole_grain=whole_grain
        )
        logger.debug(
            f"{LOGGER_PREFIX} {concept} from property lookup via transversing key based grain"
        )
        return out
    except ValueError as e:
        logger.debug(e)

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(
        f"{LOGGER_PREFIX} No source for {concept} found, neighbors {neighbors}"
    )
