from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildWhereClause,
    BuildWindowItem,
    colocatable_in_grouping_pass,
    is_grouping_identity,
    nonstandard_grouping_spec,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    concepts_to_grain_concepts,
    gen_enrichment_node,
)
from trilogy.core.processing.nodes import (
    History,
    StrategyNode,
    WhereSafetyNode,
    WindowNode,
)
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_WINDOW_NODE]"


WINDOW_TYPES = (BuildWindowItem,)


def resolve_window_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment, depth: int
) -> List[BuildConcept]:
    if not isinstance(concept.lineage, WINDOW_TYPES):
        raise ValueError
    base = list(concept.lineage.concept_arguments)
    # An aggregate argument (e.g. `order by sum(x) by a, b`) lives at its own
    # group grain. A window preserves that grain row-for-row, so we must carry
    # every grain key through as a window parent. Otherwise a dropped grain key
    # has to be recovered with a join-back whose key degrades to (kept_key,
    # aggregate_value) — non-unique and NULL-bearing for ROLLUP subtotal/total
    # rows, which then get dropped or duplicated. See rollup_window_bug_handoff.
    for arg in list(base):
        if arg.derivation == Derivation.AGGREGATE:
            for gkey in arg.grain.components:
                base.append(environment.concepts[gkey])
    # A window concept's own address is often present in its grain/keys
    # (row_number() etc. are keyed by themselves). Skip self-references so
    # we don't feed the concept back as its own parent, which sends
    # source_concepts -> _generate_window_node into infinite recursion.
    self_addr = concept.address
    if concept.grain:
        for gitem in concept.grain.components:
            if gitem == self_addr:
                continue
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} appending grain item {gitem} to base"
            )
            base.append(environment.concepts[gitem])
    if concept.keys:
        for item in concept.keys:
            if item == self_addr:
                continue
            logger.info(f"{padding(depth)}{LOGGER_PREFIX} appending key {item} to base")
            base.append(environment.concepts[item])
    return base


def equivalent_window_parent_grain(
    candidate: list[BuildConcept],
    base_grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return (
        BuildGrain.from_concepts(
            concepts=candidate,
            environment=environment,
        )
        == base_grain
    )


def gen_window_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    parent_concepts = resolve_window_parent_concepts(concept, environment, depth)
    parent_addresses = {p.address for p in parent_concepts}
    parent_grain = BuildGrain.from_concepts(
        concepts=parent_concepts,
        environment=environment,
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating window node for {concept} with parents {[x.address for x in parent_concepts]} and optional {local_optional}"
    )

    base_pass_specs = {
        spec
        for p in parent_concepts
        if (spec := nonstandard_grouping_spec(p.lineage)) is not None
    }
    additional_outputs = []
    additional_parent_concepts: list[BuildConcept] = []
    for x in local_optional:
        if not isinstance(x.lineage, WINDOW_TYPES):
            continue
        assert isinstance(x.lineage, WINDOW_TYPES)
        parents = resolve_window_parent_concepts(x, environment, depth)

        # Sibling windows over the same ROLLUP/CUBE/GROUPING SETS pass compute
        # in this node even when their partition specs differ. Correctness no
        # longer depends on it (the pass's grain carries its grouping() flags,
        # so a join-back keys on true row identity — q86), but a single pass
        # beats materializing the rollup once per partition spec and joining.
        x_pass_specs = {
            spec
            for p in parents
            if (spec := nonstandard_grouping_spec(p.lineage)) is not None
        }
        matched = (
            {p.address for p in parents} == parent_addresses
            or equivalent_window_parent_grain(
                parents,
                parent_grain,
                environment,
            )
            or (bool(base_pass_specs) and base_pass_specs == x_pass_specs)
        )
        if matched:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found equivalent optional {x} with parents {parents}"
            )
            additional_outputs.append(x)
            additional_parent_concepts += parents
            parent_addresses.update(p.address for p in parents)

    parent_concepts = unique(parent_concepts + additional_parent_concepts, "address")
    # Grouping-set identity (grouping()/grouping_id()) lives at the window's own
    # rollup grain and can only be produced inside that grouped CTE. Thread it
    # through the single parent as a pass-through output — recovering it with a
    # join-back on the (nullable) dims collides subtotal/total rows across
    # grouping sets. See rollup_window_bug_handoff.
    grouping_passthrough = [
        x
        for x in local_optional
        if is_grouping_identity(x) and x.address not in parent_addresses
    ]
    if grouping_passthrough:
        parent_concepts = unique(parent_concepts + grouping_passthrough, "address")
        parent_addresses.update(x.address for x in grouping_passthrough)
    # Sibling outputs of the same ROLLUP/CUBE/GROUPING SETS pass the window runs
    # over (a selected co-measure, or a scalar over such measures) can only be
    # produced inside that single grouped CTE. Recovering them afterward joins
    # back on the visible dims, which are not a row identity across grouping
    # sets — subtotal/total rows collide with data-NULL leaves and fan out. Feed
    # them through the window parent so the pass co-sources them once.
    pass_specs = {
        spec
        for p in parent_concepts
        if (spec := nonstandard_grouping_spec(p.lineage)) is not None
    }
    if len(pass_specs) == 1:
        pass_spec = next(iter(pass_specs))
        pass_siblings = [
            x
            for x in local_optional
            if x.address not in parent_addresses
            and colocatable_in_grouping_pass(x, pass_spec)
        ]
        if pass_siblings:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} carrying grouping-pass siblings "
                f"{[x.address for x in pass_siblings]} through window parent"
            )
            parent_concepts = unique(parent_concepts + pass_siblings, "address")
            parent_addresses.update(x.address for x in pass_siblings)
    output_targets = parent_concepts + additional_outputs + [concept]
    # finally, the ones we'll need to enrich
    non_equivalent_optional = [
        x for x in local_optional if x.address not in output_targets
    ]

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} resolving final parents {parent_concepts + output_targets}"
    )

    parent_node: StrategyNode = source_concepts(
        mandatory_list=parent_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not parent_node:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable")
        return None
    parent_resolution = parent_node.resolve()
    if not all(
        [
            x.address in [y.address for y in parent_node.output_concepts]
            for x in parent_concepts
        ]
    ):
        missing = [
            x
            for x in parent_concepts
            if x.address not in [y.address for y in parent_node.output_concepts]
        ]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} window node parents resolved but missing "
            f"{[x.address for x in missing]}; treating as unresolvable"
        )
        return None
    _window_node = WindowNode(
        input_concepts=parent_concepts,
        output_concepts=output_targets,
        environment=environment,
        parents=[
            parent_node,
        ],
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
        nullable_concepts=[
            concept
            for concept in output_targets
            if any(
                concept.address == nullable.address
                for nullable in parent_resolution.nullable_concepts
            )
        ],
    )
    _window_node.rebuild_cache()
    _window_node.resolve()

    window_node = WhereSafetyNode(
        input_concepts=output_targets,
        output_concepts=output_targets,
        environment=environment,
        parents=[_window_node],
        preexisting_conditions=conditions.conditional if conditions else None,
        grain=BuildGrain.from_concepts(
            concepts=parent_concepts + output_targets,
            environment=environment,
        ),
    )
    if not non_equivalent_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning window node"
        )
        # prune outputs if we don't need join keys
        window_node.set_output_concepts(output_targets)
        return window_node

    missing_optional = [
        x.address
        for x in local_optional
        if x.address not in window_node.output_concepts
    ]

    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for window node, has all of {[x.address for x in local_optional]}"
        )
        return window_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} window node for {concept.address} requires enrichment, missing {missing_optional}, has {[x.address for x in window_node.output_concepts]}"
    )

    return gen_enrichment_node(
        window_node,
        join_keys=concepts_to_grain_concepts(output_targets, environment),
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
    )
