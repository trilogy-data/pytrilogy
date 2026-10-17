from trilogy.core.models.build import BuildConcept, BuildGrain, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_determines,
)

from .common import outputs_with_parent_grain_keys, parent_outputs_needed


def basic_regroups(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
) -> bool:
    """Whether the BASIC node over `parents` groups instead of projecting its
    one parent's rows as they stand."""
    return len(parents) == 1 and _grain_claim_needs_group(
        outputs_with_parent_grain_keys(outputs, parents), parents[0], environment
    )


def _grain_claim_needs_group(
    outputs: list[BuildConcept],
    parent: StrategyNode,
    environment: BuildEnvironment,
) -> bool:
    """True when this projection can only make its grain claim true by grouping.

    A projection's grain is inferred from what it projects, but a plain SELECT
    emits one row per PARENT row. Project a coarse attribute and a scalar over
    it off a finer-grain parent and the node advertises the attribute's grain
    while emitting one row per parent row; a consumer then joins it on the
    attribute and fans the other side out silently, since the SQL is valid.

    Two things can make the claim true, and only the second is ours to do here:

    - The claim is already true. Either the parent's grain keys survive in the
      projection, or the projected grain functionally determines them (the
      parent holds at most one row per it, so dropping them loses nothing).
    - The projection can be widened back to the parent's grain. That is
      ``_widen_to_parent_grain``'s job, and it needs the parent to actually
      expose those keys; ``gen_basic`` applies it before asking here.

    When the parent does NOT expose its own grain there is nothing to widen with,
    and grouping is the only repair left. The outputs are functionally determined
    by the grouped set, so the dedupe cannot drop a row; it removes exactly the
    duplicates the parent's finer grain introduced.

    Returns SelectNode's own default (False) when it does not fire: passing
    None instead would flip every other basic projection onto ``_resolve``'s
    grain-less branch, which is a plan change for nodes this has no business
    touching.
    """
    parent_grain = set(parent.resolve().grain.components)
    if not parent_grain or parent_grain.issubset({c.address for c in outputs}):
        return False
    if parent_grain.issubset({c.address for c in parent.output_concepts}):
        return False
    return not _claim_determines(outputs, parent_grain, environment)


def _claim_determines(
    outputs: list[BuildConcept], parent_grain: set[str], environment: BuildEnvironment
) -> bool:
    """Does the grain `outputs` claims fix every key of the parent's grain?"""
    claimed = BuildGrain.from_concepts(outputs).components
    return all(
        component in claimed
        or build_fd_determines(
            environment, claimed, component, include_empty_grain=False
        )
        for component in parent_grain
    )


def _widen_to_parent_grain(
    outputs: list[BuildConcept],
    parent: StrategyNode,
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Carry the parent's grain keys through a projection that would drop them.

    An ungrouped projection keeps its parent's grain: one row per parent row,
    and ``SelectNode._resolve`` stamps it so. A consumer that merges with it
    can only join on what it renders; hide the grain keys and the one shared
    column left may be a coarser attribute, and a join on that multiplies rows.
    A species-grain bucket read off a tree-grain parent counted every species
    n**2 times this way (``test_basic_projection_parent_grain_fanout``)."""
    parent_grain = set(parent.resolve().grain.components)
    present = {c.address for c in outputs}
    if not parent_grain or parent_grain.issubset(present):
        return outputs
    exposed = {c.address: c for c in parent.usable_outputs}
    if not parent_grain.issubset(exposed):
        return outputs
    if _claim_determines(outputs, parent_grain, environment):
        return outputs
    return outputs + [exposed[address] for address in sorted(parent_grain - present)]


def gen_basic(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Projection of derived basic expressions over already-built parents.

    Zero parents → ConstantNode. One parent → SelectNode (just projection).
    Multiple parents → MergeNode, which auto-joins on shared output concepts
    (typically the common grain). A SelectNode here would render with no
    join and emit `INVALID_REFERENCE_BUG_<...>` for the unjoined parent."""
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    if not parents:
        return ConstantNode(
            input_concepts=[],
            output_concepts=outputs,
            environment=environment,
            conditions=conditions.conditional if conditions else None,
            preexisting_conditions=pre,
        )
    full_outputs = outputs_with_parent_grain_keys(outputs, parents)
    if len(parents) == 1:
        full_outputs = _widen_to_parent_grain(full_outputs, parents[0], environment)
    inputs = parent_outputs_needed(full_outputs, parents, conditions)
    if len(parents) == 1:
        return SelectNode(
            input_concepts=inputs,
            output_concepts=full_outputs,
            environment=environment,
            parents=parents,
            conditions=conditions.conditional if conditions else None,
            preexisting_conditions=pre,
            force_group=_grain_claim_needs_group(full_outputs, parents[0], environment),
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=pre,
    )
