from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    RowsetNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_determines,
)
from trilogy.core.processing.v4_helper.projection import (
    renderable_addresses,
    widen_projection,
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


def _expose_boundary_handle(node: StrategyNode, handle: BuildConcept) -> bool:
    """Expose a rowset handle on the boundary under `node` whose body produces
    its content: a boundary carries the columns its reader demands, so a grain
    key nobody selected is materialized only when a reader needs the row
    identity."""
    lineage = handle.lineage
    if not isinstance(lineage, BuildRowsetItem):
        return False
    for boundary in node.parents:
        if not isinstance(boundary, RowsetNode) or not boundary.parents:
            continue
        if not any(
            isinstance(o.lineage, BuildRowsetItem)
            and o.lineage.rowset.name == lineage.rowset.name
            for o in boundary.output_concepts
        ):
            continue
        if handle.address in {o.address for o in boundary.output_concepts}:
            return True
        produced = {o.address for o in boundary.parents[0].usable_outputs}
        if lineage.content.address not in produced:
            return False
        widen_projection(
            boundary,
            [handle],
            input_candidates=[lineage.content],
            available_addresses=produced,
        )
        return True
    return False


def _expose_parent_grain(
    parent: StrategyNode, parent_grain: set[str], environment: BuildEnvironment
) -> None:
    """Widen a projecting parent with the rowset identity handles it claims
    but does not project (a merge over a rowset boundary at the boundary's
    identity): the keys are what a projection over it widens back to, and
    dedup is the repair of last resort. A root key a merge hides is left to
    that dedup, as before."""
    if parent.force_group or not isinstance(parent, (SelectNode, MergeNode)):
        return
    exposed = {c.address for c in parent.output_concepts}
    missing = [
        key
        for address in sorted(parent_grain - exposed)
        if (key := environment.concepts.get(address)) is not None
        and key.derivation == Derivation.ROWSET
    ]
    for key in missing:
        _expose_boundary_handle(parent, key)
    available = renderable_addresses(parent)
    keys = [key for key in missing if key.address in available]
    if keys:
        widen_projection(
            parent, keys, input_candidates=keys, available_addresses=available
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
      ``outputs_with_parent_grain_keys``'s job, and it needs the parent to
      actually expose those keys.

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
    _expose_parent_grain(parent, parent_grain, environment)
    if parent_grain.issubset({c.address for c in parent.output_concepts}):
        return False
    claimed = BuildGrain.from_concepts(outputs).components
    return not all(
        component in claimed
        or build_fd_determines(
            environment, claimed, component, include_empty_grain=False
        )
        for component in parent_grain
    )


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
