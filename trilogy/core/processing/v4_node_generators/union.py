from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.v4_helper.condition_injection import condition_row_args
from trilogy.core.processing.v4_helper.union_arms import is_union_concept, union_arms

from .common import collapse_conditions


def _arm_inputs(
    parents: list[StrategyNode], arm: list[BuildConcept]
) -> tuple[StrategyNode, list[BuildConcept]] | None:
    """The parent that renders every argument of one arm, with the parent
    outputs those arguments read (a constant renders inline and needs none)."""
    for parent in parents:
        by_address = {o.address: o for o in parent.output_concepts}
        inputs: list[BuildConcept] = []
        for arg in arm:
            if arg.derivation == Derivation.CONSTANT:
                continue
            source = by_address.get(arg.address) or next(
                (o for o in parent.output_concepts if arg.address in o.pseudonyms),
                None,
            )
            if source is None:
                break
            inputs.append(source)
        else:
            return parent, inputs
    return None


def gen_union(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Stack the arm parents the group graph hands us into a UNION ALL.

    `parents` holds one node per arm scope (see `union_arms`); each arm
    projects the stacked columns off its own contributing arguments, which
    stay in the arm's outputs (hidden) so the renderer's member substitution
    finds them there. A condition is a predicate over the stacked columns, so
    every arm applies it to its own rows: `all_amt > 0.15` filters arm one on
    `amt` and arm two on `pad`. A stacked column the WHERE names without
    selecting it is materialized the same way and hidden. UnionNode has no
    `conditions` slot, so the applied atoms collapse into
    `preexisting_conditions` beside the inherited ones."""
    union_outputs = [output for output in outputs if is_union_concept(output)]
    if not union_outputs:
        return None
    hidden: set[str] = set()
    selected = {output.address for output in union_outputs}
    for arg in condition_row_args(conditions):
        if is_union_concept(arg) and arg.address not in selected:
            union_outputs.append(arg)
            selected.add(arg.address)
            hidden.add(arg.address)
    arms = union_arms(union_outputs, environment)
    if arms is None:
        return None
    arm_nodes: list[StrategyNode] = []
    for arm in arms:
        found = _arm_inputs(parents, arm)
        if found is None:
            return None
        parent, inputs = found
        # A pure projection is row-preserving: carry the arm's OWN row grain,
        # not the union outputs' claimed grain. The FINAL dedup check reads the
        # first arm's grain off the stacked QDS; masking it with the output
        # grain elides the set-semantics GROUP BY and a key in both arms counts
        # twice.
        arm_nodes.append(
            SelectNode(
                input_concepts=inputs,
                output_concepts=[*union_outputs, *arm],
                environment=environment,
                parents=[parent],
                grain=parent.grain,
                conditions=conditions.conditional if conditions else None,
                hidden_concepts={arg.address for arg in arm},
            )
        )
    return UnionNode(
        input_concepts=list(union_outputs),
        output_concepts=list(union_outputs),
        environment=environment,
        parents=arm_nodes,
        preexisting_conditions=collapse_conditions(conditions, preexisting_conditions),
        hidden_concepts=hidden,
    )
