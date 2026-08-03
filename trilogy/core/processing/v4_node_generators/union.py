from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.union_node import build_layers, is_union
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.v4_helper.history import V4History

from .common import collapse_conditions, parent_outputs_needed, search_parent


def gen_union(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    *,
    history: V4History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """Stack parent outputs into a UNION ALL. Each parent contributes one
    arm; the union node is responsible for column-aligning them. UnionNode
    has no `conditions` arg, so new-at-this-group atoms collapse into
    `preexisting_conditions` alongside the inherited ones."""
    union_outputs = [output for output in outputs if is_union(output)]
    if not union_outputs:
        return None
    layers, resolved = build_layers(union_outputs)
    if not layers or not resolved:
        return None
    parent_nodes: list[StrategyNode] = []
    for layer in layers:
        parent = search_parent(
            layer,
            environment,
            history,
            g,
            conditions=[conditions] if conditions else [],
        )
        if parent is None:
            return None
        parent.add_output_concepts(resolved)
        # A pure projection is row-preserving: carry the arm's OWN row grain,
        # not the union outputs' claimed grain. The FINAL dedup check reads the
        # first arm's grain off the stacked QDS (shared with v3) — masking it
        # with the output grain elides the set-semantics GROUP BY and a key in
        # both arms counts twice (union_overlapping_keys).
        parent_nodes.append(
            SelectNode(
                input_concepts=list(parent.output_concepts),
                output_concepts=resolved,
                environment=environment,
                parents=[parent],
                grain=parent.grain,
            )
        )

    return UnionNode(
        input_concepts=parent_outputs_needed(resolved, parent_nodes, conditions),
        output_concepts=resolved,
        environment=environment,
        parents=parent_nodes,
        preexisting_conditions=collapse_conditions(conditions, preexisting_conditions),
    )
