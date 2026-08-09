from trilogy.core.enums import FunctionType
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.v4_helper.history import V4History

from .common import collapse_conditions, parent_outputs_needed, search_parent


def is_union(c: BuildConcept):
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.UNION
    )


def build_layers(
    concepts: list[BuildConcept],
) -> tuple[list[list[BuildConcept]], list[BuildConcept]]:
    sources = {
        x.address: x.lineage.concept_arguments if x.lineage else [] for x in concepts
    }
    root = concepts[0]

    built_layers = []
    # copy: concept_arguments is a shared cached list, and the pop() drain
    # below would otherwise empty it for every later consumer of this lineage
    layers = list(root.lineage.concept_arguments) if root.lineage else []
    sourced = set()
    while layers:
        layer = []
        current = layers.pop()
        sourced.add(current.address)
        layer.append(current)
        for key, values in sources.items():
            if key == current.address:
                continue
            for value in values:
                if value.address in (current.keys or []) or current.address in (
                    value.keys or []
                ):
                    layer.append(value)
                    sourced.add(value.address)
        built_layers.append(layer)
    complete = [
        x for x in concepts if all(x.address in sourced for x in sources[x.address])
    ]
    return built_layers, complete


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
        # first arm's grain off the stacked QDS — masking it
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
