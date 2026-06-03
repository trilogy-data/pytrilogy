"""ROOT generator: pick (or join) the datasources that produce the requested
concepts. Delegates to v3 because datasource search/join planning is the one
piece the v4 prototype hasn't replaced yet — the per-node `add_output_concept`
and datasource scoring lives there."""

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    inject_condition_at_node,
    split_existence_atoms,
)


def gen_root(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],  # always empty for ROOT; kept for uniform sig
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g,
) -> StrategyNode | None:
    """Try the History-cached single-datasource select first; if no single
    datasource covers `outputs`, fall back to v3's cross-datasource search.

    Existence-bearing atoms (`x IN <subselect>`) are peeled off and applied in
    a SelectNode wrapper around the v3 result — without this, v3 walks the
    existence arg's lineage and inlines the whole feeder chain, duplicating
    work the d1 group already does."""
    row_conditions, existence_conditions = split_existence_atoms(conditions)
    # When we peel an existence atom off the condition, v3 doesn't see its
    # row args either — but the wrapper still needs them in the row stream to
    # apply the IN's LHS (e.g. `substring(store.zip) IN <feeder>` requires
    # store.zip in the scan). Add them to the outputs going to v3 so the
    # column is in the resolved scan.
    inner_outputs: list[BuildConcept] = list(outputs)
    if existence_conditions is not None:
        seen = {c.address for c in inner_outputs}
        for atom in decompose_condition(existence_conditions.conditional):
            for arg in getattr(atom, "row_arguments", ()) or ():
                if arg.address not in seen:
                    inner_outputs.append(arg)
                    seen.add(arg.address)
    node = history.gen_select_node(
        inner_outputs, environment, g, depth=0, conditions=row_conditions
    )
    if node is None:
        node = v3.search_concepts(
            mandatory_list=inner_outputs,
            history=history,
            environment=environment,
            depth=1,
            g=g,
            conditions=row_conditions,
        )
    if node is None or existence_conditions is None:
        return node
    # Wrap so strategy_builder's existence wiring attaches `existence_concepts`
    # to the WHERE-emitting node (without this, the IN subselect's right-hand
    # side has no source CTE and the renderer emits INVALID_REFERENCE_BUG).
    # The wrapper projects only the originally-requested `outputs` — the
    # extra row args we added for v3 stay in the inner CTE.
    #
    # `combine_existing=False`: the inner `node` ALREADY applied `row_conditions`
    # (year/quarter etc.) and need not re-expose those filter-only columns. The
    # wrapper must emit ONLY the existence atom — re-AND-ing the row conditions
    # here references columns the inner CTE consumed and dropped (q45: a scalar
    # `date.year = 2001` next to an `item IN <rowset>`, INVALID_REFERENCE_BUG).
    return inject_condition_at_node(
        node,
        existence_conditions,
        list(outputs),
        environment=environment,
        sources=ConditionSources(),
        input_concepts=list(node.output_concepts),
        combine_existing=False,
    )
