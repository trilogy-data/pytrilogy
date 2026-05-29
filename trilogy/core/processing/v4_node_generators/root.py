"""ROOT generator: pick (or join) the datasources that produce the requested
concepts. Delegates to v3 because datasource search/join planning is the one
piece the v4 prototype hasn't replaced yet — the per-node `add_output_concept`
and datasource scoring lives there."""

from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
)
from trilogy.core.processing.nodes import History, SelectNode, StrategyNode


def _split_existence_atoms(
    conditions: BuildWhereClause | None,
) -> tuple[BuildWhereClause | None, BuildWhereClause | None]:
    """Split a clause into (row_only, existence_bearing). The existence half
    carries atoms whose RHS is a side-channel subselect (e.g. `x IN <feeder>`);
    v3 would walk those args' lineage and re-materialize them inline, while v4
    has already built that feeder as a separate group and is providing it via
    `existence_concepts`. Keep them out of v3's view so v3 only joins what it
    truly needs for the scan itself."""
    if conditions is None:
        return None, None
    row_atoms: list = []
    existence_atoms: list = []
    for atom in decompose_condition(conditions.conditional):
        ex_args = getattr(atom, "existence_arguments", None)
        if ex_args and any(ex_args):
            existence_atoms.append(atom)
        else:
            row_atoms.append(atom)
    row_combined = combine_condition_atoms(row_atoms) if row_atoms else None
    ex_combined = combine_condition_atoms(existence_atoms) if existence_atoms else None
    return (
        (
            BuildWhereClause(conditional=row_combined)
            if row_combined is not None
            else None
        ),
        BuildWhereClause(conditional=ex_combined) if ex_combined is not None else None,
    )


def gen_root(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],  # always empty for ROOT; kept for uniform sig
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
    row_conditions, existence_conditions = _split_existence_atoms(conditions)
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
    return SelectNode(
        input_concepts=list(node.output_concepts),
        output_concepts=list(outputs),
        environment=environment,
        parents=[node],
        conditions=existence_conditions.conditional,
    )
