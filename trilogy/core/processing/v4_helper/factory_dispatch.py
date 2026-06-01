"""Maps a `Derivation` to the per-shape factory that turns its concepts into
a `StrategyNode`. The v3 generators are imported and called with a uniform
kwarg signature so any signature drift between generators stays contained
here, not in the walker."""

from typing import Callable, List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.node_generators.basic_node import gen_basic_node
from trilogy.core.processing.node_generators.constant_node import gen_constant_node
from trilogy.core.processing.node_generators.filter_node import gen_filter_node
from trilogy.core.processing.node_generators.group_node import gen_group_node
from trilogy.core.processing.node_generators.group_to_node import gen_group_to_node
from trilogy.core.processing.node_generators.multiselect_node import (
    gen_multiselect_node,
)
from trilogy.core.processing.node_generators.recursive_node import gen_recursive_node
from trilogy.core.processing.node_generators.rowset_node import gen_rowset_node
from trilogy.core.processing.node_generators.subselect_node import gen_subselect_node
from trilogy.core.processing.node_generators.union_node import gen_union_node
from trilogy.core.processing.node_generators.unnest_node import gen_unnest_node
from trilogy.core.processing.nodes import History, StrategyNode

DerivationHandler = Callable[..., "StrategyNode | None"]


def _build_root(
    primary: BuildConcept,
    others: List[BuildConcept],
    *,
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
    source_concepts,
    conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    """ROOT is special: try a single-datasource select via the History cache,
    fall back to v3's search for cross-datasource join planning."""
    node = history.gen_select_node(
        [primary, *others], environment, g, depth=0, conditions=conditions
    )
    if node is not None:
        return node
    return v3.search_concepts(
        mandatory_list=[primary, *others],
        history=history,
        environment=environment,
        depth=1,
        g=g,
        conditions=conditions,
    )


def _wrap(fn: Callable) -> DerivationHandler:
    """Adapt a `gen_X_node` to the unified handler signature. Everything is
    passed by keyword so positional drift between generators is invisible."""

    def handler(
        primary: BuildConcept,
        others: List[BuildConcept],
        *,
        environment: BuildEnvironment,
        g: ReferenceGraph,
        history: History,
        source_concepts,
        conditions: BuildWhereClause | None,
    ) -> StrategyNode | None:
        return fn(
            primary,
            others,
            environment=environment,
            g=g,
            depth=1,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
        )

    return handler


_HANDLERS: dict[str, DerivationHandler] = {
    Derivation.ROOT.value: _build_root,
    Derivation.AGGREGATE.value: _wrap(gen_group_node),
    Derivation.BASIC.value: _wrap(gen_basic_node),
    Derivation.FILTER.value: _wrap(gen_filter_node),
    Derivation.CONSTANT.value: _wrap(gen_constant_node),
    Derivation.UNNEST.value: _wrap(gen_unnest_node),
    Derivation.UNION.value: _wrap(gen_union_node),
    Derivation.ROWSET.value: _wrap(gen_rowset_node),
    Derivation.RECURSIVE.value: _wrap(gen_recursive_node),
    Derivation.GROUP_TO.value: _wrap(gen_group_to_node),
    Derivation.MULTISELECT.value: _wrap(gen_multiselect_node),
    Derivation.SUBSELECT.value: _wrap(gen_subselect_node),
}


def build_node_for_group(
    *,
    derivation: str,
    primaries: List[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
    source_concepts,
    conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    """Dispatch to the registered handler for `derivation`. Unknown
    derivations log and return None instead of raising; generator exceptions
    are caught here so one bad group doesn't kill the whole walk."""
    handler = _HANDLERS.get(derivation)
    if handler is None:
        logger.info(f"[v4] no walker handler yet for derivation {derivation}")
        return None
    try:
        return handler(
            primaries[0],
            primaries[1:],
            environment=environment,
            g=g,
            history=history,
            source_concepts=source_concepts,
            conditions=conditions,
        )
    except Exception as exc:
        logger.info(f"[v4] factory failed for derivation {derivation}: {exc}")
        return None
