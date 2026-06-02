"""Per-derivation generator registry for the v4 walker. Every generator takes
explicit parent StrategyNodes (decided by stage 2, the group graph) instead of
a `source_concepts` callback."""

from typing import Callable

from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode

from .aggregate import gen_aggregate
from .basic import gen_basic
from .constant import gen_constant
from .filter import gen_filter
from .group_to import gen_group_to
from .recursive import gen_recursive
from .root import gen_root
from .rowset import gen_rowset
from .subselect import gen_subselect
from .union import gen_union
from .unnest import gen_unnest
from .window import gen_window

GeneratorFn = Callable[..., "StrategyNode | None"]


# Every group-level derivation the v4 walker can emit has a native generator.
# MULTISELECT is intentionally absent: it never reaches here — a top-level
# multiselect is intercepted in `concept_strategies_v4._search_concepts`
# (`_resolve_multiselect`), and a rowset-wrapped one is planned inside
# `resolve_rowset`. An unknown derivation reaching `build_node` is a real bug.
_GENERATORS: dict[str, GeneratorFn] = {
    Derivation.ROOT.value: gen_root,
    Derivation.BASIC.value: gen_basic,
    Derivation.AGGREGATE.value: gen_aggregate,
    Derivation.WINDOW.value: gen_window,
    Derivation.FILTER.value: gen_filter,
    Derivation.CONSTANT.value: gen_constant,
    Derivation.UNNEST.value: gen_unnest,
    Derivation.UNION.value: gen_union,
    Derivation.SUBSELECT.value: gen_subselect,
    Derivation.GROUP_TO.value: gen_group_to,
    Derivation.ROWSET.value: gen_rowset,
    Derivation.RECURSIVE.value: gen_recursive,
}


def build_node(
    *,
    derivation: str,
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None = None,
    history: History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """Dispatch on `derivation`. ROOT and ROWSET need `history`/`g` (ROOT for
    datasource selection, ROWSET to recursively plan its inner select); the
    other generators ignore them.

    Every group-level derivation is native — an unknown one is a real bug, so
    we raise rather than degrade (there is no v3 fallback)."""
    fn = _GENERATORS.get(derivation)
    if fn is None:
        raise ValueError(
            f"No v4 node generator for derivation {derivation!r}; "
            f"known: {sorted(_GENERATORS)}"
        )
    if derivation in (Derivation.ROOT.value, Derivation.ROWSET.value):
        return fn(outputs, parents, environment, conditions, history=history, g=g)
    return fn(
        outputs,
        parents,
        environment,
        conditions,
        preexisting_conditions=preexisting_conditions,
    )
