"""Per-derivation generator registry for the v4 walker. Every generator takes
explicit parent StrategyNodes (decided by stage 2, the group graph) instead of
a `source_concepts` callback."""

from collections.abc import Callable

from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)

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
_GENERATORS: dict[Derivation, GeneratorFn] = {
    Derivation.ROOT: gen_root,
    Derivation.BASIC: gen_basic,
    Derivation.AGGREGATE: gen_aggregate,
    Derivation.WINDOW: gen_window,
    Derivation.FILTER: gen_filter,
    Derivation.CONSTANT: gen_constant,
    Derivation.UNNEST: gen_unnest,
    Derivation.UNION: gen_union,
    Derivation.SUBSELECT: gen_subselect,
    Derivation.GROUP_TO: gen_group_to,
    Derivation.ROWSET: gen_rowset,
    Derivation.RECURSIVE: gen_recursive,
}


def build_node(
    *,
    derivation: Derivation,
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None = None,
    intrinsic_filter_pushdown: bool = True,
    existence_source: bool = False,
    history: History,
    g: ReferenceGraph,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
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
            f"known: {sorted(d.value for d in _GENERATORS)}"
        )
    if derivation in (
        Derivation.ROOT,
        Derivation.ROWSET,
        Derivation.UNION,
        Derivation.SUBSELECT,
    ):
        return fn(
            outputs,
            parents,
            environment,
            conditions,
            history=history,
            g=g,
            source_policy=source_policy,
        )
    if derivation == Derivation.FILTER:
        return fn(
            outputs,
            parents,
            environment,
            conditions,
            preexisting_conditions=preexisting_conditions,
            intrinsic_filter_pushdown=intrinsic_filter_pushdown,
            existence_source=existence_source,
        )
    return fn(
        outputs,
        parents,
        environment,
        conditions,
        preexisting_conditions=preexisting_conditions,
    )
