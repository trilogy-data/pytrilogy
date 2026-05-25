"""Per-derivation generator registry for the v4 walker. Parallels
`v4_helper/factory_dispatch.py`, but the new generators take explicit
parent StrategyNodes instead of a `source_concepts` callback."""

from typing import Callable, List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_helper.factory_dispatch import (
    build_node_for_group as _v3_build_node_for_group,
)

from .aggregate import gen_aggregate
from .basic import gen_basic
from .constant import gen_constant
from .filter import gen_filter
from .group_to import gen_group_to
from .root import gen_root
from .subselect import gen_subselect
from .union import gen_union
from .unnest import gen_unnest
from .window import gen_window

GeneratorFn = Callable[..., "StrategyNode | None"]


# Derivations the new flat generators handle. Anything not in here is
# delegated to the v3-backed factory_dispatch — useful while the prototype
# fills in the harder cases (rowset, multiselect, recursive).
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
}


def build_node(
    *,
    derivation: str,
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None = None,
    history: History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """Dispatch on `derivation`. ROOT needs `history`/`g` for datasource
    selection; the other v4 generators ignore them. Derivations without a v4
    generator fall back to the v3-backed factory_dispatch (which itself runs
    the legacy gen_X_node via a source-concepts callback)."""
    fn = _GENERATORS.get(derivation)
    if fn is None:
        return _fallback_to_v3(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=conditions,
            history=history,
            g=g,
        )
    try:
        if derivation == Derivation.ROOT.value:
            return fn(
                outputs, parents, environment, conditions, history=history, g=g
            )
        return fn(
            outputs,
            parents,
            environment,
            conditions,
            preexisting_conditions=preexisting_conditions,
        )
    except Exception as exc:
        logger.info(f"[v4-gen] {derivation} failed: {exc!r}; falling back to v3")
        return _fallback_to_v3(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=conditions,
            history=history,
            g=g,
        )


def _fallback_to_v3(
    *,
    derivation: str,
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    history: History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """The v3 dispatch uses a source-concepts callback (it doesn't know about
    our pre-built parents). Build a callback that just returns the first
    pre-built parent whose outputs cover the requested concepts; otherwise
    let v3 search from scratch."""
    from trilogy.core.processing import concept_strategies_v3 as v3

    def cb(
        mandatory_list: List[BuildConcept],
        history: History,
        environment: BuildEnvironment,
        depth: int,
        g: ReferenceGraph,
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        requested = {c.address for c in mandatory_list}
        for parent in parents:
            covered = {c.address for c in parent.output_concepts}
            if requested <= covered:
                return parent.copy()
        return v3.search_concepts(
            mandatory_list=mandatory_list,
            history=history,
            environment=environment,
            depth=depth,
            g=g,
            accept_partial=accept_partial,
            conditions=conditions,
        )

    return _v3_build_node_for_group(
        derivation=derivation,
        primaries=outputs,
        environment=environment,
        g=g,
        history=history,
        source_concepts=cb,
        conditions=conditions,
    )
