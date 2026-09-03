"""Per-derivation StrategyNode factories for the v4 planner.

Each dispatched generator takes the same minimal inputs:

    outputs      : the concepts this group should produce
    parents      : already-built StrategyNodes for upstream groups
    environment  : the build environment
    conditions   : clauses injected at or above this group (optional)
    history, g   : the re-entry handle, for generators that plan a sub-search
                   (ROOT's datasource selection, ROWSET/UNION/SUBSELECT)

The topological walker hands each generator its actual parents and asks it to
project the listed outputs, nothing else.

The nested-select constructs live here too, even though they are not group
derivations the walker dispatches: `multiselect` and `union_select` are
intercepted by `concept_strategies_v4._search_concepts` because their arms are
independent sub-plans rather than one source graph. All three share
`nested_select` (build a sub-select in its own scope, gate connectivity,
search, apply its HAVING and LIMIT) and `condition_sources` (source a clause's
inputs, then inject it over a materialized producer).
"""

from .dispatch import build_node

__all__ = ["build_node"]
