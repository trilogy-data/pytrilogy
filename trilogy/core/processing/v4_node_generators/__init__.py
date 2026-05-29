"""Simplified per-derivation StrategyNode factories for the v4 planner.

Each generator takes the same minimal inputs:

    outputs      — the concepts this group should produce
    parents      — already-built StrategyNodes for upstream groups
    environment  — the build environment
    conditions   — clauses injected at or above this group (optional)
    history, g   — only used by ROOT for datasource selection

Stripped from the v3 generators: no sibling/equivalent-optional discovery,
no optional-merging, no callback-based parent resolution. The topological
walker hands each generator its actual parents and asks it to project the
listed outputs — nothing else. Generators that need extra plumbing (rowset,
multiselect, recursive — they re-enter `get_query_node` on a sub-select)
aren't ported yet; the dispatch falls back to the v3 generators for those.
"""

from .dispatch import build_node

__all__ = ["build_node"]
