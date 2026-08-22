"""The field report's plan no longer gives `PruneInvisibleOuterJoins` anything
to do.

The rule exists to delete a LEFT join whose right side the consumer never
references, the shape a branch produced when it padded a `~` span the FINAL
assembly then re-anchored elsewhere. Span ownership is now elected before any
node is built (docs/extent_ownership.md), so only the owning branch pads and no
invisible join is manufactured. Pinning byte-equality with the rule off keeps
the plan-level fix honest: a regression that reintroduces the dead subtree shows
up here even when the optimizer hides it from the rendered statement.
"""

from tests.engine.test_duckdb_partial_fk_field_report import MODEL, QUERY
from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig


def _render(tmp_path) -> str:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(tmp_path)), conf=DuckDBConfig()
    )
    engine.parse_text(MODEL)
    return engine.generate_sql(QUERY)[-1]


def test_field_report_needs_no_invisible_join_prune(tmp_path):
    pruned_sql = _render(tmp_path)

    original = CONFIG.optimizations.prune_invisible_outer_joins
    CONFIG.optimizations.prune_invisible_outer_joins = False
    try:
        kept_sql = _render(tmp_path)
    finally:
        CONFIG.optimizations.prune_invisible_outer_joins = original

    assert kept_sql == pruned_sql
    assert "is not distinct from" not in kept_sql, kept_sql
