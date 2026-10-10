from dataclasses import replace
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core import optimization

TPCDS = Path(__file__).parent.parent / "modeling" / "tpc_ds_duckdb"


def test_reinlining_a_filtered_scan_keeps_its_computed_columns(monkeypatch):
    """A rule re-fired late (`inline_datasource.after_existence_fold`) can meet
    a scan whose WHERE reads a presence probe the scan computes; folding it
    would move that WHERE onto a consumer that binds only the raw columns."""
    original = optimization.build_optimization_rule_plan

    def plan(*args, **kwargs):
        phases = original(*args, **kwargs)
        (inline,) = [p for p in phases if p.name == "inline_datasource"]
        return phases + [
            replace(inline, name="inline_again", depends_on=(), refires_after=())
        ]

    monkeypatch.setattr(optimization, "build_optimization_rule_plan", plan)
    sql = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=TPCDS)
    ).generate_sql((TPCDS / "query84.preql").read_text())[-1]
    assert "INVALID_ALIAS" not in sql
