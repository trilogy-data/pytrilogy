from tests.engine.test_duckdb_partial_fk_field_report import MODEL, QUERY, _sort_key
from tests.modeling._row_compare import rows_match
from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig


def _fresh(tmp_path) -> Executor:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(tmp_path)), conf=DuckDBConfig()
    )
    engine.parse_text(MODEL)
    return engine


def test_invisible_left_join_pruned_and_row_identical(tmp_path):
    engine = _fresh(tmp_path)
    targets = ", ".join(engine.environment.datasources.keys())
    engine.execute_text(f"mock datasources {targets};")

    engine.environment = Environment(working_path=str(tmp_path))
    engine.parse_text(MODEL)
    pruned_sql = engine.generate_sql(QUERY)[-1]

    original = CONFIG.optimizations.prune_invisible_outer_joins
    CONFIG.optimizations.prune_invisible_outer_joins = False
    try:
        engine.environment = Environment(working_path=str(tmp_path))
        engine.parse_text(MODEL)
        kept_sql = engine.generate_sql(QUERY)[-1]
    finally:
        CONFIG.optimizations.prune_invisible_outer_joins = original

    # The metric branch's padded axis contributor is output-invisible: with
    # the rule off it survives as a LEFT join (plus its feeder CTE); with the
    # rule on the join and its subtree disappear.
    assert kept_sql.count("LEFT OUTER JOIN") > pruned_sql.count("LEFT OUTER JOIN")
    assert kept_sql.count(" as (") > pruned_sql.count(" as (")

    pruned = sorted(
        (tuple(r) for r in engine.execute_raw_sql(pruned_sql).fetchall()),
        key=_sort_key,
    )
    kept = sorted(
        (tuple(r) for r in engine.execute_raw_sql(kept_sql).fetchall()), key=_sort_key
    )
    assert len(pruned) == len(kept), (len(pruned), len(kept))
    for p, k in zip(pruned, kept):
        assert rows_match(p, k), (p, k)
