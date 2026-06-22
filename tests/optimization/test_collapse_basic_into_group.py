"""Unit + e2e coverage for the BASIC-into-GROUP fold gate in
``CollapseSingleParent`` (`basic_fold_into_group_is_safe`).

A scalar row projection over a GROUP parent (e.g. `sum(a)/sum(b)`) folds into the
GROUP's SELECT list -- valid SQL and the single-CTE shape v3 emits natively. The
gate restricts that to the row-preserving subset: every output the child derives
*locally* must be a scalar, while aggregate/window columns merely passed through
from the GROUP parent are fine."""

from types import SimpleNamespace

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.optimizations.collapse_single_parent import (
    basic_fold_into_group_is_safe,
)


def _col(address: str, derivation: Derivation, lineage=None) -> SimpleNamespace:
    return SimpleNamespace(address=address, derivation=derivation, lineage=lineage)


def _parent(output_lcl: set[str]) -> SimpleNamespace:
    return SimpleNamespace(output_lcl=output_lcl)


def _child(*columns: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(output_columns=list(columns))


def test_gate_allows_scalar_over_group():
    parent = _parent({"local.week", "local.agg_a", "local.agg_b"})
    child = _child(
        _col("local.week", Derivation.ROOT),
        _col("local.agg_a", Derivation.AGGREGATE),  # passthrough
        _col("local.agg_b", Derivation.AGGREGATE),  # passthrough
        _col("local.ratio", Derivation.BASIC),  # new scalar over the aggregates
    )
    assert basic_fold_into_group_is_safe(parent, child)


def test_gate_allows_dimension_join_passthrough_aggregates():
    parent = _parent({"local.key", "local.total"})
    child = _child(
        _col("local.key", Derivation.ROOT),
        _col("local.total", Derivation.AGGREGATE),  # passthrough
        _col("local.label", Derivation.BASIC),  # joined-on dimension label
    )
    assert basic_fold_into_group_is_safe(parent, child)


def test_gate_blocks_new_aggregate():
    parent = _parent({"local.key"})
    child = _child(
        _col("local.key", Derivation.ROOT),
        _col("local.new_agg", Derivation.AGGREGATE),  # NOT in parent -> derived anew
    )
    assert not basic_fold_into_group_is_safe(parent, child)


def test_gate_blocks_new_window_and_unnest():
    parent = _parent({"local.key"})
    for unsafe in (Derivation.WINDOW, Derivation.UNNEST, Derivation.RECURSIVE):
        child = _child(
            _col("local.key", Derivation.ROOT),
            _col("local.derived", unsafe),
        )
        assert not basic_fold_into_group_is_safe(parent, child)


def test_basic_ratio_over_group_renders_single_group_cte():
    query = """
key id int;
property id.amount float;
key bucket string;

datasource facts (
    id: id,
    amount: amount,
    bucket: bucket
)
grain (id)
address facts_table;

select
    bucket,
    sum(amount ? bucket = 'a') / sum(amount ? bucket = 'b') as ratio
;
"""
    env = Environment()
    env, statements = env.parse(query)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(statements[-1])[-1]
    # The division folds into the grouped SELECT -- no trailing projection CTE.
    assert sql.count("GROUP BY") == 1, sql
    assert "/ " in sql or "/" in sql
