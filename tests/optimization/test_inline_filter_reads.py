from trilogy import Environment
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.optimizations.inline_datasource import _filter_reads, _lineage_reads
from trilogy.parser import parse

MODEL = """
key run_date date;
key run_org string;
property <run_date, run_org>.runs_total int;
key other_org string;

auto rwindow_1d <- run_date = date_add(current_date(), day, -1);
auto org_total <- sum(runs_total) by run_org;
auto big_org <- org_total > 0;

auto windowed_org <- run_org ? rwindow_1d and runs_total > 0;
auto big_windowed_org <- run_org ? big_org;
auto known_org <- run_org ? run_org in other_org;
"""

COLUMNS = {"local.run_date", "local.run_org", "local.runs_total"}


def _build() -> BuildEnvironment:
    env, _ = parse(MODEL, Environment())
    return env.materialize_for_select()


def _concept(env: BuildEnvironment, name: str) -> BuildConcept:
    return env.concepts[f"local.{name}"]


def test_filter_reads_derived_predicate():
    env = _build()
    assert _filter_reads(_concept(env, "windowed_org"), COLUMNS) == {"local.rwindow_1d"}


def test_filter_reads_rejects_aggregate_under_scalar():
    env = _build()
    assert _filter_reads(_concept(env, "big_windowed_org"), COLUMNS) is None
    assert _lineage_reads([_concept(env, "big_org")], COLUMNS) is None


def test_filter_reads_rejects_existence():
    env = _build()
    assert _filter_reads(_concept(env, "known_org"), COLUMNS) is None


def test_filter_reads_non_filter_is_empty():
    env = _build()
    assert _filter_reads(_concept(env, "rwindow_1d"), COLUMNS) == set()
