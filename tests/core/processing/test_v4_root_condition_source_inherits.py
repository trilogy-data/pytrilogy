"""A ROOT group whose condition names a DERIVED row arg re-plans that arg, and
the re-plan must inherit the atoms its ancestor groups applied.

`gen_root` sources from datasources, not from the `parents` the group graph
handed it, so a condition like `total > 0` sends the derived `total` back
through a fresh `search_concepts`. That sub-search does not see the rows any
ancestor group already filtered — passing it no conditions dropped those atoms
outright, and nothing downstream re-applied them.

Distilled from tpc-ds q11: `where billing_customer.sk is not null` beside
`sum(...) by billing_customer.sk` rebuilt the aggregate unfiltered, so the
NULL-key group survived the LEFT OUTER join to the customer dimension as an
all-NULL output row (98,993 rows where 98,992 are correct). The same mechanism
silently no-op'd q11's `channel in (...)` and `year in (...)`.

`test_not_null_on_aggregate_grain_key_is_enforced` (tpc_ds_duckdb) is the
row-level guard; these pin the plumbing that produces it, without a database.
"""

import inspect
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.processing import concept_strategies_v4
from trilogy.core.processing.v4_node_generators import root as root_generator

_WORKING = Path(__file__).resolve().parents[3] / "tests" / "modeling" / "tpc_ds_duckdb"

_QUERY = """import all_sales as sales;
auto total <- sum(sales.ext_list_price) by sales.billing_customer.sk;
where sales.billing_customer.sk is not null and total > 0
select sales.billing_customer.id;"""


def _generate() -> str:
    env = Environment(working_path=_WORKING)
    return Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)[-1]


def test_ancestor_atom_renders_in_the_rebuilt_aggregates_scan():
    sql = _generate()
    assert sql.count("is not null") == 3, sql


def test_dimension_join_is_not_left_outer():
    """With the NULL keys filtered out the customer join upgrades to INNER. A
    surviving LEFT OUTER means the atom went missing again — the preserved side
    is exactly what let the NULL group through."""
    assert "LEFT OUTER JOIN" not in _generate()


def test_condition_source_subsearch_receives_the_inherited_atoms(monkeypatch):
    """The plumbing itself: `gen_root`'s re-plan of the derived condition arg is
    handed the ancestor atoms rather than an empty condition list."""
    seen: list[list] = []
    original = concept_strategies_v4.search_concepts

    def spy(*args, **kwargs):
        seen.append(list(kwargs.get("conditions") or []))
        return original(*args, **kwargs)

    monkeypatch.setattr(concept_strategies_v4, "search_concepts", spy)
    # `gen_root` imports it inside the function body, so patching the defining
    # module is what takes effect.
    _generate()

    inherited = [str(clause.conditional) for call in seen for clause in call]
    assert any(
        "is not" in rendered for rendered in inherited
    ), f"no inherited not-null reached a condition-source sub-search: {inherited}"


def test_only_atoms_expressible_on_the_request_are_inherited():
    """The boundary, read off q11 itself. Its `billing_customer.sk is not null`
    is a grain key of the aggregates being re-planned, so it comes along and
    renders once per union arm. Its `sale_date.year in (...)` is not — applying
    it would narrow the aggregates' INPUT, which the population/select
    dual-scope split forbids (`test_where_select_dual_scope`), so the year stays
    on the outer group and date_dim is still joined above the union."""
    env = Environment(working_path=_WORKING)
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
        (_WORKING / "query11.preql").read_text()
    )[-1]
    # Three arm-level guards (catalog/store/web) plus one re-check on the union
    # output. That fourth is redundant -- every arm already null-rejects the
    # column -- but proving it needs an "all arms enforce this" implication that
    # StripRedundantNotNull deliberately lacks: the column IS nullable at the
    # base tables, and its ground-truth-nullability gate is what keeps an
    # authored NOT NULL on a nullable FK from vanishing (q78). Assert what the
    # planner emits; the arm count is the part this test is about.
    assert sql.count("is not null") == 4, sql
    # Pushing the year into the arms would give each its own date_dim join.
    assert sql.count("date_dim") == 1, sql


def test_gen_root_accepts_preexisting_conditions():
    """Guard the wiring: dispatch passes this by keyword, so a signature change
    that drops it would otherwise fail only as a silent behavior regression."""
    assert (
        "preexisting_conditions"
        in inspect.signature(root_generator.gen_root).parameters
    )
