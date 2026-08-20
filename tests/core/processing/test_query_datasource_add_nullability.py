"""``QueryDatasource.__add__`` must carry ``nullable_concepts`` through a merge.

Outer-join padding nullability is stamped on the QDS at resolve time by join
analysis; it cannot be re-derived from the concepts, because ``__post_init__``
restores only INTRINSIC (``?``) nullability. ``__add__`` omitted the field, so
the copy produced when two same-identifier parent QDSs merge (``merge_node``
``_resolve``) lost it. Downstream ``get_modifiers`` then saw one nullable side
and one not, rendered the merge key as a null-rejecting ``=``, and that licensed
``join_upgrade`` to flip the shared scan's dimension join LEFT OUTER -> INNER.

Distilled from tpc-ds q47: the same monthly aggregate returned 53,194 rows
without lag/lead and 50,537 with them, the 2,657 missing groups being exactly
the NULL-store padding rows. `CTE.__add__` already unions the field correctly.

Repro: evals/tpcds_agent/bug_q47_window_rowset_churn.md (bug B).
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import Purpose, SourceType
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import QueryDatasource

_WORKING = Path(__file__).resolve().parents[3] / "tests" / "modeling" / "tpc_ds_duckdb"

_MODEL = """import store_sales as ss;

auto month_total <- sum(ss.sales_price)
    by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name,
       ss.sale_date.year, ss.sale_date.month_of_year;
auto avg_monthly <- avg(month_total)
    by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name,
       ss.sale_date.year;
auto prev_total <- lag(month_total, 1)
    over (partition by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name
        order by ss.sale_date.year asc, ss.sale_date.month_of_year asc);
auto next_total <- lead(month_total, 1)
    over (partition by ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name
        order by ss.sale_date.year asc, ss.sale_date.month_of_year asc);
"""

_WHERE = """where (ss.sale_date.year = 1999)
    or (ss.sale_date.year = 1998 and ss.sale_date.month_of_year = 12)
    or (ss.sale_date.year = 2000 and ss.sale_date.month_of_year = 1)
"""

# The output aliases matter: they are what splits the month-grain group into
# same-identifier copies that then merge through `__add__`.
_SELECT = """select
    ss.item.category, ss.item.brand_name, ss.store.name, ss.store.company_name,
    ss.sale_date.year, ss.sale_date.month_of_year,
    avg_monthly as avg_monthly_sales, month_total as this_month_total{extra};"""

_WINDOWED = _WHERE + _SELECT.format(
    extra=", prev_total as prior_month_total, next_total as next_month_total"
)
_PLAIN = _WHERE + _SELECT.format(extra="")


def _concept(address: str) -> BuildConcept:
    """Padding-nullable only: no NULLABLE modifier, so `__post_init__` cannot
    restore it and `__add__` is the only thing that can carry it."""
    namespace, name = address.split(".")
    return BuildConcept(
        name=name,
        canonical_name=name,
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(components=set()),
    )


def _qds(nullable_concepts: list[BuildConcept]) -> QueryDatasource:
    return QueryDatasource(
        input_concepts=[],
        output_concepts=list(nullable_concepts),
        datasources=[],
        source_map={c.address: set() for c in nullable_concepts},
        grain=BuildGrain(components=set()),
        joins=[],
        source_type=SourceType.SELECT,
        nullable_concepts=list(nullable_concepts),
    )


def test_add_unions_padding_nullability_from_both_sides():
    left = _concept("local.padded_left")
    right = _concept("local.padded_right")
    merged = _qds([left]) + _qds([right])
    assert {c.address for c in merged.nullable_concepts} == {
        "local.padded_left",
        "local.padded_right",
    }


@pytest.fixture(scope="module")
def sql():
    env = Environment(working_path=_WORKING)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_text(_MODEL)
    return {
        "windowed": executor.generate_sql(_WINDOWED)[-1],
        "plain": executor.generate_sql(_PLAIN)[-1],
    }


@pytest.mark.parametrize("variant", ["windowed", "plain"])
def test_store_dimension_join_is_preserved(sql, variant):
    assert 'LEFT OUTER JOIN "memory"."store"' in sql[variant], sql[variant]


@pytest.mark.parametrize("variant", ["windowed", "plain"])
def test_merge_on_padded_dimension_is_null_safe(sql, variant):
    assert '"ss_store_name" is not distinct from ' in sql[variant], sql[variant]
