"""What BigQuery's FULL OUTER JOIN planner actually accepts in an ON clause.

`trilogy/dialect/bigquery.py`'s null wrapper rests entirely on this table, and
none of it is checkable offline: BigQuery exempts UNNEST/constant inputs from
the restriction, so a scratch repro over `unnest([1,2,3])` passes and proves
nothing. It needs real tables, which is what this does -- and it asserts the
rejections as well as the acceptances, since a rule that quietly relaxed would
leave the encoding as pure cost.

Needs a dataset the credentials may create and drop tables in
(TRILOGY_BIGQUERY_TEST_DATASET).
"""

from collections.abc import Generator
from uuid import uuid4

import pytest

from trilogy import Dialects, Executor

pytestmark = pytest.mark.bigquery_execution

# left (x, y) x right (x). `y` is NULL on every row, so `coalesce(a.x, a.y)` is
# `a.x` semantically while staying an expression -- the minimal stand-in for
# the merged key `coalesce(a.x, b.x, c.x)` that broke sales_reporting.
LEFT_ROWS = "(1, null), (2, null), (null, null)"
RIGHT_ROWS = "(1), (3), (null)"
# 1 matches; 2 and NULL are left-only; 3 and NULL are right-only
ROWS_NULLS_APART = 5
# ... unless NULL matches NULL, which collapses two of those rows into one
ROWS_NULLS_TOGETHER = 4

FIELD = "`a`.`x` = `b`.`x`"
EXPR = "coalesce(`a`.`x`, `a`.`y`) = `b`.`x`"
ENCODED = "TO_JSON_STRING(coalesce(`a`.`x`, `a`.`y`)) = TO_JSON_STRING(`b`.`x`)"


def null_safe_or(key: str) -> str:
    left, _, right = key.partition(" = ")
    return f"({key} or ({left} is null and {right} is null))"


@pytest.fixture(scope="module")
def live_executor(bq_write_dataset) -> Generator[Executor, None, None]:
    try:
        executor = Dialects.BIGQUERY.default_executor()
    except Exception as e:
        pytest.skip(f"BigQuery not available: {e}")
    yield executor
    executor.close()


@pytest.fixture(scope="module")
def join_tables(
    live_executor, bq_write_dataset
) -> Generator[tuple[str, str], None, None]:
    suffix = uuid4().hex[:8]
    left = f"{bq_write_dataset}.trilogy_fulljoin_l_{suffix}"
    right = f"{bq_write_dataset}.trilogy_fulljoin_r_{suffix}"
    try:
        live_executor.execute_raw_sql(f"create table `{left}` (x int64, y int64)")
        live_executor.execute_raw_sql(f"create table `{right}` (x int64)")
        live_executor.execute_raw_sql(f"insert into `{left}` values {LEFT_ROWS}")
        live_executor.execute_raw_sql(f"insert into `{right}` values {RIGHT_ROWS}")
        yield left, right
    finally:
        live_executor.execute_raw_sql(f"drop table if exists `{left}`")
        live_executor.execute_raw_sql(f"drop table if exists `{right}`")


def full_join_rows(live_executor, join_tables, on: str) -> int:
    """Rows the FULL join emits, or a raised error if BigQuery refuses to plan
    the ON clause."""
    left, right = join_tables
    rows = live_executor.execute_raw_sql(
        f"select count(*) from `{left}` as `a` full join `{right}` as `b` on {on}"
    ).fetchall()
    return rows[0][0]


def rejects(live_executor, join_tables, on: str) -> bool:
    try:
        full_join_rows(live_executor, join_tables, on)
    except Exception as e:  # the driver wraps the 400 differently per version
        assert "equality of fields" in str(e), str(e)
        return True
    return False


def test_field_keys_plan_in_both_forms(live_executor, join_tables):
    """Both operands are fields, so the OR-expansion is recognised -- and it is
    the form that matches the two NULL keys to each other."""
    assert full_join_rows(live_executor, join_tables, FIELD) == ROWS_NULLS_APART
    assert (
        full_join_rows(live_executor, join_tables, null_safe_or(FIELD))
        == ROWS_NULLS_TOGETHER
    )


def test_expression_key_plans_bare_but_not_under_or(live_executor, join_tables):
    """The asymmetry the wrapper keys off: a bare equality of expressions is
    accepted -- so a non-nullable merged key needs no encoding -- while the
    same equality inside the null-safe OR is not."""
    assert full_join_rows(live_executor, join_tables, EXPR) == ROWS_NULLS_APART
    assert rejects(live_executor, join_tables, null_safe_or(EXPR))


def test_is_not_distinct_from_is_rejected(live_executor, join_tables):
    assert rejects(live_executor, join_tables, "`a`.`x` is not distinct from `b`.`x`")


def test_constant_condition_is_accepted(live_executor, join_tables):
    """A keyless FULL join renders `on 1=1`, which the error message's wording
    ("fields from both sides") reads as illegal. It is not: a constant folds to
    a cross product, which BigQuery is happy to plan. The restriction is only
    ever about the OR/IS-NOT-DISTINCT-FROM shapes it cannot reduce to a key."""
    assert full_join_rows(live_executor, join_tables, "1=1") == 9
    assert full_join_rows(live_executor, join_tables, "true") == 9


def test_json_encoding_plans_and_is_null_safe(live_executor, join_tables):
    """The shape the wrapper emits: accepted, and it matches NULL to NULL the
    way the OR-expansion it replaces does."""
    assert full_join_rows(live_executor, join_tables, ENCODED) == ROWS_NULLS_TOGETHER
