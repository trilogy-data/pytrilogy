from trilogy import Dialects
from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.enums import Granularity

MODEL = """
key id int;
property id.all_rows int;
property id.amount int;
datasource t (id: id, all_rows: all_rows, amount: amount)
grain (id)
query '''select 1 as id, 1 as all_rows, 10 as amount
union all select 2, 1, 20
union all select 3, 2, 5''';
auto per_bucket <- sum(amount) by all_rows;
auto total <- sum(amount) by *;
"""


def test_user_concept_named_all_rows_is_an_ordinary_grain() -> None:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    concepts = executor.environment.concepts
    assert concepts["local.per_bucket"].granularity == Granularity.MULTI_ROW
    assert concepts["local.total"].granularity == Granularity.SINGLE_ROW
    assert concepts["local.total"].grain.components == {ALL_ROWS_ADDRESS}
    rows = executor.execute_text(
        "select all_rows, per_bucket, total order by all_rows asc;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [(1, 30, 35), (2, 5, 35)]
