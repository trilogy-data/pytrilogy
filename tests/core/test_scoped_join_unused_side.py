"""A scoped join whose one side contributes only its join key is planned
without that side: `subset join prem.k = pa.k` with nothing else from `prem`
scans `pa` alone, and even a projected `prem.k` renders from `pa`. Consistent
with "joins never drop a row", but the reader gets no sign the narrow side was
dropped, so the plan carries a warning naming the pin and the inversion.
"""

from trilogy import Dialects
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement

MODEL = """
key amount_id int;
property amount_id.amount int;
key premium_id int;

datasource amounts (
    id: amount_id,
    amount: amount,
)
grain (amount_id)
query '''
select 1 as id, 100 as amount union all
select 2 as id, 200 as amount union all
select 3 as id, 300 as amount
''';

datasource premiums (
    id: premium_id,
)
grain (premium_id)
query '''
select 2 as id
''';
"""


def _plan(query: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    env, statements = executor.environment.parse(query)
    (statement,) = [s for s in statements if isinstance(s, SelectStatement)]
    return process_query(env, statement)


def test_unused_subset_side_is_flagged():
    processed = _plan("select sum(amount) as total subset join premium_id = amount_id;")
    (warning,) = processed.plan_warnings
    assert warning["kind"] == "scoped_join_side_unused"
    assert warning["side"] == "premium_id"
    assert "where premium_id is not null" in warning["message"]
    assert "subset join amount_id = premium_id" in warning["message"]


def test_pinned_side_is_not_flagged():
    processed = _plan(
        "where premium_id is not null select sum(amount) as total "
        "subset join premium_id = amount_id;"
    )
    assert processed.plan_warnings == []


def test_inverted_join_drives_from_the_narrow_side():
    processed = _plan("select sum(amount) as total subset join amount_id = premium_id;")
    assert processed.plan_warnings == []


def test_warning_reaches_the_result_payload():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    rows = executor.execute_text(
        "select sum(amount) as total subset join premium_id = amount_id;"
    )[-1].fetchall()
    # The elided side changes nothing about the rows; only the warning does.
    assert rows == [(600,)]
