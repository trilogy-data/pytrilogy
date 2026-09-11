"""The separate-import hint must see through select aliases and must not
depend on which of two equal-sized subgraphs happens to come first."""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException


@pytest.fixture
def project(tmp_path):
    (tmp_path / "dates.preql").write_text(
        "key date_id int;\n"
        "property date_id.year int;\n"
        "datasource dates (id: date_id, yr: year) grain (date_id)\n"
        "query '''select 1 id, 2001 yr''';\n"
    )
    (tmp_path / "sales.preql").write_text(
        "import dates as date;\n"
        "key sale_id int;\n"
        "property sale_id.amt float;\n"
        "auto total <- sum(amt);\n"
        "datasource sales (id: sale_id, amt: amt, d: date.date_id)\n"
        "grain (sale_id)\n"
        "query '''select 1 id, 9.0 amt, 1 d''';\n"
    )
    return Dialects.DUCK_DB.default_executor(working_path=tmp_path)


def _message(engine, sql: str) -> str:
    with pytest.raises(DisconnectedConceptsException) as exc:
        engine.generate_sql(sql)
    return str(exc.value)


def test_hint_sees_through_select_aliases(project):
    message = _message(
        project,
        "import sales as all_sales;\n"
        "import dates as date;\n"
        "select date.year as yr, all_sales.total as amount;\n",
    )
    assert "yr (= date.year)" in message
    assert "amount (= all_sales.total)" in message
    assert (
        "`date.year` (as `yr`) is disconnected, did you mean `all_sales.date.year`"
        in message
    )
    assert "join or merge" not in message


def test_hint_does_not_depend_on_subgraph_order(project):
    """Two singletons: whichever is listed first, the stranded copy is the
    one with a twin in the other side's component."""
    for select in (
        "select date.year, all_sales.total;",
        "select all_sales.total, date.year;",
    ):
        message = _message(
            project, f"import sales as all_sales;\nimport dates as date;\n{select}\n"
        )
        assert "did you mean `all_sales.date.year`" in message, message
