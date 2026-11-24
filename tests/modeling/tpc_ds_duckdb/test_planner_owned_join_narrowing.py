"""Join narrowings the planner owns, that the optimizer's join upgrade used to
make after it (handoff 2.1): a merge's own `IS NOT NULL` on a column only one
side of a FULL holds, and a later INNER equality on a padded side's key."""

from pathlib import Path

from trilogy import Environment
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join, QueryDatasource
from trilogy.core.query_processor import get_query_datasources

QUERY = """import all_sales as s;
where s.return_amount is not null
select s.sale_date.date, s.return_date.date;"""


def _join_types(ds: QueryDatasource) -> dict[str, JoinType]:
    out = {
        join.right.identifier: join.join_type
        for join in ds.joins
        if isinstance(join, Join)
    }
    for source in ds.datasources:
        if isinstance(source, QueryDatasource):
            out |= _join_types(source)
    return out


def _planned(query: str) -> dict[str, JoinType]:
    env = Environment(working_path=Path(__file__).parent)
    _, statements = env.parse(query)
    return _join_types(get_query_datasources(env, statements[-1]))


def test_side_only_not_null_narrows_full_at_plan_time():
    types = _planned(QUERY)
    returns = [t for name, t in types.items() if "returns" in name]
    assert returns == [JoinType.RIGHT_OUTER], types
    assert JoinType.FULL not in types.values(), types


def test_a_later_inner_on_a_padded_key_narrows_at_plan_time():
    query = (Path(__file__).parent / "query91.preql").read_text()
    types = _planned(query)
    customer = "cs.return_customer.customers_at_cs_return_customer_sk"
    address = (
        "cs.return_customer.current_address."
        "customer_address_at_cs_return_customer_current_address_sk"
    )
    assert types[address] == JoinType.INNER, types
    assert types[customer] == JoinType.INNER, types
