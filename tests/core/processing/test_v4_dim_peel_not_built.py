"""A dimension peel (`_split_root_dimension_clusters`) is not built when it
cannot be read: keyed by the bucket's own row key, it rescans the fact; keyed
by an extension region's span, the region domain takes its members and the
domain is what FINAL reads."""

from tests.helpers.models import LINE_ITEMS
from tests.helpers.planning import built_groups, recorded
from tests.helpers.rows import executor_for, fetch_rows


def _built_peels(executor, query: str) -> list[str]:
    """The dimension peels built; the regraft's solid source is
    `grp:root:root:∅:basic_input:<key>` and is not one."""
    return [g for g in built_groups(recorded(executor, query)) if ":dim:" in g]


def test_no_peel_keyed_by_a_span_the_domain_takes():
    executor = executor_for(LINE_ITEMS)
    query = "select user_id, state, sum(sale_price) as revenue order by user_id asc;"
    assert not _built_peels(executor, query)
    rows = fetch_rows(executor, query)
    assert rows == [(1, "ca", 12.0), (2, "ny", 3.0), (3, "wa", None)]


def test_no_peel_keyed_by_the_facts_own_key():
    executor = executor_for(LINE_ITEMS)
    query = """
    select order_id, line_id, user_id, product_id, revenue, margin
    order by line_id asc nulls last, user_id asc nulls last, product_id asc nulls last;
    """
    assert not _built_peels(executor, query)
    rows = fetch_rows(executor, query)
    assert rows == [
        (10, 1, 1, 1, 5.0, 4.0),
        (10, 2, 1, 2, 7.0, 5.0),
        (11, 3, 2, 1, 3.0, 2.0),
        (None, None, 3, None, None, None),
        (None, None, None, 3, None, None),
    ]
