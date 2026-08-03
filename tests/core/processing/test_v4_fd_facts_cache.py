from trilogy import Environment
from trilogy.core.processing.v4_helper import functional_dependency as fd

FD_MODEL = """
key order_id int;
key customer_id int;
property order_id.order_total float;
property customer_id.customer_name string;

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    order_total: order_total,
)
grain (order_id)
query '''select 1 order_id, 10 customer_id, 5.0 order_total''';

datasource customers (
    customer_id: customer_id,
    customer_name: customer_name,
)
grain (customer_id)
query '''select 10 customer_id, 'acme' customer_name''';
"""


def _build(model: str):
    env = Environment()
    env.parse(model)
    return env.materialize_for_select()


class TestFDFactsCache:
    def test_table_is_reused_across_calls_on_one_environment(self):
        benv = _build(FD_MODEL)

        first = fd._fd_facts(benv)
        second = fd._fd_facts(benv)

        assert first is second

    def test_separate_environments_get_separate_tables(self):
        one = _build(FD_MODEL)
        two = _build(FD_MODEL)

        assert fd._fd_facts(one) is not fd._fd_facts(two)

    def test_cached_closure_matches_a_table_built_from_scratch(self):
        benv = _build(FD_MODEL)
        determinants = {"local.order_id"}

        cached = fd.build_fd_closure(benv, determinants)
        fd._FACTS_CACHE.clear()
        fresh = fd.build_fd_closure(benv, determinants)

        assert cached == fresh
        assert "local.order_total" in cached
        # the order's customer is determined, and through it the customer's own
        # property — the fixpoint's second hop
        assert "local.customer_id" in cached
        assert "local.customer_name" in cached

    def test_table_is_dropped_when_its_environment_dies(self):
        benv = _build(FD_MODEL)
        fd._fd_facts(benv)
        key = id(benv)
        assert key in fd._FACTS_CACHE

        del benv

        assert key not in fd._FACTS_CACHE
