from trilogy.core.models import Environment
from trilogy import Executor


def test_merge_discovery(test_environment: Environment, test_executor: Executor):
    # test keys

    test_select = """

auto product_store_name <- store_name || product_name;
auto filtered <- filter product_store_name where store_id = 2;
SELECT
    filtered
where 
    filtered
;
"""

    assert len(list(test_executor.execute_text(test_select)[0].fetchall())) == 2
