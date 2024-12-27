from trilogy import Executor
from trilogy.core.models import BoundEnvironment


def test_statement_grains(test_environment: BoundEnvironment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    manufacturer,
    count(order_id)->order_count,
    count(product_id)->product_count
order by manufacturer asc;
"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())

    assert results[0] == ("maker1", 2, 1)
    assert results[1] == (None, 2, 1)
