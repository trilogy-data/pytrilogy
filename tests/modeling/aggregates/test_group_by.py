from trilogy import Executor, parse
from trilogy.core.models.author import Grain
from trilogy.core.models.environment import Environment


def test_group_by(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    sum(qty) by stores.name-> total_qty
ORDER BY 
    total_qty desc
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (4)

    test_select = """
auto total_qty <- sum(qty);
SELECT
 total_qty by stores.name-> total_qty_stores
ORDER BY total_qty_stres desc
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (4)