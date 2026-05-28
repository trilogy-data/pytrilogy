from trilogy import Executor, parse
from trilogy.core.enums import Derivation, FunctionType
from trilogy.core.models.author import Function, Grain
from trilogy.core.models.environment import Environment


def test_group_by(test_environment: Environment, test_executor: Executor):

    test_select2 = """
auto total_qty <- sum(qty);

SELECT
    group total_qty by stores.name-> total_qty_stores,
    stores.name
ORDER BY 
    total_qty_stores desc
;"""

    _, _ = parse(test_select2, test_environment)
    tqs = test_environment.concepts["total_qty_stores"]
    assert isinstance(tqs.lineage, Function)
    assert tqs.lineage.operator == FunctionType.GROUP
    assert tqs.derivation == Derivation.GROUP_TO
    assert tqs.keys == {"stores.name"}
    assert tqs.grain == Grain(components={"stores.name"})

    results = test_executor.execute_text(test_select2)[0].fetchall()

    assert results[0] == (4, "store1")


def test_count_by_distribution_auto(
    test_environment: Environment, test_executor: Executor
):
    """Q13 shape via auto/metric: should group by the per-Y count value."""
    test_select = """
auto orders_per_store_auto <- count(order_id) by stores.id;
SELECT
    orders_per_store_auto,
    count(stores.id) -> store_count_auto
ORDER BY
    store_count_auto desc,
    orders_per_store_auto desc
;"""

    results = test_executor.execute_text(test_select)[0].fetchall()
    assert results == [(2, 2), (1, 1)]


def test_count_by_distribution_coalesce(
    test_environment: Environment, test_executor: Executor
):
    """The exact shape used in tests/modeling/tpc_h/query13.preql."""
    test_select = """
auto orders_per_store_co <- count(order_id) by stores.id;
SELECT
    coalesce(orders_per_store_co, 0) -> c_count,
    count(stores.id) -> custdist
ORDER BY
    custdist desc,
    c_count desc
;"""

    results = test_executor.execute_text(test_select)[0].fetchall()
    assert results == [(2, 2), (1, 1)]


def test_count_by_distribution_inline(
    test_environment: Environment, test_executor: Executor
):
    """Same as above but inline `count(X) by Y -> name`. Should behave identically."""
    test_select = """
SELECT
    count(order_id) by stores.id -> orders_per_store,
    count(stores.id) -> store_count
ORDER BY
    store_count desc,
    orders_per_store desc
;"""

    results = test_executor.execute_text(test_select)[0].fetchall()
    assert results == [(2, 2), (1, 1)]
