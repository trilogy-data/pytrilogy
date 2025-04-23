from trilogy import Executor, parse
from trilogy.core.models.author import Grain
from trilogy.core.models.environment import Environment


def test_key_fetch_cardinality(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
auto aspen_store <- filter stores.name where stores.name = 'aspen';
SELECT
    stores.name,
    aspen_store
;"""

    _, statements = parse(test_select, test_environment)
    select = statements[-1]
    assert select.grain.components == {
        "stores.name",
        "local.aspen_store",
    }

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert len(results) == 2
    assert ("store1", None) in results
    assert ("aspen", "aspen") in results


def test_key_count_cardinality(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    count(stores.name) ->name_count
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (3,)


def test_filtered_key_count_cardinality(
    test_environment: Environment, test_executor: Executor
):
    # test keys
    test_select = """
auto aspen_store <- filter stores.name where stores.name = 'aspen';

SELECT
    count(stores.name) ->name_count,
    count(aspen_store) ->aspen_count
;"""

    _, statements = parse(test_select, test_environment)
    assert test_environment.concepts["aspen_store"].grain == Grain(
        components=[test_environment.concepts["stores.id"]]
    )

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (3, 2)


def test_aggregates(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    sum(qty) -> total_qty
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (7,)


def test_computed(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    stores.upper_name,
    sum(qty) -> total_qty
ORDER BY total_qty desc
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == ("STORE1", 4)

