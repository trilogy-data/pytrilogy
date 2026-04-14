from pytest import raises

from trilogy import Dialects
from trilogy.parsing.v2.model import HydrationError


def test_array():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    const num_list <- [1,2,3,3,4,5];

    SELECT
        len(num_list) AS length,
        array_sum(num_list) AS total,
        array_distinct(num_list) AS distinct_values,
        array_sort(num_list, asc) AS sorted_values,
    ;"""
    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0][0] == 6  # length
    assert results[0][1] == 18  # total
    assert set(results[0][2]) == {1, 2, 3, 4, 5}, "distinct matches"  # distinct_values
    assert results[0][3] == [1, 2, 3, 3, 4, 5]  # sorted_values


def test_array_agg():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    const num_list <- unnest([1,2,3,3,4,5]);

    SELECT
        array_agg(num_list) AS aggregated_values,
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == ([1, 2, 3, 3, 4, 5],)  # aggregated_values


def test_array_filter():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    const num_list <- [1,2,3,3,4,5];

    def filter(x) -> x > 2;

    SELECT
        array_filter(num_list, @filter) AS filtered_values,
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == ([3, 3, 4, 5],)  # filtered_values

    with raises(HydrationError) as e:
        test_select_invalid = """
        const num<- 5;

        SELECT
            array_filter(num, @filter) AS filtered_values,
        ;"""
        list(test_executor.execute_text(test_select_invalid)[0].fetchall())
    assert "Array filter function must be applied to an array, not INTEGER" in str(
        e.value
    )
    with raises(HydrationError) as e:

        test_select_invalid = """

        def filter(x, y) -> x > 2;
        SELECT
            array_filter(num_list, @filter) AS filtered_values,
        ;"""
        list(test_executor.execute_text(test_select_invalid)[0].fetchall())
    assert "Array filter lambda must have exactly 1 argument" in str(e.value)


def test_transform():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    const num_list <- [1,2,3,4,5];

    def double_value(x)->  x * 2;

    SELECT
        array_transform(num_list, @double_value) AS transformed_values,
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == ([2, 4, 6, 8, 10],)  # transformed_values


def test_generate_array():
    test_executor = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    test_select = """
    const num_list <- generate_array(1,5,1);

    def double_value(x)->  x * 2;

    SELECT
        array_transform(num_list, @double_value) AS transformed_values,
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == ([2, 4, 6, 8, 10],)  # transformed_values
