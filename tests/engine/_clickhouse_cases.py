"""Shared trilogy DSL smoke cases for ClickHouse, run against both server
(test_clickhouse_server.py) and chdb (test_clickhouse_chdb.py).

Each entry is constant-input so it doesn't depend on real datasource state.
"""

from datetime import date, datetime
from typing import Any

from trilogy import Executor, parse

# (id, trilogy_expression, expected_value)
SMOKE_CASES: list[tuple[str, str, Any]] = [
    # math
    ("add", "1 + 2", 3),
    ("subtract", "5 - 3", 2),
    ("multiply", "3 * 4", 12),
    ("divide", "10.0 / 4", 2.5),
    ("mod", "mod(10, 3)", 1),
    ("power", "2 ** 10", 1024),
    ("abs", "abs(-5)", 5),
    ("floor", "floor(3.7)", 3),
    ("ceil", "ceil(3.2)", 4),
    ("round", "round(3.567, 2)", 3.57),
    ("sqrt", "sqrt(16.0)", 4.0),
    ("log10", "log(100.0, 10)", 2.0),
    ("log2", "log(8.0, 2)", 3.0),
    # string
    ("upper", "upper('hello')", "HELLO"),
    ("lower", "lower('HELLO')", "hello"),
    ("len", "len('hello')", 5),
    ("substring", "substring('hello', 1, 3)", "hel"),
    ("strpos", "strpos('hello world', 'world')", 7),
    ("contains", "contains('hello world', 'world')", True),
    ("trim", "trim('  hello  ')", "hello"),
    ("ltrim", "ltrim('  hello')", "hello"),
    ("rtrim", "rtrim('hello  ')", "hello"),
    ("replace", "replace('hello', 'l', 'L')", "heLLo"),
    ("concat", "concat('hello', ' ', 'world')", "hello world"),
    ("split", "split('a,b,c', ',')", ["a", "b", "c"]),
    ("hex", "hex('AB')", "4142"),
    ("hash_md5", "hash('abc', md5)", "900150983cd24fb0d6963f7d28e17f72"),
    ("hash_sha256", "len(hash('abc', sha256))", 64),
    # regex
    ("regexp_contains", "regexp_contains('hello123', '[0-9]+')", True),
    ("regexp_extract", "regexp_extract('hello123', '[0-9]+')", "123"),
    ("regexp_replace", "regexp_replace('hello123', '[0-9]+', 'X')", "helloX"),
    # like
    ("like", "like('hello', 'h%')", True),
    ("ilike", "ilike('Hello', 'h%')", True),
    # date extract
    ("year", "year(cast('2024-03-15' as date))", 2024),
    ("month", "month(cast('2024-03-15' as date))", 3),
    ("day", "day(cast('2024-03-15' as date))", 15),
    ("quarter", "quarter(cast('2024-08-15' as date))", 3),
    ("week", "week(cast('2024-03-15' as date))", 11),
    ("day_of_week", "day_of_week(cast('2024-03-15' as date))", 5),  # Friday=5
    ("day_name", "day_name(cast('2024-03-15' as date))", "Friday"),
    ("month_name", "month_name(cast('2024-03-15' as date))", "March"),
    ("hour", "hour(cast('2024-03-15 10:20:30' as datetime))", 10),
    ("minute", "minute(cast('2024-03-15 10:20:30' as datetime))", 20),
    ("second", "second(cast('2024-03-15 10:20:30' as datetime))", 30),
    # date manipulation
    (
        "date_trunc",
        "date_trunc(cast('2024-03-15' as date), month)",
        date(2024, 3, 1),
    ),
    (
        "date_add",
        "date_add(cast('2024-03-15' as date), day, 5)",
        date(2024, 3, 20),
    ),
    (
        "date_sub",
        "date_sub(cast('2024-03-15' as date), day, 5)",
        date(2024, 3, 10),
    ),
    (
        "date_diff",
        "date_diff(cast('2024-03-01' as date), cast('2024-03-15' as date), day)",
        14,
    ),
    ("date_cast", "date('2024-03-15')", date(2024, 3, 15)),
    (
        "datetime_cast",
        "datetime('2024-03-15 10:20:30')",
        datetime(2024, 3, 15, 10, 20, 30),
    ),
    (
        "format_time",
        "format_time(cast('2024-03-15' as date), '%Y/%m/%d')",
        "2024/03/15",
    ),
    # constants — checked for non-null only
    ("current_date_not_null", "current_date()", None),
    ("current_datetime_not_null", "current_datetime()", None),
]


# Aggregate / array / map / struct cases. body must define an `__AGG__` concept
# whose value is the expected result. `<CASE>` is replaced with case_id for
# uniqueness across the parametrized run.
AGG_CASES: list[tuple[str, str, str, Any]] = [
    (
        "sum",
        "auto __VALS__ <- unnest([1,2,3,4,5]); auto __AGG__ <- sum(__VALS__);",
        "agg",
        15,
    ),
    (
        "avg",
        "auto __VALS__ <- unnest([2.0,4.0,6.0]); auto __AGG__ <- avg(__VALS__);",
        "agg",
        4.0,
    ),
    (
        "max",
        "auto __VALS__ <- unnest([3,1,4,1,5,9,2,6]); auto __AGG__ <- max(__VALS__);",
        "agg",
        9,
    ),
    (
        "min",
        "auto __VALS__ <- unnest([3,1,4,1,5,9,2,6]); auto __AGG__ <- min(__VALS__);",
        "agg",
        1,
    ),
    (
        "count",
        "auto __VALS__ <- unnest([1,2,3,4,5]); auto __AGG__ <- count(__VALS__);",
        "agg",
        5,
    ),
    (
        "count_distinct",
        "auto __VALS__ <- unnest([1,1,2,2,3]); auto __AGG__ <- count_distinct(__VALS__);",
        "agg",
        3,
    ),
    (
        "any",
        "auto __VALS__ <- unnest([7,7,7]); auto __AGG__ <- any(__VALS__);",
        "agg",
        7,
    ),
    (
        "bool_or_true",
        "auto __VALS__ <- unnest([false, true, false]); auto __AGG__ <- bool_or(__VALS__);",
        "agg",
        True,
    ),
    (
        "bool_or_false",
        "auto __VALS__ <- unnest([false, false, false]); auto __AGG__ <- bool_or(__VALS__);",
        "agg",
        False,
    ),
    (
        "bool_and_true",
        "auto __VALS__ <- unnest([true, true, true]); auto __AGG__ <- bool_and(__VALS__);",
        "agg",
        True,
    ),
    (
        "bool_and_false",
        "auto __VALS__ <- unnest([true, false, true]); auto __AGG__ <- bool_and(__VALS__);",
        "agg",
        False,
    ),
    (
        "array_agg",
        "auto __VALS__ <- unnest([1,2,3]); auto __AGG__ <- array_agg(__VALS__);",
        "agg",
        [1, 2, 3],
    ),
    (
        "array_sort",
        "auto __VALS__ <- unnest([3,1,2]); auto __AGG__ <- array_sort([3,1,2]);",
        "agg",
        [1, 2, 3],
    ),
    (
        "array_distinct",
        "auto __VALS__ <- unnest([1,2,2,3]); auto __AGG__ <- array_distinct([1,2,2,3]);",
        "agg",
        [1, 2, 3],
    ),
    (
        "array_sum",
        "auto __VALS__ <- unnest([1,2,3]); auto __AGG__ <- array_sum([1,2,3]);",
        "agg",
        6,
    ),
    (
        "array_to_string",
        "auto __VALS__ <- unnest(['a','b','c']); auto __AGG__ <- array_to_string(['a','b','c'], ',');",
        "agg",
        "a,b,c",
    ),
    (
        "array_transform",
        (
            "const xs_<CASE> <- [1,2,3,4]; "
            "def double_<CASE>(x) -> x * 2; "
            "auto __AGG__ <- array_sum(array_transform(xs_<CASE>, @double_<CASE>));"
        ),
        "agg",
        20,
    ),
    (
        "array_filter",
        (
            "const xs_<CASE> <- [1,2,3,4,5]; "
            "def gt2_<CASE>(x) -> x > 2; "
            "auto __AGG__ <- array_filter(xs_<CASE>, @gt2_<CASE>);"
        ),
        "agg",
        [3, 4, 5],
    ),
    (
        "generate_array",
        "auto __VALS__ <- unnest([0]); auto __AGG__ <- array_sum(generate_array(1, 5, 1));",
        "agg",
        15,
    ),
    (
        "index_access",
        "auto __VALS__ <- unnest([0]); auto __AGG__ <- [10, 20, 30][2];",
        "agg",
        20,
    ),
    (
        "map_keys",
        ("const m_<CASE> <- {'a': 1, 'b': 2}; " "auto __AGG__ <- map_keys(m_<CASE>);"),
        "agg",
        ["a", "b"],
    ),
    (
        "map_values",
        (
            "const m_<CASE> <- {'a': 1, 'b': 2}; "
            "auto __AGG__ <- map_values(m_<CASE>);"
        ),
        "agg",
        [1, 2],
    ),
    (
        "map_access",
        "const m_<CASE> <- {'a': 1, 'b': 2}; auto __AGG__ <- m_<CASE>['a'];",
        "agg",
        1,
    ),
    (
        "struct_attr_int",
        "const s_<CASE> <- struct(1->a, 2.5->b); auto __AGG__ <- s_<CASE>.a;",
        "agg",
        1,
    ),
    (
        "struct_attr_float",
        "const s_<CASE> <- struct(1->a, 2.5->b); auto __AGG__ <- s_<CASE>.b;",
        "agg",
        2.5,
    ),
    (
        "struct_attr_str",
        (
            "const s_<CASE> <- struct('hi'->name, 42->n); "
            "auto __AGG__ <- s_<CASE>.name;"
        ),
        "agg",
        "hi",
    ),
]


def run_smoke_case(executor: Executor, case_id: str, expr: str, expected: Any) -> None:
    name = f"smoke_{case_id}"
    text = f"const {name} <- {expr}; select {name};"
    parse(text, environment=executor.environment)
    rows = executor.execute_text(f"select {name};")[0].fetchall()
    actual = rows[0][0]
    if expected is None:
        assert actual is not None, f"{case_id}: got None"
        return
    assert actual == expected, f"{case_id}: got {actual!r}, want {expected!r}"


def run_agg_case(
    executor: Executor,
    case_id: str,
    body: str,
    select_name: str,
    expected: Any,
) -> None:
    suffix = f"_{case_id}"
    vals_name = f"vals{suffix}"
    agg_name = f"agg{suffix}"
    text = (
        body.replace("__VALS__", vals_name)
        .replace("__AGG__", agg_name)
        .replace("<CASE>", case_id)
    )
    full_name = agg_name if select_name == "agg" else f"{select_name}{suffix}"
    parse(text, environment=executor.environment)
    rows = executor.execute_text(f"select {full_name};")[0].fetchall()
    actual = rows[0][0]
    assert actual == expected, f"{case_id}: got {actual!r}, want {expected!r}"
