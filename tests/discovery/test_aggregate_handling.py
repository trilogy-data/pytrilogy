from trilogy import Dialects

SETUP_CODE = """

key id int;
key id_class int;
property id.val int;

auto total_val <- sum(val);
auto total_val_class <- sum(val) by id_class;
datasource raw_ids (
id,
    id_class,
    val
)
grain (id)
query '''
SELECT
    1 as id,
    10 as id_class,
    100 as val
UNION ALL
SELECT
    2 as id,
    20 as id_class,
    200 as val
UNION ALL
SELECT
    3 as id,
    10 as id_class,
    300 as val
''';

datasource aggregated_class (
    id_class,
    total_val,
    total_val:total_val_class
)
grain (id_class)
query '''
SELECT
    10 as id_class,
    400 as total_val

UNION ALL
SELECT
    20 as id_class,
    200 as total_val
''';

"""


def test_aggregate_handling():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    q1 = SETUP_CODE
    exec = Dialects.DUCK_DB.default_executor()

    exec.parse_text(q1)

    generated = exec.generate_sql("""
SELECT
    total_val
            ;
""")[-1]
    # Grand-total `SELECT total_val;` can roll up from `aggregated_class`
    # (SUM of the per-id_class totals == grand total). Picking the
    # pre-aggregated source over a raw scan is safe and cheaper.
    assert "aggregated_class" in generated, generated
    assert 'sum("aggregated_class"."total_val")' in generated, generated

    generated = exec.generate_sql("""
SELECT
    total_val_class
            ;
""")[-1]
    assert "aggregated_class" in generated, generated


def test_aggregate_handling_alias():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    q1 = SETUP_CODE
    exec = Dialects.DUCK_DB.default_executor()

    exec.parse_text(q1)

    generated = exec.generate_sql("""
SELECT
    id_class,
    sum(val) as total_value
            ;
""")[-1]
    assert "aggregated_class" in generated, generated


SETUP_CODE_ALL = """

key id int;
key id_class int;
property id.val int;

auto total_val <- sum(val);
auto total_total_val <- sum(val) by *;
auto total_val_class <- sum(val) by id_class;
datasource raw_ids (
id,
    id_class,
    val
)
grain (id)
query '''
SELECT
    1 as id,
    10 as id_class,
    100 as val
UNION ALL
SELECT
    2 as id,
    20 as id_class,
    200 as val
UNION ALL
SELECT
    3 as id,
    10 as id_class,
    300 as val
''';


datasource aggregated_all (
    total_total_val
)   
query '''
SELECT
    600 as total_total_val
;
''';
"""


def test_aggregate_handling_abstract():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    exec.parse_text(SETUP_CODE_ALL)

    generated = exec.generate_sql("""
SELECT
    sum(val) by * as total_value
            ;
""")[-1]
    assert "aggregated_all" in generated, generated


PARTIAL_AGG_SETUP = """
key id int;
key origin_code string;
key destination_code string;
property id.flight_date date;
property id.distance int;
property id.cancelled string;

auto flight_count <- count(id);
auto total_distance <- sum(distance);
auto avg_distance <- avg(distance);
auto distinct_destinations <- count_distinct(destination_code);

datasource flight (
    id:id,
    origin_code:origin_code,
    destination_code:destination_code,
    flight_date:flight_date,
    distance:distance,
    cancelled:cancelled,
)
grain (id)
query '''
select 1 as id, 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date, 10 as distance, 'N' as cancelled
union all select 2, 'A', 'Y', '2024-01-01'::date, 20, 'N'
union all select 3, 'B', 'X', '2024-01-01'::date, 30, 'Y'
''';

datasource flight_count_by_source_dest_date (
    origin_code:origin_code,
    destination_code:destination_code,
    flight_date:flight_date,
    flight_count:flight_count,
    total_distance:total_distance,
    avg_distance:avg_distance,
    distinct_destinations:distinct_destinations,
)
grain (origin_code, destination_code, flight_date)
query '''
select 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date, 1 as flight_count, 10 as total_distance, 10 as avg_distance, 1 as distinct_destinations
union all select 'A', 'Y', '2024-01-01'::date, 1, 20, 20, 1
union all select 'B', 'X', '2024-01-01'::date, 1, 30, 30, 1
''';
"""


UNSAFE_PARTIAL_AGG_SETUP = """
key id int;
key origin_code string;
key destination_code string;
property id.flight_date date;

auto flight_count <- count(id);

datasource flight (
    id:id,
    origin_code:origin_code,
    flight_date:flight_date,
)
grain (id)
query '''
select 1 as id, 'A' as origin_code, '2024-01-01'::date as flight_date
union all select 2, 'A', '2024-01-01'::date
''';

datasource flight_count_by_source_dest_date (
    origin_code:origin_code,
    destination_code:destination_code,
    flight_date:flight_date,
    flight_count:flight_count,
)
grain (origin_code, destination_code, flight_date)
query '''
select 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date, 1 as flight_count
union all select 'A', 'Y', '2024-01-01'::date, 1
''';
"""


def test_partial_additive_aggregate_rollup_sql():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PARTIAL_AGG_SETUP)

    generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    flight_count
;
""")[-1]

    assert "flight_count_by_source_dest_date" in generated, generated
    assert 'sum("flight_count_by_source_dest_date"."flight_count")' in generated
    assert "GROUP BY\n    1,\n    2" in generated


def test_partial_sum_aggregate_rollup_sql():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PARTIAL_AGG_SETUP)

    generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    total_distance
;
""")[-1]

    assert "flight_count_by_source_dest_date" in generated, generated
    assert 'sum("flight_count_by_source_dest_date"."total_distance")' in generated


def test_partial_aggregate_rollup_execution_matches_raw_result():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PARTIAL_AGG_SETUP)

    results = exec.execute_text("""
SELECT
    origin_code,
    flight_date,
    flight_count,
    total_distance
ORDER BY
    origin_code asc
;
""")[-1].fetchall()

    assert [(row[0], row[2], row[3]) for row in results] == [
        ("A", 2, 30),
        ("B", 1, 30),
    ]


def test_partial_aggregate_rollup_rejects_unsupported_aggregates():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PARTIAL_AGG_SETUP)

    avg_generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    avg_distance
;
""")[-1]
    distinct_generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    distinct_destinations
;
""")[-1]

    assert "flight_count_by_source_dest_date" not in avg_generated, avg_generated
    assert (
        'count(distinct "flight_count_by_source_dest_date"."destination_code")'
        in distinct_generated
    ), distinct_generated
    assert (
        '"flight_count_by_source_dest_date"."distinct_destinations"'
        not in distinct_generated
    ), distinct_generated


def test_partial_aggregate_rollup_requires_dropped_grain_to_depend_on_input_key():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(UNSAFE_PARTIAL_AGG_SETUP)

    generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    flight_count
;
""")[-1]

    assert "flight_count_by_source_dest_date" not in generated, generated


def test_partial_aggregate_rollup_rejects_foreign_filter():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(PARTIAL_AGG_SETUP)

    generated = exec.generate_sql("""
SELECT
    origin_code,
    flight_date,
    flight_count
WHERE
    cancelled = 'N'
;
""")[-1]

    assert "flight_count_by_source_dest_date" not in generated, generated
