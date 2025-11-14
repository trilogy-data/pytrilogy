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

    generated = exec.generate_sql(
        """
SELECT
    total_val
            ;
"""
    )[-1]
    assert "aggregated_class" not in generated

    generated = exec.generate_sql(
        """
SELECT
    total_val_class
            ;
"""
    )[-1]
    assert "aggregated_class" in generated, generated


def test_aggregate_handling_alias():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    q1 = SETUP_CODE
    exec = Dialects.DUCK_DB.default_executor()

    exec.parse_text(q1)

    generated = exec.generate_sql(
        """
SELECT
    id_class,
    sum(val) as total_value
            ;
"""
    )[-1]
    assert "aggregated_class" in generated, generated
