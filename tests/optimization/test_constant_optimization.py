from trilogy import Dialects


def test_constant_optimization():

    test_query = """
    const x <- 1;

    auto array <- unnest([1,2,3,4,5,6,7,8,9,10]);

    SELECT
        x,
        array
    WHERE
        array = x
    ;
    """

    exec = Dialects.DUCK_DB.default_executor()

    generated = exec.generate_sql(test_query)[0]
    assert '"array" = 1' in generated


def test_constant_filter():
    # validate that the constant is inlined into the filter
    test_query = """
    const x <- 1;

    auto array <- unnest([1,2,3,4,5,6,7,8,9,10]);

    SELECT
        array
    WHERE
        array = x
    ;
    """

    exec = Dialects.DUCK_DB.default_executor()

    generated = exec.generate_sql(test_query)[0]
    assert '"array" = 1' in generated
