from trilogy import Dialects


def test_constant_optimization():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
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
    assert '"array" = :x' in generated, generated


def test_constant_filter():
    # validate that the constant is inlined into the filter
    from trilogy.hooks.query_debugger import DebuggingHook

    test_query = """
    const x <- 1;

    auto array <- unnest([1,2,3,4,5,6,7,8,9,10]);

    SELECT
        array
    WHERE
        array = x
    ;
    """

    exec = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    generated = exec.generate_sql(test_query)[0]
    print(generated)
    results = exec.execute_text(test_query)[0].fetchall()
    assert results == [(1,)]
