from trilogy import Dialects
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentConfig,
)
from trilogy.core.statements.execute import ProcessedQuery


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


def test_processed_query_parameters_populated():
    test_query = """
    const x <- 'hello';

    SELECT x;
    """
    executor = Dialects.DUCK_DB.default_executor()
    _, statements = executor.environment.parse(test_query)
    processed = executor.generator.generate_queries(
        executor.environment, [statements[-1]]
    )
    pq = processed[0]
    assert isinstance(pq, ProcessedQuery)
    assert "x" in pq.parameters
    assert pq.parameters["x"] == "hello"


def test_compile_statement_with_params_returns_sql_and_dict():
    test_query = """
    const filter_val <- 'A319';

    auto array <- unnest(['A319', 'B737', 'C172']);

    SELECT array WHERE array = filter_val;
    """
    executor = Dialects.DUCK_DB.default_executor()
    _, statements = executor.environment.parse(test_query)
    processed = executor.generator.generate_queries(
        executor.environment, [statements[-1]]
    )
    sql, params = executor.generator.compile_statement_with_params(processed[0])
    assert ":filter_val" in sql
    assert params == {
        "_virt_705671875681119": [
            "A319",
            "B737",
            "C172",
        ],
        "filter_val": "A319",
    }


def test_compile_statement_with_params_imported_namespace():
    env = Environment(
        config=EnvironmentConfig(
            import_resolver=DictImportResolver(
                content={"filters": "const threshold <- 42;"}
            )
        )
    )
    env.parse("import filters as filters;")
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    _, statements = env.parse("select filters.threshold;")
    processed = executor.generator.generate_queries(env, [statements[-1]])
    pq = processed[0]
    assert isinstance(pq, ProcessedQuery)
    # safe_address for namespace 'filters', name 'threshold' → 'filters_threshold'
    assert "filters_threshold" in pq.parameters
    assert pq.parameters["filters_threshold"] == 42

    sql, params = executor.generator.compile_statement_with_params(pq)
    assert ":filters_threshold" in sql
    assert params == {"filters_threshold": 42}


def test_compile_statement_with_params_no_params_when_rendering_disabled():
    from trilogy.constants import Rendering
    from trilogy.dialect.duckdb import DuckDBDialect

    test_query = """
    const x <- 99;

    SELECT x;
    """
    env = Environment()
    gen = DuckDBDialect(rendering=Rendering(parameters=False))
    _, statements = env.parse(test_query)
    processed = gen.generate_queries(env, [statements[-1]])
    sql, params = gen.compile_statement_with_params(processed[0])
    assert params == {}
    assert "99" in sql
