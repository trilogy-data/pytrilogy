import pytest

from trilogy import Dialects
from trilogy.core.enums import ConceptSource, Derivation, Purpose
from trilogy.core.query_processor import process_auto
from trilogy.core.statements.execute import ProcessedQueryPersist
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.parser import parse

TEST_DIALECTS: list[BaseDialect] = [
    BaseDialect(),
    BigqueryDialect(),
    DuckDBDialect(),
    SqlServerDialect(),
    SnowflakeDialect(),
]


def test_derivations_reparse():
    declarations = """
    key category_id int;
    property category_id.category_name string;
    datasource category_source (
        category_id:category_id,
        category_name:category_name,
    )
    grain (category_id)
    address category;

    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    persist bool_is_upper_name into upper_name from
    select
        test_upper_case_2
    ;
    
    select 
    test_upper_case_2;

    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    select 
    test_upper_case_2;

    """
    env, parsed = parse(declarations)
    for dialect in TEST_DIALECTS:
        compiled = []

        for statement in parsed[3:]:
            processed = process_auto(env, statement)
            if processed:
                compiled.append(dialect.compile_statement(processed))
                # force add since we didn't run it
                if isinstance(processed, ProcessedQueryPersist):
                    env.add_datasource(processed.datasource)
        #     env, _ = parse(
        #         """    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

        # select
        # test_upper_case_2;""",
        #         environment=env,
        #     )
        test_concept = env.concepts["test_upper_case_2"]
        assert test_concept.purpose == Purpose.PROPERTY

        build_env = env.materialize_for_select()
        assert test_concept.address in build_env.materialized_concepts

        # test that the rendered SQL didn't need to use a cASE
        assert "CASE" not in compiled[-1]


def test_derivations_reparse_new():
    declarations = """
    key category_id int;
    property category_id.category_name string;
    datasource category_source (
        category_id:category_id,
        category_name:category_name,
    )
    grain (category_id)
    address category;

    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    persist bool_is_upper_name into upper_name from
    select
        test_upper_case_2
    ;
    
    select 
    test_upper_case_2;



    """
    env, parsed = parse(declarations)
    for dialect in TEST_DIALECTS:
        compiled = []

        for statement in parsed[3:]:
            processed = process_auto(env, statement)
            if processed:
                compiled.append(dialect.compile_statement(processed))
                # force add since we didn't run it
                if isinstance(processed, ProcessedQueryPersist):
                    env.add_datasource(processed.datasource)
        env, _parsed2 = parse(
            """    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then False else True END;
    select 
    test_upper_case_2;""",
            environment=env,
        )

        compiled.append(dialect.compile_statement(process_auto(env, parsed[-1])))

        test_concept = env.concepts["local.test_upper_case_2"]
        build_env = env.materialize_for_select()
        assert test_concept.purpose == Purpose.PROPERTY
        assert test_concept.metadata.concept_source == ConceptSource.MANUAL
        assert test_concept not in build_env.materialized_concepts
        assert test_concept.derivation == Derivation.BASIC

        # test that the rendered SQL did need to use a case
        print(compiled[-1])
        assert "CASE" in compiled[-1], compiled[-1]


def test_persist_with_where():
    declarations = """
    key category_id int;
    property category_id.category_name string;
    datasource category_source (
        category_id:category_id,
        category_name:category_name,
    )
    grain (category_id)
    address category;

    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    persist bool_is_upper_name into upper_name from
    select
        test_upper_case_2
    where 
        category_id = 1
    ;
    
    select 
        test_upper_case_2
    where
        category_id = 1;


    """
    env, parsed = parse(declarations)
    for dialect in TEST_DIALECTS:
        compiled = []

        for statement in parsed[3:]:
            processed = process_auto(env, statement)
            if processed:
                compiled.append(dialect.compile_statement(processed))
                # force add since we didn't run it
                if isinstance(processed, ProcessedQueryPersist):
                    env.add_datasource(processed.datasource)
        env, _ = parse(
            """    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    select 
    test_upper_case_2;""",
            environment=env,
        )
        test_concept = env.concepts["test_upper_case_2"]
        assert test_concept.purpose == Purpose.PROPERTY

        # test that the rendered SQL didn't need to use a cASE
        assert "CASE" not in compiled[-1]
        assert "category_id" not in compiled[-1]


def test_persist_overwrite():

    base = """
    key x int;
    property x.y string;

    datasource ds0 (
    x, y)
    grain (x)
    query '''
    select 
    1 as x, 'fun' as y
    union all 
    select 2 as x, 'fun' as y
    '''
    ;

    datasource ds1 (
        fun:x,
        fun_y: y
    )
    grain (x)
    address test_table;

    create or replace datasource ds1;

    overwrite ds1;

    validate datasource ds1;

    """
    env, parsed = parse(base)
    executor = Dialects.DUCK_DB.default_executor(environment=env)

    for statement in parsed:
        executor.execute_statement(statement)

    rows = executor.execute_raw_sql("select * from test_table;").fetchall()

    assert len(rows) == 2
    for row in rows:
        assert row.fun_y == "fun"


_CREATE_MODE_BASE = """
    key x int;
    property x.y string;

    datasource ds0 (
    x, y)
    grain (x)
    query '''
    select
    1 as x, 'fun' as y
    union all
    select 2 as x, 'fun' as y
    '''
    ;

    datasource ds1 (
        fun:x,
        fun_y: y
    )
    grain (x)
    address test_table;

"""


def _run(script: str) -> Dialects:
    env, parsed = parse(_CREATE_MODE_BASE + script)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    for statement in parsed:
        executor.execute_statement(statement)
    return executor


def _rows(executor):
    return executor.execute_raw_sql("select * from test_table;").fetchall()


def test_create_with_data_populates():
    """`with data` runs the target's own query after the DDL."""
    rows = _rows(_run("create datasource ds1 with data;"))
    assert {row.fun for row in rows} == {1, 2}


def test_create_or_replace_with_data_populates():
    """`create or replace ... with data` is the DDL plus the load — the same
    SQL as `overwrite`."""
    rows = _rows(_run("create or replace datasource ds1 with data;"))
    assert {row.fun for row in rows} == {1, 2}


def test_create_modes_without_with_data_leave_table_empty():
    """A bare create is DDL: it makes the table, it does not load it. The empty
    table is what a following `append` expects."""
    assert _rows(_run("create datasource ds1;")) == []
    assert _rows(_run("create or replace datasource ds1;")) == []
    assert _rows(_run("create if not exists datasource ds1;")) == []


def test_create_if_not_exists_rejects_with_data():
    """The table may already hold rows, so the load would double it."""
    with pytest.raises(Exception) as exc_info:
        _run("create if not exists datasource ds1 with data;")
    assert "with data" in str(exc_info.value)
