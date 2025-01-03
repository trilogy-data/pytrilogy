# from trilogy.compiler import compile
import networkx as nx

from trilogy.core.enums import ConceptSource, Purpose, PurposeLineage
from trilogy.core.env_processor import (
    concept_to_node,
    datasource_to_node,
    generate_graph,
)
from trilogy.core.execute_models import (
    ProcessedQueryPersist,
)
from trilogy.core.author_models import PersistStatement, Grain
from trilogy.core.processing.node_generators import (
    gen_select_node,
)
from trilogy.core.processing.nodes.select_node_v2 import SelectNode
from trilogy.core.query_processor import process_auto
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse

TEST_DIALECTS: list[BaseDialect] = [
    BaseDialect(),
    BigqueryDialect(),
    DuckDBDialect(),
    SqlServerDialect(),
    SnowflakeDialect(),
]


def test_derivations():
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

        for idx, statement in enumerate(parsed[-2:]):
            if idx > 0:
                hooks = [DebuggingHook()]
            else:
                hooks = []
            processed = process_auto(env, statement, hooks=hooks)
            compiled.append(dialect.compile_statement(processed))
            # force add since we didn't run it
            if isinstance(processed, ProcessedQueryPersist):
                env.add_datasource(processed.datasource)

        test_concept = env.concepts["test_upper_case_2"]
        assert test_concept.purpose == Purpose.PROPERTY
        assert test_concept.address in env.materialized_concepts
        assert test_concept.derivation == PurposeLineage.ROOT

        persist: PersistStatement = parsed[-2]
        assert persist.select.grain == Grain(components=[test_concept])
        assert len(compiled) == 2

        g = generate_graph(env)

        path = nx.shortest_path(
            g,
            source=datasource_to_node(env.datasources["bool_is_upper_name"]),
            target=concept_to_node(test_concept.with_default_grain()),
        )
        assert len(path) == 3, path

        # test that the full function returns the value
        static = gen_select_node(
            concept=test_concept,
            local_optional=[],
            environment=env,
            g=g,
            depth=0,
        )
        assert static

        # test that the rendered SQL didn't need to use a cASE
        assert "CASE" not in compiled[-1]

        assert test_concept.purpose == Purpose.PROPERTY
        assert env.datasources["bool_is_upper_name"].grain == Grain(
            components=[test_concept]
        )

        # test that we can resolve a select
        test = SelectNode(
            [test_concept.with_default_grain()],
            [],
            env,
            g,
            parents=[],
        )
        resolved = test.resolve()
        assert resolved is not None


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

        for idx, statement in enumerate(parsed[3:]):
            if idx > 0:
                hooks = [DebuggingHook()]
            else:
                hooks = []
            processed = process_auto(env, statement, hooks=hooks)
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
        assert test_concept.metadata.concept_source == ConceptSource.PERSIST_STATEMENT
        assert test_concept.address in env.materialized_concepts
        assert test_concept.derivation == PurposeLineage.ROOT

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

        for idx, statement in enumerate(parsed[3:]):
            if idx > 0:
                hooks = [DebuggingHook()]
            else:
                hooks = []
            processed = process_auto(env, statement, hooks=hooks)
            if processed:
                compiled.append(dialect.compile_statement(processed))
                # force add since we didn't run it
                if isinstance(processed, ProcessedQueryPersist):
                    env.add_datasource(processed.datasource)
        env, parsed2 = parse(
            """    auto test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then False else True END;
    select 
    test_upper_case_2;""",
            environment=env,
        )

        compiled.append(dialect.compile_statement(process_auto(env, parsed[-1])))

        test_concept = env.concepts["local.test_upper_case_2"]
        assert test_concept.purpose == Purpose.PROPERTY
        assert test_concept.metadata.concept_source == ConceptSource.MANUAL
        assert test_concept not in env.materialized_concepts
        assert test_concept.derivation == PurposeLineage.BASIC

        # test that the rendered SQL did need to use a case
        assert "CASE" in compiled[-1]


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

        for idx, statement in enumerate(parsed[3:]):
            if idx > 0:
                hooks = [DebuggingHook()]
            else:
                hooks = []
            processed = process_auto(env, statement, hooks=hooks)
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
