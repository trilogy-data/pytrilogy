# from preql.compiler import compile
from preql.core.enums import Purpose
from preql.core.models import Environment, Grain, ProcessedQueryPersist
from preql.core.query_processor import process_auto
from preql.dialect.base import BaseDialect
from preql.dialect.bigquery import BigqueryDialect
from preql.dialect.duckdb import DuckDBDialect
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse
from preql.core.processing.nodes.select_node_v2 import SelectNode
from preql.core.env_processor import (
    generate_graph,
    datasource_to_node,
    concept_to_node,
)

TEST_DIALECTS: list[BaseDialect] = [
    BaseDialect(),
    BigqueryDialect(),
    DuckDBDialect(),
    SqlServerDialect(),
]


def test_derivations(test_environment: Environment):
    declarations = """
    key test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    persist bool_is_upper_name into upper_name from
    select
        test_upper_case_2
    ;
    
    select 
    test_upper_case_2;
    """
    env, parsed = parse(declarations, environment=test_environment)
    for dialect in TEST_DIALECTS:
        compiled = []

        for statement in parsed[1:]:
            processed = process_auto(test_environment, statement)
            compiled.append(dialect.compile_statement(processed))
            # force add since we didn't run it
            if isinstance(processed, ProcessedQueryPersist):
                test_environment.add_datasource(processed.datasource)
        assert (
            test_environment.concepts["test_upper_case_2"]
            in test_environment.materialized_concepts
        )
        assert len(compiled) == 2
        print(compiled[-1])
        assert "CASE" not in compiled[-1]

        concept = test_environment.concepts["test_upper_case_2"]
        assert concept.purpose == Purpose.KEY
        assert test_environment.datasources["bool_is_upper_name"].grain == Grain(
            components=[concept]
        )

        g = generate_graph(test_environment)
        import networkx as nx

        test = SelectNode(
            [concept.with_default_grain()],
            [],
            test_environment,
            g,
            parents=[],
        )
        path = nx.shortest_path(
            g,
            source=datasource_to_node(
                test_environment.datasources["bool_is_upper_name"]
            ),
            target=concept_to_node(concept.with_default_grain()),
        )
        assert len(path) == 2, path
        resolved = test.resolve()
        assert resolved is not None
