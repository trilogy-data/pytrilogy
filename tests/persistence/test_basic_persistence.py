# from preql.compiler import compile
from pytest import raises
from preql.core.enums import DataType, Purpose
from preql.core.exceptions import InvalidSyntaxException
from preql.core.models import Select, Environment, Grain
from preql.core.query_processor import process_auto
from preql.dialect.base import BaseDialect
from preql.dialect.bigquery import BigqueryDialect
from preql.dialect.duckdb import DuckDBDialect
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse

TEST_DIALECTS:list[BaseDialect] = [BaseDialect(), BigqueryDialect(), DuckDBDialect(), SqlServerDialect()]



def test_derivations(test_environment:Environment):
    declarations = """
    key test_upper_case_2 <- CASE WHEN category_name = upper(category_name) then True else False END;

    persist bool_is_upper_name into upper_name
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
            compiled.append(dialect.compile_statement(process_auto(test_environment, statement)))
        assert len(compiled) == 2
        concept = test_environment.concepts['test_upper_case_2']
        assert concept.purpose == Purpose.KEY
        assert test_environment.datasources['bool_is_upper_name'] .grain == Grain(components=[concept])

        from preql.core.processing.nodes.select_node import SelectNode
        from preql.core.env_processor import generate_graph, datasource_to_node, concept_to_node
        g = generate_graph(test_environment)
        import networkx as nx
        test = SelectNode(
                        [concept.with_default_grain()],
                        [],
                        test_environment,
                        g,
                        parents=[
                        ],
                    )
        path = nx.shortest_path(
                       g,
                        source=datasource_to_node(test_environment.datasources['bool_is_upper_name']),
                        target=concept_to_node(concept.with_default_grain()),
                    )
        assert len(path) == 2, path
        resolved = test.resolve_direct_select()
        assert resolved is not None
        assert 'CASE' not in  compiled[-1]
    

