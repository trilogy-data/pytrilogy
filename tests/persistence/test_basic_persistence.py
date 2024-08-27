# from trilogy.compiler import compile
from trilogy.core.enums import Purpose
from trilogy.core.models import (
    Grain,
    ProcessedQueryPersist,
    SelectStatement,
    PersistStatement,
)
from trilogy.core.query_processor import process_auto
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.core.enums import PurposeLineage
from trilogy.parser import parse
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.nodes.select_node_v2 import SelectNode
from trilogy.core.processing.node_generators import (
    gen_select_node,
)
from trilogy.core.env_processor import (
    generate_graph,
    datasource_to_node,
    concept_to_node,
)
from trilogy.hooks.query_debugger import DebuggingHook

import networkx as nx

TEST_DIALECTS: list[BaseDialect] = [
    BaseDialect(),
    BigqueryDialect(),
    DuckDBDialect(),
    SqlServerDialect(),
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
        assert test_concept in env.materialized_concepts
        assert test_concept.derivation == PurposeLineage.BASIC

        persist: PersistStatement = parsed[-2]
        select: SelectStatement = parsed[-1]
        assert persist.select.grain == Grain(components=[test_concept])
        assert select.output_components == [test_concept]
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
            source_concepts=search_concepts,
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
