from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.authoring import Concept, Datasource, ConceptRef
from trilogy.core.enums import Purpose
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
)
from trilogy.core.statements.execute import CTE, ProcessedQuery
from trilogy.hooks import DebuggingHook


def easy_query(
    concepts: list[BuildConcept], datasource: BuildDatasource, env: Environment
):
    """
    A simple function to create a ProcessedQuery with a CTE.
    """

    cte = CTE(
        name="iris_data",
        source=QueryDatasource(
            input_concepts=concepts,
            output_concepts=concepts,
            datasources=[datasource],
            joins=[],
            source_map={concept.address: set([datasource]) for concept in concepts},
            grain=datasource.grain,
        ),
        output_columns=concepts,
        source_map={datasource.name: [concept.address for concept in concepts]},
        environment=env,
        grain=datasource.grain,
    )

    # for c in concepts:
    #     found= cte.get_alias(c)
    #     assert found == ' dgs'
    return ProcessedQuery(
        output_columns=[ConceptRef(address=concept.address) for concept in concepts],
        ctes=[cte],
        base=cte,
        local_concepts={}
    )


def validate_property_concept(concept: Concept):
    pass


def validate_concept(concept: Concept):
    if concept.purpose == Purpose.PROPERTY:
        validate_property_concept(concept)


def validate_datasource(datasource: Datasource, build_env, exec: Executor):

    query = easy_query(
        concepts=[build_env.concepts[name] for name in datasource.grain.components],
        datasource=datasource,
        env=exec.environment,
    )
    sql = exec.generate_sql(query)

    print(sql)
    assert 1 == 0


def test_iris_join():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris;""")
    build_env = env.materialize_for_select()
    for datasource in build_env.datasources.values():
        validate_datasource(datasource, build_env, exec)
