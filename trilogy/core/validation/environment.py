from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import ComparisonOperator
from trilogy.authoring import Concept, Datasource, ConceptRef, Function, DataType
from trilogy.core.enums import Purpose, FunctionType
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildConditional,
    BuildComparison,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
)
from trilogy.core.statements.execute import CTE, ProcessedQuery
from trilogy.hooks import DebuggingHook
from trilogy.parsing.common import function_to_concept
from trilogy.core.validation.datasource import validate_datasource
from trilogy.core.validation.concept import validate_concept


def validate_environment(env: Environment, exec: Executor):


    grain_check = function_to_concept(
        parent=Function(
            operator=FunctionType.SUM,
            arguments=[1],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
        ),
        name="grain_check",
        environment=env,
    )
    env.add_concept(grain_check)
    new_concepts = []
    for concept in env.concepts.values():
        concept_grain_check = function_to_concept(
            parent=Function(
                operator=FunctionType.COUNT_DISTINCT,
                arguments=[1],
                output_datatype=DataType.INTEGER,
                output_purpose=Purpose.METRIC,
            ),
            name=f"grain_check_{concept.safe_address}",
            environment=env,
        )
        new_concepts.append(concept_grain_check)
    for concept in new_concepts:
        env.add_concept(concept)
    build_env = env.materialize_for_select()
    for datasource in build_env.datasources.values():
        validate_datasource(datasource, build_env, exec)
    for concept in build_env.concepts.values():
        validate_concept(concept, build_env, exec)