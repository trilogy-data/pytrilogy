from trilogy import Environment, Executor
from trilogy.authoring import DataType, Function
from trilogy.core.enums import FunctionType, Purpose, ValidationScope
from trilogy.core.exceptions import (
    ModelValidationError,
)
from trilogy.core.validation.common import ValidationTest
from trilogy.core.validation.concept import validate_concept
from trilogy.core.validation.datasource import validate_datasource
from trilogy.parsing.common import function_to_concept


def validate_environment(
    env: Environment,
    scope: ValidationScope = ValidationScope.ALL,
    targets: list[str] | None = None,
    exec: Executor | None = None,
    generate_only: bool = False,
) -> list[ValidationTest]:
    # avoid mutating the environment for validation
    generate_only = exec is None or generate_only
    env = env.duplicate()
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
                arguments=[concept.reference],
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
    results: list[ValidationTest] = []
    if scope == ValidationScope.ALL or scope == ValidationScope.DATASOURCES:
        for datasource in build_env.datasources.values():
            if targets and datasource.name not in targets:
                continue
            results += validate_datasource(datasource, env, build_env, exec)
    if scope == ValidationScope.ALL or scope == ValidationScope.CONCEPTS:

        for bconcept in build_env.concepts.values():
            if targets and bconcept.address not in targets:
                continue
            results += validate_concept(bconcept, env, build_env, exec)

    # raise a nicely formatted union of all exceptions
    exceptions: list[ModelValidationError] = [e.result for e in results if e.result]
    if exceptions:
        if not generate_only:
            messages = "\n".join([str(e) for e in exceptions])
            raise ModelValidationError(
                f"Environment validation failed with the following errors:\n{messages}",
                children=exceptions,
            )

    return results
