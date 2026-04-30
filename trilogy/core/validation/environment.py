from contextlib import nullcontext
from typing import Callable

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

# Callback signature: (kind, target_name, results_for_target)
# kind is "datasource" or "concept"; results contain only the tests run for that
# specific target (so callers can group failures by target).
TargetCompleteCallback = Callable[[str, str, list[ValidationTest]], None]


def validate_environment(
    env: Environment,
    scope: ValidationScope = ValidationScope.ALL,
    targets: list[str] | None = None,
    exec: Executor | None = None,
    generate_only: bool = False,
    on_target_complete: TargetCompleteCallback | None = None,
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
    validation_scope = exec.validation_scope() if exec else nullcontext()
    with validation_scope:
        if scope == ValidationScope.ALL or scope == ValidationScope.DATASOURCES:
            for datasource in build_env.datasources.values():
                if targets and datasource.name not in targets:
                    continue
                ds_results = validate_datasource(datasource, env, build_env, exec)
                results += ds_results
                if on_target_complete:
                    on_target_complete("datasource", datasource.name, ds_results)
        if scope == ValidationScope.ALL or scope == ValidationScope.CONCEPTS:

            for bconcept in build_env.concepts.values():
                if targets and bconcept.address not in targets:
                    continue
                c_results = validate_concept(bconcept, env, build_env, exec)
                results += c_results
                if on_target_complete:
                    on_target_complete("concept", bconcept.address, c_results)

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
