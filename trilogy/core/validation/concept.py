from trilogy import Executor
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.exceptions import (
    ConceptModelValidationError,
    DatasourceModelValidationError,
)
from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.validation.common import ExpectationType, ValidationTest, easy_query


def validate_property_concept(
    concept: BuildConcept, generate_only: bool = False
) -> list[ValidationTest]:
    return []


def validate_key_concept(
    concept: BuildConcept,
    build_env: BuildEnvironment,
    exec: Executor,
    generate_only: bool = False,
):
    results: list[ValidationTest] = []
    seen = {}
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            assignment = [
                x for x in datasource.columns if x.concept.address == concept.address
            ][0]
            type_query = easy_query(
                concepts=[
                    # build_env.concepts[concept.address],
                    build_env.concepts[f"grain_check_{concept.safe_address}"],
                ],
                datasource=datasource,
                env=exec.environment,
                limit=1,
            )
            type_sql = exec.generate_sql(type_query)[-1]

            rows = exec.execute_raw_sql(type_sql).fetchall()
            if generate_only and assignment.is_complete:
                results.append(
                    ValidationTest(
                        query=type_sql,
                        check_type=ExpectationType.ROWCOUNT,
                        expected=f"equal_max_{concept.safe_address}",
                        result=None,
                        ran=False,
                    )
                )
                continue
            seen[datasource.name] = rows[0][0] if rows else None
    if generate_only:
        return results
    max_seen = max([v for v in seen.values() if v is not None], default=0)
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            assignment = [
                x for x in datasource.columns if x.concept.address == concept.address
            ][0]
            err = None
            if (seen[datasource.name] or 0) < max_seen and assignment.is_complete:
                err = DatasourceModelValidationError(
                    f"Key concept {concept.address} is missing values in datasource {datasource.name} (max cardinality in data {max_seen}, datasource has {seen[datasource.name]} values) but is not marked as partial."
                )
            results.append(
                ValidationTest(
                    query=None,
                    check_type=ExpectationType.ROWCOUNT,
                    expected=str(max_seen),
                    result=err,
                    ran=True,
                )
            )

    return results


def validate_datasources(
    concept: BuildConcept, build_env: BuildEnvironment
) -> list[ValidationTest]:
    if concept.lineage:
        return []
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            return []
    if not concept.derivation == Derivation.ROOT:
        return []
    if concept.name.startswith("__") or (
        concept.namespace and concept.namespace.startswith("__")
    ):
        return []
    return [
        ValidationTest(
            query=None,
            check_type=ExpectationType.LOGICAL,
            expected=None,
            result=ConceptModelValidationError(
                f"Concept {concept.address} is a root concept but has no datasources bound"
            ),
            ran=True,
        )
    ]


def validate_concept(
    concept: BuildConcept,
    build_env: BuildEnvironment,
    exec: Executor,
    generate_only: bool = False,
) -> list[ValidationTest]:
    base: list[ValidationTest] = []
    base += validate_datasources(concept, build_env)
    if concept.purpose == Purpose.PROPERTY:
        base += validate_property_concept(concept, generate_only)
    elif concept.purpose == Purpose.KEY:
        base += validate_key_concept(concept, build_env, exec, generate_only)
    return base
