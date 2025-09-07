from trilogy import Environment, Executor
from trilogy.core.enums import Derivation, Modifier, Purpose
from trilogy.core.exceptions import (
    ConceptModelValidationError,
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
)
from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.validation.common import ExpectationType, ValidationTest, easy_query


def validate_property_concept(
    concept: BuildConcept, exec: Executor | None = None
) -> list[ValidationTest]:
    return []


def validate_key_concept(
    concept: BuildConcept,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
):
    results: list[ValidationTest] = []
    seen: dict[str, int] = {}

    count = 0
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            count += 1
    # if it only has one source, it's a key
    if count <= 1:
        return results

    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            assignment = [
                x for x in datasource.columns if x.concept.address == concept.address
            ][0]
            # if it's not a partial, skip it
            if not assignment.is_complete:
                continue
            type_query = easy_query(
                concepts=[
                    # build_env.concepts[concept.address],
                    build_env.concepts[f"grain_check_{concept.safe_address}"],
                ],
                datasource=datasource,
                env=env,
                limit=1,
            )
            if exec:
                type_sql = exec.generate_sql(type_query)[-1]

                rows = exec.execute_raw_sql(type_sql).fetchall()
                seen[datasource.name] = rows[0][0] if rows else 0
            else:
                results.append(
                    ValidationTest(
                        raw_query=type_query,
                        check_type=ExpectationType.ROWCOUNT,
                        expected=f"equal_max_{concept.safe_address}",
                        result=None,
                        ran=False,
                    )
                )

    if not exec:
        return results
    max_seen: int = max([v for v in seen.values() if v is not None], default=0)
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            assignment = [
                x for x in datasource.columns if x.concept.address == concept.address
            ][0]
            err = None
            datasource_count: int = seen.get(datasource.name, 0)
            if datasource_count < max_seen and assignment.is_complete:
                err = DatasourceColumnBindingError(
                    address=datasource.identifier,
                    errors=[
                        DatasourceColumnBindingData(
                            address=concept.address,
                            value=None,
                            value_type=concept.datatype,
                            value_modifiers=[Modifier.PARTIAL],
                            actual_type=concept.datatype,
                            actual_modifiers=concept.modifiers,
                        )
                    ],
                    message=f"Key concept {concept.address} is missing values in datasource {datasource.name} (max cardinality in data {max_seen}, datasource has {seen[datasource.name]} values) but is not marked as partial.",
                )
            results.append(
                ValidationTest(
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
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
) -> list[ValidationTest]:
    base: list[ValidationTest] = []
    base += validate_datasources(concept, build_env)
    if concept.purpose == Purpose.PROPERTY:
        base += validate_property_concept(concept)
    elif concept.purpose == Purpose.KEY:
        base += validate_key_concept(concept, env, build_env, exec)
    return base
