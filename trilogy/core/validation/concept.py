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
from trilogy.core.validation.common import easy_query


def validate_property_concept(concept: BuildConcept):
    return []


def validate_key_concept(
    concept: BuildConcept, build_env: BuildEnvironment, exec: Executor
):
    exceptions = []
    seen = {}
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
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
            seen[datasource.name] = rows[0][0] if rows else None

    max_seen = max([v for v in seen.values() if v is not None], default=0)
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            assignment = [
                x for x in datasource.columns if x.concept.address == concept.address
            ][0]

            if (seen[datasource.name] or 0) < max_seen and assignment.is_complete:
                exceptions.append(
                    DatasourceModelValidationError(
                        f"Key concept {concept.address} is missing values in datasource {datasource.name} (max cardinality in data {max_seen}, datasource has {seen[datasource.name]} values) but is not marked as partial."
                    )
                )

    return exceptions


def validate_datasources(concept: BuildConcept, build_env: BuildEnvironment):
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
        ConceptModelValidationError(
            f"Concept {concept.address} is a root concept but has no datasources bound"
        )
    ]


def validate_concept(
    concept: BuildConcept, build_env: BuildEnvironment, exec: Executor
) -> list[ConceptModelValidationError | DatasourceModelValidationError]:
    base = []
    base += validate_datasources(concept, build_env)
    if concept.purpose == Purpose.PROPERTY:
        base += validate_property_concept(concept)
    elif concept.purpose == Purpose.KEY:
        base += validate_key_concept(concept, build_env, exec)
    return base
