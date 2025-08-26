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
from trilogy.core.exceptions import DatasourceModelValidationError


from trilogy.core.validation.common import easy_query


def validate_property_concept(concept: Concept):
    pass


def validate_key_concept(
    concept: BuildConcept, build_env: BuildEnvironment, exec: Executor
):
    seen = {}
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            type_query = easy_query(
                concepts=[build_env.concepts[f"grain_check_{concept.safe_address}"]],
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
            if seen[datasource.name] <= max_seen and assignment.is_complete:
                raise DatasourceModelValidationError(
                    f"Key concept {concept.address} is missing values in datasource {datasource.name} (max cardinality in data {max_seen}, datasource has {seen[datasource.name]} values) but is not marked as partial."
                )


def validate_concept(concept: Concept, build_env: BuildEnvironment, exec: Executor):
    if concept.purpose == Purpose.PROPERTY:
        validate_property_concept(concept)
    elif concept.purpose == Purpose.KEY:
        validate_key_concept(concept, build_env, exec)
