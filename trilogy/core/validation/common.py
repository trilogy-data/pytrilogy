from dataclasses import dataclass
from enum import Enum

from trilogy import Environment
from trilogy.authoring import ConceptRef
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.environment import EnvironmentConceptDict
from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
)
from trilogy.core.statements.execute import ProcessedQuery


class ExpectationType(Enum):
    LOGICAL = "logical"
    ROWCOUNT = "rowcount"
    DATA_TYPE_LIST = "data_type_list"
    ASSERTION = "assertion"


@dataclass
class ValidationTest:
    check_type: ExpectationType
    raw_query: ProcessedQuery | None = None
    generated_query: str | None = None
    expected: str | None = None
    result: ModelValidationError | None = None
    ran: bool = True


def grain_check_address(concept: BuildConcept | ConceptRef) -> str:
    """Address of a concept's grain-check helper.

    Derived from the concept's namespace and name rather than its
    ``safe_address``: flattening `.` to `_` is lossy, so a namespace ``product``
    and a sibling property ``product_category`` share one flattened name. Keyed
    on that, the two concepts would share (and overwrite) one grain check, and
    validation would plan one concept's check against the other's datasource.
    """
    return f"{concept.namespace}.grain_check_{concept.name}"


def non_null_count_address(concept: BuildConcept | ConceptRef) -> str:
    """Address of a key's `count(k)` helper, beside its grain check."""
    return f"{concept.namespace}.grain_non_null_{concept.name}"


def easy_query(
    concepts: list[BuildConcept],
    datasource: BuildDatasource,
    env: Environment,
    condition: BuildConditional | BuildComparison | None = None,
    limit: int | None = 100,
    grain: BuildGrain | None = None,
    name: str | None = None,
):
    """
    Build basic datasource specific queries.
    """
    prefix = name or f"datasource_{datasource.name}"
    datasource_outputs = {c.address: c for c in datasource.concepts}
    first_qds_concepts = datasource.concepts + concepts
    root_qds = QueryDatasource(
        input_concepts=first_qds_concepts,
        output_concepts=concepts,
        datasources=[datasource],
        joins=[],
        source_map={
            concept.address: (
                {datasource} if concept.address in datasource_outputs else set()
            )
            # include all base datasource conepts for convenience
            for concept in first_qds_concepts
        },
        grain=datasource.grain,
        base_datasource=datasource,
    )
    query_grain = grain or datasource.grain
    cte = CTE(
        name=f"{prefix}_base",
        source=root_qds,
        output_columns=concepts,
        source_map={
            concept.address: (
                [datasource.safe_identifier]
                if concept.address in datasource_outputs
                else []
            )
            for concept in first_qds_concepts
        },
        grain=query_grain,
        group_to_grain=True,
        base_alias_override=datasource.safe_identifier,
    )
    filter_cte = CTE(
        name=f"{prefix}_filter",
        source=QueryDatasource(
            datasources=[root_qds],
            input_concepts=cte.output_columns,
            output_concepts=cte.output_columns,
            joins=[],
            source_map={concept.address: ({root_qds}) for concept in concepts},
            grain=cte.grain,
            base_datasource=root_qds,
        ),
        parent_ctes=[cte],
        output_columns=cte.output_columns,
        source_map={
            concept.address: [cte.identifier] for concept in cte.output_columns
        },
        grain=cte.grain,
        condition=condition,
        limit=limit,
    )

    return ProcessedQuery(
        output_columns=[ConceptRef(address=concept.address) for concept in concepts],
        ctes=[cte, filter_cte],
        base=cte,
        local_concepts=EnvironmentConceptDict(),
    )
