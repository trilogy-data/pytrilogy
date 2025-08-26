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

from enum import Enum

class ValidationType(Enum):
    DATASOURCES = "datasources"
    CONCEPTS = "concepts"


def easy_query(
    concepts: list[BuildConcept],
    datasource: BuildDatasource,
    env: Environment,
    condition: BuildConditional = None,
    limit: int = 100,
):
    """
    A simple function to create a ProcessedQuery with a CTE.
    """
    datasource_outputs = {c.address: c for c in datasource.concepts}
    root_qds = QueryDatasource(
        input_concepts=concepts,
        output_concepts=concepts,
        datasources=[datasource],
        joins=[],
        source_map={
            concept.address: (
                set([datasource]) if concept.address in datasource_outputs else set()
            )
            for concept in concepts
        },
        grain=datasource.grain,
    )
    cte = CTE(
        name=f"datasource_{datasource.name}_base",
        source=root_qds,
        output_columns=concepts,
        source_map={
            concept.address: (
                [datasource.safe_identifier] if concept.address in datasource_outputs else []
            )
            for concept in concepts
        },
        environment=env,
        grain=datasource.grain,
        group_to_grain=True,
        base_alias_override=datasource.safe_identifier,
    )

    filter_cte = CTE(
        name=f"datasource_{datasource.name}_filter",
        source=QueryDatasource(
            datasources=[root_qds],
            input_concepts=cte.output_columns,
            output_concepts=cte.output_columns,
            joins=[],
            source_map={concept.address: (set([root_qds])) for concept in concepts},
            grain=cte.grain,
        ),
        parent_ctes=[cte],
        output_columns=cte.output_columns,
        source_map={concept.address: [cte.identifier] for concept in cte.output_columns},
        environment=env,
        grain=cte.grain,
        condition=condition,
        limit=limit,
    )

    return ProcessedQuery(
        output_columns=[ConceptRef(address=concept.address) for concept in concepts],
        ctes=[cte, filter_cte],
        base=cte,
        local_concepts={},
    )
