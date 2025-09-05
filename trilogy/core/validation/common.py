from dataclasses import dataclass
from enum import Enum

from trilogy import Environment
from trilogy.authoring import (
    ConceptRef,
    DataType,
    Ordering,
    Purpose,
)
from trilogy.constants import MagicConstants
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.build import (
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildOrderBy,
    BuildOrderItem,
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


@dataclass
class ValidationTest:
    check_type: ExpectationType
    raw_query: ProcessedQuery | None = None
    generated_query: str | None = None
    expected: str | None = None
    result: ModelValidationError | None = None
    ran: bool = True


class ValidationType(Enum):
    DATASOURCES = "datasources"
    CONCEPTS = "concepts"


def build_order_args(concepts: list[BuildConcept]) -> list[BuildFunction]:
    order_args = []
    for concept in concepts:
        order_args.append(
            BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=concept,
                            operator=ComparisonOperator.IS,
                            right=MagicConstants.NULL,
                        ),
                        expr=1,
                    ),
                    BuildCaseElse(expr=0),
                ],
                output_data_type=DataType.INTEGER,
                output_purpose=Purpose.PROPERTY,
                arg_count=2,
            )
        )

    return order_args


def easy_query(
    concepts: list[BuildConcept],
    datasource: BuildDatasource,
    env: Environment,
    condition: BuildConditional | BuildComparison | None = None,
    limit: int = 100,
):
    """
    Build basic datasource specific queries.
    """
    datasource_outputs = {c.address: c for c in datasource.concepts}
    first_qds_concepts = datasource.concepts + concepts
    root_qds = QueryDatasource(
        input_concepts=first_qds_concepts,
        output_concepts=concepts,
        datasources=[datasource],
        joins=[],
        source_map={
            concept.address: (
                set([datasource]) if concept.address in datasource_outputs else set()
            )
            # include all base datasource conepts for convenience
            for concept in first_qds_concepts
        },
        grain=datasource.grain,
    )
    cte = CTE(
        name=f"datasource_{datasource.name}_base",
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
        source_map={
            concept.address: [cte.identifier] for concept in cte.output_columns
        },
        grain=cte.grain,
        condition=condition,
        limit=limit,
        order_by=BuildOrderBy(
            items=[
                BuildOrderItem(
                    expr=BuildFunction(
                        operator=FunctionType.SUM,
                        arguments=build_order_args(concepts),
                        output_data_type=DataType.INTEGER,
                        output_purpose=Purpose.PROPERTY,
                        arg_count=len(concepts),
                    ),
                    order=Ordering.DESCENDING,
                )
            ]
        ),
    )

    return ProcessedQuery(
        output_columns=[ConceptRef(address=concept.address) for concept in concepts],
        ctes=[cte, filter_cte],
        base=cte,
        local_concepts=EnvironmentConceptDict(**{}),
    )
