from trilogy.core.statements.author import CreateStatement, Datasource
from trilogy.core.models.core import TraitDataType
from trilogy.core.statements.execute import (
    ProcessedCreateStatement,
    ColumnInfo,
    CreateTableInfo,
)
from trilogy.core.enums import Modifier
from trilogy.core.models.environment import Environment


def process_create_statement(
    statement: CreateStatement,
    environment: Environment,
) -> ProcessedCreateStatement:
    # Process the create statement to extract table info
    targets_info = []
    print(statement)
    for target in statement.targets:
        datasource: Datasource = environment.datasources.get(target)
        if not datasource:
            raise ValueError(f"Datasource {target} not found in environment.")
        columns_info = [
            ColumnInfo(
                name=col.alias,
                type=col.concept.output_datatype,
                description=(
                    col.concept.metadata.description if col.concept.metadata else None
                ),
                nullable=Modifier.OPTIONAL in col.modifiers,
                primary_key=col.concept.address in datasource.grain
            )
            for col in datasource.columns
        ]

        targets_info.append(
            CreateTableInfo(name=target, columns=columns_info, partition_keys=[])
        )

    return ProcessedCreateStatement(scope=statement.scope, targets=targets_info)
