from trilogy.core.enums import Modifier, PersistMode
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import CreateStatement, PersistStatement
from trilogy.core.statements.execute import (
    ColumnInfo,
    CreateTableInfo,
    ProcessedCreateStatement,
)


def datasource_to_create_table_info(
    datasource: Datasource,
) -> CreateTableInfo:
    address_field_map: dict[str, str] = {
        column.concept.address: column.alias  # type: ignore
        for column in datasource.columns
        if column.is_concrete
    }
    columns_info = [
        ColumnInfo(
            # the is_concrete restricts this
            name=col.alias,  # type: ignore
            type=col.concept.output_datatype,
            description=(
                col.concept.metadata.description if col.concept.metadata else None
            ),
            nullable=Modifier.OPTIONAL in col.modifiers,
            primary_key=col.concept.address in datasource.grain.components,
        )
        for col in datasource.columns
        if col.is_concrete
    ]

    missing = [c for c in datasource.partition_by if c.address not in address_field_map]
    if missing:
        raise ValueError(
            f"Datasource {datasource.name} partitions by "
            f"{', '.join(c.address for c in missing)}, which is not a concrete "
            "column of the datasource; partition columns must be selected."
        )

    return CreateTableInfo(
        name=(
            datasource.address.location
            if isinstance(datasource.address, Address)
            else datasource.address
        ),
        columns=columns_info,
        partition_keys=[address_field_map[c.address] for c in datasource.partition_by],
    )


def _require_datasource(target: str, environment: Environment) -> Datasource:
    datasource: Datasource | None = environment.datasources.get(target)
    if not datasource:
        raise ValueError(f"Datasource {target} not found in environment.")
    return datasource


def create_statement_to_persists(
    statement: CreateStatement,
    environment: Environment,
) -> list[PersistStatement]:
    """`create ... with data` as persists: an OVERWRITE already emits its own
    DDL ahead of the insert, so the create mode rides along and the statement
    stays one processed query."""
    persists = []
    for target in statement.targets:
        datasource = _require_datasource(target, environment)
        persists.append(
            PersistStatement(
                select=datasource.create_update_statement(
                    environment, None, line_no=None
                ),
                datasource=datasource,
                persist_mode=PersistMode.OVERWRITE,
                create_mode=statement.create_mode,
            )
        )
    return persists


def process_create_statement(
    statement: CreateStatement,
    environment: Environment,
) -> ProcessedCreateStatement:
    # Process the create statement to extract table info
    targets_info = []
    for target in statement.targets:
        create_table_info = datasource_to_create_table_info(
            _require_datasource(target, environment)
        )
        targets_info.append(create_table_info)

    return ProcessedCreateStatement(
        scope=statement.scope, targets=targets_info, create_mode=statement.create_mode
    )
