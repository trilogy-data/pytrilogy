"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables."""
from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.authoring import DataType, ConceptDeclarationStatement, Address
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.models.author import Concept, Grain
from trilogy.executor import Executor

from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.dialect.enums import Dialects
from trilogy.parsing.render import Renderer
from trilogy.scripts.common import (
    create_executor,
    get_runtime_config,
    handle_execution_exception,
)
from trilogy.scripts.display import print_error, print_info, print_success
from datetime import datetime
from trilogy.parsing.render import Renderer

def infer_datatype_from_sql_type(sql_type: str) -> DataType:
    """Infer Trilogy datatype from SQL type string."""
    sql_type_lower = sql_type.lower()

    # Integer types
    if any(
        t in sql_type_lower
        for t in ["int", "integer", "smallint", "tinyint", "mediumint"]
    ):
        return DataType.INTEGER
    if any(t in sql_type_lower for t in ["bigint", "long"]):
        return DataType.BIGINT

    # Numeric/decimal types
    if any(t in sql_type_lower for t in ["numeric", "decimal", "money"]):
        return DataType.FLOAT
    if any(t in sql_type_lower for t in ["float", "double", "real"]):
        return DataType.FLOAT

    # String types
    if any(
        t in sql_type_lower
        for t in ["char", "varchar", "text", "string", "clob", "nchar", "nvarchar"]
    ):
        return DataType.STRING

    # Boolean
    if any(t in sql_type_lower for t in ["bool", "boolean", "bit"]):
        return DataType.BOOL

    # Date/Time types
    if "timestamp" in sql_type_lower or "datetime" in sql_type_lower:
        return DataType.TIMESTAMP
    if "date" in sql_type_lower:
        return DataType.DATE

    # Default to string for unknown types
    return DataType.STRING


def get_table_schema(exec, table_name: str, schema: str | None = None):
    """Query the table schema from information_schema."""
    # Build qualified table name
    if schema:
        qualified_name = f"{schema}.{table_name}"
    else:
        qualified_name = table_name

    # Query column information
    column_query = f"""
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    if schema:
        column_query += f" AND table_schema = '{schema}'"
    column_query += " ORDER BY ordinal_position"

    try:
        rows = exec.execute_raw_sql(column_query).fetchall()
    except Exception as e:
        print_error(f"Failed to query schema for {qualified_name}: {e}")
        raise Exit(1) from e

    if not rows:
        print_error(f"No columns found for table {qualified_name}")
        raise Exit(1)

    return rows


def get_table_primary_keys(exec, table_name: str, schema: str | None = None):
    """Query primary keys from information_schema."""
    pk_query = f"""
    SELECT column_name
    FROM information_schema.key_column_usage
    WHERE table_name = '{table_name}'
    """
    if schema:
        pk_query += f" AND table_schema = '{schema}'"
    pk_query += " AND constraint_name LIKE '%primary%' OR constraint_name = 'PRIMARY'"

    # TODO: fallback, investigate our row sample for suggested unique keys
    rows = exec.execute_raw_sql(pk_query).fetchall()
    return [row[0] for row in rows]



def create_datasource_from_table(
    exec:Executor, table_name: str, schema: str | None = None
) -> Datasource:
    """Create a Datasource object from a warehouse table."""
    # Get table schema
    # TODO: move these methods the Dialect on The Executor
    columns = get_table_schema(exec, table_name, schema)

    # Get primary keys
    # TODO: move these methods to the DIalect on the Exeuctor
    primary_keys = get_table_primary_keys(exec, table_name, schema)

    # Build qualified table name
    # TODO: use the DDialect on the Executor to quote this appropriately
    if schema:
        qualified_name = f"{schema}.{table_name}"
    else:
        qualified_name = table_name

    # Create column assignments for each column
    column_assignments = []
    grain_components = []
    concepts:list[Concept] = []

    for col in columns:
        column_name = col[0]
        data_type_str = col[1]
        is_nullable = col[2].upper() == "YES" if len(col) > 2 else True

        # Infer Trilogy datatype
        trilogy_type = infer_datatype_from_sql_type(data_type_str)

        # Determine purpose
        if column_name in primary_keys:
            purpose = Purpose.KEY
            grain_components.append(column_name)
        else:
            purpose = Purpose.PROPERTY

        # Create concept
        modifiers = [Modifier.NULLABLE] if is_nullable else []

        concept = Concept(
            name=column_name,
            datatype=trilogy_type,
            purpose=purpose,
            modifiers=modifiers,
        )
        concepts.append(concept)

        # Create column assignment
        column_assignment = ColumnAssignment(
            alias=column_name, concept=concept.reference, modifiers=modifiers
        )
        column_assignments.append(column_assignment)

    # Create grain
    grain = Grain(components=set(grain_components)) if grain_components else Grain()

    # Build address clause - list column names
    address = Address(location=qualified_name, quoted=True)

    # Create datasource
    datasource = Datasource(
        name=table_name.replace(".", "_"),
        grain=grain,
        columns=column_assignments,
        address=address,
    )

    return datasource, concepts


@argument("tables", type=str)
@argument("dialect", type=str, required=False)
@option("--output", "-o", type=Path(), help="Output path for generated scripts")
@option("--schema", "-s", type=str, help="Schema/database to ingest from")
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def ingest(
    ctx,
    tables: str,
    dialect: str | None,
    output: str | None,
    schema: str | None,
    config,
    conn_args,
):
    """Bootstrap one or more datasources from tables in your warehouse.

    Connects to a warehouse and generates Trilogy datasource definitions
    from existing tables.

    Args:
        tables: Comma-separated list of table names to ingest
        dialect: Database dialect (e.g., duckdb, postgres, snowflake)
        output: Output path for generated scripts
        schema: Schema/database to ingest from
        config: Path to trilogy.toml configuration file
        conn_args: Additional connection arguments
    """
    # Parse table names
    table_list = [t.strip() for t in tables.split(",")]

    if not table_list:
        print_error("No tables specified")
        raise Exit(1)

    # Determine output directory
    if output:
        output_dir = PathlibPath(output)
    elif config:
        config_path = PathlibPath(config)
        output_dir = config_path.parent / "raw"
    else:
        # Try to find trilogy.toml in current or parent directories
        current = PathlibPath.cwd()
        config_path = None
        for parent in [current] + list(current.parents):
            if (parent / "trilogy.toml").exists():
                config_path = parent / "trilogy.toml"
                break

        if config_path:
            output_dir = config_path.parent / "raw"
        else:
            output_dir = PathlibPath.cwd() / "raw"

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    print_info(f"Ingesting tables: {', '.join(table_list)}")
    print_info(f"Output directory: {output_dir}")

    # Get runtime config
    runtime_config = (
        get_runtime_config(PathlibPath(config)) if config else get_runtime_config(PathlibPath.cwd())
    )

    # Determine dialect
    if dialect:
        edialect = Dialects(dialect)
    elif runtime_config.engine_dialect:
        edialect = runtime_config.engine_dialect
    else:
        print_error(
            "No dialect specified. Provide dialect as argument or set engine.dialect in config file."
        )
        raise Exit(1)

    # Create executor
    try:
        exec = create_executor(
            param=(),
            directory=PathlibPath.cwd(),
            conn_args=conn_args,
            edialect=edialect,
            debug=ctx.obj["DEBUG"],
            config=runtime_config,
        )
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])

    # Ingest each table
    ingested_files = []
    renderer = Renderer()
    for table_name in table_list:
        print_info(f"Processing table: {table_name}")

        try:
            datasource, concepts = create_datasource_from_table(exec, table_name, schema)

            # Build qualified table name
            if schema:
                qualified_name = f"{schema}.{table_name}"
            else:
                qualified_name = table_name

            # Generate Trilogy script manually
            script_lines = [
                f"# Datasource ingested from {qualified_name}",
                f"# Generated on {datetime.now()}",
                "",
            ]
            output_file = output_dir / f"{datasource.name}.preql"
            for line in script_lines:
                output_file.write_text(line)
            for concept in concepts:
                output_file.write_text(renderer.to_string(ConceptDeclarationStatement(concept=concept)))
            output_file.write_text(renderer.to_string(datasource))

            ingested_files.append(output_file)
            print_success(f"Created {output_file}")

        except Exception as e:
            print_error(f"Failed to ingest {table_name}: {e}")
            if ctx.obj["DEBUG"]:
                import traceback

                print_error(traceback.format_exc())
            continue

    # Close executor
    exec.close()

    if ingested_files:
        print_success(
            f"\nSuccessfully ingested {len(ingested_files)} table(s) to {output_dir}"
        )
    else:
        print_error("No tables were successfully ingested")
        raise Exit(1)
