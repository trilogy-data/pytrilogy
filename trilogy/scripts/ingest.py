"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables."""
from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.authoring import DataType, ConceptDeclarationStatement, Address, Comment
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
    if any(t in sql_type_lower for t in ["bigint", "long", "int64"]):
        return DataType.BIGINT

    # Numeric/decimal types
    if any(t in sql_type_lower for t in ["numeric", "decimal", "money"]):
        return DataType.FLOAT
    if any(t in sql_type_lower for t in ["float", "double", "real", "float64"]):
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


def detect_unique_key_combinations(
    column_names: list[str], sample_rows: list[tuple], max_key_size: int = 3
) -> list[list[str]]:
    """Detect unique key combinations from sample data.

    Returns a list of column combinations that uniquely identify rows,
    ordered by size (smallest first).
    """
    if not sample_rows or not column_names:
        return []

    unique_combinations = []

    # Try single columns first
    for i, col_name in enumerate(column_names):
        values = set()
        is_unique = True
        for row in sample_rows:
            value = row[i]
            if value in values:
                is_unique = False
                break
            values.add(value)

        if is_unique and len(values) == len(sample_rows):
            unique_combinations.append([col_name])

    # If we found single-column keys, prefer those
    if unique_combinations:
        return unique_combinations

    # Try combinations of 2 columns
    if max_key_size >= 2:
        from itertools import combinations
        for col_combo in combinations(enumerate(column_names), 2):
            indices = [idx for idx, _ in col_combo]
            col_names = [name for _, name in col_combo]

            values = set()
            is_unique = True
            for row in sample_rows:
                value = tuple(row[idx] for idx in indices)
                if value in values:
                    is_unique = False
                    break
                values.add(value)

            if is_unique and len(values) == len(sample_rows):
                unique_combinations.append(col_names)

    # Try combinations of 3 columns if needed
    if not unique_combinations and max_key_size >= 3:
        from itertools import combinations
        for col_combo in combinations(enumerate(column_names), 3):
            indices = [idx for idx, _ in col_combo]
            col_names = [name for _, name in col_combo]

            values = set()
            is_unique = True
            for row in sample_rows:
                value = tuple(row[idx] for idx in indices)
                if value in values:
                    is_unique = False
                    break
                values.add(value)

            if is_unique and len(values) == len(sample_rows):
                unique_combinations.append(col_names)

    return unique_combinations


def detect_nullability_from_sample(
    column_index: int, sample_rows: list[tuple]
) -> bool:
    """Detect if a column is nullable based on sample data.

    Returns True if any NULL values are found in the sample.
    """
    for row in sample_rows:
        if row[column_index] is None:
            return True
    return False





def create_datasource_from_table(
    exec:Executor, table_name: str, schema: str | None = None
) -> tuple[Datasource, list[Concept]]:
    """Create a Datasource object from a warehouse table."""
    # Get the dialect generator (BaseDialect instance) from the executor
    dialect = exec.generator

    # Get table schema using dialect-specific method
    try:
        columns = dialect.get_table_schema(exec, table_name, schema)
    except Exception as e:
        print_error(f"Failed to query schema for {table_name}: {e}")
        raise Exit(1) from e

    if not columns:
        print_error(f"No columns found for table {table_name}")
        raise Exit(1)

    # Get primary keys from DB (may be empty)
    try:
        db_primary_keys = dialect.get_table_primary_keys(exec, table_name, schema)
    except Exception as e:
        print_info(f"Could not fetch primary keys from metadata: {e}")
        db_primary_keys = []

    # Get sample data to detect grain and nullability
    try:
        sample_rows = dialect.get_table_sample(exec, table_name, schema)
        print_info(f"Analyzing {len(sample_rows)} sample rows for grain and nullability detection")
    except Exception as e:
        print_info(f"Could not fetch sample data: {e}")
        sample_rows = []

    # Build qualified table name
    if schema:
        qualified_name = f"{schema}.{table_name}"
    else:
        qualified_name = table_name

    # Extract column names for grain detection
    column_names = [col[0] for col in columns]

    # Detect unique key combinations from sample data
    suggested_keys = []
    if sample_rows:
        suggested_keys = detect_unique_key_combinations(column_names, sample_rows)
        if suggested_keys:
            print_info(f"Detected potential unique key combinations: {suggested_keys}")

    # Determine grain: prefer DB PKs, fall back to detected keys
    if db_primary_keys:
        grain_components = db_primary_keys
        print_info(f"Using database primary keys as grain: {grain_components}")
    elif suggested_keys:
        # Use the smallest unique key combination
        grain_components = suggested_keys[0]
        print_info(f"Using detected unique key as grain: {grain_components}")
    else:
        grain_components = []
        print_info("No unique grain detected - table may have duplicate rows")

    # Create column assignments for each column
    column_assignments = []
    concepts:list[Concept] = []

    for idx, col in enumerate(columns):
        column_name = col[0]
        data_type_str = col[1]
        schema_is_nullable = col[2].upper() == "YES" if len(col) > 2 else True

        # Infer Trilogy datatype
        trilogy_type = infer_datatype_from_sql_type(data_type_str)

        # Determine purpose based on grain
        if column_name in grain_components:
            purpose = Purpose.KEY
        else:
            purpose = Purpose.PROPERTY

        # Determine nullability: check sample data first, fall back to schema
        if sample_rows:
            has_nulls = detect_nullability_from_sample(idx, sample_rows)
        else:
            has_nulls = schema_is_nullable

        # Create concept
        modifiers = [Modifier.NULLABLE] if has_nulls else []

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

    # Build address clause
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

            # Generate Trilogy script
            output_file = output_dir / f"{datasource.name}.preql"

            script_content = []
            script_content.append(Comment(text=f"# Datasource ingested from {qualified_name}"))
            script_content.append(Comment(text=f"# Generated on {datetime.now()}"))


            # Add concept declarations
            for concept in concepts:
                script_content.append(ConceptDeclarationStatement(concept=concept))

            # Add datasource
            script_content.append(datasource)

            # Write the complete file
            output_file.write_text(renderer.render_statement_string(script_content))

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
