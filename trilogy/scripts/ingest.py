"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables."""

import re
from datetime import datetime
from pathlib import Path as PathlibPath
from typing import Any

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.authoring import (
    Address,
    Comment,
    ConceptDeclarationStatement,
    DataType,
    ImportStatement,
)
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.models.author import Concept, Grain, Metadata
from trilogy.core.models.core import TraitDataType
from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.dialect.enums import Dialects
from trilogy.executor import Executor
from trilogy.parsing.render import Renderer
from trilogy.scripts.common import (
    create_executor,
    get_runtime_config,
    handle_execution_exception,
)
from trilogy.scripts.display import print_error, print_info, print_success
from itertools import combinations


def to_snake_case(name: str) -> str:
    """Convert a string to snake_case.

    Handles CamelCase, PascalCase, and names with spaces/special chars.
    """
    # Handle spaces and special characters first
    name = re.sub(r"[^\w\s]", "_", name)
    name = re.sub(r"\s+", "_", name)

    # Insert underscores before uppercase letters (for CamelCase)
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)

    # Convert to lowercase and remove duplicate underscores
    name = name.lower()
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    return name.strip("_")


# Rich type detection mappings
RICH_TYPE_PATTERNS: dict[str, dict[str, Any]] = {
    "geography": {
        "latitude": {
            "patterns": [r"(?:^|_)lat(?:$|_)", r"(?:^|_)latitude(?:$|_)"],
            "import": "std.geography",
            "type_name": "latitude",
            "base_type": DataType.FLOAT,
        },
        "longitude": {
            "patterns": [
                r"(?:^|_)lon(?:$|_)",
                r"(?:^|_)lng(?:$|_)",
                r"(?:^|_)long(?:$|_)",
                r"(?:^|_)longitude(?:$|_)",
            ],
            "import": "std.geography",
            "type_name": "longitude",
            "base_type": DataType.FLOAT,
        },
        "city": {
            "patterns": [r"(?:^|_)city(?:$|_)"],
            "import": "std.geography",
            "type_name": "city",
            "base_type": DataType.STRING,
        },
        "country": {
            "patterns": [r"(?:^|_)country(?:$|_)"],
            "import": "std.geography",
            "type_name": "country",
            "base_type": DataType.STRING,
        },
        "country_code": {
            "patterns": [r"country_code", r"countrycode"],
            "import": "std.geography",
            "type_name": "country_code",
            "base_type": DataType.STRING,
        },
        "us_state": {
            "patterns": [r"(?:^|_)state(?:$|_)", r"us_state"],
            "import": "std.geography",
            "type_name": "us_state",
            "base_type": DataType.STRING,
        },
        "us_zip_code": {
            "patterns": [r"(?:^|_)zip(?:$|_)", r"zipcode", r"zip_code", r"postal_code"],
            "import": "std.geography",
            "type_name": "us_zip_code",
            "base_type": DataType.STRING,
        },
    },
    "net": {
        "email_address": {
            "patterns": [r"(?:^|_)email(?:$|_)", r"email_address"],
            "import": "std.net",
            "type_name": "email_address",
            "base_type": DataType.STRING,
        },
        "url": {
            "patterns": [r"(?:^|_)url(?:$|_)", r"(?:^|_)website(?:$|_)"],
            "import": "std.net",
            "type_name": "url",
            "base_type": DataType.STRING,
        },
        "ipv4_address": {
            "patterns": [r"(?:^|_)ip(?:$|_)", r"(?:^|_)ipv4(?:$|_)", r"ip_address"],
            "import": "std.net",
            "type_name": "ipv4_address",
            "base_type": DataType.STRING,
        },
    },
}


def detect_rich_type(
    column_name: str, base_datatype: DataType
) -> tuple[str, str] | tuple[None, None]:
    """Detect if a column name matches a rich type pattern.

    Returns: (import_path, type_name) or (None, None) if no match

    Note: When multiple patterns match, the one with the longest matched
    string is preferred to ensure more specific matches win.
    """
    column_lower = column_name.lower()

    # Collect all matches and sort by matched string length (longest first) to prefer more specific matches
    matches = []

    for category, types in RICH_TYPE_PATTERNS.items():
        for type_name, config in types.items():
            # Only consider if base types match
            if config["base_type"] != base_datatype:
                continue

            # Check if any pattern matches
            for pattern in config["patterns"]:
                match = re.search(pattern, column_lower)
                if match:
                    # Store match with the length of the matched string for sorting
                    matched_length = len(match.group())
                    matches.append(
                        (matched_length, config["import"], config["type_name"])
                    )
                    break  # Only need one match per type

    # Return the most specific match (longest matched string)
    if matches:
        matches.sort(reverse=True)  # Sort by matched string length descending
        return str(matches[0][1]), str(matches[0][2])

    return None, None


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
        return DataType.NUMERIC
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
    if "timestamp" in sql_type_lower:
        return DataType.TIMESTAMP
    if "datetime" in sql_type_lower:
        return DataType.DATETIME
    if "date" in sql_type_lower:
        return DataType.DATE

    # Default to string for unknown types
    return DataType.STRING


def _check_column_combination_uniqueness(
    indices: list[int], sample_rows: list[tuple]
) -> bool:
    if not sample_rows:
        return False

    values = set()
    for row in sample_rows:
        # For single column, use scalar value; for multiple columns, use tuple
        if len(indices) == 1:
            value = row[indices[0]]
        else:
            value = tuple(row[idx] for idx in indices)

        if value in values:
            return False
        values.add(value)

    # Verify we have as many unique values as rows
    return len(values) == len(sample_rows)


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
        if _check_column_combination_uniqueness([i], sample_rows):
            unique_combinations.append([col_name])

    # If we found single-column keys, prefer those
    if unique_combinations:
        return unique_combinations

    # Try combinations of 2+ columns
    for size in range(2, max_key_size + 1):
        for col_combination in combinations(enumerate(column_names), size):
            indices = [idx for idx, _ in col_combination]
            col_names = [name for _, name in col_combination]

            if _check_column_combination_uniqueness(indices, sample_rows):
                unique_combinations.append(col_names)

        # If we found keys of this size, return them (prefer smaller keys)
        if unique_combinations:
            return unique_combinations

    return unique_combinations


def detect_nullability_from_sample(column_index: int, sample_rows: list[tuple]) -> bool:
    """Detect if a column is nullable based on sample data.

    Returns True if any NULL values are found in the sample.
    """
    for row in sample_rows:
        if row[column_index] is None:
            return True
    return False


def _process_column(
    idx: int,
    col: tuple,
    grain_components: list[str],
    sample_rows: list[tuple],
) -> tuple[Concept, ColumnAssignment, str | None]:
    """Process a single column and create its Concept and ColumnAssignment.

    Args:
        idx: Column index
        col: Column metadata tuple (name, type, nullable, comment)
        grain_components: List of grain component names
        sample_rows: Sample data for nullability detection

    Returns:
        Tuple of (Concept, ColumnAssignment, import_path or None)
    """
    column_name = col[0]
    data_type_str = col[1]
    schema_is_nullable = col[2].upper() == "YES" if len(col) > 2 else True
    column_comment = col[3] if len(col) > 3 else None

    # Normalize to snake_case for Trilogy convention
    concept_name = to_snake_case(column_name)

    # Infer Trilogy datatype
    trilogy_type = infer_datatype_from_sql_type(data_type_str)

    # Try to detect rich type
    trait_import, trait_type_name = detect_rich_type(concept_name, trilogy_type)
    if trait_import and trait_type_name:
        final_datatype : TraitDataType | DataType = TraitDataType(type=trilogy_type, traits=[trait_type_name])
        print_info(f"Detected rich type for '{concept_name}': {trait_type_name}")
    else:
        final_datatype = trilogy_type
        trait_import = None

    # Determine purpose based on grain
    if concept_name in grain_components:
        purpose = Purpose.KEY
    else:
        purpose = Purpose.PROPERTY

    # Determine nullability: check sample data first, fall back to schema
    if sample_rows:
        has_nulls = detect_nullability_from_sample(idx, sample_rows)
    else:
        has_nulls = schema_is_nullable

    # Get description from column comment if available
    description = column_comment if column_comment and column_comment.strip() else None

    # Create concept metadata if we have a description
    metadata = Metadata()
    if description:
        metadata = Metadata(description=description)

    # Create concept
    modifiers = [Modifier.NULLABLE] if has_nulls else []

    concept = Concept(
        name=concept_name,
        datatype=final_datatype,
        purpose=purpose,
        modifiers=modifiers,
        metadata=metadata,
    )

    # Create column assignment
    column_assignment = ColumnAssignment(
        alias=column_name, concept=concept.reference, modifiers=modifiers
    )

    return concept, column_assignment, trait_import


def create_datasource_from_table(
    exec: Executor, table_name: str, schema: str | None = None
) -> tuple[Datasource, list[Concept], set[str]]:
    """Create a Datasource object from a warehouse table.

    Returns: (datasource, concepts, required_imports)
    """
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
        print_info(
            f"Analyzing {len(sample_rows)} sample rows for grain and nullability detection"
        )
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

    # Normalize grain components to snake_case
    if db_primary_keys:
        grain_components = [to_snake_case(pk) for pk in db_primary_keys]
        print_info(f"Using database primary keys as grain: {grain_components}")
    elif suggested_keys:
        # Use the smallest unique key combination
        grain_components = [to_snake_case(key) for key in suggested_keys[0]]
        print_info(f"Using detected unique key as grain: {grain_components}")
    else:
        grain_components = []
        print_info("No unique grain detected - table may have duplicate rows")

    # Track required imports for rich types
    required_imports: set[str] = set()

    # Create column assignments for each column
    column_assignments = []
    concepts: list[Concept] = []

    for idx, col in enumerate(columns):
        concept, column_assignment, rich_import = _process_column(
            idx, col, grain_components, sample_rows
        )
        concepts.append(concept)
        column_assignments.append(column_assignment)
        if rich_import:
            required_imports.add(rich_import)

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

    return datasource, concepts, required_imports


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
        get_runtime_config(PathlibPath(config))
        if config
        else get_runtime_config(PathlibPath.cwd())
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
            datasource, concepts, required_imports = create_datasource_from_table(
                exec, table_name, schema
            )

            # Build qualified table name
            if schema:
                qualified_name = f"{schema}.{table_name}"
            else:
                qualified_name = table_name

            # Generate Trilogy script
            output_file = output_dir / f"{datasource.name}.preql"

            script_content: list[
                Datasource | Comment | ConceptDeclarationStatement | ImportStatement
            ] = []
            script_content.append(
                Comment(text=f"# Datasource ingested from {qualified_name}")
            )
            script_content.append(Comment(text=f"# Generated on {datetime.now()}"))

            # Add imports for rich types if needed
            if required_imports:
                for import_path in sorted(required_imports):
                    # This doesn't matter, stdlib imports are resolved automatically from memory
                    file_path = import_path.replace(".", "/")
                    script_content.append(
                        ImportStatement(
                            input_path=import_path,
                            alias="",  # No alias, direct import
                            path=PathlibPath(file_path),
                        )
                    )

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
