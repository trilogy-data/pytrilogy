"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables."""

from datetime import datetime
from itertools import combinations
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
    find_trilogy_config,
    get_runtime_config,
    handle_execution_exception,
)
from trilogy.scripts.display import print_error, print_info, print_success
from trilogy.scripts.ingest_helpers.foreign_keys import (
    apply_foreign_key_references,
    parse_foreign_keys,
)
from trilogy.scripts.ingest_helpers.formatting import (
    canonicalize_names,
)
from trilogy.scripts.ingest_helpers.typing import (
    detect_rich_type,
    infer_datatype_from_sql_type,
)


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
    for row in sample_rows:
        if row[column_index] is None:
            return True
    return False


def _process_column(
    idx: int,
    col: tuple[str, str, str | None, str | None],
    grain_components: list[str],
    sample_rows: list[tuple],
    concept_mapping: dict[str, str],
) -> tuple[Concept, ColumnAssignment, str | None]:

    column_name = col[0]
    data_type_str = col[1]
    schema_is_nullable = col[2].upper() == "YES" if len(col) > 2 and col[2] else True
    column_comment = col[3] if len(col) > 3 else None
    # Apply prefix stripping if mapping provided
    concept_name = concept_mapping[column_name]

    # Infer Trilogy datatype
    trilogy_type = infer_datatype_from_sql_type(data_type_str)

    # Try to detect rich type
    trait_import, trait_type_name = detect_rich_type(concept_name, trilogy_type)
    if trait_import and trait_type_name:
        final_datatype: TraitDataType | DataType = TraitDataType(
            type=trilogy_type, traits=[trait_type_name]
        )
        print_info(f"Detected rich type for '{concept_name}': {trait_type_name}")
    else:
        final_datatype = trilogy_type
        trait_import = None

    # Determine purpose based on grain
    if concept_name in grain_components or not grain_components:
        purpose = Purpose.KEY
        keys = set()
    else:
        purpose = Purpose.PROPERTY
        keys = set(grain_components)

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
        keys=keys,
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

    dialect = exec.generator

    columns = dialect.get_table_schema(exec, table_name, schema)

    if not columns:
        print_error(f"No columns found for table {table_name}")
        raise Exit(1)

    # Build qualified table name
    if schema:
        qualified_name = f"{schema}.{table_name}"
    else:
        qualified_name = table_name

    # Extract column names for grain detection
    column_names = [col[0] for col in columns]

    # Detect and strip common prefix from all column names BEFORE grain detection

    column_concept_mapping = canonicalize_names(column_names)

    # Detect unique key combinations from sample data
    suggested_keys = []

    # Normalize grain components to snake_case and apply prefix stripping
    db_primary_keys = dialect.get_table_primary_keys(exec, table_name, schema)
    # we always need sample rows for column detection, so fetch here to setup for later.
    sample_rows = dialect.get_table_sample(exec, table_name, schema)
    if db_primary_keys:
        keys = db_primary_keys
        print_info(f"Using primary key from database as grain: {db_primary_keys}")
    else:
        # Get sample data to detect grain and nullability
        print_info(
            f"Analyzing {len(sample_rows)} sample rows for grain and nullability detection"
        )
        suggested_keys = detect_unique_key_combinations(column_names, sample_rows)
        if suggested_keys:
            print_info(f"Detected potential unique key combinations: {suggested_keys}")
            print_info(f"Using detected unique key as grain: {suggested_keys[0]}")
            keys = suggested_keys[0]
        else:
            keys = []
            print_info(
                "No primary key or unique grain detected; defaulting to no grain"
            )
    grain_components = []
    for key in keys:
        stripped = column_concept_mapping.get(key, key)
        grain_components.append(stripped)

    # Track required imports for rich types
    required_imports: set[str] = set()

    # Create column assignments for each column
    column_assignments = []
    concepts: list[Concept] = []
    for idx, col in enumerate(columns):
        concept, column_assignment, rich_import = _process_column(
            idx, col, grain_components, sample_rows, column_concept_mapping
        )
        concepts.append(concept)
        column_assignments.append(column_assignment)
        if rich_import:
            required_imports.add(rich_import)

    grain = Grain(components=set(grain_components)) if grain_components else Grain()

    address = Address(location=qualified_name, quoted=True)

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
@option(
    "--fks",
    type=str,
    help="Foreign key relationships in format: table.column:ref_table.column (comma-separated)",
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
    fks: str | None,
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
        fks: Foreign key relationships to establish
        conn_args: Additional connection arguments
    """
    # Parse table names
    table_list = [t.strip() for t in tables.split(",") if t.strip()]

    if not table_list:
        print_error("No tables specified")
        raise Exit(1)

    # Parse foreign keys
    fk_map = parse_foreign_keys(fks) if fks else {}

    # Determine output directory
    if output:
        output_dir = PathlibPath(output)
    elif config:
        config_path = PathlibPath(config)
        output_dir = config_path.parent / "raw"
    else:
        found_config = find_trilogy_config()
        if found_config:
            output_dir = found_config.parent / "raw"
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
    ingested_data: dict[str, tuple[Datasource, list[Concept], set[str], list[Any]]] = {}
    renderer = Renderer()
    datasources = {}
    for table_name in table_list:
        print_info(f"Processing table: {table_name}")

        try:
            datasource, concepts, required_imports = create_datasource_from_table(
                exec, table_name, schema
            )

            datasources[table_name] = datasource

            # Build qualified table name
            if schema:
                qualified_name = f"{schema}.{table_name}"
            else:
                qualified_name = table_name

            # Generate Trilogy script content
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

            # Store for FK processing
            ingested_data[table_name] = (
                datasource,
                concepts,
                required_imports,
                script_content,
            )

        except Exception as e:
            print_error(f"Failed to ingest {table_name}: {e}")
            if ctx.obj["DEBUG"]:
                import traceback

                print_error(traceback.format_exc())
            continue

    # Write all ingested files, applying FK references where needed
    if fk_map:
        print_info("Processing foreign key relationships...")

    for table_name, (
        datasource,
        concepts,
        required_imports,
        script_content,
    ) in ingested_data.items():
        output_file = output_dir / f"{datasource.name}.preql"

        # Check if this table has FK relationships
        if fk_map and table_name in fk_map:
            column_mappings = fk_map[table_name]
            modified_content = apply_foreign_key_references(
                table_name, datasource, datasources, script_content, column_mappings
            )
            output_file.write_text(modified_content)
            ingested_files.append(output_file)
            print_success(f"Created {output_file} with FK references")
        else:
            # No FK references for this table, write as-is
            output_file.write_text(renderer.render_statement_string(script_content))
            ingested_files.append(output_file)
            print_success(f"Created {output_file}")

    # Close executor
    exec.close()

    if ingested_files:
        print_success(
            f"\nSuccessfully ingested {len(ingested_files)} table(s) to {output_dir}"
        )
    else:
        print_error("No tables were successfully ingested")
        raise Exit(1)
