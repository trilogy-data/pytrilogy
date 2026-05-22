"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables or files."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path as PathlibPath

from click import UNPROCESSED, Choice, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.authoring import (
    Address,
    Comment,
    ConceptDeclarationStatement,
    DataType,
    ImportStatement,
    PropertiesDeclarationStatement,
)
from trilogy.constants import REMOTE_PREFIXES
from trilogy.core.enums import AddressType, Modifier, Purpose
from trilogy.core.models.author import Concept, Grain, Metadata
from trilogy.core.models.core import EnumType, TraitDataType
from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.enums import Dialects
from trilogy.executor import Executor
from trilogy.parsing.render import Renderer
from trilogy.scripts.common import (
    create_executor,
    find_trilogy_config,
    get_runtime_config,
    handle_execution_exception,
)
from trilogy.scripts.display import print_error, print_info, print_warning
from trilogy.scripts.display_ingest import (
    ingest_progress,
    show_fk_summary,
    show_ingest_header,
    show_ingest_summary,
)
from trilogy.scripts.ingest_helpers.fk_inference import (
    InferredFK,
    build_table_fk_info,
    infer_foreign_keys,
    merge_fk_maps,
)
from trilogy.scripts.ingest_helpers.foreign_keys import (
    apply_foreign_key_references,
    parse_foreign_keys,
)
from trilogy.scripts.ingest_helpers.formatting import (
    canonicalize_names,
    canonicolize_name,
)
from trilogy.scripts.ingest_helpers.typing import (
    detect_enum_types,
    detect_rich_type,
)

# Filename suffix → AddressType. JSON intentionally omitted: the Trilogy
# grammar's `file` rule only accepts these extensions natively.
_FILE_EXT_TO_TYPE: dict[str, AddressType] = {
    ".csv": AddressType.CSV,
    ".tsv": AddressType.TSV,
    ".parquet": AddressType.PARQUET,
}

ScriptStatement = (
    Datasource
    | Comment
    | ConceptDeclarationStatement
    | PropertiesDeclarationStatement
    | ImportStatement
)


@dataclass
class IngestRecord:
    """One row of ingest results, used both for FK linking and the summary table."""

    source: str
    datasource: Datasource
    concepts: list[Concept]
    required_imports: set[str]
    script: list[ScriptStatement]


@dataclass
class IngestSummaryRow:
    """User-facing summary of one source's ingest outcome."""

    source: str
    output: str
    columns: str
    grain: str
    status: str

    @property
    def ok(self) -> bool:
        return self.status == "ok"


def _looks_like_file_source(arg: str) -> bool:
    """True if `arg` should be treated as a file/URL rather than a warehouse table."""
    if arg.startswith(REMOTE_PREFIXES):
        return True
    suffix = PathlibPath(arg).suffix.lower()
    return suffix in _FILE_EXT_TO_TYPE


def _resolve_file_source(arg: str) -> tuple[str, AddressType]:
    """Return (location, AddressType) for a file/URL source argument.

    Local paths are resolved to absolute so the generated .preql isn't tied
    to the cwd at ingest time. URLs and globs pass through unchanged.
    """
    suffix = PathlibPath(arg).suffix.lower()
    addr_type = _FILE_EXT_TO_TYPE.get(suffix)
    if addr_type is None:
        raise ValueError(
            f"Unsupported file extension '{suffix}'. "
            f"Supported: {sorted(_FILE_EXT_TO_TYPE)}"
        )
    if arg.startswith(REMOTE_PREFIXES) or any(c in arg for c in "*?["):
        return arg, addr_type
    return str(PathlibPath(arg).expanduser().resolve()), addr_type


def _datasource_name_from_path(arg: str) -> str:
    """Derive a clean snake_case datasource name from a path or URL."""
    # Strip query string / fragment from URLs before deriving a stem.
    bare = arg.split("?", 1)[0].split("#", 1)[0]
    stem = PathlibPath(bare).stem or "source"
    return canonicolize_name(stem) or "source"


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


def _base_datatype(dialect: BaseDialect, sql_type: str) -> DataType:
    """Resolve a DB type string to a Trilogy DataType, defaulting unknowns to STRING."""
    resolved = dialect.normalize_db_type(sql_type)
    return DataType.STRING if resolved == DataType.UNKNOWN else resolved


def _process_column(
    idx: int,
    col: tuple[str, str, str | None, str | None],
    grain_components: list[str],
    sample_rows: list[tuple],
    concept_mapping: dict[str, str],
    dialect: BaseDialect,
    enum_type: EnumType | None = None,
) -> tuple[Concept, ColumnAssignment, str | None]:

    column_name = col[0]
    data_type_str = col[1]
    schema_is_nullable = col[2].upper() == "YES" if len(col) > 2 and col[2] else True
    column_comment = col[3] if len(col) > 3 else None
    # Apply prefix stripping if mapping provided
    concept_name = concept_mapping[column_name]

    # Infer the base Trilogy datatype via the dialect's type map — the same
    # mapping datasource validation's schema-drift check uses (normalize_db_type).
    trilogy_type = _base_datatype(dialect, data_type_str)

    # A column can be both an enum (a constrained domain, detected from the
    # source's true distinct values) and a rich type (a name-based trait). When
    # both apply, the trait wraps the enum — e.g. enum<string>['x@y.com']::email_address.
    # Rich types are validated against the column's actual values so a column
    # merely named like one (a Y/N "channel_email" flag) isn't misclassified;
    # an enum already carries its full distinct value set.
    if enum_type is not None:
        rich_values: list = list(enum_type.values)
    else:
        rich_values = list({row[idx] for row in sample_rows if row[idx] is not None})
    trait_import, trait_type_name = detect_rich_type(
        concept_name, trilogy_type, rich_values
    )
    base: DataType | EnumType = trilogy_type if enum_type is None else enum_type
    final_datatype: TraitDataType | DataType | EnumType
    if trait_import and trait_type_name:
        final_datatype = TraitDataType(type=base, traits=[trait_type_name])
        print_info(f"Detected rich type for '{concept_name}': {trait_type_name}")
    else:
        trait_import = None
        final_datatype = base
    if enum_type is not None:
        print_info(f"Detected enum type for '{concept_name}': {enum_type}")

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


def _file_introspection_source(location: str, addr_type: AddressType) -> str:
    """SQL fragment that DuckDB can read this file with."""
    quoted = location.replace("'", "''")
    if addr_type == AddressType.CSV:
        return f"read_csv_auto('{quoted}')"
    if addr_type == AddressType.TSV:
        return f"read_csv_auto('{quoted}', delim='\\t')"
    if addr_type == AddressType.PARQUET:
        return f"read_parquet('{quoted}')"
    raise ValueError(f"Unsupported file address type: {addr_type}")


def _maybe_load_httpfs(exec: Executor, location: str) -> None:
    """Ensure DuckDB's httpfs extension is loaded for remote URLs.

    Cloud URLs (gs://, s3://) typically need this; for plain http(s) we still
    install eagerly because DuckDB requires it for any web fetch.
    """
    if not location.startswith(REMOTE_PREFIXES):
        return
    try:
        exec.execute_raw_sql("INSTALL httpfs; LOAD httpfs;")
    except Exception as e:
        print_warning(f"Failed to load httpfs extension (remote reads may fail): {e}")


def create_datasource_from_file(
    exec: Executor,
    arg: str,
    name_override: str | None = None,
    root: bool = True,
) -> tuple[Datasource, list[Concept], set[str], str]:
    """Build a Datasource from a local file path or URL via DuckDB.

    Returns (datasource, concepts, required_imports, resolved_location).
    """
    location, addr_type = _resolve_file_source(arg)
    _maybe_load_httpfs(exec, location)

    source_expr = _file_introspection_source(location, addr_type)

    describe_rows = exec.execute_raw_sql(
        f"DESCRIBE SELECT * FROM {source_expr}"
    ).fetchall()
    if not describe_rows:
        print_error(f"No columns found in {arg}")
        raise Exit(1)

    # DuckDB DESCRIBE: (column_name, column_type, null, key, default, extra)
    columns: list[tuple[str, str, str | None, str | None]] = [
        (row[0], row[1], row[2] if len(row) > 2 else "YES", None)
        for row in describe_rows
    ]
    column_names = [c[0] for c in columns]

    sample_rows = exec.execute_raw_sql(
        f"SELECT * FROM {source_expr} LIMIT 10000"
    ).fetchall()

    column_concept_mapping = canonicalize_names(column_names)
    suggested_keys = detect_unique_key_combinations(column_names, sample_rows)
    if suggested_keys:
        print_info(f"Detected potential unique key combinations: {suggested_keys}")
        keys = suggested_keys[0]
    else:
        keys = []
        print_info("No primary key or unique grain detected; defaulting to no grain")
    grain_components = [column_concept_mapping.get(k, k) for k in keys]

    enum_map = detect_enum_types(
        exec,
        source_expr,
        [(c[0], _base_datatype(exec.generator, c[1])) for c in columns],
    )

    required_imports: set[str] = set()
    column_assignments = []
    concepts: list[Concept] = []
    for idx, col in enumerate(columns):
        concept, column_assignment, rich_import = _process_column(
            idx,
            col,
            grain_components,
            sample_rows,
            column_concept_mapping,
            exec.generator,
            enum_map.get(col[0]),
        )
        concepts.append(concept)
        column_assignments.append(column_assignment)
        if rich_import:
            required_imports.add(rich_import)

    grain = Grain(components=set(grain_components)) if grain_components else Grain()

    address = Address(location=location, type=addr_type)
    name = name_override or _datasource_name_from_path(arg)
    datasource = Datasource(
        name=name,
        grain=grain,
        columns=column_assignments,
        address=address,
        is_root=root,
    )
    return datasource, concepts, required_imports, location


def create_datasource_from_table(
    exec: Executor, table_name: str, schema: str | None = None, root: bool = False
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

    enum_map = detect_enum_types(
        exec,
        dialect.safe_quote(qualified_name),
        [(c[0], _base_datatype(dialect, c[1])) for c in columns],
    )

    # Track required imports for rich types
    required_imports: set[str] = set()

    # Create column assignments for each column
    column_assignments = []
    concepts: list[Concept] = []
    for idx, col in enumerate(columns):
        concept, column_assignment, rich_import = _process_column(
            idx,
            col,
            grain_components,
            sample_rows,
            column_concept_mapping,
            exec.generator,
            enum_map.get(col[0]),
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
        is_root=root,
    )

    return datasource, concepts, required_imports


def _build_script_content(
    source_label: str,
    datasource: Datasource,
    concepts: list[Concept],
    required_imports: set[str],
) -> list[ScriptStatement]:
    script_content: list[ScriptStatement] = [
        Comment(text=f"# Datasource ingested from {source_label}"),
        Comment(text=f"# Generated on {datetime.now()}"),
    ]
    for import_path in sorted(required_imports):
        file_path = import_path.replace(".", "/")
        script_content.append(
            ImportStatement(
                input_path=import_path,
                alias="",
                path=PathlibPath(file_path),
            )
        )
    # Keys render individually; properties sharing a key are emitted as a single
    # grouped `properties <key> (...)` declaration.
    keys = [c for c in concepts if c.purpose == Purpose.KEY]
    grouped: dict[frozenset, list[Concept]] = defaultdict(list)
    for concept in concepts:
        if concept.purpose == Purpose.PROPERTY:
            grouped[frozenset(concept.keys or [])].append(concept)
    for concept in keys:
        script_content.append(ConceptDeclarationStatement(concept=concept))
    for group in grouped.values():
        if len(group) > 1:
            script_content.append(PropertiesDeclarationStatement(concepts=group))
        else:
            script_content.append(ConceptDeclarationStatement(concept=group[0]))
    script_content.append(datasource)
    return script_content


def _grain_label(datasource: Datasource) -> str:
    components = sorted(datasource.grain.components)
    if not components:
        return "-"
    return ", ".join(c.split(".", 1)[-1] for c in components)


@argument("sources", type=str, required=False, default="")
@argument("dialect", type=str, required=False)
@option("--output", "-o", type=Path(), help="Output path for generated scripts")
@option("--schema", "-s", type=str, help="Schema/database to ingest from")
@option(
    "--all",
    "all_tables",
    is_flag=True,
    default=False,
    help="Ingest every table in the database (table mode; no SOURCES needed).",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@option(
    "--fks",
    type=str,
    help="Foreign key relationships in format: table.column:ref_table.column (comma-separated)",
)
@option(
    "--fk-infer-level",
    type=Choice(["off", "fast", "full"]),
    default="full",
    show_default=True,
    help=(
        "Infer foreign keys to link the generated datasources: 'off' disables "
        "it, 'fast' matches on column names only, 'full' also verifies matches "
        "with value sniffing. Explicit --fks always override inferred relationships."
    ),
)
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--name",
    type=str,
    help="Override the generated datasource name (only valid for a single source)",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def ingest(
    ctx,
    sources: str,
    dialect: str | None,
    output: str | None,
    schema: str | None,
    all_tables: bool,
    config,
    fks: str | None,
    fk_infer_level: str,
    env,
    name: str | None,
    conn_args,
):
    """Bootstrap one or more datasources from tables or files.

    `sources` may be a comma-separated list of either:
      - warehouse table names (introspected via the configured dialect), or
      - file paths / URLs (.csv, .tsv, .parquet — local or remote, ingested via
        DuckDB's read_* functions).

    Mixing tables and file paths is supported under the duckdb dialect (where
    a single executor can introspect both); other dialects must use only
    tables.
    """
    source_list = [s.strip() for s in sources.split(",") if s.strip()]
    if all_tables and source_list:
        print_error("Pass either explicit SOURCES or --all, not both.")
        raise Exit(1)
    if all_tables and name:
        print_error("--name cannot be combined with --all.")
        raise Exit(1)
    if not source_list and not all_tables:
        print_error("No sources specified (pass table names or --all).")
        raise Exit(1)

    file_sources = [s for s in source_list if _looks_like_file_source(s)]
    has_files = bool(file_sources)

    if name and len(source_list) > 1:
        print_error("--name can only be set when ingesting a single source")
        raise Exit(1)

    explicit_fk_map = parse_foreign_keys(fks) if fks else {}

    # Determine output directory.
    if output:
        output_dir = PathlibPath(output)
    elif config:
        output_dir = PathlibPath(config).parent / "raw"
    else:
        found_config = find_trilogy_config()
        output_dir = (
            (found_config.parent / "raw") if found_config else PathlibPath.cwd() / "raw"
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    runtime_config = (
        get_runtime_config(PathlibPath(config))
        if config
        else get_runtime_config(PathlibPath.cwd())
    )

    # Determine dialect. File sources require DuckDB (the only engine that
    # introspects arbitrary local/remote files via `read_*` functions);
    # mixed table+file is allowed because DuckDB also reads attached tables.
    if dialect:
        edialect = Dialects(dialect)
    elif runtime_config.engine_dialect:
        edialect = runtime_config.engine_dialect
    elif has_files:
        edialect = Dialects.DUCK_DB
    else:
        print_error(
            "No dialect specified. Provide dialect as argument or set engine.dialect in config file."
        )
        raise Exit(1)

    if has_files and edialect != Dialects.DUCK_DB:
        print_error(
            f"File ingest requires the duckdb dialect; got '{edialect.value}'. "
            f"File sources: {file_sources}"
        )
        raise Exit(1)

    if env:
        from trilogy.execution.config import apply_env_vars
        from trilogy.scripts.environment import parse_env_vars

        try:
            cli_env_vars = parse_env_vars(env)
        except ValueError as e:
            print_error(str(e))
            raise Exit(1) from e
        apply_env_vars(cli_env_vars)

    try:
        exec = create_executor(
            param=(),
            directory=PathlibPath.cwd(),
            conn_args=conn_args,
            edialect=edialect,
            debug=ctx.obj["DEBUG"],
            config=runtime_config,
            debug_file=ctx.obj.get("DEBUG_FILE"),
        )
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])

    if all_tables:
        try:
            source_list = [t[0] for t in exec.generator.list_tables(exec, schema)]
        except Exception as e:
            handle_execution_exception(e, debug=ctx.obj["DEBUG"])
        if not source_list:
            print_error("No tables found in the database.")
            raise Exit(1)

    show_ingest_header(
        sources=source_list,
        output_dir=str(output_dir),
        dialect=edialect.value,
        config_path=str(config) if config else None,
    )

    # Pre-load httpfs once if any source is remote — avoids re-installing per
    # source and avoids the install landing in an aborted transaction after a
    # failed introspection.
    if has_files and any(s.startswith(REMOTE_PREFIXES) for s in file_sources):
        try:
            exec.execute_raw_sql("INSTALL httpfs; LOAD httpfs;")
        except Exception as e:
            print_warning(f"Failed to load httpfs (remote reads may fail): {e}")

    ingested_files: list[PathlibPath] = []
    ingested: dict[str, IngestRecord] = {}
    summary_rows: list[IngestSummaryRow] = []
    renderer = Renderer()

    with ingest_progress(total=len(source_list)) as progress:
        for source in source_list:
            progress.step(source, "introspecting schema")
            is_file = _looks_like_file_source(source)
            try:
                if is_file:
                    datasource, concepts, required_imports, location = (
                        create_datasource_from_file(
                            exec, source, name_override=name, root=True
                        )
                    )
                    source_label = location
                else:
                    datasource, concepts, required_imports = (
                        create_datasource_from_table(exec, source, schema, root=True)
                    )
                    source_label = f"{schema}.{source}" if schema else source
                if name and not is_file:
                    datasource.name = name

                progress.step(source, "writing")
                ingested[source] = IngestRecord(
                    source=source,
                    datasource=datasource,
                    concepts=concepts,
                    required_imports=required_imports,
                    script=_build_script_content(
                        source_label, datasource, concepts, required_imports
                    ),
                )
            except Exception as e:
                print_error(f"Failed to ingest {source}: {e}")
                if ctx.obj["DEBUG"]:
                    import traceback

                    print_error(traceback.format_exc())
                # DuckDB leaves the SQLAlchemy connection in an aborted
                # transaction after a failed introspection — clear it so
                # subsequent sources don't all fail with "transaction aborted".
                try:
                    exec.connection.rollback()
                except Exception:
                    pass
                summary_rows.append(
                    IngestSummaryRow(
                        source=source,
                        output="-",
                        columns="-",
                        grain="-",
                        status=f"failed: {type(e).__name__}",
                    )
                )
                progress.advance()
                continue
            progress.advance()

    # Second pass: infer foreign keys across the just-introspected tables, then
    # merge with any explicit --fks (explicit wins per column).
    inferred_fks: list[InferredFK] = []
    if fk_infer_level != "off":
        table_records = [
            rec for rec in ingested.values() if not _looks_like_file_source(rec.source)
        ]
        if len(table_records) >= 2:
            try:
                fk_infos = [
                    build_table_fk_info(rec.source, rec.datasource, exec.generator)
                    for rec in table_records
                ]
                inferred_fks = infer_foreign_keys(fk_infos, exec, fk_infer_level)
            except Exception as e:
                print_warning(f"Foreign-key inference failed: {e}")
                if ctx.obj["DEBUG"]:
                    import traceback

                    print_warning(traceback.format_exc())
                try:
                    exec.connection.rollback()
                except Exception:
                    pass
    fk_map = merge_fk_maps(inferred_fks, explicit_fk_map)

    if fk_map:
        print_info("Processing foreign key relationships...")

    fk_datasources = {key: rec.datasource for key, rec in ingested.items()}
    for source, rec in ingested.items():
        output_file = output_dir / f"{rec.datasource.name}.preql"
        if fk_map and source in fk_map:
            column_mappings = fk_map[source]
            modified_content = apply_foreign_key_references(
                source, rec.datasource, fk_datasources, rec.script, column_mappings
            )
            output_file.write_text(modified_content)
        else:
            output_file.write_text(renderer.render_statement_string(rec.script))
        ingested_files.append(output_file)
        summary_rows.append(
            IngestSummaryRow(
                source=source,
                output=str(output_file),
                columns=str(len(rec.concepts)),
                grain=_grain_label(rec.datasource),
                status="ok",
            )
        )

    exec.close()
    show_ingest_summary(summary_rows)
    show_fk_summary(inferred_fks, explicit_fk_map)

    if not ingested_files:
        raise Exit(1)
