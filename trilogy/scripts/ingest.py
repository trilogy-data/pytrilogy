"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables or files."""

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path as PathlibPath
from typing import Iterator

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
from trilogy.core.models.core import EnumType, TraitDataType, ValidatedType
from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.dialect.base import BaseDialect, TableColumn, nullable_from_str
from trilogy.dialect.duckdb import DUCKDB_SAMPLE_SEED
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
    _fk_stem,
    _stem_related,
    build_table_fk_info,
    enrich_explicit_fks_partial,
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
from trilogy.scripts.ingest_helpers.introspection import IntrospectionLevel
from trilogy.scripts.ingest_helpers.typing import (
    detect_enum_types,
    detect_numeric_bounds,
    detect_rich_type,
)
from trilogy.utility import safe_open

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


@contextmanager
def _rollback_on_error(exec: Executor) -> Iterator[None]:
    """Run a block; on failure roll the connection back (best-effort) and re-raise.

    DuckDB leaves the SQLAlchemy connection in an aborted transaction after a
    failed query, so callers that fail mid-introspection must rollback to keep
    the executor usable. If rollback itself fails there is nothing more to do.
    """
    try:
        yield
    except Exception:
        try:
            exec.connection.rollback()
        except Exception:
            pass
        raise


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


def _hashable_cell(value: object) -> object:
    """Sample cells from LIST/STRUCT columns arrive as python lists/dicts,
    which can't go in the uniqueness set — canonicalize them recursively."""
    if isinstance(value, list):
        return tuple(_hashable_cell(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _hashable_cell(v)) for k, v in value.items()))
    return value


def _check_column_combination_uniqueness(
    indices: list[int], sample_rows: list[tuple]
) -> bool:
    if not sample_rows:
        return False

    values = set()
    for row in sample_rows:
        # For single column, use scalar value; for multiple columns, use tuple
        if len(indices) == 1:
            value = _hashable_cell(row[indices[0]])
        else:
            value = tuple(_hashable_cell(row[idx]) for idx in indices)

        if value in values:
            return False
        values.add(value)

    # Verify we have as many unique values as rows
    return len(values) == len(sample_rows)


# Decimal/float types are measures; their sample uniqueness is a coincidence.
_MEASURE_TYPES = frozenset({DataType.FLOAT, DataType.NUMERIC, DataType.NUMBER})
# Temporal columns lose to a non-temporal partner: a fact's grain is its entity
# + transaction number, not when it happened. Caught by type, or by name for a
# surrogate key whose stem carries a date/time token (``returned_date_sk``).
_TEMPORAL_TYPES = frozenset({DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP})
_TEMPORAL_NAME_TOKENS = ("date", "time")

# Per-column grain penalties, summed over a candidate (lower wins). Tiered so a
# measure never beats a key, a temporal column loses only to a non-temporal one,
# and a foreign key loses only when a natural key exists; gaps exceed the largest
# plausible per-key count of the tier below.
_PENALTY_MEASURE = 100
_PENALTY_TEMPORAL = 10
_PENALTY_FK = 1
# Boost a column whose name *is* a bare key token (``r_reason_sk`` -> ``sk`` once
# the shared prefix is stripped) or a qualified ``*_id`` business key. Equal
# boost, so an sk/id tie falls to column order. Excludes qualified ``*_sk``:
# those are foreign keys and keep their tier.
_IDENTIFIER_TOKENS = ("id", "sk", "key", "fk")
_BOOST_IDENTIFIER_NAME = -1


def _column_grain_penalty(
    canonical: str, datatype: DataType, table_canonical: str
) -> int:
    """Cost of using one column in a grain (lower is a better key column).

    The table's own identity column — the surrogate/natural key whose name stem
    is the table's entity (``call_center_sk`` in ``call_center``) — is its best
    grain and never penalized, even though it looks like a foreign key.
    """
    stem = _fk_stem(canonical)
    if stem and _stem_related(stem, table_canonical):
        return 0
    if canonical in _IDENTIFIER_TOKENS or canonical.endswith("_id"):
        return _BOOST_IDENTIFIER_NAME
    if datatype in _MEASURE_TYPES:
        return _PENALTY_MEASURE
    if datatype in _TEMPORAL_TYPES or (
        stem and any(t in stem.split("_") for t in _TEMPORAL_NAME_TOKENS)
    ):
        return _PENALTY_TEMPORAL
    return _PENALTY_FK if stem is not None else 0


def _rank_key_candidates(
    candidates: list[list[str]],
    column_order: dict[str, int],
    penalties: dict[str, int],
) -> list[list[str]]:
    """Order equal-size unique keys best-first by summed column penalty, then a
    stable tie-break on original column position for determinism."""

    def sort_key(combo: list[str]) -> tuple[int, tuple[int, ...]]:
        total = sum(penalties.get(c, 0) for c in combo)
        return (total, tuple(sorted(column_order[c] for c in combo)))

    return sorted(candidates, key=sort_key)


def detect_unique_key_combinations(
    column_names: list[str],
    sample_rows: list[tuple],
    max_key_size: int = 3,
    penalties: dict[str, int] | None = None,
) -> list[list[str]]:
    """Detect unique key combinations from sample data.

    Returns column combinations that uniquely identify rows in the sample,
    smallest size first. Within a size, ``penalties`` (raw column name → cost,
    from ``_grain_penalties``) ranks candidates so a natural key beats an
    equally-unique combination of measures/foreign keys; absent it, column order.
    """
    if not sample_rows or not column_names:
        return []

    column_order = {name: i for i, name in enumerate(column_names)}
    penalties = penalties or {}

    single = [
        [name]
        for i, name in enumerate(column_names)
        if _check_column_combination_uniqueness([i], sample_rows)
    ]
    if single:
        return _rank_key_candidates(single, column_order, penalties)

    for size in range(2, max_key_size + 1):
        sized: list[list[str]] = []
        for col_combination in combinations(enumerate(column_names), size):
            indices = [idx for idx, _ in col_combination]
            col_names = [name for _, name in col_combination]
            if _check_column_combination_uniqueness(indices, sample_rows):
                sized.append(col_names)
        if sized:
            return _rank_key_candidates(sized, column_order, penalties)

    return []


def _grain_penalties(
    table_name: str,
    columns: list[TableColumn],
    canonical: dict[str, str],
) -> dict[str, int]:
    """Map each raw column name to its grain penalty (see ``_column_grain_penalty``)."""
    table_canonical = canonicolize_name(table_name)
    return {
        col.column_name: _column_grain_penalty(
            canonical.get(col.column_name) or canonicolize_name(col.column_name),
            col.trilogy_type,
            table_canonical,
        )
        for col in columns
    }


# Cap on full-table uniqueness checks per source: ranking puts the real key near
# the front, so this only bounds a pathologically wide table's candidate list.
_MAX_GRAIN_VERIFICATIONS = 16


def _is_unique_key(
    exec: Executor, relation: str, raw_columns: list[str], dialect: BaseDialect
) -> bool:
    """True if `raw_columns` uniquely identifies every row of the full relation.

    Sample uniqueness is necessary but not sufficient (a clustered table can
    fake it); a GROUP BY over the whole relation is the ground truth. NULLs in
    a key column collapse into one group, so a nullable key reads as non-unique
    — correctly, since it can't stand in as the grain.
    """
    cols = ", ".join(dialect.safe_quote(c) for c in raw_columns)
    sql = (
        f"SELECT MAX(_n) FROM "
        f"(SELECT COUNT(*) AS _n FROM {relation} GROUP BY {cols}) _g"
    )
    rows = exec.execute_raw_sql(sql).fetchall()
    return bool(rows) and rows[0][0] == 1


def _select_verified_grain(
    exec: Executor,
    relation: str,
    candidates: list[list[str]],
    dialect: BaseDialect,
) -> list[str] | None:
    """First ranked candidate that is unique over the full relation, or None if
    none qualify (caller then falls back to no grain).

    A verification query that errors propagates: grain detection is part of the
    ingest request, so a failure surfaces for investigation rather than silently
    degrading to an unverified guess.
    """
    for candidate in candidates[:_MAX_GRAIN_VERIFICATIONS]:
        if _is_unique_key(exec, relation, candidate, dialect):
            return candidate
        print_info(f"Rejected grain candidate {candidate}: not unique on full table")
    return None


def detect_nullability_from_sample(column_index: int, sample_rows: list[tuple]) -> bool:
    for row in sample_rows:
        if row[column_index] is None:
            return True
    return False


def _concrete_datatype(dt: DataType) -> DataType:
    """A writable concrete type for ingest output, mapping UNKNOWN to STRING."""
    return DataType.STRING if dt == DataType.UNKNOWN else dt


def _process_column(
    idx: int,
    col: TableColumn,
    grain_components: list[str],
    sample_rows: list[tuple],
    concept_mapping: dict[str, str],
    enum_type: EnumType | None = None,
    bounds_type: ValidatedType | None = None,
) -> tuple[Concept, ColumnAssignment, str | None]:

    column_name = col.column_name
    schema_is_nullable = col.is_nullable
    column_comment = col.comment
    # Apply prefix stripping if mapping provided
    concept_name = concept_mapping[column_name]

    trilogy_type = _concrete_datatype(col.trilogy_type)

    # A column can be both an enum (constrained domain) and a rich type
    # (name-based trait); when both apply the trait wraps the enum. Rich types
    # are validated against actual values, so a Y/N "channel_email" flag named
    # like one isn't misclassified.
    if enum_type is not None:
        rich_values: list = list(enum_type.values)
    else:
        # Dedupe via _hashable_cell so LIST/STRUCT sample cells don't blow up
        # the set; keep the original values for rich-type validation.
        seen: set = set()
        rich_values = []
        for row in sample_rows:
            cell = row[idx]
            if cell is None:
                continue
            hashed = _hashable_cell(cell)
            if hashed not in seen:
                seen.add(hashed)
                rich_values.append(cell)
    trait_import, trait_type_name = detect_rich_type(
        concept_name, trilogy_type, rich_values
    )
    base: DataType | EnumType = trilogy_type if enum_type is None else enum_type
    final_datatype: TraitDataType | DataType | EnumType | ValidatedType
    if trait_import and trait_type_name:
        # traits may carry their own stdlib validator; observed bounds would
        # conflict with it on re-parse, so trait columns skip bound inference
        final_datatype = TraitDataType(type=base, traits=[trait_type_name])
        print_info(f"Detected rich type for '{concept_name}': {trait_type_name}")
    elif enum_type is None and bounds_type is not None:
        trait_import = None
        final_datatype = bounds_type
        print_info(f"Inferred value bounds for '{concept_name}': {bounds_type}")
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
    columns: list[TableColumn] = [
        exec.generator.make_table_column(
            row[0], row[1], nullable_from_str(row[2]) if len(row) > 2 else True
        )
        for row in describe_rows
    ]
    column_names = [c.column_name for c in columns]

    # Reservoir-sample (not a head LIMIT) so a file clustered on a leading key
    # doesn't hide grain columns / fake uniqueness; fixed seed keeps it stable.
    sample_rows = exec.execute_raw_sql(
        f"SELECT * FROM {source_expr} USING SAMPLE 10000 ROWS "
        f"(reservoir, {DUCKDB_SAMPLE_SEED})"
    ).fetchall()

    column_concept_mapping = canonicalize_names(column_names)
    suggested_keys = detect_unique_key_combinations(
        column_names,
        sample_rows,
        penalties=_grain_penalties(
            name_override or _datasource_name_from_path(arg),
            columns,
            column_concept_mapping,
        ),
    )
    if suggested_keys:
        print_info(f"Detected potential unique key combinations: {suggested_keys}")
        verified = _select_verified_grain(
            exec, source_expr, suggested_keys, exec.generator
        )
        if verified is not None:
            keys = verified
            print_info(f"Using verified unique grain: {verified}")
        else:
            keys = []
            print_warning(
                "No candidate is unique on the full file; defaulting to no grain"
            )
    else:
        keys = []
        print_info("No primary key or unique grain detected; defaulting to no grain")
    grain_components = [column_concept_mapping.get(k, k) for k in keys]

    enum_map = detect_enum_types(
        exec,
        source_expr,
        [(c.column_name, _concrete_datatype(c.trilogy_type)) for c in columns],
    )
    bounds_map = detect_numeric_bounds(
        exec,
        source_expr,
        [(c.column_name, _concrete_datatype(c.trilogy_type)) for c in columns],
        skip=set(keys) | set(enum_map.keys()),
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
            enum_map.get(col.column_name),
            bounds_map.get(col.column_name),
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
    column_names = [col.column_name for col in columns]

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
        suggested_keys = detect_unique_key_combinations(
            column_names,
            sample_rows,
            penalties=_grain_penalties(table_name, columns, column_concept_mapping),
        )
        if suggested_keys:
            print_info(f"Detected potential unique key combinations: {suggested_keys}")
            verified = _select_verified_grain(
                exec, dialect.safe_quote(qualified_name), suggested_keys, dialect
            )
            if verified is not None:
                keys = verified
                print_info(f"Using verified unique grain: {verified}")
            else:
                keys = []
                print_warning(
                    "No candidate is unique on the full table; defaulting to no grain"
                )
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
        [(c.column_name, _concrete_datatype(c.trilogy_type)) for c in columns],
    )
    bounds_map = detect_numeric_bounds(
        exec,
        dialect.safe_quote(qualified_name),
        [(c.column_name, _concrete_datatype(c.trilogy_type)) for c in columns],
        skip=set(keys) | set(enum_map.keys()),
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
            enum_map.get(col.column_name),
            bounds_map.get(col.column_name),
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
    "--infer-level",
    type=Choice([lvl.value for lvl in IntrospectionLevel]),
    default=IntrospectionLevel.FULL.value,
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
    infer_level: str,
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
    introspection_level = IntrospectionLevel(infer_level)

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

    cli_env_vars: dict[str, str] = {}
    if env:
        from trilogy.scripts.environment import parse_env_vars

        try:
            cli_env_vars = parse_env_vars(env)
        except ValueError as e:
            print_error(str(e))
            raise Exit(1) from e

    runtime_config = get_runtime_config(
        PathlibPath(config) if config else PathlibPath.cwd(),
        extra_env=cli_env_vars or None,
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
    # merge with any explicit --fks (explicit wins per column). FK linking is
    # an opt-in part of the user's ingest request — failures propagate rather
    # than warn-and-continue, since silently producing un-linked .preql files
    # would be worse than a hard failure.
    inferred_fks: list[InferredFK] = []
    fk_infos = []
    if introspection_level is not IntrospectionLevel.OFF:
        table_records = [
            rec for rec in ingested.values() if not _looks_like_file_source(rec.source)
        ]
        if len(table_records) >= 2:
            with _rollback_on_error(exec):
                fk_infos = [
                    build_table_fk_info(rec.source, rec.datasource, exec.generator)
                    for rec in table_records
                ]
                inferred_fks = infer_foreign_keys(fk_infos, exec, introspection_level)
    # Explicit --fks arrive marked partial=True. In full mode we have the
    # executor; sniff reverse coverage to upgrade complete edges.
    if introspection_level is IntrospectionLevel.FULL and explicit_fk_map and fk_infos:
        with _rollback_on_error(exec):
            enrich_explicit_fks_partial(
                explicit_fk_map, {t.name: t for t in fk_infos}, exec
            )
    fk_map = merge_fk_maps(inferred_fks, explicit_fk_map)

    if fk_map:
        print_info("Processing foreign key relationships...")

    fk_datasources = {key: rec.datasource for key, rec in ingested.items()}
    for source, rec in ingested.items():
        output_file = output_dir / f"{rec.datasource.name}.preql"
        if fk_map and source in fk_map:
            column_mappings = fk_map[source]
            content = apply_foreign_key_references(
                source, rec.datasource, fk_datasources, rec.script, column_mappings
            )
        else:
            content = renderer.render_statement_string(rec.script)
        # Trailing newline + LF line endings match what `trilogy fmt` writes,
        # so a freshly ingested file is already format-stable.
        if not content.endswith("\n"):
            content += "\n"
        with safe_open(str(output_file), "w", newline="\n") as f:
            f.write(content)
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
