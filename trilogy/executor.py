import datetime
import json
import random
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace as dc_replace
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, TypeVar, cast

from trilogy.constants import MagicConstants, Rendering, logger
from trilogy.core.enums import (
    AddressType,
    ComparisonOperator,
    CreateMode,
    FunctionType,
    Granularity,
    IOType,
    PersistMode,
    ValidationScope,
)
from trilogy.core.exceptions import ConfigurationException, QueryTimeoutException
from trilogy.core.models.author import Comment, Comparison, Concept, Function
from trilogy.core.models.build import BuildDatasource, BuildFunction
from trilogy.core.models.core import ListWrapper, MapWrapper
from trilogy.core.models.datasource import Address, Datasource, UpdateKeys
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import collect_source_addresses
from trilogy.core.statements.author import (
    STATEMENT_TYPES,
    CallStatement,
    ChartStatement,
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    CopyStatement,
    CreateStatement,
    FunctionDeclaration,
    ImportStatement,
    KeyMergeStatement,
    MergeStatementV2,
    MockStatement,
    MultiSelectStatement,
    NaturalSelectStatement,
    PersistStatement,
    PropertiesDeclarationStatement,
    PublishStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectStatement,
    ShowStatement,
    TypeDeclaration,
    ValidateNaturalStatement,
    ValidateStatement,
)
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedCallStatement,
    ProcessedChartCopyStatement,
    ProcessedChartStatement,
    ProcessedCopyStatement,
    ProcessedCreateStatement,
    ProcessedMockStatement,
    ProcessedNaturalSelectStatement,
    ProcessedPublishStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    ProcessedValidateNaturalStatement,
    ProcessedValidateStatement,
)
from trilogy.core.validation.common import (
    ValidationTest,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.cancel import resolve_query_canceller
from trilogy.dialect.config import DialectConfig, RetryPolicy
from trilogy.dialect.enums import Dialects
from trilogy.dialect.metadata import (
    handle_concept_declaration,
    handle_datasource,
    handle_import_statement,
    handle_merge_statement,
    handle_processed_show_statement,
    handle_processed_validate_statement,
    handle_publish_statement,
    handle_show_statement_outputs,
)
from trilogy.dialect.mock import handle_processed_mock_statement
from trilogy.dialect.results import BufferedResult, ChartResult, MockResult
from trilogy.engine import (
    EngineConnection,
    ExecutionEngine,
    ResultProtocol,
    SupportsNativePersist,
    escape_literal_colons,
)
from trilogy.hooks.base_hook import BaseHook
from trilogy.parser import parse_text
from trilogy.render import get_dialect_generator
from trilogy.staging import StagingConfig
from trilogy.utility import safe_open

ValidationDatasourceT = TypeVar("ValidationDatasourceT", Datasource, BuildDatasource)

#: Partition values inlined into one refresh filter before it is split across
#: statements. A rendering limit (engines cap statement size and IN-list length),
#: not a scheduling knob — how wide to fan out is the orchestrator's call.
MAX_PARTITION_FILTER_VALUES = 500

# Statement types that produce output (and so are "executable"). Everything else
# parsed from a file (rowset/concept/import/datasource definitions) registers into
# the environment but yields nothing on its own.
GENERATABLE_STATEMENT_TYPES = (
    SelectStatement,
    PersistStatement,
    MultiSelectStatement,
    ShowStatement,
    RawSQLStatement,
    CopyStatement,
    CallStatement,
    ValidateStatement,
    ValidateNaturalStatement,
    NaturalSelectStatement,
    CreateStatement,
    PublishStatement,
    MockStatement,
    ChartStatement,
)

# Human labels for the non-executable (definition) statement kinds, used by CLI
# summaries of files that parse but produce nothing to run.
DEFINITION_STATEMENT_LABELS: tuple[tuple[type, str], ...] = (
    (RowsetDerivationStatement, "rowset"),
    (ConceptDeclarationStatement, "concept"),
    (ConceptDerivationStatement, "concept"),
    (PropertiesDeclarationStatement, "property"),
    (ImportStatement, "import"),
    (Datasource, "datasource"),
    (MergeStatementV2, "merge"),
    (KeyMergeStatement, "merge"),
    (FunctionDeclaration, "function"),
    (TypeDeclaration, "type"),
)


def label_definition_statement(statement: object) -> str:
    for cls, label in DEFINITION_STATEMENT_LABELS:
        if isinstance(statement, cls):
            return label
    return "definition"


def serialize_call_arg(value: Any) -> str | None:
    """A call arg as an argv token; None omits the flag so script defaults apply."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, default=str)
    return str(value)


_CHART_COPY_SIZE_KEYS = {"width", "height"}
_CHART_COPY_SAVE_KEYS = {"scale": "scale_factor", "ppi": "ppi"}
_CHART_COPY_STYLE_KEYS = {"theme", "background"}
_CHART_COPY_ALLOWED = (
    _CHART_COPY_SIZE_KEYS | _CHART_COPY_SAVE_KEYS.keys() | _CHART_COPY_STYLE_KEYS
)


def _chart_copy_options(
    options: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], str | None, str | None]:
    if not options:
        return {}, {}, None, None
    unknown = set(options) - _CHART_COPY_ALLOWED
    if unknown:
        raise ValueError(
            f"Unknown copy option(s) for chart output: {sorted(unknown)}."
            f" Allowed: {sorted(_CHART_COPY_ALLOWED)}"
        )
    size_props = {k: options[k] for k in _CHART_COPY_SIZE_KEYS if k in options}
    save_kwargs = {
        dest: options[src]
        for src, dest in _CHART_COPY_SAVE_KEYS.items()
        if src in options
    }
    return size_props, save_kwargs, options.get("theme"), options.get("background")


# DuckDB aborts the loser of a concurrent catalog write with this marker. It is
# reachable from ordinary use: a directory-wide probe or refresh evaluates
# scripts on a thread pool, and every executor runs the same DuckDB setup DDL on
# connect. Against an in-memory database each executor owns a private catalog so
# they never meet; against an on-disk one they all write the same catalog.
_CATALOG_WRITE_CONFLICT = "write-write conflict"
_SETUP_CONFLICT_ATTEMPTS = 5


def _is_catalog_write_conflict(error: BaseException) -> bool:
    return _CATALOG_WRITE_CONFLICT in str(error)


def _fire_cancel(fired: threading.Event, cancel: Callable[[], None]) -> None:
    """Timer body for a query timeout: record that we cancelled, then cancel.

    The flag is set first so the executing thread can never see the driver's
    abort error without also seeing why it happened."""
    fired.set()
    cancel()


class Executor:
    def __init__(
        self,
        dialect: Dialects,
        engine: ExecutionEngine,
        environment: Environment | None = None,
        rendering: Rendering | None = None,
        hooks: list[BaseHook] | None = None,
        config: DialectConfig | None = None,
        staging: StagingConfig | None = None,
        chart_theme: str | None = None,
        datasource_transform: Callable[[Datasource], None] | None = None,
        query_timeout: float | None = None,
    ):

        self.dialect: Dialects = dialect
        # Seconds a single statement may run before the driver is asked to
        # cancel it. None (the default) means unbounded. Read by connect(), so
        # it has to be set before the connection is opened.
        self.query_timeout = query_timeout
        self._cancel_query: Callable[[], None] | None = None
        self.engine = engine
        self.environment = environment or Environment()
        # Physical-address rewrite (e.g. deployment-env prefixing) applied to
        # every datasource after parse, before statement processing.
        self.datasource_transform = datasource_transform
        self.generator: BaseDialect
        self.logger = logger
        self.hooks = hooks
        self.config = config
        self.staging = staging or StagingConfig()
        # default theme for chart copy output (from trilogy.toml [report].theme);
        # a per-statement copy (theme=...) overrides it
        self.chart_theme = chart_theme
        self._instance_id = str(uuid.uuid4())
        # transaction this executor implicitly opened (see _flush_transaction)
        self._owned_transaction: Any = None
        self.generator = get_dialect_generator(
            self.dialect,
            rendering,
            config,
            staging=self.staging,
            instance_id=self._instance_id,
        )
        self.connection = self.connect()
        self._validation_datasource_cache: (
            dict[str, Datasource | BuildDatasource] | None
        ) = None
        self._validation_temp_tables: list[str] = []
        # TODO: make generic
        if self.dialect == Dialects.DATAFRAME:
            self.engine.setup(self.environment, self.connection)
        # Setup DuckDB extensions
        if self.dialect == Dialects.DUCK_DB:
            self._setup_duckdb_python_datasources()
            self._setup_duckdb_gcs()
            self._setup_duckdb_spatial()

    def connect(self) -> EngineConnection:
        self.connection = self.engine.connect()
        self.connected = True
        self._owned_transaction = None
        if self.query_timeout is not None:
            self._cancel_query = resolve_query_canceller(self.connection)
            if self._cancel_query is None:
                raise ConfigurationException(
                    f"A query timeout was requested, but the {self.dialect.value} "
                    "driver exposes no way to cancel a running statement. Remove "
                    "the timeout rather than let it silently not apply."
                )
        return self.connection

    def commit(self) -> None:
        """Commit the open transaction on this executor's connection."""
        self.connection.commit()
        self._owned_transaction = None

    def _flush_transaction(self) -> None:
        """Commit work in a transaction this executor implicitly opened.

        Drivers auto-begin on the first statement and roll back on close, so
        without this every CREATE/persist would be discarded when the executor
        closes. A transaction the caller opened themselves is left alone."""
        owned = self._owned_transaction
        self._owned_transaction = None
        if owned is None or not owned.is_active:
            return
        if self.connection.get_transaction() is not owned:
            return
        self.connection.commit()

    def _execute_setup_ddl(self, sql: str) -> None:
        """Run connect-time DuckDB setup, retrying a lost catalog race.

        Every executor issues this same DDL on connect, so N of them sharing one
        on-disk warehouse collide and DuckDB aborts all but the winner. The
        statements are idempotent, so retrying settles it. The whole block is
        re-run rather than the conflict being swallowed: the rollback also drops
        session state set alongside the DDL (the per-instance ``uv_run`` temp
        dir variable), which the macro body then reads at query time.
        """
        for attempt in range(_SETUP_CONFLICT_ATTEMPTS):
            try:
                self.execute_raw_sql(sql)
                self.commit()
                return
            except Exception as e:
                if not _is_catalog_write_conflict(e):
                    raise
                self.connection.rollback()
                self._owned_transaction = None
                if attempt == _SETUP_CONFLICT_ATTEMPTS - 1:
                    raise
                # Jittered, or the same losers collide again on every retry.
                time.sleep(random.uniform(0.01, 0.05) * (attempt + 1))

    def _duckdb_macro_exists(self, name: str, marker: str) -> bool:
        """Whether a macro is already defined with a body containing ``marker``.

        Read-only, so it never joins the catalog write contention it exists to
        avoid. A failed lookup reports False: the caller then attempts the write
        and finds out for real.

        Leaves the connection's transactional state exactly as it found it. A
        read that implicitly opens a transaction and leaves it open is not
        harmless here: ``_execute_with_retry`` claims ownership of a transaction
        only when the statement itself began one, so an already-open transaction
        makes every later write look caller-managed, and ``_flush_transaction``
        then declines to commit and ``close`` discards the lot. That failure is
        silent — a refresh reports success having written nothing.
        """
        from sqlalchemy import text

        # Everything is inside the guard, including reading and restoring the
        # transaction state: this is only an optimization, and every way it can
        # fail — a connection that does not expose transactions, an unreadable
        # catalog — has the same right answer, which is to go do the write.
        try:
            implicit = not self.connection.in_transaction()
            try:
                rows = self.connection.execute(
                    text(
                        "select macro_definition from duckdb_functions() "
                        "where function_name = :name"
                    ),
                    {"name": name},
                ).fetchall()
            finally:
                if implicit and self.connection.in_transaction():
                    self.connection.rollback()
        except Exception:
            return False
        return any(row[0] and marker in row[0] for row in rows)

    def _setup_duckdb_python_datasources(self) -> None:
        """Setup DuckDB macro for Python script datasources."""
        import sys

        from trilogy.dialect.config import DuckDBConfig
        from trilogy.dialect.duckdb import (
            PYTHON_DATASOURCE_GUARD_MARKER,
            get_python_datasource_setup_sql,
        )

        # A read-only handle can't CREATE the guard macro — and python
        # datasources (which need write access) are unusable anyway, so skip it.
        if isinstance(self.config, DuckDBConfig) and self.config.read_only:
            return
        enabled = (
            isinstance(self.config, DuckDBConfig)
            and self.config.enable_python_datasources
        )
        # The disabled form is a pure error guard: no extensions to load and no
        # session state to establish, so an already-defined one is correct as it
        # stands and the write can be skipped outright. That is the default and
        # the overwhelmingly common case, so concurrent executors sharing an
        # on-disk warehouse normally never contend for the catalog at all.
        # The enabled form cannot take this shortcut — it must LOAD extensions
        # and SET the per-instance temp dir variable in every new session.
        if not enabled and self._duckdb_macro_exists(
            "uv_run", PYTHON_DATASOURCE_GUARD_MARKER
        ):
            return
        is_windows = sys.platform == "win32"
        self._execute_setup_ddl(
            get_python_datasource_setup_sql(
                enabled, is_windows, self._instance_id, self.staging
            )
        )

    def _setup_duckdb_gcs(self) -> None:
        """Setup DuckDB GCS extension with application default credentials."""
        from trilogy.dialect.config import DuckDBConfig
        from trilogy.dialect.duckdb import get_gcs_setup_sql

        enabled = isinstance(self.config, DuckDBConfig) and self.config.enable_gcs
        if not enabled:
            return
        sql = get_gcs_setup_sql(enabled)
        if sql:
            # CREATE SECRET is a catalog write, so it races the same way.
            self._execute_setup_ddl(sql)

    def _setup_duckdb_spatial(self) -> None:
        """Setup DuckDB spatial extension for geospatial functions."""
        from trilogy.dialect.config import DuckDBConfig

        enabled = isinstance(self.config, DuckDBConfig) and self.config.enable_spatial
        if not enabled:
            return
        self.execute_raw_sql("INSTALL spatial;")
        self.execute_raw_sql("LOAD spatial;")
        self.commit()

    def close(self) -> None:
        self.generator.teardown()
        if self.connected:
            self._flush_transaction()
            self.connection.close()
        self.engine.dispose(close=True)
        if self.dialect == Dialects.DUCK_DB:
            import gc

            gc.collect()
        self.connected = False

    @contextmanager
    def validation_scope(self) -> Generator[None, None, None]:
        if self._validation_datasource_cache is not None:
            yield
            return

        self._validation_datasource_cache = {}
        self._validation_temp_tables = []
        try:
            yield
        finally:
            quote = self.generator.QUOTE_CHARACTER
            for table_name in reversed(self._validation_temp_tables):
                try:
                    self.execute_raw_sql(
                        f"DROP TABLE IF EXISTS {quote}{table_name}{quote}"
                    )
                except Exception as e:
                    self.logger.debug(
                        "Failed to drop validation temp table %s: %s", table_name, e
                    )
            self._validation_datasource_cache = None
            self._validation_temp_tables = []

    def get_validation_cached_datasource(
        self, datasource: ValidationDatasourceT
    ) -> ValidationDatasourceT:
        cache = self._validation_datasource_cache
        if cache is None or self.dialect != Dialects.DUCK_DB:
            return datasource

        address = datasource.address
        if (
            not isinstance(address, Address)
            or address.type != AddressType.PYTHON_SCRIPT
        ):
            return datasource

        cached = cache.get(datasource.identifier)
        if cached is not None:
            return cast(ValidationDatasourceT, cached)

        table_name = f"__trilogy_validation_cache_{datasource.safe_identifier}"
        quote = self.generator.QUOTE_CHARACTER
        source_sql = self.generator.render_source(address)
        self.execute_raw_sql(
            f"CREATE TEMP TABLE {quote}{table_name}{quote} AS SELECT * FROM {source_sql}"
        )

        cached_address = Address(location=table_name, type=AddressType.TABLE)
        cached_datasource: Datasource | BuildDatasource
        if isinstance(datasource, Datasource):
            cached_datasource = datasource.duplicate()
            cached_datasource.address = cached_address
        else:
            cached_datasource = dc_replace(datasource, address=cached_address)

        cache[datasource.identifier] = cached_datasource
        self._validation_temp_tables.append(table_name)
        return cast(ValidationDatasourceT, cached_datasource)

    def update_datasource(
        self,
        datasource: Datasource,
        keys: UpdateKeys | None = None,
        dry_run: bool = False,
        partitions: list | None = None,
    ) -> str | None:
        """Update a datasource with optional filtering based on update keys.

        Returns the compiled persist SQL, or None if not applicable.

        ``partitions`` narrows the refresh to specific slices of a partitioned
        datasource (see ``StaleAsset.partitions``). The staged partitioned append
        replaces exactly the keys the select produces, so N slices cost one
        statement and healthy neighbours are untouched — a hole in the middle of
        a range is filled without rebuilding the range. Slice count is chunked
        only to keep the rendered filter within statement-size limits; how wide
        to fan out is the orchestrator's call, not this method's.
        """
        chunks = self._partition_chunks(partitions)
        if chunks is not None:
            rendered = [
                sql
                for chunk in chunks
                if (
                    sql := self._update_datasource_once(
                        datasource, None, dry_run, chunk
                    )
                )
            ]
            # No chunks means no stale slices, which means nothing to refresh —
            # never an unfiltered rebuild.
            return "\n".join(rendered) if rendered else None
        return self._update_datasource_once(datasource, keys, dry_run, None)

    def _partition_chunks(self, partitions: list | None) -> list[list] | None:
        """Split a slice list into statement-sized chunks, or None if unsliced.

        An empty list is NOT unsliced: "no stale slices" must never render as
        "no filter", which would rebuild the whole table."""
        if partitions is None:
            return None
        cap = MAX_PARTITION_FILTER_VALUES
        return [partitions[i : i + cap] for i in range(0, len(partitions), cap)]

    def _update_datasource_once(
        self,
        datasource: Datasource,
        keys: UpdateKeys | None,
        dry_run: bool,
        partitions: list | None,
    ) -> str | None:
        from trilogy.execution.state.partitions import partition_filter

        if partitions is not None:
            # Slices REPLACE the incremental filter rather than narrowing it: a
            # missing slice may hold rows older than the watermark, and ANDing
            # the two would exclude the rows the refresh exists to write.
            where = partition_filter(datasource, self.environment, partitions)
            if where is None:
                return None
        else:
            where = keys.to_where_clause(self.environment) if keys else None
        # Skip CREATE for file-backed datasources (parquet, csv, etc.) - the file is the source
        is_file_backed = (
            isinstance(datasource.address, Address) and datasource.address.is_file
        )
        if not dry_run and not is_file_backed:
            create_stmt = CreateStatement(
                scope=ValidationScope.DATASOURCES,
                create_mode=CreateMode.CREATE_IF_NOT_EXISTS,
                targets=[datasource.name],
            )
            self.execute_statement(create_stmt)
        select_stmt = datasource.create_update_statement(
            self.environment, where, line_no=None
        )
        # APPEND when the refresh is scoped — to incremental keys, or to slices.
        # OVERWRITE on a scoped refresh would drop everything outside the scope.
        persist_mode = (
            PersistMode.APPEND
            if ((keys and keys.keys) or partitions)
            else PersistMode.OVERWRITE
        )
        statement = PersistStatement(
            datasource=datasource,
            select=select_stmt,
            persist_mode=persist_mode,
            # A declared partitioning makes an incremental append idempotent per
            # slice: the dialect replaces the partitions this select covers
            # instead of blindly adding rows to them.
            partition_by=(
                datasource.partition_by if persist_mode == PersistMode.APPEND else []
            ),
        )
        generated = self._generate([statement])
        if not generated:
            return None
        processed = generated[0]
        if not dry_run:
            self.execute_query(processed)
        if isinstance(processed, ProcessedQueryPersist):
            return self.generator.compile_statement(processed)
        return None

    def _generate(
        self, statements: Sequence[STATEMENT_TYPES]
    ) -> list[PROCESSED_STATEMENT_TYPES]:
        """Process author statements against this executor's environment/hooks.
        Non-generatable members of the union are rejected by the generator."""
        return self.generator.generate_queries(
            self.environment, statements, hooks=self.hooks  # type: ignore[arg-type]
        )

    def _generate_sql(self, statements: Sequence[STATEMENT_TYPES]) -> list[str]:
        return [self.generator.compile_statement(x) for x in self._generate(statements)]

    def execute_statement(
        self,
        statement: PROCESSED_STATEMENT_TYPES | STATEMENT_TYPES,
    ) -> ResultProtocol | None:
        if isinstance(statement, STATEMENT_TYPES):
            generate = self._generate([statement])
            if not generate:
                return None
            statement = generate[0]

        if not isinstance(statement, PROCESSED_STATEMENT_TYPES):
            return None

        return self.execute_query(statement)

    @singledispatchmethod
    def execute_query(self, query) -> ResultProtocol | None:
        raise NotImplementedError(f"Cannot execute type {type(query)}")

    @execute_query.register
    def _(self, query: Comment) -> ResultProtocol | None:
        return None

    @execute_query.register
    def _(self, query: ConceptDeclarationStatement) -> ResultProtocol | None:
        return handle_concept_declaration(query)

    @execute_query.register
    def _(self, query: Datasource) -> ResultProtocol | None:
        return handle_datasource(query)

    @execute_query.register
    def _(self, query: str) -> ResultProtocol | None:
        results = self.execute_text(query)
        if results:
            return results[-1]
        return None

    # Author statements with a SQL form: generate, then execute the processed
    # statement the generator produced.
    @execute_query.register(SelectStatement)
    @execute_query.register(PersistStatement)
    @execute_query.register(ShowStatement)
    @execute_query.register(ValidateNaturalStatement)
    @execute_query.register(NaturalSelectStatement)
    def _(
        self,
        query: (
            SelectStatement
            | PersistStatement
            | ShowStatement
            | ValidateNaturalStatement
            | NaturalSelectStatement
        ),
    ) -> ResultProtocol | None:
        return self.execute_query(self._generate([query])[0])

    @execute_query.register
    def _(self, query: RawSQLStatement) -> ResultProtocol | None:
        return self.execute_write_sql(query.text)

    @execute_query.register
    def _(self, query: ProcessedShowStatement) -> ResultProtocol | None:
        return handle_processed_show_statement(
            query,
            [
                self.generator.compile_statement(x)
                for x in query.output_values
                if isinstance(x, (ProcessedQuery, ProcessedQueryPersist))
            ],
        )

    @execute_query.register
    def _(self, query: ProcessedValidateStatement) -> ResultProtocol | None:
        return handle_processed_validate_statement(
            query, self.generator, self.validate_environment
        )

    @execute_query.register
    def _(self, query: ProcessedValidateNaturalStatement) -> ResultProtocol | None:
        # The expected select was compiled at generate time (the free tier);
        # the LLM loop only runs under `trilogy unit/integration --include-type
        # agent`, never during normal execution.
        return MockResult(
            values=[
                {
                    "label": query.name or "",
                    "question": query.question,
                    "status": (
                        "skipped - agent validation runs under trilogy "
                        "unit/integration --include-type agent"
                    ),
                }
            ],
            columns=["label", "question", "status"],
        )

    @execute_query.register
    def _(self, query: ProcessedNaturalSelectStatement) -> ResultProtocol | None:
        from trilogy.scripts.validate_agent import execute_natural_select

        return execute_natural_select(self, query.question)

    @execute_query.register
    def _(self, query: ProcessedMockStatement) -> ResultProtocol | None:

        return handle_processed_mock_statement(query, self.environment, self)

    @execute_query.register
    def _(self, query: ProcessedCreateStatement) -> ResultProtocol | None:
        return self.execute_write_statements(self.generator.compile_statements(query))

    @execute_query.register
    def _(self, query: ProcessedPublishStatement) -> ResultProtocol | None:
        return handle_publish_statement(query, self.environment)

    @execute_query.register
    def _(self, query: ImportStatement) -> ResultProtocol | None:
        return handle_import_statement(query)

    @execute_query.register
    def _(self, query: MergeStatementV2) -> ResultProtocol | None:
        return handle_merge_statement(query, self.environment)

    @execute_query.register
    def _(self, query: ProcessedRawSQLStatement) -> ResultProtocol | None:
        return self.execute_write_sql(query.text)

    def _prepare_query_sources(self, query: ProcessedQuery) -> None:
        """Let the dialect materialize sources it can only reference by name.

        DuckDB reads python scripts and files lazily in the query itself;
        BigQuery has to stage them first. Runs before compilation because
        rendering the source assumes the staged artifact's name.
        """
        if not self.generator.REQUIRES_SOURCE_PREPARATION:
            return
        addresses = collect_source_addresses(query.ctes)
        if addresses:
            self.generator.prepare_sources(addresses, self)

    def compile_for_execution(self, query: ProcessedQuery) -> str:
        """Compile a statement that is about to run, rather than be displayed.

        The only place the dialect's ``prepare_sources`` hook fires, so every
        path that turns a processed statement into SQL it then executes — plain
        selects, persists, copies, chart layers — must come through here.
        ``generator.compile_statement`` stays side-effect free for the paths
        that only render SQL (``generate_sql``, `show`, metadata)."""
        self._prepare_query_sources(query)
        return self.generator.compile_statement(query)

    @execute_query.register
    def _(self, query: ProcessedQuery) -> ResultProtocol | None:
        sql = self.compile_for_execution(query)
        output = self.execute_raw_sql(sql, local_concepts=query.local_concepts)
        return output

    def _address_type_to_io_type(self, addr_type: AddressType) -> IOType:
        if addr_type == AddressType.PARQUET:
            return IOType.PARQUET
        elif addr_type == AddressType.CSV:
            return IOType.CSV
        raise NotImplementedError(f"File persist not supported for type {addr_type}")

    @execute_query.register
    def _(self, query: ProcessedQueryPersist) -> ResultProtocol | None:
        # Check if target is a file - convert to CopyStatement
        addr = query.output_to.address
        if addr.is_file:
            io_type = self._address_type_to_io_type(addr.type)
            # Build column alias mapping from datasource columns
            column_aliases: dict[str, str] = {}
            for col in query.datasource.columns:
                if col.is_concrete and isinstance(col.alias, str):
                    column_aliases[col.concept.address] = col.alias
            copy_statement = ProcessedCopyStatement(
                output_columns=query.output_columns,
                ctes=query.ctes,
                base=query.base,
                hidden_columns=query.hidden_columns,
                limit=query.limit,
                order_by=query.order_by,
                local_concepts=query.local_concepts,
                locally_derived=query.locally_derived,
                target=addr.write_location or addr.location,
                target_type=io_type,
                column_aliases=column_aliases,
            )
            self.execute_query(copy_statement)
            if query.persist_mode == PersistMode.OVERWRITE:
                self.environment.add_datasource(query.datasource)
            return None

        output = self._execute_persist(query)

        if query.persist_mode == PersistMode.OVERWRITE:
            self.environment.add_datasource(query.datasource)
        return output

    def _execute_persist(self, query: ProcessedQueryPersist) -> ResultProtocol:
        """Offer the write to the engine's own API, then fall back to SQL.

        Source preparation runs first either way — a native writer still reads
        through whatever the dialect had to stage (BigQuery's python datasources
        land in GCS before any job can name them), and it renders its select
        from the same processed statement."""
        self._prepare_query_sources(query)
        if isinstance(self.engine, SupportsNativePersist):
            native = self.engine.execute_persist(query, self)
            if native is not None:
                return native
        return self.execute_write_statements(
            self.generator.compile_statements(query),
            local_concepts=query.local_concepts,
        )

    def _build_aliased_copy_sql(self, query: ProcessedCopyStatement) -> str:
        """Build SQL with column aliases for file output."""
        base_sql = self.compile_for_execution(query)
        if not query.column_aliases:
            return base_sql
        quote = self.generator.QUOTE_CHARACTER
        alias_clauses = []
        for col in query.output_columns:
            target_name = query.column_aliases.get(col.address)
            if target_name:
                alias_clauses.append(
                    f"{quote}{col.safe_address}{quote} as {quote}{target_name}{quote}"
                )
            else:
                alias_clauses.append(f"{quote}{col.safe_address}{quote}")
        select_clause = ", ".join(alias_clauses)
        return f"SELECT {select_clause} FROM ({base_sql}) as _copy_source"

    def _resolve_copy_target(self, target: str) -> str:
        """Resolve copy target path, making relative paths relative to working_path."""
        target_path = Path(target)
        if not target_path.is_absolute() and not target.startswith(("gcs://", "gs://")):
            return str(self.environment.working_path / target_path)
        return target

    @execute_query.register
    def _(self, query: ProcessedCopyStatement) -> ResultProtocol | None:
        sql = self._build_aliased_copy_sql(query)
        target = self._resolve_copy_target(query.target)
        if self.dialect == Dialects.DUCK_DB:
            # Check for GCS write credentials if target is a GCS path
            if target.startswith(("gcs://", "gs://")):
                from trilogy.dialect.duckdb import check_gcs_write_credentials

                check_gcs_write_credentials()

            if query.target_type == IOType.PARQUET:
                copy_sql = f"COPY ({sql}) TO '{target}' (FORMAT PARQUET)"
            elif query.target_type == IOType.CSV:
                copy_sql = f"COPY ({sql}) TO '{target}' (FORMAT CSV, HEADER)"
            elif query.target_type == IOType.JSON:
                copy_sql = f"COPY ({sql}) TO '{target}' (FORMAT JSON, ARRAY true)"
            else:
                raise NotImplementedError(f"Unsupported IO Type {query.target_type}")
            self.execute_raw_sql(copy_sql, local_concepts=query.local_concepts)
        else:
            raise NotImplementedError(
                f"COPY statement not supported for dialect {self.dialect}"
            )
        return MockResult(
            [{"query": self.generator.compile_statement(query)}],
            ["query"],
        )

    @execute_query.register
    def _(self, query: ProcessedCallStatement) -> ResultProtocol | None:
        from trilogy.dialect.python_source import build_script_command

        arg_pairs: list[tuple[str, Any]] = []
        if query.query is not None:
            sql = self.compile_for_execution(query.query)
            result = self.execute_raw_sql(
                sql, local_concepts=query.query.local_concepts
            )
            if result is None:
                raise ValueError(
                    f"call script '{query.target}': argument select returned no result set"
                )
            rows = result.fetchall()
            if len(rows) != 1:
                raise ValueError(
                    f"call script '{query.target}': argument select must return "
                    f"exactly one row, got {len(rows)}"
                )
            visible = [
                c
                for c in query.query.output_columns
                if c.address not in query.query.hidden_columns
            ]
            row = rows[0]
            if len(row) != len(visible):
                raise ValueError(
                    f"call script '{query.target}': argument select returned "
                    f"{len(row)} columns for {len(visible)} visible outputs"
                )
            for ref, value in zip(visible, row):
                arg_pairs.append((ref.address.rsplit(".", 1)[-1], value))
        command = build_script_command(self._resolve_copy_target(query.target))
        for name, value in arg_pairs:
            serialized = serialize_call_arg(value)
            if serialized is None:
                continue
            command.extend([f"--{name}", serialized])
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=str(self.environment.working_path),
            check=False,
        )
        if completed.stdout and completed.stdout.strip():
            logger.info(
                f"call script '{query.target}' stdout: {completed.stdout.strip()}"
            )
        if completed.returncode != 0:
            detail = (completed.stderr or "").strip() or (
                completed.stdout or ""
            ).strip()
            raise RuntimeError(
                f"call script '{query.target}' failed "
                f"(exit {completed.returncode}): {detail}"
            )
        return MockResult(
            [{"target": query.target, "status": "success"}], ["target", "status"]
        )

    def _run_chart_layers(self, query: ProcessedChartStatement) -> list[list[dict]]:
        layer_data: list[list[dict]] = []
        for layer in query.layers:
            if layer.query is None:
                layer_data.append([])
                continue
            sql = self.compile_for_execution(layer.query)
            result = self.execute_raw_sql(
                sql, local_concepts=layer.query.local_concepts
            )
            if result is None:
                layer_data.append([])
                continue
            layer_data.append(
                [dict(zip(result.keys(), row)) for row in result.fetchall()]
            )
        return layer_data

    def _resolve_chart_theme(self, override: str | None = None):
        from trilogy.rendering.theme import DEFAULT_THEME, get_theme

        return get_theme(override or self.chart_theme or DEFAULT_THEME.name)

    @execute_query.register
    def _(self, query: ProcessedChartStatement) -> ResultProtocol | None:
        from trilogy.rendering.altair_renderer import ALTAIR_AVAILABLE, AltairRenderer

        layer_data = self._run_chart_layers(query)
        chart = None
        if ALTAIR_AVAILABLE:
            renderer = AltairRenderer(theme=self._resolve_chart_theme())
            chart = renderer.render(query, layer_data)

        return ChartResult(chart=chart, data=layer_data, statement=query)

    @execute_query.register
    def _(self, query: ProcessedChartCopyStatement) -> ResultProtocol | None:
        from trilogy.rendering.altair_renderer import ALTAIR_AVAILABLE, AltairRenderer
        from trilogy.rendering.chart_theme import theme_chart

        if not ALTAIR_AVAILABLE:
            raise RuntimeError(
                "Copying a chart to a file requires altair. Install with 'pip install altair vl-convert-python'."
            )
        size_props, save_kwargs, theme_name, background = _chart_copy_options(
            query.options
        )
        theme = self._resolve_chart_theme(theme_name)
        layer_data = self._run_chart_layers(query.chart)
        renderer = AltairRenderer(theme=theme)
        chart = renderer.render(query.chart, layer_data)
        if chart is None:
            raise RuntimeError("Chart renderer returned no chart to save.")
        chart = theme_chart(chart, theme)
        if background:
            chart = chart.properties(background=background)
        if size_props:
            chart = chart.properties(**size_props)
        target = self._resolve_copy_target(query.target)
        chart.save(target, format=query.target_type.value, **save_kwargs)
        return MockResult([{"target": target}], ["target"])

    @singledispatchmethod
    def generate_sql(self, command) -> list[str]:
        raise NotImplementedError(f"Cannot generate sql for type {type(command)}")

    # Already-processed statements: compiling is all that's left.
    @generate_sql.register(ProcessedQuery)
    @generate_sql.register(ProcessedCopyStatement)
    @generate_sql.register(ProcessedCreateStatement)
    @generate_sql.register(ProcessedPublishStatement)
    def _(
        self,
        command: (
            ProcessedQuery
            | ProcessedCopyStatement
            | ProcessedCreateStatement
            | ProcessedPublishStatement
        ),
    ) -> list[str]:
        return [self.generator.compile_statement(command)]

    @generate_sql.register
    def _(self, command: ProcessedShowStatement) -> list[str]:
        output = []
        for statement in command.output_values:
            if isinstance(statement, (ProcessedQuery, ProcessedQueryPersist)):
                compiled_sql = self.generator.compile_statement(statement)
                output.append(compiled_sql)
        return output

    @generate_sql.register(SelectStatement)
    @generate_sql.register(MultiSelectStatement)
    def _(self, command: SelectStatement | MultiSelectStatement) -> list[str]:
        return self._generate_sql([command])

    @generate_sql.register
    def _(self, command: str) -> list[str]:
        # Same statement set parse_text executes, so text in and statements in
        # produce the same SQL — a narrower filter here silently drops DDL.
        return [
            self.generator.compile_statement(x)
            for x in self.parse_text_generator(command)
        ]

    def parse_file(
        self, file: str | Path, persist: bool = False
    ) -> list[PROCESSED_STATEMENT_TYPES]:
        return list(self.parse_file_generator(file, persist=persist))

    def parse_file_generator(
        self, file: str | Path, persist: bool = False
    ) -> Generator[
        PROCESSED_STATEMENT_TYPES,
        None,
        None,
    ]:
        file = Path(file)
        candidates = [file, self.environment.working_path / file]
        err = None
        for file in candidates:
            try:
                with safe_open(file) as f:
                    command = f.read()
                    return self.parse_text_generator(
                        command, persist=persist, root=file
                    )
            except FileNotFoundError as e:
                if not err:
                    err = e
                continue
        if err:
            raise err
        raise FileNotFoundError(f"File {file} not found")

    def parse_text(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> list[PROCESSED_STATEMENT_TYPES]:
        return list(self.parse_text_generator(command, persist=persist, root=root))

    def _apply_datasource_transform(self, parsed: Sequence[Any]) -> None:
        """Run the installed physical-address rewrite over everything a parse
        produced: registered datasources plus not-yet-registered persist
        targets. Must run before ``_generate`` so processed statements bake
        the rewritten addresses."""
        if not self.datasource_transform:
            return
        for ds in self.environment.datasources.values():
            self.datasource_transform(ds)
        for statement in parsed:
            if isinstance(statement, PersistStatement):
                self.datasource_transform(statement.datasource)

    def parse_text_with_definitions(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> tuple[list[PROCESSED_STATEMENT_TYPES], list[Any]]:
        """Parse, returning both the executable queries and the non-executable
        definition statements (rowsets, concepts, imports, datasources, ...).
        Lets callers warn when a file parses cleanly but has no statement that
        produces output (a definitions-only file does nothing on its own)."""
        _, parsed = parse_text(command, self.environment, root=root)
        self._apply_datasource_transform(parsed)
        definitions = [
            x
            for x in parsed
            if not isinstance(x, GENERATABLE_STATEMENT_TYPES)
            and not isinstance(x, Comment)
        ]
        queries: list[PROCESSED_STATEMENT_TYPES] = []
        for t in parsed:
            if not isinstance(t, GENERATABLE_STATEMENT_TYPES):
                continue
            for x in self._generate([t]):
                if persist and isinstance(x, ProcessedQueryPersist):
                    self.environment.add_datasource(x.datasource)
                queries.append(x)
        return queries, definitions

    def parse_text_generator(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> Generator[
        PROCESSED_STATEMENT_TYPES,
        None,
        None,
    ]:
        """Process a preql text command"""
        _, parsed = parse_text(command, self.environment, root=root)
        self._apply_datasource_transform(parsed)
        generatable = [x for x in parsed if isinstance(x, GENERATABLE_STATEMENT_TYPES)]
        while generatable:
            t = generatable.pop(0)
            # One author statement can process into several: `create ... with
            # data` yields a persist per target.
            for x in self._generate([t]):
                yield x

                if persist and isinstance(x, ProcessedQueryPersist):
                    self.environment.add_datasource(x.datasource)

    def _atom_to_value(self, val: Any) -> Any:
        if val == MagicConstants.NULL:
            return None
        return val

    def _concept_to_value(
        self,
        concept: Concept,
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> Any:
        if not concept.granularity == Granularity.SINGLE_ROW:
            raise SyntaxError(
                f"Cannot bind non-singleton concept {concept.address} ({concept.granularity}, lineage {concept.lineage}) to a parameter."
            )
        # TODO: to get rid of function here - need to figure out why it's getting passed in
        if (
            isinstance(concept.lineage, (BuildFunction, Function))
            and concept.lineage.operator == FunctionType.CONSTANT
        ):
            rval = concept.lineage.arguments[0]
            if isinstance(rval, ListWrapper):
                return [self._atom_to_value(x) for x in rval]
            if isinstance(rval, MapWrapper):
                # duckdb expects maps in this format as variables
                if self.dialect == Dialects.DUCK_DB:
                    return {
                        "key": [self._atom_to_value(x) for x in rval],
                        "value": [self._atom_to_value(rval[x]) for x in rval],
                    }
                return {k: self._atom_to_value(v) for k, v in rval.items()}
            # if isinstance(rval, ConceptRef):
            #     return self._concept_to_value(self.environment.concepts[rval.address], local_concepts=local_concepts)
            return rval
        elif isinstance(concept.lineage, Comparison):
            # evaluate the comparison to get the value
            left_value = self._atom_to_value(concept.lineage.left)
            right_value = self._atom_to_value(concept.lineage.right)
            operator = concept.lineage.operator
            if operator == ComparisonOperator.EQ:
                return left_value == right_value
            elif operator == ComparisonOperator.NE:
                return left_value != right_value
            elif operator == ComparisonOperator.LT:
                return left_value < right_value
            elif operator == ComparisonOperator.LTE:
                return left_value <= right_value
            elif operator == ComparisonOperator.GT:
                return left_value > right_value
            elif operator == ComparisonOperator.GTE:
                return left_value >= right_value
            elif operator == ComparisonOperator.IS:
                return left_value is right_value
            elif operator == ComparisonOperator.IS_NOT:
                return left_value is not right_value
            else:
                raise SyntaxError(
                    f"Cannot bind comparison with operator {operator} to a parameter."
                )

        else:
            results = self.execute_query(f"select {concept.name} limit 1;")
            if results:
                fetcher = results.fetchone()
                if fetcher:
                    return fetcher[0]
            return None

    def _hydrate_param(
        self, param: str, local_concepts: Mapping[str, Concept] | None = None
    ) -> Any:
        matched = [
            v
            for v in self.environment.concepts.values()
            if v.safe_address == param or v.address == param
        ]
        if local_concepts and not matched:
            matched = [
                v
                for v in local_concepts.values()
                if v.safe_address == param or v.address == param
            ]
        if not matched:
            raise SyntaxError(f"No concept found for parameter {param};")

        concept: Concept = matched.pop()
        return self._concept_to_value(concept, local_concepts=local_concepts)

    def _get_retry_policy(self, error: Exception) -> RetryPolicy | None:
        """Get retry policy for an error if configured."""
        if not self.config or not self.config.retry_config:
            return None
        return self.config.retry_config.get_policy_for_error(str(error))

    def _execute_now(self, statement: Any, final_params: dict | None) -> ResultProtocol:
        if final_params:
            return self.connection.execute(statement, final_params)
        return self.connection.execute(statement)

    def _execute_bounded(
        self,
        statement: Any,
        final_params: dict | None,
        timeout: float,
        cancel: Callable[[], None],
    ) -> ResultProtocol:
        """Run a statement, asking the driver to abort it once the timeout passes.

        Only the cancel runs off-thread; the statement itself stays here, so the
        failure surfaces at the call site and the connection is left rolled back
        and reusable rather than abandoned mid-cursor. A timer that fires just
        after the statement finished is harmless — every canceller in
        ``dialect/cancel.py`` is a no-op against an idle connection."""
        fired = threading.Event()
        timer = threading.Timer(timeout, _fire_cancel, args=(fired, cancel))
        timer.start()
        try:
            return self._execute_now(statement, final_params)
        except Exception as e:
            if not fired.is_set():
                raise
            self.connection.rollback()
            self._owned_transaction = None
            raise QueryTimeoutException(
                f"Query cancelled after exceeding the {timeout:g}s timeout."
            ) from e
        finally:
            timer.cancel()

    def _execute_with_retry(
        self,
        command: str,
        final_params: dict | None,
    ) -> ResultProtocol:
        """Execute SQL with retry logic based on configured retry policy."""
        import time

        from sqlalchemy import text

        attempt = 0

        while True:
            attempt += 1
            implicit = not self.connection.in_transaction()
            try:
                statement = text(command)
                cancel, timeout = self._cancel_query, self.query_timeout
                if cancel is None or timeout is None:
                    result = self._execute_now(statement, final_params)
                else:
                    result = self._execute_bounded(
                        statement, final_params, timeout, cancel
                    )
                if implicit and self.connection.in_transaction():
                    self._owned_transaction = self.connection.get_transaction()
                return result
            except QueryTimeoutException:
                raise
            except Exception as e:
                policy = self._get_retry_policy(e)
                if policy is None or attempt >= policy.max_attempts:
                    raise
                delay = policy.get_delay(attempt)
                self.logger.warning(
                    f"Query failed (attempt {attempt}/{policy.max_attempts}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                time.sleep(delay)

    def execute_raw_sql(
        self,
        command: str | Path,
        variables: dict | None = None,
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> ResultProtocol:
        """Run a command against the raw underlying
        execution engine."""

        if isinstance(command, Path):
            with safe_open(command) as f:
                command = f.read()
        if variables:
            # Supplied bindings stand in for hydration: the markers need not
            # name concepts at all, so do not try to resolve them.
            return self._execute_with_retry(escape_literal_colons(command), variables)
        return self._execute_with_retry(
            *self.prepare_sql(command, local_concepts=local_concepts)
        )

    def prepare_sql(
        self, command: str, local_concepts: Mapping[str, Concept] | None = None
    ) -> tuple[str, dict | None]:
        """Escape raw SQL and hydrate whatever bind markers it carries.

        Shared with writers that bypass ``execute_raw_sql`` to run a statement
        through an engine API instead (see ``SupportsNativePersist``): a
        rendered select can carry bind markers, and they have to be hydrated the
        same way whichever path ends up running it."""
        from sqlalchemy import text

        command = escape_literal_colons(command)
        params = text(command).compile().params
        if not params:
            return command, None
        return command, {
            x: self._hydrate_param(x, local_concepts=local_concepts) for x in params
        }

    def execute_write_statements(
        self,
        statements: Sequence[str | Path],
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> ResultProtocol:
        """Run statements that change database state, in order, then commit.

        One driver call per statement: sqlite3 rejects a multi-statement execute,
        so a persist's DDL and INSERT cannot be sent together. The last result is
        copied out before committing, which discards an unconsumed cursor on some
        drivers (duckdb among them)."""
        buffered = BufferedResult([], [])
        for statement in statements:
            result = self.execute_raw_sql(statement, local_concepts=local_concepts)
            if result.returns_rows:
                buffered = BufferedResult(list(result.keys()), list(result.fetchall()))
            else:
                buffered = BufferedResult([], [])
        self._flush_transaction()
        return buffered

    def execute_write_sql(
        self,
        command: str | Path,
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> ResultProtocol:
        return self.execute_write_statements([command], local_concepts=local_concepts)

    def execute_text(
        self, command: str, non_interactive: bool = False
    ) -> list[ResultProtocol]:
        if not self.connected:
            self.connect()

        """Run a trilogy query expressed as text."""
        output: list[ResultProtocol] = []
        # connection = self.engine.connect()
        for statement in self.parse_text_generator(command):
            if isinstance(statement, ProcessedShowStatement):
                results = handle_show_statement_outputs(
                    statement,
                    [
                        self.generator.compile_statement(x)
                        for x in statement.output_values
                        if isinstance(x, (ProcessedQuery, ProcessedQueryPersist))
                    ],
                    self.environment,
                    self.generator,
                )
                output.extend(results)
                continue
            elif isinstance(statement, ProcessedValidateStatement):
                validate_result = handle_processed_validate_statement(
                    statement, self.generator, self.validate_environment
                )
                if validate_result:
                    output.append(validate_result)
                continue
            if non_interactive and not isinstance(
                statement,
                (
                    ProcessedCopyStatement,
                    ProcessedCallStatement,
                    ProcessedQueryPersist,
                    ProcessedValidateStatement,
                    ProcessedRawSQLStatement,
                    ProcessedPublishStatement,
                ),
            ):
                continue
            result = self.execute_statement(statement)
            if result:
                output.append(result)
        return output

    def execute_file(
        self, file: str | Path, non_interactive: bool = False
    ) -> list[ResultProtocol]:
        file = Path(file)
        candidates = [file, self.environment.working_path / file]
        err = None
        for file in candidates:
            if not file.exists():
                continue
            with safe_open(file) as f:
                command = f.read()
            if file.suffix == ".sql":
                return [self.execute_write_sql(command)]
            else:
                return self.execute_text(command, non_interactive=non_interactive)
        if err:
            raise err
        raise FileNotFoundError(f"File {file} not found")

    def validate_environment(
        self,
        scope: ValidationScope = ValidationScope.ALL,
        targets: list[str] | None = None,
        generate_only: bool = False,
    ) -> list[ValidationTest]:
        from trilogy.core.validation.environment import validate_environment

        return validate_environment(
            self.environment, scope, targets, exec=None if generate_only else self
        )
