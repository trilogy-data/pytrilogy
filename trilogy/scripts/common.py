"""Common helper functions used across all CLI commands."""

import traceback
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path as PathlibPath
from typing import TYPE_CHECKING, Any

from click.exceptions import Exit

from trilogy import Executor
from trilogy.constants import DEFAULT_NAMESPACE, logger
from trilogy.core.enums import ValidationScope
from trilogy.core.exceptions import ConfigurationException, ModelValidationError
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedQueryPersist,
    ProcessedValidateStatement,
)
from trilogy.dialect.config import DialectConfig
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig, apply_env_vars, load_config_file
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.display import (
    print_error,
    print_info,
    print_success,
)
from trilogy.scripts.environment import extra_to_kwargs, parse_env_params

# Re-exported: lives in a stdlib-only module so lightweight commands can find a
# project config without importing this one. Callers here are unchanged.
from trilogy.scripts.project_config import (  # noqa: F401
    MODEL_ROOT_DIR,
    TRILOGY_CONFIG_NAME,
    find_trilogy_config,
)
from trilogy.utility import safe_open

if TYPE_CHECKING:
    from trilogy.core.models.datasource import Datasource
    from trilogy.execution.state import DatasourceWatermark, RefreshPolicy

# Default stat types to display in output; easily configurable
DEFAULT_STAT_TYPES: list[str] = ["persist", "update", "validate"]


@dataclass
class RefreshQuery:
    """A refresh query that was executed, with the target datasource and compiled SQL."""

    datasource_id: str
    sql: str


@dataclass
class ExecutionStats:
    """Statistics about statements executed in a script."""

    persist_count: int = 0
    update_count: int = 0
    validate_count: int = 0
    agent_question_count: int = 0
    agent_passed: int = 0
    agent_skipped: int = 0
    refresh_queries: list[RefreshQuery] = field(default_factory=list)
    #: Counts describe what a refresh *would* do, not what happened.
    dry_run: bool = False

    def __add__(self, other: "ExecutionStats") -> "ExecutionStats":
        return ExecutionStats(
            persist_count=self.persist_count + other.persist_count,
            update_count=self.update_count + other.update_count,
            validate_count=self.validate_count + other.validate_count,
            agent_question_count=self.agent_question_count + other.agent_question_count,
            agent_passed=self.agent_passed + other.agent_passed,
            agent_skipped=self.agent_skipped + other.agent_skipped,
            refresh_queries=self.refresh_queries + other.refresh_queries,
            dry_run=self.dry_run or other.dry_run,
        )


def _format_stat(count: int, noun: str, verb: str, dry_run: bool) -> str:
    label = noun if count == 1 else f"{noun}s"
    action = f"would be {verb}" if dry_run else verb
    return f"{count} {label} {action}"


def format_stats(stats: ExecutionStats, stat_types: list[str] | None = None) -> str:
    """Format execution stats for display."""
    if stat_types is None:
        stat_types = DEFAULT_STAT_TYPES

    parts = []
    if "persist" in stat_types and stats.persist_count > 0:
        parts.append(
            _format_stat(stats.persist_count, "table", "persisted", stats.dry_run)
        )
    if "update" in stat_types and stats.update_count > 0:
        parts.append(
            _format_stat(stats.update_count, "datasource", "updated", stats.dry_run)
        )
    if "validate" in stat_types and stats.validate_count > 0:
        parts.append(
            _format_stat(stats.validate_count, "datasource", "validated", stats.dry_run)
        )
    if "validate" in stat_types and stats.agent_question_count > 0:
        label = "question" if stats.agent_question_count == 1 else "questions"
        parts.append(
            f"{stats.agent_passed}/{stats.agent_question_count} agent {label} passed"
        )
    if "validate" in stat_types and stats.agent_skipped > 0:
        label = "question" if stats.agent_skipped == 1 else "questions"
        parts.append(f"{stats.agent_skipped} agent {label} skipped")

    return "; ".join(parts)


@dataclass
class RefreshParams:
    """Parameters specific to the refresh command."""

    print_watermarks: bool = False
    force_sources: frozenset[str] = frozenset()
    interactive: bool = False
    dry_run: bool = False
    #: ``--partition`` concept address -> value: the slice this run owns.
    #: Empty means "let staleness decide", which is the normal refresh.
    partitions: Mapping[str, str] = field(default_factory=dict)

    def policy(self) -> "RefreshPolicy":
        """The planning half of these params — THE CLI-to-plan mapping.

        A new planning option is added to `RefreshPolicy` and mapped here, once;
        every call site that plans a refresh then carries it unedited. The rest
        of this dataclass is presentation and does not cross into the plan.
        """
        from trilogy.execution.state import RefreshPolicy

        return RefreshPolicy(
            force_sources=frozenset(self.force_sources),
            partition_selector=dict(self.partitions),
        )


def parse_force_sources(force_sources: Iterable[str]) -> frozenset[str]:
    """Normalize --force values, including comma-separated input."""
    return frozenset(
        item.strip()
        for value in force_sources
        for item in value.split(",")
        if item.strip()
    )


def validate_force_sources(
    force_sources: set[str] | frozenset[str] | None,
    available_sources: Iterable[str],
) -> None:
    """Fail fast when --force includes unknown datasource names."""
    if not force_sources:
        return

    missing = sorted(set(force_sources) - set(available_sources))
    if not missing:
        return

    noun = "datasource" if len(missing) == 1 else "datasources"
    print_error(f"Unknown {noun} passed to --force: {', '.join(missing)}")
    raise Exit(1)


def validate_partition_selector(
    selector: Mapping[str, str],
    available_keys: Iterable[str],
) -> None:
    """Fail fast when --partition names a concept nothing is partitioned by.

    A selector matching no datasource does not narrow anything — the plan keeps
    whatever staleness decided and the written snapshot claims the whole table.
    Both are the widening the flag exists to prevent, and both are silent.
    """
    if not selector:
        return

    missing = sorted(set(selector) - set(available_keys))
    if not missing:
        return

    noun = "concept" if len(missing) == 1 else "concepts"
    print_error(
        f"No datasource is partitioned by {noun} passed to --partition:"
        f" {', '.join(missing)}"
    )
    raise Exit(1)


def validate_refresh_policy(policy: "RefreshPolicy", environment: Environment) -> None:
    """Fail fast on --force/--partition values this model cannot honor."""
    from trilogy.execution.state import partition_key_addresses

    validate_force_sources(policy.force_sources, environment.datasources)
    validate_partition_selector(
        policy.partition_selector,
        partition_key_addresses(environment.datasources.values()),
    )


def _observed_nothing(watermark: "DatasourceWatermark | None") -> bool:
    """No watermark value at all — the table is missing or holds no rows."""
    if watermark is None:
        return True
    return not any(key.value is not None for key in watermark.keys.values())


def require_a_source_of_truth(
    datasources: Iterable["Datasource"],
    watermarks: Mapping[str, "DatasourceWatermark"],
) -> None:
    """Fail a refresh that found nothing to do over an asset that is empty and
    has no source of truth to fill it from.

    With no ``root datasource`` there is no expected side, so everything reads
    fresh — including a target that has never been built. "All assets are up to
    date" is then false in a way that looks exactly like success.

    Narrow on purpose; every condition must hold. Callers only reach here with
    an **empty plan**, so ``--force`` never does. Beyond that: no root is
    declared anywhere; some asset declares an incremental/freshness key (which
    is what marks it something refresh maintains, as opposed to an inline-query
    source, which is not a table anyone builds); and that asset observed no
    watermark value, i.e. it is missing or empty. A populated table that merely
    cannot be judged is uninformative, not a lie, and stays a plain no-op.

    ``trilogy state`` never calls this — observing what exists is useful with or
    without roots, and it already reports ``level: scan``.
    """
    sources = list(datasources)
    if any(ds.is_root for ds in sources):
        return

    unbuilt = sorted(
        ds.identifier
        for ds in sources
        if (ds.incremental_by or ds.freshness_by)
        and _observed_nothing(watermarks.get(ds.identifier))
    )
    if not unbuilt:
        return

    print_error(
        f"Nothing can be refreshed, but {', '.join(unbuilt)} "
        f"{'is' if len(unbuilt) == 1 else 'are'} empty: this project declares no "
        "`root datasource`, so there is no source of truth to compare against "
        "and every asset reports fresh whether or not it holds anything.\n"
        "Mark the authoritative sources (the tables you do not build) as "
        "`root datasource`, or pass --force to rebuild a target explicitly."
    )
    raise Exit(1)


@dataclass
class CLIRuntimeParams:
    """Parameters provided via CLI for execution."""

    input: str
    dialect: Dialects | None = None
    parallelism: int | None = None
    param: tuple[str, ...] = ()
    conn_args: tuple[str, ...] = ()
    debug: bool = False
    debug_file: str | None = None
    config_path: PathlibPath | None = None
    execution_strategy: str = "eager_bfs"
    env: tuple[str, ...] = ()
    refresh_params: RefreshParams | None = None
    # Cap on rows displayed per statement result. ``None`` falls back to the
    # global ``FETCH_LIMIT``. Lowered for agents (sample, not firehose); humans
    # can raise it explicitly with ``--displayed-rows`` or use ``--all-rows``.
    row_limit: int | None = None
    # Render the derived-value scope block after each result table (`run
    # --scope`). JSON mode always carries the scopes regardless of this flag.
    show_scopes: bool = False
    # Seconds a single statement may run before the driver is asked to cancel
    # it (`run --timeout`). ``None`` leaves statements unbounded.
    timeout: float | None = None


def merge_runtime_config(
    cli_params: CLIRuntimeParams, file_config: RuntimeConfig
) -> tuple[Dialects, int]:
    """
    Merge CLI parameters with config file settings.
    CLI parameters take precedence over config file.

    Returns:
        tuple of (dialect, parallelism)

    Raises:
        Exit: If no dialect is specified in either CLI or config
    """
    # Resolve dialect: CLI argument takes precedence over config
    if cli_params.dialect:
        dialect = cli_params.dialect
    elif file_config.engine_dialect:
        dialect = file_config.engine_dialect
    else:
        print_error(
            "No dialect specified. Provide dialect as argument or set engine.dialect in config file."
        )
        raise Exit(1)

    # Resolve parallelism: CLI argument takes precedence over config
    parallelism = (
        cli_params.parallelism
        if cli_params.parallelism is not None
        else file_config.parallelism
    )

    return dialect, parallelism


def resolve_input(path: PathlibPath) -> list[PathlibPath]:
    # Directory
    if path.is_dir():
        pattern = "**/*.preql"
        return sorted(path.glob(pattern))
    # Single file
    if path.exists() and path.is_file():
        return [path]

    raise FileNotFoundError(f"Input path '{path}' does not exist.")


def get_runtime_config(
    path: PathlibPath,
    config_override: PathlibPath | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> RuntimeConfig:
    config_path: PathlibPath | None = None

    if config_override:
        config_path = config_override
    else:
        config_path = find_trilogy_config(path)

    if not config_path:
        # extra_env (CLI --env) must reach os.environ even without a config
        # file; with one, load_config_file applies it.
        if extra_env:
            apply_env_vars(dict(extra_env))
        return RuntimeConfig(startup_trilogy=[], startup_sql=[])

    try:
        return load_config_file(config_path, extra_env=extra_env)
    except Exception as e:
        print_error(f"Failed to load configuration file {config_path}: {e}")
        handle_execution_exception(e)
        # This won't be reached due to handle_execution_exception raising Exit
        return RuntimeConfig(startup_trilogy=[], startup_sql=[])


def _looks_like_path(input: str) -> bool:
    """Check if input looks like a file/directory path rather than inline query.

    Inline SQL legitimately contains ``/`` (division), so whitespace or a
    statement terminator means we treat it as inline regardless of separators.
    """
    if any(c.isspace() for c in input) or ";" in input:
        return False
    # Contains path separators
    if "/" in input or "\\" in input:
        return True
    # Has a file extension commonly used
    return bool(input.endswith((".preql", ".sql", ".toml")))


def resolve_input_information(
    input: str,
    config_path_input: PathlibPath | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> tuple[Iterable[PathlibPath | StringIO], PathlibPath, str, str, RuntimeConfig]:
    input_as_path = PathlibPath(input)
    files: Iterable[StringIO | PathlibPath]
    if input_as_path.exists():
        pathlib_path = input_as_path
        files = resolve_input(pathlib_path)

        if pathlib_path.is_dir():
            directory = pathlib_path
            input_type = "directory"
        else:
            directory = pathlib_path.parent
            input_type = "file"
        config = get_runtime_config(pathlib_path, config_path_input, extra_env)

        input_name = pathlib_path.name
    else:
        # If input looks like a path but doesn't exist, raise error
        if _looks_like_path(input):
            raise FileNotFoundError(f"Input path '{input}' does not exist.")
        script = input
        files = [StringIO(script)]
        directory = PathlibPath.cwd()
        input_type = "query"
        input_name = "inline"
        config = get_runtime_config(directory, config_path_input, extra_env)
    return files, directory, input_type, input_name, config


def validate_required_connection_params(
    conn_dict: dict[str, Any],
    required_keys: list[str],
    optional_keys: list[str],
    dialect_name: str,
) -> dict:
    missing = [key for key in required_keys if key not in conn_dict]
    extra = [
        key
        for key in conn_dict
        if key not in required_keys and key not in optional_keys
    ]
    if missing:
        raise ConfigurationException(
            f"Missing required {dialect_name} connection parameters: {', '.join(missing)}"
        )
    if extra:
        valid = ", ".join(required_keys + optional_keys) or "none"
        raise ConfigurationException(
            f"Unknown {dialect_name} connection parameters: {', '.join(extra)}. "
            f"Valid parameters: {valid}"
        )
    return conn_dict


def _file_connection_params(stored: DialectConfig) -> dict[str, Any]:
    """CLI parameter name -> value for what a file [engine.config] can supply.

    Only dialects whose config has required constructor arguments need this:
    their conn_dict must already be complete when validation runs, so the merge
    at the end of get_dialect_config is too late to save them.
    """
    from trilogy.dialect.config import (
        MySQLConfig,
        PostgresConfig,
        PrestoConfig,
        SnowflakeConfig,
        SQLServerConfig,
    )

    if isinstance(stored, MySQLConfig):
        return {
            "host": stored.host,
            "port": stored.port,
            "username": stored.username,
            "password": stored.password,
            "database": stored.database,
            "charset": stored.charset,
        }
    if isinstance(stored, (PostgresConfig, SQLServerConfig)):
        return {
            "host": stored.host,
            "port": stored.port,
            "username": stored.username,
            "password": stored.password,
            "database": stored.database,
        }
    # also covers TrinoConfig
    if isinstance(stored, PrestoConfig):
        return {
            "host": stored.host,
            "port": stored.port,
            "username": stored.username,
            "password": stored.password,
            "catalog": stored.catalog,
            "schema": stored.schema,
        }
    if isinstance(stored, SnowflakeConfig):
        return {
            "account": stored.account,
            "username": stored.username,
            "password": stored.password,
            "database": stored.database,
            "schema": stored.schema,
        }
    return {}


def _seed_compatible(
    stored: DialectConfig | None, expected: type[DialectConfig]
) -> bool:
    """Whether a file config can supply connection params for ``expected``.

    The same class (or a subclass) always can. Presto and Trino additionally
    seed each other: ``TrinoConfig`` subclasses ``PrestoConfig`` with an
    identical constructor, so the *parameters* are interchangeable even though
    the classes are not. Only the parameters cross over — the dialect being
    built still constructs its own class, because the engine factory
    type-checks and a PrestoConfig is not a TrinoConfig.
    """
    from trilogy.dialect.config import PrestoConfig

    if stored is None:
        return False
    if isinstance(stored, expected):
        return True
    return isinstance(stored, PrestoConfig) and issubclass(expected, PrestoConfig)


def seed_conn_dict(
    conn_dict: dict[str, Any],
    stored: DialectConfig | None,
    expected: type[DialectConfig],
) -> dict[str, Any]:
    """Fill missing connection params from a compatible file config.

    CLI-supplied params win over the ones already on the config.
    """
    if stored is None or not _seed_compatible(stored, expected):
        return conn_dict
    seeded = {k: v for k, v in _file_connection_params(stored).items() if v is not None}
    seeded.update(conn_dict)
    return seeded


def get_dialect_config(
    edialect: Dialects, conn_dict: dict[str, Any], runtime_config: RuntimeConfig
) -> Any:
    """Get dialect configuration based on dialect type."""
    conf: Any | None = None

    if edialect == Dialects.DUCK_DB:
        from trilogy.dialect.config import DuckDBConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            [],
            [
                "path",
                "enable_python_datasources",
                "enable_gcs",
                "enable_spatial",
                "gcs_cache_bust",
            ],
            "DuckDB",
        )
        conf = DuckDBConfig(**conn_dict)
    elif edialect == Dialects.SQLITE:
        from trilogy.dialect.config import SQLiteConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            [],
            ["path"],
            "SQLite",
        )
        conf = SQLiteConfig(**conn_dict)
    elif edialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.config import SnowflakeConfig

        conn_dict = seed_conn_dict(
            conn_dict, runtime_config.engine_config, SnowflakeConfig
        )
        conn_dict = validate_required_connection_params(
            conn_dict,
            ["username", "password", "account"],
            ["database", "schema"],
            "Snowflake",
        )
        conf = SnowflakeConfig(**conn_dict)
    elif edialect == Dialects.SQL_SERVER:
        from trilogy.dialect.config import SQLServerConfig

        conn_dict = seed_conn_dict(
            conn_dict, runtime_config.engine_config, SQLServerConfig
        )
        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "database"],
            [],
            "SQL Server",
        )
        conf = SQLServerConfig(**conn_dict)
    elif edialect == Dialects.POSTGRES:
        from trilogy.dialect.config import PostgresConfig

        conn_dict = seed_conn_dict(
            conn_dict, runtime_config.engine_config, PostgresConfig
        )
        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "database"],
            [],
            "Postgres",
        )
        conf = PostgresConfig(**conn_dict)
    elif edialect == Dialects.MYSQL:
        from trilogy.dialect.config import MySQLConfig

        conn_dict = seed_conn_dict(conn_dict, runtime_config.engine_config, MySQLConfig)
        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "username", "password", "database"],
            ["port", "charset"],
            "MySQL",
        )
        conf = MySQLConfig(**conn_dict)
    elif edialect == Dialects.BIGQUERY:
        from trilogy.dialect.config import BigQueryConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            [],
            [
                "project",
                "staging_dataset",
                "staging_uri",
                "enable_python_datasources",
                "use_sqlalchemy",
            ],
            "BigQuery",
        )
        conf = BigQueryConfig(**conn_dict)
    elif edialect == Dialects.PRESTO:
        from trilogy.dialect.config import PrestoConfig

        conn_dict = seed_conn_dict(
            conn_dict, runtime_config.engine_config, PrestoConfig
        )
        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "catalog"],
            ["schema"],
            "Presto",
        )
        conf = PrestoConfig(**conn_dict)
    elif edialect == Dialects.TRINO:
        from trilogy.dialect.config import TrinoConfig

        conn_dict = seed_conn_dict(conn_dict, runtime_config.engine_config, TrinoConfig)
        # Still conditional on having *something*: with neither CLI args nor a
        # usable file config, Trino falls through to the default engine rather
        # than erroring, which is the behaviour it has always had.
        if conn_dict:
            conn_dict = validate_required_connection_params(
                conn_dict,
                ["host", "port", "username", "password", "catalog"],
                ["schema"],
                "Trino",
            )
            conf = TrinoConfig(**conn_dict)
    elif edialect == Dialects.CLICKHOUSE and conn_dict:
        from trilogy.dialect.config import ClickhouseConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            [],
            [
                "host",
                "port",
                "username",
                "password",
                "database",
                "secure",
                "mode",
                "chdb_path",
            ],
            "ClickHouse",
        )
        conf = ClickhouseConfig(**conn_dict)
    elif conn_dict:
        raise ConfigurationException(
            f"Dialect {edialect.value} does not accept connection parameters "
            f"via the CLI; got: {', '.join(conn_dict)}"
        )
    # Only merge the file config when it is exactly the class the dialect just
    # built. The executing dialect can differ from the toml's (`trilogy unit`
    # always runs on DuckDB), and `merge_config` returns *self*, so merging a
    # related-but-different class would hand the engine factory a config of the
    # wrong type — a Presto/Trino pair passes isinstance in one direction and
    # fails it in the other. Those siblings cross over through `seed_conn_dict`
    # instead, which keeps the constructed class the dialect's own.
    stored = runtime_config.engine_config
    if conf is not None and stored is not None and type(stored) is type(conf):
        conf = stored.merge_config(conf)
    return conf


def create_executor(
    param: tuple[str, ...],
    directory: PathlibPath,
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
    config: RuntimeConfig,
    debug_file: str | None = None,
    query_timeout: float | None = None,
) -> Executor:
    # Parse environment parameters from dedicated flag
    namespace = DEFAULT_NAMESPACE
    try:
        env_params = parse_env_params(param)
        from trilogy.scripts.display import show_environment_params

        show_environment_params(env_params)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    # Parse connection arguments from remaining args
    try:
        conn_dict = extra_to_kwargs(conn_args)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    # Configure dialect
    try:
        conf = get_dialect_config(edialect, conn_dict, runtime_config=config)
    except Exception as e:
        handle_execution_exception(e)

    # Create environment and set additional parameters if any exist
    environment = Environment(
        working_path=str(directory),
        namespace=namespace,
        import_paths=list(config.import_paths),
    )
    if env_params:
        environment.set_parameters(**env_params)

    from trilogy.execution.envs import datasource_transform_from_active

    exec = Executor(
        dialect=edialect,
        engine=edialect.default_engine(conf=conf),
        environment=environment,
        hooks=(
            [DebuggingHook(output_file=PathlibPath(debug_file))] if debug_file else []
        ),
        config=conf,
        staging=config.staging,
        chart_theme=config.report_theme,
        datasource_transform=datasource_transform_from_active(PathlibPath(directory)),
        query_timeout=query_timeout,
    )
    if config.startup_sql:
        for script in config.startup_sql:
            print_info(f"Executing startup SQL script: {script.name}...")
            exec.execute_file(script)
            print_success(f"Completed startup SQL script: {script.name}")
    if config.startup_trilogy:
        for script in config.startup_trilogy:
            print_info(f"Executing startup Trilogy script: {script.name}...")
            exec.execute_file(script)
            print_success(f"Completed startup Trilogy script: {script.name}")
    return exec


def create_executor_for_script(
    node: ScriptNode,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
    config: RuntimeConfig,
    debug_file: str | None = None,
    query_timeout: float | None = None,
) -> Executor:
    """
    Create an executor for a specific script node.

    Each script gets its own executor with its own environment,
    using the script's parent directory as the working path.
    """
    directory = node.path.parent
    return create_executor(
        param, directory, conn_args, edialect, debug, config, debug_file, query_timeout
    )


_PROGRESS_LABEL_CALLBACK: "ContextVar[Callable[[str], None] | None]" = ContextVar(
    "_PROGRESS_LABEL_CALLBACK", default=None
)


def set_progress_label_callback(cb: "Callable[[str], None] | None") -> Any:
    """Register a thread-local callback that receives the current validation
    target label. Used by parallel mode to surface per-script progress in the
    shared progress tracker. Returns the previous value as a Token for reset.
    """
    return _PROGRESS_LABEL_CALLBACK.set(cb)


def reset_progress_label_callback(token: Any) -> None:
    _PROGRESS_LABEL_CALLBACK.reset(token)


def _emit_progress_label(label: str) -> None:
    cb = _PROGRESS_LABEL_CALLBACK.get()
    if cb is not None:
        try:
            cb(label)
        except Exception as e:
            logger.debug("Progress label callback failed: %s", e)


def validate_environment(
    executor: Executor,
    mock: bool = False,
    quiet: bool = False,
    scope: ValidationScope = ValidationScope.ALL,
    scale_factor: int | None = None,
) -> None:
    """Validate the executor's environment (datasources + concepts) with consistent error handling.

    Args:
        exec: The executor instance
        mock: If True, mock datasources before validation (for unit tests)
        quiet: If True, suppress informational messages (for parallel execution)
        scope: What to validate; narrowed by the unit/integration test-type flags
        scale_factor: Rows for the shallowest mocked entity, if not the default

    Raises:
        Exit: If validation fails
    """
    from trilogy.core.validation.environment import (
        validate_environment as core_validate_environment,
    )
    from trilogy.scripts.display import (
        ValidationFailure,
        ValidationProgressContext,
        show_validation_failures,
        show_validation_success,
        show_validation_targets,
    )

    datasources = list(executor.environment.datasources.keys())
    if not datasources:
        if not quiet:
            message = "unit" if mock else "integration"
            print_success(f"No datasources found to {message} test.")
        return

    if mock:
        config = f" with (scale_factor={scale_factor})" if scale_factor else ""
        executor.execute_text(
            "mock datasources {}{};".format(", ".join(datasources), config)
        )

    failures: list[ValidationFailure] = []

    if quiet:
        # Parallel path: collect per-target failures via callback so the
        # ParallelProgressTracker can surface what's currently being validated.

        def on_target_complete_quiet(kind: str, name: str, results: list[Any]) -> None:
            _emit_progress_label(name)
            for r in results:
                if r.result is not None:
                    failures.append(
                        ValidationFailure(
                            kind=kind, target=name, message=r.result.message
                        )
                    )

        try:
            core_validate_environment(
                executor.environment,
                scope,
                None,
                exec=executor,
                on_target_complete=on_target_complete_quiet,
            )
        except ModelValidationError as e:
            # Surface synthesis errors that aren't tied to a single target.
            if not failures:
                failures.append(
                    ValidationFailure(kind="environment", target="-", message=e.message)
                )

        if failures:
            raise ModelValidationError(_format_failure_summary(failures))
        return

    # Rich/interactive path: discovery → progress → grouped failures.
    concept_count = len(executor.environment.concepts)
    show_validation_targets(datasources, concept_count, mock=mock)

    # Only advance the bar for datasources (slow SQL); update the label for
    # concepts so users still see live progress without an inflated total.
    progress_ctx = ValidationProgressContext(len(datasources))
    start_time = datetime_now_seconds()
    with progress_ctx:

        def on_target_complete(kind: str, name: str, results: list[Any]) -> None:
            progress_ctx.set_label(f"{kind} {name}")
            if kind == "datasource":
                progress_ctx.advance()
            for r in results:
                if r.result is not None:
                    failures.append(
                        ValidationFailure(
                            kind=kind, target=name, message=r.result.message
                        )
                    )

        try:
            core_validate_environment(
                executor.environment,
                scope,
                None,
                exec=executor,
                on_target_complete=on_target_complete,
            )
        except ModelValidationError as e:
            if not failures:
                failures.append(
                    ValidationFailure(kind="environment", target="-", message=e.message)
                )

    if failures:
        show_validation_failures(failures)
        raise Exit(1) from ModelValidationError(_format_failure_summary(failures))

    duration = datetime_now_seconds() - start_time
    show_validation_success(
        mock=mock,
        datasource_count=len(datasources),
        duration_seconds=duration,
    )


def _format_failure_summary(failures: "list[Any]") -> str:
    return "\n".join(f"[{f.kind}] {f.target}: {f.message}" for f in failures)


def datetime_now_seconds() -> float:
    """Wall-clock seconds; isolated so tests can monkeypatch easily."""
    from datetime import datetime

    return datetime.now().timestamp()


def handle_execution_exception(
    e: Exception, debug: bool = False, source: str | None = None
) -> None:
    if isinstance(e, Exit):
        raise e
    from trilogy.core.exceptions import (
        DisconnectedConceptsException,
        FunctionArgumentException,
        InvalidSyntaxException,
        NothingExecutedException,
        QueryTimeoutException,
        UndefinedConceptException,
        UnresolvableQueryException,
    )
    from trilogy.parsing.v2.model import HydrationError

    location = f" in {source}" if source else ""
    # Syntax/validation errors carry actionable, user-facing guidance; label them
    # as such instead of "Unexpected error:" so the reader (or agent) treats them
    # as a fixable mistake rather than an internal crash.
    if isinstance(e, UndefinedConceptException):
        # An undefined concept is an authoring mistake; use `.message` to avoid
        # the `(self, message)` tuple repr that `str(e)` produces.
        print_error(f"Syntax error{location}: {e.message}")
    elif isinstance(e, HydrationError):
        # An authored constraint caught during hydration; the diagnostic carries
        # the source position, so surface it rather than dropping it.
        meta = e.diagnostic.meta
        span = (
            f" (line {meta.line}, column {meta.column})"
            if meta is not None and meta.line is not None
            else ""
        )
        print_error(f"Syntax error{location}: {e.diagnostic.message}{span}")
    elif isinstance(e, NothingExecutedException):
        # The script parsed; it just does nothing. Labelling it a syntax error
        # would send the reader looking for a parse mistake that isn't there.
        print_error(f"{e}")
    elif isinstance(e, (SyntaxError, InvalidSyntaxException)):
        print_error(f"Syntax error{location}: {e}")
    elif isinstance(e, (DisconnectedConceptsException, UnresolvableQueryException)):
        # A disconnected/unresolvable query is a fixable modeling mistake (a
        # missing join/merge), not an internal crash.
        print_error(f"Resolution error{location}: {e}")
    elif isinstance(e, FunctionArgumentException):
        # A function called on the wrong argument type is a fixable author
        # mistake (e.g. `year()` on an integer key), not an internal crash.
        print_error(f"Type error{location}: {e}")
    elif isinstance(e, QueryTimeoutException):
        # The caller asked for this abort; reporting it as an unexpected error
        # would send the reader hunting for a fault that isn't there.
        print_error(f"Timeout{location}: {e}")
    elif isinstance(e, ConfigurationException):
        # A bad connection/config parameter is a fixable invocation mistake.
        print_error(f"Configuration error{location}: {e}")
    elif isinstance(e, RecursionError):
        # A planner RecursionError is ALWAYS a framework bug (an unguarded cycle
        # in resolution), not a user mistake — and not reliably worked around by
        # reformulating. Say so plainly rather than emitting the opaque
        # "Unexpected error: maximum recursion depth exceeded", which reads as a
        # crash to retry verbatim.
        print_error(
            f"Resolution error{location}: query could not be planned; this is a bug."
        )
    elif isinstance(e, ImportError):
        # A bad import target is a fixable authoring mistake — the raised message
        # already carries a "Did you mean ...?" hint. Label it so the reader (or
        # agent) fixes the path rather than re-issuing the same import as if it
        # were an internal crash.
        print_error(f"Import error{location}: {e}")
    else:
        # A message-less exception (a bare `assert`, a bare raise) would print
        # nothing after the colon, handing the reader zero signal to act on.
        # The class name is the minimum floor.
        detail = str(e) or f"{type(e).__name__} (no message); this is a bug."
        print_error(f"Unexpected error{location}: {detail}")
    if debug:
        print_error(f"Full traceback:\n{traceback.format_exc()}")
    raise Exit(1) from e


def flush_debugging_hooks(exec: Executor) -> None:
    """Flush any debugging hooks attached to the executor."""
    for hook in exec.hooks or []:
        if isinstance(hook, DebuggingHook):
            hook.write()
            print_info(f"Debug log written to: {hook.output_file}")


def count_statement_stats(
    statements: Sequence[PROCESSED_STATEMENT_TYPES],
    existing_stats: ExecutionStats | None = None,
) -> ExecutionStats:
    """Count persist and validate statements in a list of processed statements."""
    persist_count = sum(1 for s in statements if isinstance(s, ProcessedQueryPersist))
    validate_count = sum(
        1 for s in statements if isinstance(s, ProcessedValidateStatement)
    )
    if existing_stats:
        existing_stats.persist_count += persist_count
        existing_stats.validate_count += validate_count
        return existing_stats
    return ExecutionStats(persist_count=persist_count, validate_count=validate_count)


def execute_script_with_stats(
    exec: Executor, script_path: PathlibPath, run_statements: bool = True
) -> ExecutionStats:
    """Parse and optionally execute a script, returning execution stats."""
    from datetime import datetime

    from trilogy.execution.report import emit_statement_end

    with safe_open(script_path) as f:
        queries = exec.parse_text(f.read())
    stats = ExecutionStats()
    if not run_statements:
        return stats
    total = len(queries)
    for idx, query in enumerate(queries):
        start = datetime.now()
        error: Exception | None = None
        try:
            exec.execute_query(query)
        except Exception as e:
            error = e
            raise
        finally:
            emit_statement_end(
                idx,
                total,
                type(query).__name__,
                (datetime.now() - start).total_seconds(),
                error is None,
                error=error,
                file=str(script_path),
            )
        stats = count_statement_stats([query], stats)
    return stats
