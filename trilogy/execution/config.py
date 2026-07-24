import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tomllib import loads

from trilogy.ai.enums import Provider
from trilogy.constants import REMOTE_PREFIXES, logger
from trilogy.core.exceptions import ConfigurationException
from trilogy.dialect import (
    BigQueryConfig,
    DialectConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLiteConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import Dialects
from trilogy.staging import StagingConfig
from trilogy.utility import safe_open

DEFAULT_PARALLELISM = 4
DB_LOCATION_KEY = "db_location"
DEFAULT_STUDIO_URL = "https://trilogydata.dev/trilogy-studio-core/"


def resolve_db_location(raw: str, toml_parent: Path) -> str:
    if raw.startswith(REMOTE_PREFIXES):
        return raw
    return str((toml_parent / raw).resolve())


def load_env_file(env_file_path: Path) -> dict[str, str] | None:
    """Load environment variables from a .env file."""
    env_vars: dict[str, str] = {}
    if not env_file_path.exists():
        logger.info(f"Environment file not found: {env_file_path}")
        return None
    with safe_open(env_file_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Remove surrounding quotes if present
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            env_vars[key] = value
    return env_vars


def apply_env_vars(env_vars: dict[str, str]) -> None:
    """Apply environment variables to os.environ."""
    for key, value in env_vars.items():
        os.environ[key] = value


# First alternative is the escape form: ``$${env:X}`` yields a literal
# ``${env:X}``.
_ENV_REF = re.compile(
    r"\$\$\{env:[A-Za-z_][A-Za-z0-9_]*\}|\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}"
)

# Subtrees advertised verbatim to clients (serve /index.json): references
# there are published unresolved rather than interpolated.
_INTERPOLATION_SKIP = frozenset({"serve.connection"})


def _interpolate_string(
    value: str, env: Mapping[str, str], path: str, missing: list[str]
) -> str:
    parts: list[str] = []
    last = 0
    for match in _ENV_REF.finditer(value):
        parts.append(value[last : match.start()])
        var = match.group(1)
        if var is None:
            parts.append(match.group(0)[1:])
        elif var in env:
            parts.append(env[var])
        else:
            missing.append(f"{var} (in {path})")
            parts.append(match.group(0))
        last = match.end()
    parts.append(value[last:])
    return "".join(parts)


def _interpolate_node(
    node: Any,
    path: str,
    env: Mapping[str, str],
    skip: frozenset[str],
    missing: list[str],
) -> Any:
    if isinstance(node, dict):
        out: dict = {}
        for key, value in node.items():
            child = f"{path}.{key}" if path else key
            out[key] = (
                value
                if child in skip
                else _interpolate_node(value, child, env, skip, missing)
            )
        return out
    if isinstance(node, list):
        return [
            _interpolate_node(item, f"{path}[{i}]", env, skip, missing)
            for i, item in enumerate(node)
        ]
    if isinstance(node, str):
        return _interpolate_string(node, env, path, missing)
    return node


def interpolate_env_refs(
    config_data: dict,
    env: Mapping[str, str] | None = None,
    skip: frozenset[str] = frozenset(),
) -> dict:
    """Resolve ``${env:VAR}`` references in string values of a parsed config.

    Single pass: resolved values are never re-expanded. Dotted paths in
    ``skip`` pass through verbatim. Raises ConfigurationException naming every
    unresolved variable; never echoes resolved values.
    """
    env_map: Mapping[str, str] = os.environ if env is None else env
    missing: list[str] = []
    resolved = _interpolate_node(config_data, "", env_map, skip, missing)
    if missing:
        raise ConfigurationException(
            "trilogy.toml references undefined environment variable(s): "
            + ", ".join(missing)
        )
    return resolved


@dataclass
class AgentConfig:
    provider: Provider | None = None
    model: str | None = None
    api_key_env: str | None = None
    max_iterations: int = 50
    tool_output_limit: int = 8192
    # Drop ``show_message`` from the tool list (and its discipline rule from
    # the system prompt). Narration messages compound quadratically through
    # history replays, so quiet mode is much cheaper for long unattended runs.
    quiet: bool = False
    # Drop ``todo`` from the tool list (and its discipline rule + tool-list
    # mention from the system prompt). Eval data shows todo-heavy runs
    # correlate strongly with iteration exhaustion (avg 6.1 todos on
    # exhausted vs 3.4 on pass); the .preql file itself is sufficient
    # scratch space for the single-query tasks we evaluate.
    disable_todo: bool = False
    # When False (default) the agent loop sends ``tool_choice: auto`` — the model
    # may reason in plain text before acting. Forcing ``tool_choice: required``
    # (set True) makes the model act every turn with no preamble, which on hard
    # tasks degenerates into no-commit explore loops (eval: full-suite exhausted
    # 11->2 and total tokens -51% when this was flipped to auto). Thinking-mode
    # models also reject forced tool_choice, so auto is the more portable default.
    force_tool_choice: bool = False
    # When False, the agent's `trilogy` tool refuses `database list`/`database
    # describe` and the prompt omits them. Raw-table introspection is for
    # building a model (ingest); during query generation the model already
    # exists under raw/, and listing tables just adds a confusing, unneeded
    # surface. The per-query eval turns this off; the ingest eval leaves it on.
    allow_database_introspection: bool = True
    # When False, the ``trilogy`` tool refuses ``file read`` (a gentle deny that
    # points the agent at ``explore``) and the prompt drops the read bullet.
    # ``file list`` still works. Query-generation evals turn this off — the model
    # is meant to discover schema via ``explore``, not by reading raw files.
    allow_file_read: bool = True
    # When True, skip the post-submit reviewer gate (the one-shot LLM check that
    # the agent didn't self-signal "still working" before return_control_to_user).
    # Off by default; an A/B knob to measure whether the gate helps or just adds
    # false kickbacks.
    disable_reviewer: bool = False


@dataclass
class ServeConnectionConfig:
    """Connection advertised to clients on /index.json. Non-secret fields only."""

    type: str
    options: dict[str, str] = field(default_factory=dict)


@dataclass
class RuntimeConfig:

    startup_trilogy: list[Path]
    startup_sql: list[Path]
    parallelism: int = DEFAULT_PARALLELISM
    engine_dialect: Dialects | None = None
    engine_config: DialectConfig | None = None
    source_path: Path | None = None
    env_files: list[Path] = field(default_factory=list)
    staging: StagingConfig = field(default_factory=StagingConfig)
    serve_studio_url: str = DEFAULT_STUDIO_URL
    serve_connection: ServeConnectionConfig | None = None
    project_name: str | None = None
    agent: AgentConfig = field(default_factory=AgentConfig)
    # default visual theme for report rendering and chart copy output
    report_theme: str | None = None


# Schema of known fields. `[engine.config]` is intentionally omitted (validated
# per-dialect by the config dataclasses) and `[serve.connection.options]` is
# free-form dialect kwargs. None as the value means: known section, sub-keys
# are not audited.
_KNOWN_TOP_LEVEL: set[str] = {
    "parallelism",
    "env_file",
    "engine",
    "setup",
    "staging",
    "serve",
    "project",
    "agent",
    "report",
}
_KNOWN_SECTIONS: dict[str, set[str] | None] = {
    "engine": {"dialect", "config", "env_file", "parallelism"},
    "engine.config": None,
    "setup": {"trilogy", "sql"},
    "staging": {"path"},
    "serve": {"studio_url", "connection"},
    "serve.connection": {"type", "options"},
    "serve.connection.options": None,
    "project": {"name"},
    "report": {"theme"},
    "agent": {
        "provider",
        "model",
        "api_key_env",
        "max_iterations",
        "tool_output_limit",
        "quiet",
        "disable_todo",
        "force_tool_choice",
        "allow_database_introspection",
        "allow_file_read",
        "disable_reviewer",
    },
}


def audit_config_file(path: Path) -> list[str]:
    """Return warning messages for unknown fields in a trilogy.toml.

    Detects unexpected top-level keys and unexpected sub-keys in known sections.
    Sections with `None` in `_KNOWN_SECTIONS` (per-dialect config, free-form
    options) are skipped because their schema is enforced elsewhere.
    """
    with safe_open(path) as f:
        config_data = loads(f.read())

    warnings: list[str] = []

    def visit(prefix: str, table: dict) -> None:
        allowed = _KNOWN_SECTIONS.get(prefix) if prefix else _KNOWN_TOP_LEVEL
        if allowed is None:
            return
        for key, value in table.items():
            qualified = f"{prefix}.{key}" if prefix else key
            if key not in allowed:
                location = f"[{prefix}]" if prefix else "top-level"
                warnings.append(
                    f"Unknown trilogy.toml field '{key}' in {location} of {path}"
                )
                continue
            if isinstance(value, dict) and qualified in _KNOWN_SECTIONS:
                visit(qualified, value)

    visit("", config_data)
    return warnings


def load_config_file(
    path: Path, extra_env: Mapping[str, str] | None = None
) -> RuntimeConfig:
    with safe_open(path) as f:
        toml_content = f.read()
    config_data = loads(toml_content)

    # Parse env_file - can be a single string or list of strings.
    # Top-level; falls back to [engine].env_file for backwards compat.
    env_raw = config_data.get(
        "env_file", config_data.get("engine", {}).get("env_file", [])
    )
    if isinstance(env_raw, str):
        env_raw = [env_raw]
    # env_file paths may themselves reference ${env:...}; resolve against the
    # environment as it stands before any file is loaded.
    env_raw = interpolate_env_refs(
        {"env_file": env_raw}, env={**os.environ, **(extra_env or {})}
    )["env_file"]
    env_files = [path.parent / p for p in env_raw]

    # env_file values apply before interpolation so a project can be fully
    # self-contained; extra_env (CLI --env) applies last and wins.
    for env_file in env_files:
        file_vars = load_env_file(env_file)
        if file_vars:
            apply_env_vars(file_vars)
    if extra_env:
        apply_env_vars(dict(extra_env))

    config_data = interpolate_env_refs(config_data, skip=_INTERPOLATION_SKIP)

    staging_raw: dict = config_data.get("staging", {})
    staging = StagingConfig(path=staging_raw.get("path"))

    engine_raw: dict = config_data.get("engine", {})
    engine_config_raw = engine_raw.get("config", {})
    engine = Dialects(engine_raw.get("dialect")) if engine_raw.get("dialect") else None
    engine_config: DialectConfig | None
    if engine:
        if engine == Dialects.DUCK_DB:
            if engine_config_raw:
                if DB_LOCATION_KEY in engine_config_raw:
                    engine_config_raw["path"] = resolve_db_location(
                        engine_config_raw.pop(DB_LOCATION_KEY), path.parent
                    )
                engine_config = DuckDBConfig(**engine_config_raw)
            else:
                engine_config = None
        elif engine == Dialects.SQLITE:
            if engine_config_raw:
                if DB_LOCATION_KEY in engine_config_raw:
                    engine_config_raw["path"] = resolve_db_location(
                        engine_config_raw.pop(DB_LOCATION_KEY), path.parent
                    )
                engine_config_raw["staging_path"] = staging.resolved_root.rstrip("/")
                engine_config = SQLiteConfig(**engine_config_raw)
            else:
                engine_config = None
        elif engine == Dialects.POSTGRES:
            engine_config = (
                PostgresConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.PRESTO:
            engine_config = (
                PrestoConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.SNOWFLAKE:
            engine_config = (
                SnowflakeConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.SQL_SERVER:
            engine_config = (
                SQLServerConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.BIGQUERY:
            engine_config = (
                BigQueryConfig(**engine_config_raw) if engine_config_raw else None
            )
        else:
            engine_config = None
    else:
        engine_config = None
    setup: dict = config_data.get("setup", {})

    serve_raw: dict = config_data.get("serve", {})
    serve_studio_url = serve_raw.get("studio_url", DEFAULT_STUDIO_URL)

    serve_connection_raw: dict = serve_raw.get("connection", {})
    serve_connection: ServeConnectionConfig | None = None
    if serve_connection_raw:
        conn_type = serve_connection_raw.get("type")
        if not conn_type:
            raise ValueError("[serve.connection] requires a 'type' field")
        conn_options_raw = serve_connection_raw.get("options", {})
        serve_connection = ServeConnectionConfig(
            type=conn_type,
            options={str(k): str(v) for k, v in conn_options_raw.items()},
        )

    project_raw: dict = config_data.get("project", {})
    project_name: str | None = project_raw.get("name")

    report_raw: dict = config_data.get("report", {})
    report_theme: str | None = report_raw.get("theme")

    agent_raw: dict = config_data.get("agent", {})
    agent_provider: Provider | None = None
    if raw_provider := agent_raw.get("provider"):
        try:
            agent_provider = Provider(raw_provider.lower())
        except ValueError:
            raise ValueError(
                f"Unknown agent provider '{raw_provider}'. "
                f"Valid options: {', '.join(p.value for p in Provider)}"
            )
    agent = AgentConfig(
        provider=agent_provider,
        model=agent_raw.get("model"),
        api_key_env=agent_raw.get("api_key_env"),
        max_iterations=int(agent_raw.get("max_iterations", 50)),
        tool_output_limit=int(agent_raw.get("tool_output_limit", 8192)),
        quiet=bool(agent_raw.get("quiet", False)),
        disable_todo=bool(agent_raw.get("disable_todo", False)),
        force_tool_choice=bool(agent_raw.get("force_tool_choice", False)),
        allow_database_introspection=bool(
            agent_raw.get("allow_database_introspection", True)
        ),
        allow_file_read=bool(agent_raw.get("allow_file_read", True)),
        disable_reviewer=bool(agent_raw.get("disable_reviewer", False)),
    )

    # Canonical location is [engine].parallelism (matches docs and `trilogy init`
    # template). Top-level is accepted as a fallback for prior installs.
    parallelism = int(
        engine_raw.get(
            "parallelism", config_data.get("parallelism", DEFAULT_PARALLELISM)
        )
    )

    return RuntimeConfig(
        startup_trilogy=[path.parent / p for p in setup.get("trilogy", [])],
        startup_sql=[path.parent / p for p in setup.get("sql", [])],
        parallelism=parallelism,
        engine_dialect=engine,
        engine_config=engine_config,
        source_path=path,
        env_files=env_files,
        staging=staging,
        serve_studio_url=serve_studio_url,
        serve_connection=serve_connection,
        project_name=project_name,
        agent=agent,
        report_theme=report_theme,
    )
