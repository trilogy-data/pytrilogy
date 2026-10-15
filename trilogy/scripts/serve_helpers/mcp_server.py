"""MCP endpoint for `trilogy serve --mcp`: lets an agent explore and query the
served model over streamable HTTP at ``/mcp``."""

from __future__ import annotations

import datetime
import decimal
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from trilogy import Dialects, Environment, Executor
from trilogy.execution.config import RuntimeConfig
from trilogy.scripts.serve_helpers.file_discovery import (
    extract_description_from_file,
    find_preql_files,
    get_relative_model_name,
)

MCP_PATH = "/mcp"
DEFAULT_ROW_LIMIT = 100
MAX_ROW_LIMIT = 1000

INSTRUCTIONS = """Query a Trilogy semantic model. Trilogy is SQL-like but has no FROM, JOIN or GROUP BY: \
import a model, then select its fields and joins/grouping are resolved for you.
Workflow: list_models -> describe_model(model) for its fields -> run_query with \
`import <model>; select ...;`. Read get_syntax once before writing queries."""


def check_mcp_available() -> bool:
    try:
        import mcp.server  # noqa: F401
    except ImportError:
        return False
    return True


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    return str(value)


class ModelSession:
    """The served directory as seen by MCP tools.

    One executor is built on first use and queries are serialized on it, since
    a DuckDB connection is not safe across threads. Each query runs in a copy of
    the post-startup environment, so concepts one query defines do not leak
    into the next.
    """

    def __init__(
        self, directory: Path, dialect: Dialects, config: RuntimeConfig
    ) -> None:
        self.directory = directory
        self.dialect = dialect
        self.config = config
        self._lock = threading.Lock()
        self._executor: Executor | None = None
        self._base: Environment | None = None

    def _ensure_executor(self) -> tuple[Executor, Environment]:
        if self._executor is None or self._base is None:
            from trilogy.scripts.common import create_executor

            self._executor = create_executor(
                param=(),
                directory=self.directory,
                conn_args=(),
                edialect=self.dialect,
                debug=False,
                config=self.config,
            )
            self._base = self._executor.environment.duplicate()
        return self._executor, self._base

    def model_names(self) -> dict[str, Path]:
        return {
            get_relative_model_name(path, self.directory).replace("/", "."): path
            for path in find_preql_files(self.directory)
        }

    def list_models(self) -> dict[str, list[dict[str, str]]]:
        """List the models in this project. Use a model's name in `import <name>;`."""
        return {
            "models": [
                {"name": name, "description": extract_description_from_file(path)}
                for name, path in self.model_names().items()
            ]
        }

    def describe_model(self, model: str) -> str:
        """Describe the fields of one model (from list_models): names, types, keys and descriptions."""
        from trilogy.ai.prompts import concepts_to_fields_prompt

        if model not in self.model_names():
            raise _tool_error(f"Unknown model '{model}'; call list_models.")
        env = Environment(
            working_path=self.directory, import_paths=self.config.import_paths
        )
        try:
            env.parse(f"import {model};")
        except Exception as e:
            raise _tool_error(f"Model '{model}' failed to load: {e}") from e
        return concepts_to_fields_prompt(env)

    def get_syntax(self, example: str | None = None) -> str:
        """Trilogy syntax reference. Pass `example` (a name from the reference's example list) for a full worked example."""
        from trilogy.ai.prompts import get_trilogy_syntax_reference
        from trilogy.ai.syntax_examples import available_names, render_example

        if example is None:
            return get_trilogy_syntax_reference().replace(
                "`trilogy agent-info syntax example <name>`",
                "get_syntax(example=<name>)",
            )
        rendered = render_example(example)
        if rendered is None:
            raise _tool_error(
                f"No example '{example}'; choose from {', '.join(available_names())}."
            )
        return rendered

    def run_query(self, query: str, limit: int = DEFAULT_ROW_LIMIT) -> dict[str, Any]:
        """Run Trilogy statements, e.g. `import process; select name, count(pid) -> n order by n desc limit 10;`. Returns the last result set, capped at `limit` rows (max 1000)."""
        limit = max(1, min(limit, MAX_ROW_LIMIT))
        with self._lock:
            executor, base = self._ensure_executor()
            executor.environment = base.duplicate()
            try:
                results = executor.execute_text(query)
                rows_result = next(
                    (r for r in reversed(results) if r.returns_rows), None
                )
                if rows_result is None:
                    return {"columns": [], "rows": [], "truncated": False}
                columns = list(rows_result.keys())
                rows = rows_result.fetchmany(limit + 1)
            except Exception as e:
                raise _tool_error(f"{type(e).__name__}: {e}") from e
        return {
            "columns": columns,
            "rows": [_jsonable(list(row)) for row in rows[:limit]],
            "truncated": len(rows) > limit,
        }

    def tools(self) -> list[Callable[..., Any]]:
        return [self.list_models, self.describe_model, self.get_syntax, self.run_query]


def _tool_error(message: str) -> Exception:
    # Any other exception type reaches the agent as a bare "Error executing tool".
    from mcp.server.mcpserver.exceptions import ToolError

    return ToolError(message)


class _TokenGate:
    """ASGI wrapper requiring the serve token, as X-Trilogy-Token or a bearer token."""

    def __init__(self, inner: Callable[..., Any], token: str | None) -> None:
        self.inner = inner
        self.token = token

    def _authorized(self, scope: dict[str, Any]) -> bool:
        headers = dict(scope.get("headers", []))
        offered = {
            headers.get(b"x-trilogy-token", b"").decode(),
            headers.get(b"authorization", b"").decode().removeprefix("Bearer "),
        }
        return self.token in offered

    async def __call__(self, scope, receive, send) -> None:
        if self.token is not None and not self._authorized(scope):
            from starlette.responses import JSONResponse

            response = JSONResponse(
                {"detail": "Invalid or missing X-Trilogy-Token header"},
                status_code=401,
            )
            await response(scope, receive, send)
            return
        await self.inner(scope, receive, send)


def mount_mcp(app, session: ModelSession, host: str, token: str | None) -> None:
    """Serve ``session`` as an MCP server at ``/mcp`` on the FastAPI ``app``."""
    from contextlib import asynccontextmanager

    from mcp.server import MCPServer
    from starlette.routing import Route

    server = MCPServer("trilogy", instructions=INSTRUCTIONS)
    for tool in session.tools():
        server.add_tool(tool)
    # Builds the session manager; its own Starlette app is not used, since the
    # route is attached directly and its lifespan is folded into the app's.
    server.streamable_http_app(host=host)
    manager = server.session_manager
    app.router.routes.append(
        Route(MCP_PATH, endpoint=_TokenGate(manager.handle_request, token))
    )
    outer = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app_):
        async with manager.run(), outer(app_) as state:
            yield state

    app.router.lifespan_context = lifespan
