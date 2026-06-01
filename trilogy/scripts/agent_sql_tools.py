"""Reduced no-Trilogy toolset for the eval SQL baselines.

Backs the ``trilogy agent --toolset sql`` mode: the agent answers a business
question by writing and running plain DuckDB SQL — no Trilogy language, no
``trilogy`` CLI. The toolset is deliberately small: ``write_file``,
``read_file``, ``list_files``, ``run_file``, ``run_query``. SQL executes against
the same workspace DuckDB the scorer uses, so a passing ``run_query`` is a real
signal. Tool results use the same ``exit_code/stdout/stderr`` envelope as the
``trilogy`` tool so ``monitor.py`` and the JSONL metrics parser are unchanged.
"""

from __future__ import annotations

from pathlib import Path

from trilogy.ai.models import LLMToolDefinition
from trilogy.scripts.agent_tools import (
    LIST_FILES_TOOL,
    AgentState,
    handle_list_files,
    truncate_middle,
)

# Leading keywords a read-only exploration/answer statement may start with. The
# workspace DB is a disposable per-worker copy, so this is a guard against an
# agent corrupting its own session mid-run, not a security boundary. The scored
# answer file must be a single self-contained SELECT anyway (the scorer runs
# only its last statement, in a fresh engine).
_READONLY_LEADING = frozenset(
    {
        "select",
        "with",
        "from",
        "describe",
        "show",
        "pragma",
        "explain",
        "summarize",
        "values",
        "table",
    }
)

_MAX_RESULT_ROWS = 50

# One executor per process (each agent runs in its own subprocess), built lazily
# from the workspace trilogy.toml engine config.
_ENGINE = None


def _get_engine():
    global _ENGINE
    if _ENGINE is None:
        from trilogy.scripts.common import create_executor, get_runtime_config

        cfg = get_runtime_config(Path.cwd())
        _ENGINE = create_executor(
            param=(),
            directory=Path.cwd(),
            conn_args=(),
            edialect=cfg.engine_dialect,
            debug=False,
            config=cfg,
        )
    return _ENGINE


def _last_statement(sql: str) -> str:
    parts = [s.strip() for s in sql.split(";")]
    nonempty = [s for s in parts if s]
    return nonempty[-1] if nonempty else ""


def _readonly_violation(sql: str) -> str | None:
    leading = sql.lstrip().split(None, 1)
    if not leading:
        return "empty SQL."
    kw = leading[0].lower()
    if kw not in _READONLY_LEADING:
        return (
            f"statement starts with '{leading[0]}'. This baseline is read-only — "
            "use SELECT/WITH/DESCRIBE/SHOW/PRAGMA/SUMMARIZE only. The answer must "
            "be a single self-contained SELECT (no temp tables or setup statements)."
        )
    return None


def _format_result(keys: list[str], rows: list) -> str:
    total = len(rows)
    shown = rows[:_MAX_RESULT_ROWS]
    lines = []
    if keys:
        lines.append(" | ".join(str(k) for k in keys))
    for row in shown:
        lines.append(" | ".join("NULL" if v is None else str(v) for v in row))
    body = "\n".join(lines) if lines else "(no columns)"
    footer = f"\n[{total} row(s)"
    if total > _MAX_RESULT_ROWS:
        footer += f"; first {_MAX_RESULT_ROWS} shown"
    footer += "]"
    return body + footer


def _envelope(exit_code: int, stdout: str, stderr: str) -> str:
    return (
        f"exit_code: {exit_code}\n"
        f"--- stdout ---\n{stdout}\n"
        f"--- stderr ---\n{stderr}"
    )


def _execute_sql(state: AgentState, sql: str) -> str:
    statement = _last_statement(sql)
    violation = _readonly_violation(statement)
    if violation is not None:
        return _envelope(1, "", violation)
    try:
        result = _get_engine().execute_raw_sql(statement)
        rows = result.fetchall()
        try:
            keys = list(result.keys())
        except Exception:
            keys = []
    except Exception as exc:
        return _envelope(1, "", f"{type(exc).__name__}: {exc}")
    out = truncate_middle(_format_result(keys, rows), state.tool_output_limit)
    return _envelope(0, out, "")


WRITE_FILE_TOOL = LLMToolDefinition(
    name="write_file",
    description=(
        "Write (or overwrite) a text file in the workspace. Use this to save "
        "your SQL answer as query<NN>.sql."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "File path to write."},
            "content": {"type": "string", "description": "Full file contents."},
        },
        "required": ["path", "content"],
    },
)

READ_FILE_TOOL = LLMToolDefinition(
    name="read_file",
    description="Read a text file's contents (e.g. schema.md, a query file).",
    input_schema={
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
    },
)

RUN_FILE_TOOL = LLMToolDefinition(
    name="run_file",
    description=(
        "Execute the SQL in a file against the database and return the result "
        "rows (read-only). Use this to validate query<NN>.sql before finishing."
    ),
    input_schema={
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
    },
)

RUN_QUERY_TOOL = LLMToolDefinition(
    name="run_query",
    description=(
        "Execute an ad-hoc SQL statement against the database and return the "
        "result rows (read-only: SELECT/WITH/DESCRIBE/SHOW/PRAGMA/SUMMARIZE). "
        "Use 'SHOW TABLES' and 'DESCRIBE <table>' to explore the schema, and "
        "SELECTs to test ideas before saving your answer."
    ),
    input_schema={
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
    },
)

RETURN_CONTROL_TOOL = LLMToolDefinition(
    name="return_control_to_user",
    description="Indicate you are done with the task, with an optional message.",
    input_schema={
        "type": "object",
        "properties": {"message": {"type": "string"}},
        "required": ["message"],
    },
)


def handle_write_file(state: AgentState, args: dict) -> str:
    path = args.get("path")
    content = args.get("content")
    if not isinstance(path, str) or not path:
        return "write_file error: 'path' must be a non-empty string."
    if not isinstance(content, str):
        return "write_file error: 'content' must be a string."
    target = Path(path)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return f"write_file: wrote {len(content)} char(s) to {path}"


def handle_read_file(state: AgentState, args: dict) -> str:
    path = args.get("path")
    if not isinstance(path, str) or not path:
        return "read_file error: 'path' must be a non-empty string."
    target = Path(path)
    if not target.exists():
        return f"read_file error: no such file: {path}"
    if not target.is_file():
        return f"read_file error: not a file: {path}"
    text = target.read_text(encoding="utf-8", errors="replace")
    return truncate_middle(text, state.tool_output_limit)


def handle_run_file(state: AgentState, args: dict) -> str:
    path = args.get("path")
    if not isinstance(path, str) or not path:
        return "run_file error: 'path' must be a non-empty string."
    target = Path(path)
    if not target.exists():
        return f"run_file error: no such file: {path}"
    return _execute_sql(state, target.read_text(encoding="utf-8"))


def handle_run_query(state: AgentState, args: dict) -> str:
    sql = args.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return "run_query error: 'sql' must be a non-empty string."
    return _execute_sql(state, sql)


def handle_return_control(state: AgentState, args: dict) -> str:
    message = args.get("message")
    if not isinstance(message, str):
        return "return_control_to_user error: 'message' must be a string."
    state.todos = []
    state.done = True
    state.farewell = message
    return "return_control_to_user: ok"


SQL_TOOLS: list[LLMToolDefinition] = [
    WRITE_FILE_TOOL,
    READ_FILE_TOOL,
    LIST_FILES_TOOL,
    RUN_FILE_TOOL,
    RUN_QUERY_TOOL,
    RETURN_CONTROL_TOOL,
]

SQL_TOOL_HANDLERS = {
    WRITE_FILE_TOOL.name: handle_write_file,
    READ_FILE_TOOL.name: handle_read_file,
    LIST_FILES_TOOL.name: handle_list_files,
    RUN_FILE_TOOL.name: handle_run_file,
    RUN_QUERY_TOOL.name: handle_run_query,
    RETURN_CONTROL_TOOL.name: handle_return_control,
}


def sql_system_prompt(has_schema_md: bool) -> str:
    schema_line = (
        "A `schema.md` file in the working directory lists every table and "
        "column — read it first with read_file('schema.md').\n"
        if has_schema_md
        else "Discover the schema yourself: run_query('SHOW TABLES') then "
        "run_query('DESCRIBE <table>').\n"
    )
    return (
        "You answer the business question in the task by writing plain DuckDB "
        "SQL against the configured database. You are NOT using Trilogy — write "
        "standard SQL.\n\n"
        "Available tools:\n"
        "- write_file(path, content): save your SQL answer.\n"
        "- read_file(path): read a file (e.g. schema.md).\n"
        "- list_files(path='.', recursive=True): list workspace files.\n"
        "- run_query(sql): run an ad-hoc read-only SQL statement; returns rows.\n"
        "- run_file(path): run the SQL in a file; returns rows.\n"
        "- return_control_to_user(message): finish.\n\n"
        f"{schema_line}\n"
        "Workflow:\n"
        "1. Understand the schema (schema.md or SHOW TABLES / DESCRIBE).\n"
        "2. Draft SQL and test it with run_query until the result looks right.\n"
        "3. Save the final answer with write_file to `query<NN>.sql` (the NN in "
        "the task), validate it with run_file, then return_control_to_user.\n\n"
        "The answer file MUST be a single self-contained SELECT statement — no "
        "temp tables, no setup statements, no trailing semicolon games (only the "
        "last statement is scored, run in a fresh connection). Bias toward "
        "action; do not re-issue an identical failing query without changing it."
    )
