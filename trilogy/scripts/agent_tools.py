"""Tool layer for the Trilogy agent CLI.

Holds the agent's mutable state, the LLM tool definitions the agent calls,
the handler implementations that back each tool, and shared helpers used by
both the handlers and the reviewer pass (``truncate_middle``). The
conversation loop and CLI entrypoint live in ``agent.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from trilogy.ai.models import LLMToolDefinition
from trilogy.scripts.display_core import _pretty, print_info
from trilogy.scripts.file_helpers import preql_description

MARKER_TEMPLATE = "\n...[truncated {n} bytes]...\n"

# Tighter cap applies ONLY to broad `trilogy explore` calls — those without
# any `--regex` filter, or those asking for `--show all`. Both can dump
# 30-40KB of every concept in every imported namespace, which the agent then
# has to skim rather than commit to a draft. With a regex (any regex), the
# call is already deliberately narrowed; let it use the general budget so
# targeted lookups don't get truncated mid-output. See ``_explore_output_cap``.
_TRILOGY_EXPLORE_BROAD_CAP = 8192


def truncate_middle(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    dropped = len(text) - limit
    marker = MARKER_TEMPLATE.format(n=dropped)
    budget = max(limit - len(marker), 0)
    head = budget // 2
    tail = budget - head
    return f"{text[:head]}{marker}{text[-tail:]}" if tail else f"{text[:head]}{marker}"


def truncate_json_events(text: str, limit: int) -> str:
    """Cap a stream of concatenated JSON events at ``limit`` bytes WITHOUT
    slicing through an object — middle-truncating a JSON document hands the
    agent invalid JSON it can't parse. Keep whole leading events until the
    budget is hit (always at least the first, even if it alone exceeds the
    cap), drop the rest, and append a synthetic ``output_truncated`` event so
    the agent knows to narrow the call. Non-JSON text (e.g. ``--format rich``)
    falls back to the byte-level middle truncation."""
    if len(text) <= limit:
        return text
    decoder = json.JSONDecoder()
    raws: list[str] = []
    idx, n = 0, len(text)
    while idx < n:
        while idx < n and text[idx].isspace():
            idx += 1
        if idx >= n:
            break
        try:
            _, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            return truncate_middle(text, limit)
        raws.append(text[idx:end])
        idx = end
    kept: list[str] = []
    used = 0
    for raw in raws:
        if kept and used + len(raw) > limit:
            break
        kept.append(raw)
        used += len(raw) + 1
    dropped = len(raws) - len(kept)
    if dropped:
        note = _pretty(
            {
                "event": "output_truncated",
                "dropped_events": dropped,
                "note": (
                    "Output exceeded the tool cap; trailing events dropped. "
                    "Narrow the call (--regex, --show, fewer rows) to see the rest."
                ),
            }
        )
        kept.append(note)
    return "\n".join(kept)


@dataclass
class TodoItem:
    id: str
    description: str
    completed: bool = False


@dataclass
class AgentState:
    todos: list[TodoItem] = field(default_factory=list)
    tool_output_limit: int = 8192
    done: bool = False
    farewell: str = ""
    recent_signatures: list[str] = field(default_factory=list)
    # Number of times the reviewer pass kicked a submit back. Capped to avoid
    # infinite loops with an agent that won't accept it isn't done.
    submit_kickbacks: int = 0
    # Set by return_control_to_user(force=true): the agent asserts it is done and
    # the reviewer pass is skipped. Lets the agent override a mistaken kickback
    # (it has context the reviewer lacks, e.g. a cosmetic display-truncation note).
    force_return: bool = False
    # When False, the `trilogy` tool refuses `database` (list/describe) calls —
    # raw-table introspection is for ingest, not query generation against an
    # already-built model. Mirrors AgentConfig.allow_database_introspection.
    allow_db_introspection: bool = True


SHOW_MESSAGE_TOOL = LLMToolDefinition(
    name="show_message",
    description="Print a message to the user. Use sparingly for progress updates.",
    input_schema={
        "type": "object",
        "properties": {"message": {"type": "string"}},
        "required": ["message"],
    },
)

TRILOGY_TOOL = LLMToolDefinition(
    name="trilogy",
    description=(
        "Invoke the trilogy CLI as a subprocess. Returns captured stdout, stderr, "
        "and exit code. stdout is a stream of pretty-printed JSON event objects "
        "(newline-separated, parse successively), e.g. "
        '{"event":"result","columns":[...],"rows":[...],"row_count":N} for query '
        'output, {"event":"concepts","namespaces":{...}} for explore, '
        '{"event":"error","message":...} for failures. Read the events; there is '
        "no decorative formatting. Oversized output is capped on event "
        "boundaries (a trailing output_truncated event flags it)."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "args": {
                "type": "array",
                "items": {"type": "string"},
                "description": "CLI arguments, e.g. ['run', 'query.preql'].",
            },
            "stdin": {
                "type": ["string", "null"],
                "description": (
                    "Text piped to the subprocess stdin. REQUIRED when "
                    "writing a file — put the file's full contents here, "
                    "e.g. args ['file','write','query01.preql'] with the "
                    "query text as stdin."
                ),
            },
        },
        "required": ["args"],
    },
)

LIST_FILES_TOOL = LLMToolDefinition(
    name="list_files",
    description=(
        "List files in the workspace (default: current directory, recursive). "
        "Use this when unsure what files exist — for example, before assuming "
        "a path like './store_sales.preql' (the model files live under raw/). "
        "Returns a JSON `files` event with an `entries` array; each .preql "
        "entry carries a `description` from its leading `#` comment block."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Directory to list (default: '.').",
            },
            "recursive": {
                "type": "boolean",
                "description": "Recurse into subdirectories (default: true).",
            },
        },
    },
)

TODO_TOOL = LLMToolDefinition(
    name="todo",
    description=(
        "Manage a scratch TODO list. action 'add' takes `description` — a "
        "single task string or a list of strings to add several at once — and "
        "returns the new id(s); 'complete' and 'remove' take `id` — a single "
        "id or a list of ids — of existing item(s); 'list' returns all items."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "complete", "remove", "list"],
            },
            "id": {
                "type": ["string", "array", "null"],
                "items": {"type": "string"},
                "description": (
                    "An item id, or a list of ids, from prior 'add' calls. "
                    "Required for 'complete' and 'remove'."
                ),
            },
            "description": {
                "type": ["string", "array", "null"],
                "items": {"type": "string"},
                "description": (
                    "Task text — a single string, or a list of strings to "
                    "add several at once. Required for 'add'."
                ),
            },
        },
        "required": ["action"],
    },
)

RETURN_CONTROL_TOOL = LLMToolDefinition(
    name="return_control_to_user",
    description=(
        "Hand control back to the user when a task is finished, with an"
        " optional message. Any open TODOs are auto-discarded. Note: a"
        " reviewer pass runs on submit; if you weren't actually done, you"
        " will be kicked back to keep working. If you ARE done and a kickback"
        " is mistaken (e.g. it cites a cosmetic display/row-truncation note,"
        " or you have context the reviewer lacks), call this again with"
        " force=true and one line explaining why — that bypasses the reviewer."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "message": {"type": "string"},
            "force": {
                "type": "boolean",
                "description": (
                    "Skip the reviewer and finish now. Use only to override a"
                    " kickback you believe is wrong; state why in `message`."
                ),
            },
        },
        "required": ["message"],
    },
)

ALL_TOOLS: list[LLMToolDefinition] = [
    SHOW_MESSAGE_TOOL,
    TRILOGY_TOOL,
    LIST_FILES_TOOL,
    TODO_TOOL,
    RETURN_CONTROL_TOOL,
]


def _render_todos(todos: list[TodoItem]) -> str:
    if not todos:
        return "TODO list is empty."
    lines = [
        f"- [{('x' if t.completed else ' ')}] {t.id}: {t.description}" for t in todos
    ]
    return "Current TODOs:\n" + "\n".join(lines)


def handle_show_message(state: AgentState, args: dict) -> str:
    message = args.get("message")
    if not isinstance(message, str) or not message:
        return "show_message error: 'message' must be a non-empty string."
    print_info(message)
    return "show_message: ok"


# Workspace noise the agent never needs to see in a file listing.
_LIST_FILES_SKIP_DIRS = {"__pycache__", ".git", ".venv", "node_modules"}
_LIST_FILES_SKIP_PREFIXES = ("_worker_",)
_LIST_FILES_SKIP_SUFFIXES = (".duckdb", ".pyc")
_LIST_FILES_MAX_ENTRIES = 500

# Description-rendering constants live in the shared helper so ``trilogy
# file list`` and the agent's ``list_files`` use identical truncation. The
# `_`-prefixed re-exports below preserve the names existing agent tests
# already reference.
_LIST_FILES_DESC_LIMIT = preql_description.LIST_FILES_DESC_LIMIT
_LIST_FILES_DESC_PREFIX = preql_description.LIST_FILES_DESC_PREFIX

# Description-rendering constants live in the shared helper so ``trilogy
# file list`` and the agent's ``list_files`` use identical truncation. The
# `_`-prefixed re-exports below preserve the names existing agent tests
# already reference.
_LIST_FILES_DESC_LIMIT = preql_description.LIST_FILES_DESC_LIMIT
_LIST_FILES_DESC_PREFIX = preql_description.LIST_FILES_DESC_PREFIX


def _should_skip_entry(name: str) -> bool:
    if name in _LIST_FILES_SKIP_DIRS:
        return True
    if any(name.startswith(p) for p in _LIST_FILES_SKIP_PREFIXES):
        return True
    if any(name.endswith(s) for s in _LIST_FILES_SKIP_SUFFIXES):
        return True
    return False


def _file_entry(display_path: str, abs_path: Path) -> dict:
    """One structured listing entry. For ``.preql`` files the leading comment
    block is surfaced as a truncated ``description`` (same text the rich
    listing shows beneath the path), so the agent can route to the right model
    without opening each file."""
    entry: dict[str, object] = {"path": display_path}
    if display_path.endswith(".preql"):
        desc = preql_description.read_preql_description(abs_path)
        if desc:
            entry["description"] = preql_description.truncate_description(desc)
    return entry


def handle_list_files(state: AgentState, args: dict) -> str:
    path = args.get("path", ".")
    recursive = args.get("recursive", True)
    if not isinstance(path, str) or not path:
        return "list_files error: 'path' must be a non-empty string."
    if not isinstance(recursive, bool):
        return "list_files error: 'recursive' must be a boolean."
    root = Path(path)
    if not root.exists():
        return f"list_files error: no such path: {path}"
    if not root.is_dir():
        return f"list_files error: not a directory: {path}"
    entries: list[dict] = []
    truncated = False
    if recursive:
        for current, dirs, files in os.walk(root):
            dirs[:] = sorted(d for d in dirs if not _should_skip_entry(d))
            rel = Path(current).relative_to(root)
            for name in sorted(files):
                if _should_skip_entry(name):
                    continue
                rel_path = (rel / name).as_posix() if str(rel) != "." else name
                entries.append(_file_entry(rel_path, Path(current) / name))
                if len(entries) >= _LIST_FILES_MAX_ENTRIES:
                    truncated = True
                    break
            if truncated:
                break
    else:
        # Non-recursive: surface subdirectories (trailing `/`) so the agent
        # knows where to descend; recursive listings show the full path instead.
        for child in sorted(root.iterdir()):
            if _should_skip_entry(child.name):
                continue
            if child.is_dir():
                entries.append({"path": child.name + "/"})
            else:
                entries.append(_file_entry(child.name, child))
    payload: dict[str, object] = {
        "event": "files",
        "path": path,
        "count": len(entries),
    }
    if truncated:
        payload["truncated"] = True
    payload["entries"] = entries
    return _pretty(payload)


def _first_non_flag_arg(raw_args: list[str]) -> str | None:
    """Return the first positional (non-flag) argument from a CLI arg list, or
    ``None``. Used to detect the subcommand past any group-level flags."""
    skip_next = False
    for arg in raw_args:
        if skip_next:
            skip_next = False
            continue
        if arg in ("--debug-file",):  # group-level flags that take a value
            skip_next = True
            continue
        if arg.startswith("-"):
            continue
        return arg
    return None


def _explore_output_cap(
    subcommand: str | None, raw_args: list[str], general_limit: int
) -> int:
    """Return the truncation limit for a `trilogy explore` call. Broad calls
    (no `--regex` filter, or `--show all`) get the tighter cap; narrow regex
    calls get the general budget so a 12KB targeted result isn't sliced
    mid-output. Non-explore subcommands always get the general budget."""
    if subcommand != "explore":
        return general_limit
    has_regex = "--regex" in raw_args
    asks_for_all = False
    for i, a in enumerate(raw_args):
        if a == "--show" and i + 1 < len(raw_args) and raw_args[i + 1] == "all":
            asks_for_all = True
            break
    is_broad = not has_regex or asks_for_all
    if is_broad:
        return min(general_limit, _TRILOGY_EXPLORE_BROAD_CAP)
    return general_limit


def _trilogy_file_write_hint(raw_args: list[str]) -> str | None:
    """Detect the common ``trilogy file write`` misuse pattern where the agent
    split a single ``--content`` value across many positional args.

    Returns a guidance string when the misuse is detected, ``None`` otherwise.
    The command itself is real and works — we just intercept the most common
    mistake (treating ``--content`` as a shell-tokenised string) before
    subprocess swallows the args."""
    if len(raw_args) < 2 or raw_args[0] != "file" or raw_args[1] != "write":
        return None
    # Find a `--content`/`-c` flag and count what comes after it that is not
    # another known option flag for `file write`.
    flag_indices = [i for i, a in enumerate(raw_args) if a in ("--content", "-c")]
    if not flag_indices:
        return None
    known_flags = {
        "--content",
        "-c",
        "--from-file",
        "--from-url",
        "--escapes",
        "-e",
        "--no-create",
        "--quiet",
        "-q",
    }
    idx = flag_indices[-1]
    trailing = [a for a in raw_args[idx + 1 :] if a not in known_flags]
    if len(trailing) <= 1:
        return None
    return (
        "trilogy file write: `--content` takes a SINGLE string argument. Your "
        f"args list put {len(trailing)} separate tokens after --content "
        "(treating it like a shell command). In a tool call, pass the entire "
        "file body as one string element after --content, with newlines "
        "embedded literally — e.g.\n"
        '  {"args": ["file", "write", "query70.preql", "--content", '
        '"import raw.store_sales as store_sales;\\n\\nselect ..."]}\n'
        "Alternatively use `--escapes` with a single-line `\\n`-escaped string."
    )


def handle_trilogy(state: AgentState, args: dict) -> str:
    raw_args = args.get("args")
    if not isinstance(raw_args, list) or not all(isinstance(a, str) for a in raw_args):
        return "trilogy error: 'args' must be a list of strings."
    stdin_value = args.get("stdin")
    if stdin_value is not None and not isinstance(stdin_value, str):
        return "trilogy error: 'stdin' must be a string or null."
    hint = _trilogy_file_write_hint(raw_args)
    if hint is not None:
        return hint
    if not state.allow_db_introspection and _first_non_flag_arg(raw_args) == "database":
        return (
            "trilogy database introspection is disabled for this task. The "
            "semantic model is already built under raw/ — use "
            "`explore <file.preql>` to see queryable concepts (it chains in "
            "imported dimensions too). Do not list raw database tables."
        )
    cmd = [sys.executable, "-m", "trilogy.scripts.trilogy", *raw_args]
    # Agents consume the CLI as structured NDJSON (one JSON event per line) —
    # same information as the rich view, none of the formatting chrome that
    # wastes tokens. A flag in `raw_args` (e.g. `--format rich`) still wins,
    # since group-level flags are parsed after the env default is applied.
    child_env = {
        **os.environ,
        "PYTHONIOENCODING": "utf-8",
        "TRILOGY_OUTPUT_FORMAT": "json",
    }
    try:
        completed = subprocess.run(
            cmd,
            input=stdin_value,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=child_env,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        return "trilogy error: subprocess timed out after 600s."
    # `agent-info` is the language reference + CLI docs and must arrive whole —
    # middle-truncating it eats the syntax rules the agent needs to write queries.
    # `explore` gets a tighter cap ONLY when the call is broad (no --regex, or
    # --show all) — those can dump 30-40KB of every concept in every imported
    # namespace. Narrow regex calls already targeted the question; they use
    # the full general budget so a 12KB result doesn't get sliced mid-output.
    subcommand = _first_non_flag_arg(raw_args)
    if subcommand == "agent-info":
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        out_limit = _explore_output_cap(subcommand, raw_args, state.tool_output_limit)
        # stdout is structured JSON events — truncate on event boundaries so the
        # agent never receives a sliced, unparseable object. stderr is free text.
        stdout = truncate_json_events(completed.stdout or "", out_limit)
        stderr = truncate_middle(completed.stderr or "", state.tool_output_limit)
    return (
        f"exit_code: {completed.returncode}\n"
        f"--- stdout ---\n{stdout}\n"
        f"--- stderr ---\n{stderr}"
    )


def handle_todo(state: AgentState, args: dict) -> str:
    action = args.get("action")
    if action == "list":
        return _render_todos(state.todos)
    if action == "add":
        raw_desc = args.get("description")
        if isinstance(raw_desc, str):
            descriptions = [raw_desc.strip()] if raw_desc.strip() else []
        elif isinstance(raw_desc, list):
            descriptions = [
                d.strip() for d in raw_desc if isinstance(d, str) and d.strip()
            ]
        else:
            descriptions = []
        if not descriptions:
            return (
                "todo error: 'description' is required for add — a non-empty "
                "string, or a list of strings to add several at once."
            )
        added = []
        for desc in descriptions:
            item = TodoItem(id=uuid.uuid4().hex[:8], description=desc)
            state.todos.append(item)
            added.append(item.id)
        return f"todo added: {', '.join(added)}\n{_render_todos(state.todos)}"
    if action in ("complete", "remove"):
        raw_id = args.get("id")
        if isinstance(raw_id, str):
            ids = [raw_id] if raw_id else []
        elif isinstance(raw_id, list):
            ids = [i for i in raw_id if isinstance(i, str) and i]
        else:
            ids = []
        if not ids:
            open_ids = ", ".join(t.id for t in state.todos) or "(list is empty)"
            return (
                f"todo error: 'id' is required for {action} — pass one id or "
                f"a list of ids. Existing ids: {open_ids}."
            )
        by_id = {t.id: t for t in state.todos}
        done: list[str] = []
        missing: list[str] = []
        for tid in ids:
            todo = by_id.get(tid)
            if todo is None:
                missing.append(tid)
            elif action == "complete":
                todo.completed = True
                done.append(tid)
            else:
                state.todos.remove(todo)
                done.append(tid)
        if not done:
            return f"todo error: no item with id: {', '.join(missing)}."
        summary = f"todo {action}d: {', '.join(done)}"
        if missing:
            summary += f" (no item with id: {', '.join(missing)})"
        return f"{summary}\n{_render_todos(state.todos)}"
    return f"todo error: unknown action '{action}'."


def handle_return_control(state: AgentState, args: dict) -> str:
    message = args.get("message")
    if not isinstance(message, str):
        return "return_control_to_user error: 'message' must be a string."
    # Open TODOs are auto-discarded on exit — they were a scratch aid, not a
    # contract. Earlier runs showed agents burning iterations on
    # complete/remove just to satisfy a gate; not worth the tokens.
    state.todos = []
    state.done = True
    state.farewell = message
    state.force_return = bool(args.get("force"))
    return "return_control_to_user: ok"


TOOL_HANDLERS: dict[str, Callable[[AgentState, dict], str]] = {
    SHOW_MESSAGE_TOOL.name: handle_show_message,
    TRILOGY_TOOL.name: handle_trilogy,
    LIST_FILES_TOOL.name: handle_list_files,
    TODO_TOOL.name: handle_todo,
    RETURN_CONTROL_TOOL.name: handle_return_control,
}
