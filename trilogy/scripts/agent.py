"""Agent command for Trilogy CLI - AI-powered orchestration tasks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import click
from click import argument, option, pass_context

from trilogy.ai.conversation import Conversation
from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMRequestOptions, LLMToolCall, LLMToolDefinition
from trilogy.ai.prompts import get_trilogy_prompt
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import LLMProvider
from trilogy.ai.providers.google import GoogleProvider
from trilogy.ai.providers.openai import OpenAIProvider
from trilogy.ai.providers.openrouter import OpenRouterProvider
from trilogy.execution.config import AgentConfig, apply_env_vars
from trilogy.scripts.common import get_runtime_config
from trilogy.scripts.display_core import print_info, print_success, with_status
from trilogy.scripts.environment import parse_env_vars

DEFAULT_PROVIDER = Provider.ANTHROPIC
DEFAULT_MODEL = "claude-opus-4-7"

PROVIDER_DEFAULT_ENV: dict[Provider, str] = {
    Provider.ANTHROPIC: "ANTHROPIC_API_KEY",
    Provider.OPENAI: "OPENAI_API_KEY",
    Provider.GOOGLE: "GOOGLE_API_KEY",
    Provider.OPENROUTER: "OPENROUTER_API_KEY",
}

PROVIDER_CLASSES: dict[Provider, Callable[..., LLMProvider]] = {
    Provider.ANTHROPIC: AnthropicProvider,
    Provider.OPENAI: OpenAIProvider,
    Provider.GOOGLE: GoogleProvider,
    Provider.OPENROUTER: OpenRouterProvider,
}


def get_agent_instructions(include_show: bool = True) -> str:
    base = """You are the Trilogy CLI agent. You operate by calling tools.

Available tools:
- show_message(message): print a message to the user.
- trilogy(args, stdin=None): invoke the `trilogy` CLI as a subprocess. `args` is a
  list of string arguments. Output is captured and returned; large outputs are
  truncated from the middle. Common uses:
    * ["agent-info"] — prints the full CLI docs AND the Trilogy language
      syntax reference. CALL THIS FIRST on every task before writing any
      .preql file. Trilogy is not SQL; the language section names the rules
      you cannot afford to guess. Do not skip it.
    * ["database", "list"] — list the tables in the configured database.
      ["database", "describe", "<table>"] — show a table's columns and types.
    * ["ingest", "--all"] — generate a Trilogy semantic model (.preql files
      under raw/) for every table in the database, in one step.
    * Run a Trilogy script: ["run", "<path.preql>"].
    * ["explore", "<path.preql>"] is the canonical "what concepts can I query
      from this file?" tool. Imported references chain in — exploring the
      fact file (e.g. `sales.preql`) ALSO lists all dimensions it
      imports (`product.*`, `date.*`, `customer.*`, …) in the same output.
      You do NOT need to explore each dimension separately. Prefer this over
      `read_file` on a model file. Use `--grep` (repeatable) to filter.
      Trilogy auto-resolves joins from the model's declared relationships.
      Join discovery is not needed;
    write `select store_sales.date_dim.year, ...;` and Trilogy
      handles the join. There is no manual JOIN clause in this language.
    * Only documented subcommands work — do NOT invent `list`, `raw`, `shell`,
      etc. `trilogy agent-info` lists everything that exists.
- write_file(path, content): create or overwrite a text file; `content` is the
  exact, full file text. This is how you create every .preql query file.
- read_file(path): return the text content of a file; use this for
  code understanding; prefer explore for usage.
- list_files(path=".", recursive=True): list files in the workspace.
  Call this when you are unsure what files exist (e.g. before guessing a
  path like `./store_sales.preql` — the model files live under `raw/`).
  Skips noise (`__pycache__`, `.duckdb`, `_worker_*`).
- todo(action, id=None, description=None): scratch TODO list, for multi-step
  tasks ONLY. Actions: "add" (description: one string, or a list to add
  several), "complete"/"remove" (id: one id or a list), "list". Most tasks
  finish without ever calling this.
- return_control_to_user(message): hand control back to the user. Open TODOs
  are auto-discarded so you never need to clean them up just to exit.

Discipline:
1. Bias toward action and use of the trilogy CLI. Never repeat exploration you have already done.
2. Skip TODOs unless the task has 3+ truly independent steps. A single-query
   task does not. Never use a TODO entry as a substitute for doing the work.
3. Use `trilogy` for all CLI work. Call `return_control_to_user` only when
   the task is completely finished.
4. If a tool call fails or returns the same error you have already seen, do
   NOT immediately re-issue the same call. First emit a short plain-text
   message naming the failure and what you will try differently."""
    if include_show:
        base += """
5. Use `show_message` rarely — only for a genuine status change, never to
   narrate intent or restate the plan."""
    return base


# The Trilogy language reference has one source of truth, get_trilogy_prompt()
# (also used by `trilogy agent-info`); never paraphrase it here.
_TRILOGY_PROMPT_SECTION = get_trilogy_prompt(
    intro=(
        "When you write a Trilogy query file (.preql), follow this syntax "
        "reference exactly — Trilogy is NOT SQL:"
    )
)
SYSTEM_PROMPT = get_agent_instructions(True) + "\n\n" + _TRILOGY_PROMPT_SECTION
QUIET_SYSTEM_PROMPT = get_agent_instructions(False) + "\n\n" + _TRILOGY_PROMPT_SECTION


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


MARKER_TEMPLATE = "\n...[truncated {n} bytes]...\n"


def truncate_middle(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    dropped = len(text) - limit
    marker = MARKER_TEMPLATE.format(n=dropped)
    budget = max(limit - len(marker), 0)
    head = budget // 2
    tail = budget - head
    return f"{text[:head]}{marker}{text[-tail:]}" if tail else f"{text[:head]}{marker}"


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
        "and exit code. Large outputs are truncated from the middle."
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

WRITE_FILE_TOOL = LLMToolDefinition(
    name="write_file",
    description=(
        "Create or overwrite a text file. Writes the exact text in `content` "
        "to `path`. This is how you create every .preql query file."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Path to write, e.g. 'query01.preql'.",
            },
            "content": {
                "type": "string",
                "description": "The complete, exact text content of the file.",
            },
        },
        "required": ["path", "content"],
    },
)

READ_FILE_TOOL = LLMToolDefinition(
    name="read_file",
    description="Return the text content of the file at `path`.",
    input_schema={
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path of the file to read."},
        },
        "required": ["path"],
    },
)

LIST_FILES_TOOL = LLMToolDefinition(
    name="list_files",
    description=(
        "List files in the workspace (default: current directory, recursive). "
        "Use this when unsure what files exist — for example, before assuming "
        "a path like './store_sales.preql' (the model files live under raw/)."
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
        " optional message. Any open TODOs are auto-discarded — no need"
        " to clean them up just to exit."
    ),
    input_schema={
        "type": "object",
        "properties": {"message": {"type": "string"}},
        "required": ["message"],
    },
)

ALL_TOOLS: list[LLMToolDefinition] = [
    SHOW_MESSAGE_TOOL,
    TRILOGY_TOOL,
    WRITE_FILE_TOOL,
    READ_FILE_TOOL,
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


def _raw_path_note(path: str) -> str:
    """Guidance (not a block) when writing into raw/, which holds the generated
    data model — query files belong in the working directory."""
    if path.replace("\\", "/").startswith("raw/"):
        return (
            "\n\n[guidance] You wrote into raw/, which holds the generated data "
            "model. Query files belong in the working directory, not raw/. Only "
            "edit raw/ to deliberately fix a model definition."
        )
    return ""


# HTML entities seen written into .preql files by some models that escape their
# own tool output. We catch the comparison-operator ones explicitly because
# they're the failure mode we've observed; other entities are flagged generically.
_HTML_ENTITY_HINTS: dict[str, str] = {
    "&lt;": "<",
    "&gt;": ">",
    "&amp;": "&",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def _detect_html_escapes(content: str) -> list[str]:
    return [ent for ent in _HTML_ENTITY_HINTS if ent in content]


def _validate_preql_syntax(content: str) -> str | None:
    """Return a parse-error message for invalid Trilogy syntax, else None."""
    from trilogy.core.exceptions import InvalidSyntaxException
    from trilogy.parsing.v2.lark_backend import parse_lark

    try:
        parse_lark(content)
    except InvalidSyntaxException as exc:
        return str(exc)
    except Exception as exc:  # safety net — never let validation crash the write
        return f"{type(exc).__name__}: {exc}"
    return None


def handle_write_file(state: AgentState, args: dict) -> str:
    path = args.get("path")
    content = args.get("content")
    if not isinstance(path, str) or not path:
        return "write_file error: 'path' must be a non-empty string."
    if not isinstance(content, str):
        return "write_file error: 'content' must be a string."
    target = Path(path)
    try:
        clobbers = target.is_file() and target.stat().st_size > 0
    except OSError:
        clobbers = False
    if not content and clobbers:
        return (
            f"write_file refused: 'content' is empty and '{path}' already has "
            "content — that would erase it. Pass the file's full text in "
            "'content'."
        )
    # Pre-write validation for .preql files: HTML-escape detection + syntax
    # parse. Catching these here saves the agent a round-trip through
    # `trilogy run` and gives a more pointed error than the lark parser would.
    if path.endswith(".preql"):
        entities = _detect_html_escapes(content)
        if entities:
            decoded = ", ".join(
                f"`{e}` (use `{_HTML_ENTITY_HINTS[e]}`)" for e in entities
            )
            return (
                f"write_file refused: '{path}' contains HTML-escaped characters: "
                f"{decoded}. Trilogy parses raw operators — emit them literally "
                "(e.g. `<=` not `&lt;=`)."
            )
        syntax_error = _validate_preql_syntax(content)
        if syntax_error:
            return (
                f"write_file refused: '{path}' was not syntactically valid Trilogy.\n"
                f"\nParse error:\n{syntax_error}\n"
                "\nCorrect the syntax error above and call write_file again with "
                "the COMPLETE file body."
            )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    except OSError as exc:
        return f"write_file error: {exc}"
    return f"write_file: wrote {len(content)} chars to {path}" + _raw_path_note(path)


# Workspace noise the agent never needs to see in a file listing.
_LIST_FILES_SKIP_DIRS = {"__pycache__", ".git", ".venv", "node_modules"}
_LIST_FILES_SKIP_PREFIXES = ("_worker_",)
_LIST_FILES_SKIP_SUFFIXES = (".duckdb", ".pyc")
_LIST_FILES_MAX_ENTRIES = 500


def _should_skip_entry(name: str) -> bool:
    if name in _LIST_FILES_SKIP_DIRS:
        return True
    if any(name.startswith(p) for p in _LIST_FILES_SKIP_PREFIXES):
        return True
    if any(name.endswith(s) for s in _LIST_FILES_SKIP_SUFFIXES):
        return True
    return False


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
    entries: list[str] = []
    truncated = False
    if recursive:
        for current, dirs, files in os.walk(root):
            dirs[:] = sorted(d for d in dirs if not _should_skip_entry(d))
            rel = Path(current).relative_to(root)
            for name in sorted(files):
                if _should_skip_entry(name):
                    continue
                rel_path = (rel / name).as_posix() if str(rel) != "." else name
                entries.append(rel_path)
                if len(entries) >= _LIST_FILES_MAX_ENTRIES:
                    truncated = True
                    break
            if truncated:
                break
    else:
        for child in sorted(root.iterdir()):
            if _should_skip_entry(child.name):
                continue
            suffix = "/" if child.is_dir() else ""
            entries.append(child.name + suffix)
    if not entries:
        return f"(no files under {path})"
    header = f"files under {path} ({len(entries)}{'+' if truncated else ''}):\n"
    return header + "\n".join(entries)


def handle_read_file(state: AgentState, args: dict) -> str:
    path = args.get("path")
    if not isinstance(path, str) or not path:
        return "read_file error: 'path' must be a non-empty string."
    target = Path(path)
    if not target.is_file():
        return f"read_file error: no such file: {path}"
    try:
        text = target.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"read_file error: {exc}"
    return truncate_middle(text, state.tool_output_limit)


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
        "Alternatively use `--escapes` with a single-line `\\n`-escaped string, "
        "or use the `write_file(path, content)` tool which takes the same "
        "content directly."
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
    cmd = [sys.executable, "-m", "trilogy.scripts.trilogy", *raw_args]
    child_env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
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
    if _first_non_flag_arg(raw_args) == "agent-info":
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        stdout = truncate_middle(completed.stdout or "", state.tool_output_limit)
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
    return "return_control_to_user: ok"


TOOL_HANDLERS: dict[str, Callable[[AgentState, dict], str]] = {
    SHOW_MESSAGE_TOOL.name: handle_show_message,
    TRILOGY_TOOL.name: handle_trilogy,
    WRITE_FILE_TOOL.name: handle_write_file,
    READ_FILE_TOOL.name: handle_read_file,
    LIST_FILES_TOOL.name: handle_list_files,
    TODO_TOOL.name: handle_todo,
    RETURN_CONTROL_TOOL.name: handle_return_control,
}


def _build_provider(
    cfg: AgentConfig,
    model_override: str | None,
    provider_override: str | None = None,
) -> LLMProvider:
    if provider_override:
        try:
            provider_enum = Provider(provider_override.lower())
        except ValueError as exc:
            valid = ", ".join(p.value for p in Provider)
            raise click.ClickException(
                f"Unknown provider '{provider_override}'. Valid: {valid}."
            ) from exc
    else:
        provider_enum = cfg.provider or DEFAULT_PROVIDER
    model = model_override or os.environ.get("TRILOGY_AGENT_MODEL") or cfg.model
    if not model:
        model = DEFAULT_MODEL if provider_enum == DEFAULT_PROVIDER else ""
    if not model:
        raise click.ClickException(
            f"No model configured for provider {provider_enum.value}. "
            "Set [agent].model in trilogy.toml or pass --model."
        )
    env_var = cfg.api_key_env or PROVIDER_DEFAULT_ENV[provider_enum]
    api_key = os.environ.get(env_var)
    if not api_key:
        raise click.ClickException(
            f"Missing API key: environment variable {env_var} is not set."
        )
    cls = PROVIDER_CLASSES[provider_enum]
    return cls(
        name=f"trilogy-agent-{provider_enum.value}", model=model, api_key=api_key
    )


def _read_context_files(paths: tuple[str, ...]) -> str:
    if not paths:
        return ""
    chunks: list[str] = []
    for raw in paths:
        p = Path(raw)
        if not p.exists():
            raise click.ClickException(f"Context path does not exist: {raw}")
        if p.is_dir():
            raise click.ClickException(
                f"Context path is a directory (pass individual files): {raw}"
            )
        chunks.append(f'<context path="{p}">\n{p.read_text()}\n</context>')
    return "\n".join(chunks)


def _dispatch(state: AgentState, call: LLMToolCall) -> str:
    handler = TOOL_HANDLERS.get(call.name)
    if handler is None:
        return (
            f"Unknown tool '{call.name}'. Available: "
            f"{', '.join(TOOL_HANDLERS.keys())}."
        )
    try:
        return handler(state, call.arguments or {})
    except Exception as exc:
        return f"{call.name} raised {type(exc).__name__}: {exc}"


def _maybe_flag_loop(state: AgentState, call: LLMToolCall, result: str) -> str:
    """Append escalating guidance when the agent repeats an identical call. The
    call still runs — this only surfaces that it is not making progress."""
    sig = f"{call.name}:{json.dumps(call.arguments, sort_keys=True, default=str)}"
    state.recent_signatures.append(sig)
    del state.recent_signatures[:-12]
    repeats = 0
    for prev in reversed(state.recent_signatures):
        if prev != sig:
            break
        repeats += 1
    if repeats >= 3:
        return (
            f"{result}\n\n[guidance] You have issued this identical call "
            f"{repeats} times in a row with the same result — it is not making "
            "progress. Stop repeating it and take a different action."
        )
    return result


ARG_PREVIEW_LIMIT = 80


def _log_event(log_path: Path | None, event: dict[str, Any]) -> None:
    if log_path is None:
        return
    event = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, default=str) + "\n")


def _dump_conversation(conv: Conversation, log_path: Path) -> None:
    """Write the full final message list to a `<log>.conversation.txt` sidecar —
    every message in order, with model_info (tool calls) shown — so the exact
    history sent to the model can be inspected for append bugs."""
    dump_path = log_path.with_suffix(".conversation.txt")
    blocks: list[str] = []
    for i, msg in enumerate(conv.messages):
        block = [f"===== message {i} [{msg.role}] ====="]
        block.append(msg.content if msg.content else "(empty content)")
        info = getattr(msg, "model_info", None)
        if info:
            block.append(f"[model_info] {json.dumps(info, default=str)}")
        blocks.append("\n".join(block))
    dump_path.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")


def _format_call(call: LLMToolCall) -> str:
    parts: list[str] = []
    for key, value in (call.arguments or {}).items():
        rendered = value if isinstance(value, str) else json.dumps(value)
        if len(rendered) > ARG_PREVIEW_LIMIT:
            rendered = rendered[: ARG_PREVIEW_LIMIT - 1] + "…"
        parts.append(f"{key}={rendered}")
    return f"→ {call.name}({', '.join(parts)})"


def _status_message(call: LLMToolCall) -> str:
    if call.name == "trilogy":
        args = call.arguments.get("args") or []
        if isinstance(args, list) and args:
            return f"trilogy {' '.join(str(a) for a in args[:3])}"
    return call.name


def _run_turn(
    conv: Conversation,
    state: AgentState,
    max_iterations: int,
    log_path: Path | None = None,
    tools: list[LLMToolDefinition] | None = None,
) -> None:
    options = LLMRequestOptions(tools=tools or ALL_TOOLS, require_tool=True)
    for _ in range(max_iterations):
        with with_status("Thinking"):
            response = conv.get_response(options)
        _log_event(
            log_path,
            {
                "type": "llm_response",
                "text": response.text,
                "tool_calls": [
                    {"name": c.name, "arguments": c.arguments}
                    for c in response.tool_calls
                ],
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                },
                "finish_reason": response.finish_reason,
            },
        )
        if not response.tool_calls:
            conv.add_message(
                "You must call a tool. To finish, call return_control_to_user.",
                role="user",
            )
            continue
        for call in response.tool_calls:
            print_info(_format_call(call))
            _log_event(
                log_path,
                {"type": "tool_call", "name": call.name, "arguments": call.arguments},
            )
            if call.parse_error:
                result = (
                    f"Tool call '{call.name}' rejected: {call.parse_error}. "
                    "Re-issue the call with valid JSON arguments."
                )
            else:
                with with_status(_status_message(call)):
                    result = _dispatch(state, call)
            result = _maybe_flag_loop(state, call, result)
            _log_event(
                log_path,
                {"type": "tool_result", "name": call.name, "result": result},
            )
            payload = json.dumps({"tool": call.name, "result": result})
            conv.add_message(payload, role="user")
            if state.done:
                return
    raise click.ClickException(
        f"Agent exhausted {max_iterations} iterations without returning control."
    )


@argument("command", type=str)
@option(
    "--context",
    "-c",
    multiple=True,
    help="Additional context files for the agent (one path per flag).",
)
@option("--model", "-m", type=str, help="AI model to use (overrides trilogy.toml).")
@option(
    "--provider",
    "-p",
    type=str,
    help="LLM provider: anthropic, openai, google, openrouter (overrides trilogy.toml).",
)
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--log-file",
    "-l",
    "log_file",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help="Append every LLM response + tool call/result as JSONL to this file.",
)
@option(
    "--interactive",
    "-i",
    is_flag=True,
    help="After return_control_to_user, prompt for the next command.",
)
@option(
    "--quiet/--no-quiet",
    "quiet",
    default=None,
    help="Drop the show_message tool from the agent's toolbox to cut "
    "conversation churn (overrides [agent].quiet in trilogy.toml).",
)
@pass_context
def agent(
    ctx: click.Context,
    command: str,
    context: tuple[str, ...],
    model: str | None,
    provider: str | None,
    env: tuple[str, ...],
    log_file: str | None,
    interactive: bool,
    quiet: bool | None,
) -> None:
    """Pass off a multi-step orchestration task to an AI agent.

    Reads agent settings from the [agent] section of trilogy.toml (provider,
    model, api_key_env, max_iterations, tool_output_limit). The agent drives a
    tool loop over: show_message, trilogy, todo, return_control_to_user.

    Examples:
        trilogy agent "analyze sales trends and create a dashboard"
        trilogy agent -i "ingest new data and run validation tests"
    """
    if env:
        try:
            apply_env_vars(parse_env_vars(env))
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

    runtime = get_runtime_config(Path.cwd())
    cfg = runtime.agent
    actual_quiet = cfg.quiet if quiet is None else quiet
    llm_provider = _build_provider(cfg, model, provider)
    if actual_quiet:
        tools = [t for t in ALL_TOOLS if t.name != SHOW_MESSAGE_TOOL.name]
        system_prompt = QUIET_SYSTEM_PROMPT
    else:
        tools = ALL_TOOLS
        system_prompt = SYSTEM_PROMPT

    log_path: Path | None = None
    if log_file:
        log_path = Path(log_file)
        log_path.write_text("", encoding="utf-8")
        _log_event(
            log_path,
            {
                "type": "session_start",
                "provider": llm_provider.type.value,
                "model": llm_provider.model,
                "command": command,
                "context_files": list(context),
            },
        )

    conv = Conversation.create(llm_provider, model_prompt=system_prompt)
    state = AgentState(tool_output_limit=cfg.tool_output_limit)

    context_block = _read_context_files(context)
    initial = f"{context_block}\n\n{command}" if context_block else command
    conv.add_message(initial, role="user")

    try:
        _run_turn(conv, state, cfg.max_iterations, log_path, tools=tools)
    finally:
        if log_path:
            _dump_conversation(conv, log_path)
    if state.farewell:
        print_success(state.farewell)

    if not interactive:
        return

    while True:
        try:
            next_command = click.prompt("> ", default="", show_default=False)
        except click.exceptions.Abort:
            return
        next_command = next_command.strip()
        if next_command in ("", "exit", "quit"):
            return
        _log_event(log_path, {"type": "user_followup", "command": next_command})
        state.done = False
        state.farewell = ""
        state.todos = []
        conv.add_message(next_command, role="user")
        try:
            _run_turn(conv, state, cfg.max_iterations, log_path, tools=tools)
        finally:
            if log_path:
                _dump_conversation(conv, log_path)
        if state.farewell:
            print_success(state.farewell)
