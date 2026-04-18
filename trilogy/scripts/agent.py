"""Agent command for Trilogy CLI - AI-powered orchestration tasks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import click
from click import argument, option, pass_context

from trilogy.ai.conversation import Conversation
from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMRequestOptions, LLMToolCall, LLMToolDefinition
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import LLMProvider
from trilogy.ai.providers.google import GoogleProvider
from trilogy.ai.providers.openai import OpenAIProvider
from trilogy.ai.providers.openrouter import OpenRouterProvider
from trilogy.execution.config import AgentConfig
from trilogy.scripts.common import get_runtime_config
from trilogy.scripts.display_core import print_info, print_success

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

SYSTEM_PROMPT = """You are the Trilogy CLI agent. You operate by calling tools.

Available tools:
- show_message(message): print a message to the user.
- trilogy(args, stdin=None): invoke the `trilogy` CLI as a subprocess. `args` is a
  list of string arguments (e.g. ["run", "query.preql"]). Output is captured and
  returned; large outputs are truncated from the middle.
- todo(action, id=None, description=None): manage a scratch TODO list. Actions:
  "add" (requires description; returns new id), "complete" (requires id),
  "remove" (requires id), "list" (returns all items).
- return_control_to_user(message): hand control back to the user. This FAILS if
  any TODOs are not completed — either complete or remove them first.

Discipline:
1. Plan the work as TODO items before acting on anything non-trivial.
2. Mark items complete as you finish them. Remove items you decide not to do.
3. Use `trilogy` for any CLI work — never shell out through `show_message`.
4. Only call `return_control_to_user` when the task is done or you are blocked."""


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
                "description": "Optional stdin to pipe into the subprocess.",
            },
        },
        "required": ["args"],
    },
)

TODO_TOOL = LLMToolDefinition(
    name="todo",
    description="CRUD on the agent's TODO list. Actions: add, complete, remove, list.",
    input_schema={
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "complete", "remove", "list"],
            },
            "id": {"type": ["string", "null"]},
            "description": {"type": ["string", "null"]},
        },
        "required": ["action"],
    },
)

RETURN_CONTROL_TOOL = LLMToolDefinition(
    name="return_control_to_user",
    description=(
        "Hand control back to the user with a final message. Fails if any TODOs "
        "are not completed — complete or remove them first."
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


def handle_trilogy(state: AgentState, args: dict) -> str:
    raw_args = args.get("args")
    if not isinstance(raw_args, list) or not all(isinstance(a, str) for a in raw_args):
        return "trilogy error: 'args' must be a list of strings."
    stdin_value = args.get("stdin")
    if stdin_value is not None and not isinstance(stdin_value, str):
        return "trilogy error: 'stdin' must be a string or null."
    cmd = [sys.executable, "-m", "trilogy.scripts.trilogy", *raw_args]
    try:
        completed = subprocess.run(
            cmd,
            input=stdin_value,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        return "trilogy error: subprocess timed out after 600s."
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
        description = args.get("description")
        if not isinstance(description, str) or not description.strip():
            return "todo error: 'description' is required for add."
        item = TodoItem(id=uuid.uuid4().hex[:8], description=description.strip())
        state.todos.append(item)
        return f"todo added: {item.id}\n{_render_todos(state.todos)}"
    if action in ("complete", "remove"):
        target_id = args.get("id")
        if not isinstance(target_id, str) or not target_id:
            return f"todo error: 'id' is required for {action}."
        for idx, item in enumerate(state.todos):
            if item.id == target_id:
                if action == "complete":
                    item.completed = True
                else:
                    state.todos.pop(idx)
                return f"todo {action}d: {target_id}\n{_render_todos(state.todos)}"
        return f"todo error: no item with id {target_id}."
    return f"todo error: unknown action '{action}'."


def handle_return_control(state: AgentState, args: dict) -> str:
    message = args.get("message")
    if not isinstance(message, str):
        return "return_control_to_user error: 'message' must be a string."
    outstanding = [t for t in state.todos if not t.completed]
    if outstanding:
        pending = ", ".join(f"{t.id} ({t.description})" for t in outstanding)
        return (
            "return_control_to_user refused: uncompleted TODOs remain — "
            f"{pending}. Complete or remove them before returning control."
        )
    state.done = True
    state.farewell = message
    return "return_control_to_user: ok"


TOOL_HANDLERS: dict[str, Callable[[AgentState, dict], str]] = {
    SHOW_MESSAGE_TOOL.name: handle_show_message,
    TRILOGY_TOOL.name: handle_trilogy,
    TODO_TOOL.name: handle_todo,
    RETURN_CONTROL_TOOL.name: handle_return_control,
}


def _build_provider(cfg: AgentConfig, model_override: str | None) -> LLMProvider:
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


def _run_turn(conv: Conversation, state: AgentState, max_iterations: int) -> None:
    options = LLMRequestOptions(tools=ALL_TOOLS, require_tool=True)
    for _ in range(max_iterations):
        response = conv.get_response(options)
        if not response.tool_calls:
            conv.add_message(
                "You must call a tool. To finish, call return_control_to_user.",
                role="user",
            )
            continue
        for call in response.tool_calls:
            result = _dispatch(state, call)
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
    "--interactive",
    "-i",
    is_flag=True,
    help="After return_control_to_user, prompt for the next command.",
)
@pass_context
def agent(
    ctx: click.Context,
    command: str,
    context: tuple[str, ...],
    model: str | None,
    interactive: bool,
) -> None:
    """Pass off a multi-step orchestration task to an AI agent.

    Reads agent settings from the [agent] section of trilogy.toml (provider,
    model, api_key_env, max_iterations, tool_output_limit). The agent drives a
    tool loop over: show_message, trilogy, todo, return_control_to_user.

    Examples:
        trilogy agent "analyze sales trends and create a dashboard"
        trilogy agent -i "ingest new data and run validation tests"
    """
    runtime = get_runtime_config(Path.cwd())
    cfg = runtime.agent
    provider = _build_provider(cfg, model)

    conv = Conversation.create(provider, model_prompt=SYSTEM_PROMPT)
    state = AgentState(tool_output_limit=cfg.tool_output_limit)

    context_block = _read_context_files(context)
    initial = f"{context_block}\n\n{command}" if context_block else command
    conv.add_message(initial, role="user")

    _run_turn(conv, state, cfg.max_iterations)
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
        state.done = False
        state.farewell = ""
        state.todos = []
        conv.add_message(next_command, role="user")
        _run_turn(conv, state, cfg.max_iterations)
        if state.farewell:
            print_success(state.farewell)
