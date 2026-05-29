"""Agent command for Trilogy CLI - AI-powered orchestration tasks.

This module owns the conversation loop, the reviewer (submit-validation)
pass, and the Click CLI entrypoint. The tool definitions, their handlers,
and ``AgentState`` live in ``agent_tools.py``.
"""

from __future__ import annotations

import json
import os
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import click
from click import argument, option, pass_context

from trilogy.ai.conversation import Conversation
from trilogy.ai.enums import Provider
from trilogy.ai.models import (
    LLMMessage,
    LLMRequestOptions,
    LLMToolCall,
    LLMToolDefinition,
)
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import LLMProvider
from trilogy.ai.providers.google import GoogleProvider
from trilogy.ai.providers.openai import OpenAIProvider
from trilogy.ai.providers.openrouter import OpenRouterProvider
from trilogy.execution.config import AgentConfig, apply_env_vars
from trilogy.scripts.agent_tools import (
    ALL_TOOLS,
    SHOW_MESSAGE_TOOL,
    TOOL_HANDLERS,
    TRILOGY_TOOL,
    AgentState,
    truncate_middle,
)
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
      `read_file` on a model file. Use `--regex` (repeatable, Python regex) to filter.
      Trilogy auto-resolves joins from the model's declared relationships.
      Join discovery is not needed;
    write `select store_sales.date_dim.year, ...;` and Trilogy
      handles the join. There is no manual JOIN clause in this language.
    * Only documented subcommands work — do NOT invent `list`, `raw`, `shell`,
      etc. `trilogy agent-info` lists everything that exists.
- read_file(path): return the text content of a file; use this for
  code understanding; prefer explore for usage.

To create or overwrite a file (every .preql query file you write), use
`trilogy file write <path> --content <full body>`. Pass the complete file
body as a single string in `--content`; embed literal newlines. Trilogy
parses the body before it lands on disk — partial or broken .preql writes
are rejected with the parse error. Re-issue the call with the COMPLETE body.
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


# The Trilogy language reference is NOT inlined here — discipline rule #1
# directs the agent to call `trilogy agent-info` first, which returns the
# canonical reference. Inlining duplicated ~26KB of prompt tokens per run.
SYSTEM_PROMPT = get_agent_instructions(True)
QUIET_SYSTEM_PROMPT = get_agent_instructions(False)


MAX_SUBMIT_KICKBACKS = 2

# Exit code used when the agent stops because it ran out of iterations rather
# than because of a real crash. Distinct from the default ClickException exit
# code (1) so callers (the eval scorer in particular) can distinguish "agent
# gave up after N turns" from "agent process died unexpectedly".
EXIT_ITERATION_EXHAUSTED = 2

REVIEWER_SYSTEM_PROMPT = (
    "You are reviewing whether an AI agent actually finished its task. "
    "You will receive the original task and the agent's tool-use transcript. "
    "Reply with exactly 'DONE' or 'NOT_DONE' on the first line, then one or "
    "two sentences explaining why. Be strict — if the agent's transcript "
    "shows it (a) self-noted uncertainty, (b) didn't implement a clause the "
    "task explicitly required, (c) only wrote a query that runs cleanly but "
    "doesn't match the requested logic, or (d) cut off mid-thought, reply "
    "NOT_DONE. Otherwise reply DONE."
)

REVIEWER_TRANSCRIPT_MSG_LIMIT = 1200
REVIEWER_TRANSCRIPT_ARG_LIMIT = 600


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


_CONV_WRAP_WIDTH = 80


def _wrap_value_lines(text: str, indent: int, prefix: str = "") -> list[str]:
    """Wrap ``text`` to ``_CONV_WRAP_WIDTH`` at the given indent, preserving
    embedded newlines (each input line wraps independently). ``prefix`` is
    applied to the first physical line only; continuation lines align under
    the start of the prefix's content."""
    pad = " " * indent
    out: list[str] = []
    for line in text.splitlines() or [""]:
        first = pad + prefix
        cont = pad + " " * len(prefix)
        if len(first + line) <= _CONV_WRAP_WIDTH:
            out.append(first + line)
            continue
        wrapped = textwrap.wrap(
            line,
            width=_CONV_WRAP_WIDTH,
            initial_indent=first,
            subsequent_indent=cont,
            break_long_words=False,
            break_on_hyphens=False,
            replace_whitespace=False,
            drop_whitespace=False,
        )
        out.extend(wrapped or [first + line])
    return out


def _format_arg(key: str, value: Any, indent: int) -> list[str]:
    """Render one tool-call argument as wrapped lines. Lists/dicts expand
    one-item-per-line; scalars inline when short, block-style when long."""
    pad = " " * indent
    if isinstance(value, list):
        if not value:
            return [f"{pad}{key}: []"]
        out = [f"{pad}{key}:"]
        for item in value:
            item_str = item if isinstance(item, str) else json.dumps(item)
            out.extend(_wrap_value_lines(item_str, indent + 2, prefix="- "))
        return out
    if isinstance(value, dict):
        if not value:
            return [f"{pad}{key}: {{}}"]
        out = [f"{pad}{key}:"]
        for k, v in value.items():
            out.extend(_format_arg(k, v, indent + 2))
        return out
    sval = value if isinstance(value, str) else json.dumps(value)
    inline = f"{pad}{key}: {sval}"
    if "\n" not in sval and len(inline) <= _CONV_WRAP_WIDTH:
        return [inline]
    return [f"{pad}{key}: |", *_wrap_value_lines(sval, indent + 2)]


def _format_tool_call(tc: dict) -> list[str]:
    name = tc.get("name", "?")
    args = tc.get("arguments") or {}
    lines = [f"[tool_call] {name}"]
    if isinstance(args, dict):
        for key, value in args.items():
            lines.extend(_format_arg(key, value, indent=2))
    else:
        lines.extend(_wrap_value_lines(json.dumps(args), indent=2))
    return lines


def _try_parse_tool_result(content: str) -> dict | None:
    """Detect content of the form ``{"tool": <name>, "result": <string>}``
    that ``_run_turn`` writes for every tool result. Returns the parsed payload
    or None — None falls back to printing the raw content verbatim."""
    s = content.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        data = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict) or "tool" not in data or "result" not in data:
        return None
    return data


def _format_tool_result(payload: dict) -> list[str]:
    """Render a parsed tool-result payload as a multi-line block. The result
    string (which often has embedded newlines and can be tens of KB after
    truncation) is wrapped at 80 cols so the dump stays scannable."""
    name = payload.get("tool", "?")
    result = payload.get("result", "")
    sresult = result if isinstance(result, str) else json.dumps(result)
    lines = [f"[tool_result] {name}"]
    lines.extend(_wrap_value_lines(sresult, indent=2))
    return lines


def _dump_conversation(conv: Conversation, log_path: Path) -> None:
    """Write the full final message list to a `<log>.conversation.txt` sidecar —
    every message in order, with tool calls pretty-printed (one per line,
    wrapped at 80 cols) so the exact history sent to the model can be
    inspected for append bugs without scrolling sideways."""
    dump_path = log_path.with_suffix(".conversation.txt")
    blocks: list[str] = []
    for i, msg in enumerate(conv.messages):
        block = [f"===== message {i} [{msg.role}] ====="]
        payload = _try_parse_tool_result(msg.content) if msg.content else None
        if payload is not None:
            block.extend(_format_tool_result(payload))
        else:
            block.append(msg.content if msg.content else "(empty content)")
        info = msg.model_info or {}
        for tc in info.get("tool_calls", []) or []:
            block.extend(_format_tool_call(tc))
        extra = {k: v for k, v in info.items() if k != "tool_calls"}
        if extra:
            block.append("[model_info] " + json.dumps(extra, default=str))
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
    if call.name == TRILOGY_TOOL.name:
        args = call.arguments.get("args") or []
        if isinstance(args, list) and args:
            return f"{TRILOGY_TOOL.name} {' '.join(str(a) for a in args[:3])}"
    return call.name


def _render_reviewer_transcript(messages: list[LLMMessage]) -> str:
    blocks: list[str] = []
    for msg in messages:
        if msg.role == "system":
            continue
        info = msg.model_info or {}
        tcs = info.get("tool_calls", [])
        if msg.role == "assistant" and tcs:
            for tc in tcs:
                name = tc.get("name", "?")
                args = tc.get("arguments")
                rendered = (
                    args if isinstance(args, str) else json.dumps(args, default=str)
                )
                rendered = truncate_middle(rendered, REVIEWER_TRANSCRIPT_ARG_LIMIT)
                blocks.append(f"AGENT CALL {name}: {rendered}")
            continue
        content = truncate_middle(msg.content or "", REVIEWER_TRANSCRIPT_MSG_LIMIT)
        prefix = "USER/TOOL_RESULT" if msg.role == "user" else msg.role.upper()
        blocks.append(f"{prefix}: {content}")
    return "\n\n".join(blocks)


def _validate_completion(
    provider: LLMProvider,
    original_task: str,
    messages: list[LLMMessage],
) -> tuple[bool, str]:
    """One-shot reviewer call. Clean context, no tools. Returns (is_done, note)."""
    reviewer = Conversation.create(provider, model_prompt=REVIEWER_SYSTEM_PROMPT)
    transcript = _render_reviewer_transcript(messages)
    reviewer.add_message(
        f"ORIGINAL TASK:\n{original_task}\n\nAGENT TRANSCRIPT:\n{transcript}",
        role="user",
    )
    try:
        response = reviewer.get_response(LLMRequestOptions(require_tool=False))
    except Exception as exc:
        # On reviewer failure, fall open — don't block the agent indefinitely.
        return True, f"reviewer unavailable: {exc}"
    text = (response.text or "").strip()
    first = text.splitlines()[0].strip().upper() if text else ""
    is_done = first.startswith("DONE") and not first.startswith("NOT_DONE")
    return is_done, text


def _run_turn(
    conv: Conversation,
    state: AgentState,
    max_iterations: int,
    log_path: Path | None = None,
    tools: list[LLMToolDefinition] | None = None,
    provider: LLMProvider | None = None,
    original_task: str = "",
    validate_completion: bool = True,
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
                if (
                    validate_completion
                    and provider is not None
                    and state.submit_kickbacks < MAX_SUBMIT_KICKBACKS
                ):
                    with with_status("Reviewer checking submit"):
                        is_done, note = _validate_completion(
                            provider, original_task, conv.messages
                        )
                    _log_event(
                        log_path,
                        {
                            "type": "reviewer_verdict",
                            "is_done": is_done,
                            "note": note,
                            "kickback_count": state.submit_kickbacks,
                        },
                    )
                    if not is_done:
                        state.submit_kickbacks += 1
                        state.done = False
                        state.farewell = ""
                        print_info(
                            f"[reviewer] NOT_DONE (kickback {state.submit_kickbacks}/"
                            f"{MAX_SUBMIT_KICKBACKS})"
                        )
                        conv.add_message(
                            "A reviewer determined you might not actually be "
                            "done; keep working and submit again when you are "
                            f"truly finished. Reviewer note: {note}",
                            role="user",
                        )
                        continue
                return
    exc = click.ClickException(
        f"Agent exhausted {max_iterations} iterations without returning control."
    )
    exc.exit_code = EXIT_ITERATION_EXHAUSTED
    raise exc


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
        _run_turn(
            conv,
            state,
            cfg.max_iterations,
            log_path,
            tools=tools,
            provider=llm_provider,
            original_task=command,
        )
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
        state.submit_kickbacks = 0
        conv.add_message(next_command, role="user")
        try:
            _run_turn(
                conv,
                state,
                cfg.max_iterations,
                log_path,
                tools=tools,
                provider=llm_provider,
                original_task=next_command,
            )
        finally:
            if log_path:
                _dump_conversation(conv, log_path)
        if state.farewell:
            print_success(state.farewell)
