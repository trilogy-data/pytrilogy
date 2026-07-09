"""Assemble the natural-language task handed to the Trilogy agent.

Prompts are intentionally minimal — environment + deliverable + the question(s).
All query-authoring guidance (imports, chaining, function reference) lives in
the Trilogy language reference, which the agent loads via `trilogy agent-info`.
"""

from __future__ import annotations

import json

from .spec import BenchmarkSpec

_SINGLE_QUERY_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`{db}`) already loaded with {bench} data, and `raw/` is already
populated with ingested Trilogy model files — do NOT re-run `trilogy ingest`
and do NOT edit files in `raw/`.

Answer the ONE business question below by writing a Trilogy query file to
`query{{nn}}.preql` in the working directory (alongside `trilogy.toml`, NOT
inside `raw/`). Validate with `trilogy run query{{nn}}.preql{{validate_params}}`.

Return control once it runs cleanly to submit your result. This will be 
your final action.


Every question in this set returns at least one row at this scale factor. A
zero-row (or suspiciously tiny) result means your query is almost certainly wrong
— recheck the grain, joins, and filters (an over-restrictive or wrong-population
filter, a null-key row dropped by a join, a mis-scoped aggregate) rather than
shipping the empty result as "the answer". Do NOT, however, add/drop/loosen
filters just to force rows — find and fix the actual mistake.

Question {{id}}:
{{prompt}}{{params_block}}
"""

_PARAMS_HEADER = """

Parameters (the harness injects these via `--param NAME=VALUE` at evaluation
time; declare each in your .preql with `parameter NAME TYPE;` and reference
them as regular fields):"""

_SQL_SINGLE_QUERY_TEMPLATE = """\
DuckDB database (`{db}`) loaded with {bench} data, configured in this directory.
Answer the ONE business question below with plain DuckDB SQL.

Write your answer as a SINGLE self-contained SELECT to `query{{nn}}.sql` in the
working directory, and validate it with the run_file tool before finishing.

Return control once it runs cleanly to submit your result. This will be 
your final action.

Every question in this set returns at least one row at this scale factor. A
zero-row (or suspiciously tiny) result means your query is almost certainly wrong
— recheck the grain, joins, and filters (an over-restrictive or wrong-population
filter, a null-key row dropped by a join, a mis-scoped aggregate) rather than
shipping the empty result as "the answer". Do NOT, however, add/drop/loosen
filters just to force rows — find and fix the actual mistake.

Question {{id}}:
{{prompt}}{{params_block}}
"""

_SQL_PARAMS_HEADER = """

Use these exact constant values in your SQL (inline them as literals):"""

_TASK_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures a DuckDB database
(`{db}`) already loaded with the {bench} benchmark schema and data.

Your goal:
1. Build a Trilogy semantic data model with `trilogy ingest --all` — writes
   one .preql model file per table under the subfolder `raw/`. Do NOT overwrite files in
   `raw/` unless deliberately correcting a model definition.
2. Answer each of the {{n}} business questions below by writing a Trilogy query.

Write one query file per question alongside `trilogy.toml` (NOT inside `raw/`).
Each question below states its exact filename (`queryNN.preql`, where NN is the
question number). Validate each file with `trilogy run <file>` before moving
on. Typically, you will import one fact file from raw/ per question, though
some rare ones may require merging multiple facts.

Every question in this set returns at least one row at this scale factor. A
zero-row (or suspiciously tiny) result means your query is almost certainly wrong
— recheck the grain, joins, and filters (an over-restrictive or wrong-population
filter, a null-key row dropped by a join, a mis-scoped aggregate) rather than
shipping the empty result as "the answer". Do NOT, however, add/drop/loosen
filters just to force rows — find and fix the actual mistake.

Business questions
==================
{{questions}}
"""


def load_prompts(spec: BenchmarkSpec) -> list[dict]:
    return json.loads(spec.prompts_file.read_text(encoding="utf-8"))


def active_prompts(spec: BenchmarkSpec) -> list[dict]:
    """Prompts the eval runs. ``impossible``-graded entries stay in the catalog
    for the record but are excluded."""
    return [p for p in load_prompts(spec) if p.get("grade") != "impossible"]


def selected_ids(spec: BenchmarkSpec, num_queries: int) -> list[int]:
    return [p["id"] for p in active_prompts(spec)[:num_queries]]


def _shell_quote(value: str) -> str:
    """Quote a value for the `--param key=value` example we hand to the agent.
    Stay readable — only wrap in double quotes when there's whitespace or a
    shell-special char; embedded double quotes get backslash-escaped."""
    if not value:
        return '""'
    if any(c in value for c in ' \t"\\$`'):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value


def _render_params_block(params: dict) -> tuple[str, str]:
    """Returns (params_block, validate_params_suffix). The block is appended
    to the prompt to describe each parameter; the suffix is appended to the
    sample `trilogy run` invocation so the agent can copy-paste it."""
    if not params:
        return "", ""
    lines: list[str] = [_PARAMS_HEADER]
    cli_suffix_parts: list[str] = []
    for name, spec in params.items():
        ptype = spec.get("type", "string")
        value = spec.get("value", "")
        desc = spec.get("description", "")
        desc_tail = f" — {desc}" if desc else ""
        lines.append(f"  - {name} ({ptype}){desc_tail}")
        lines.append(f"      value: {value}")
        cli_suffix_parts.append(f"--param {name}={_shell_quote(str(value))}")
    suffix = " " + " ".join(cli_suffix_parts) if cli_suffix_parts else ""
    return "\n".join(lines), suffix


def build_single_query_task(spec: BenchmarkSpec, entry: dict) -> str:
    template = _SINGLE_QUERY_TEMPLATE.format(db=spec.db_filename, bench=spec.name)
    params_block, validate_params = _render_params_block(entry.get("params") or {})
    return template.format(
        id=entry["id"],
        nn=f"{entry['id']:02d}",
        prompt=entry["prompt"],
        params_block=params_block,
        validate_params=validate_params,
    )


def _render_sql_params_block(params: dict) -> str:
    if not params:
        return ""
    lines: list[str] = [_SQL_PARAMS_HEADER]
    for name, spec in params.items():
        desc = spec.get("description", "")
        desc_tail = f" — {desc}" if desc else ""
        lines.append(f"  - {name} = {spec.get('value', '')}{desc_tail}")
    return "\n".join(lines)


def build_single_query_task_sql(spec: BenchmarkSpec, entry: dict) -> str:
    """SQL-baseline variant of ``build_single_query_task``: the agent writes
    plain DuckDB SQL to ``query{nn}.sql`` (no Trilogy)."""
    template = _SQL_SINGLE_QUERY_TEMPLATE.format(db=spec.db_filename, bench=spec.name)
    return template.format(
        id=entry["id"],
        nn=f"{entry['id']:02d}",
        prompt=entry["prompt"],
        params_block=_render_sql_params_block(entry.get("params") or {}),
    )


def build_task(spec: BenchmarkSpec, num_queries: int) -> str:
    prompts = active_prompts(spec)[:num_queries]
    questions = "\n\n".join(
        f"Question {p['id']} -> write `query{p['id']:02d}.preql`\n{p['prompt']}"
        for p in prompts
    )
    template = _TASK_TEMPLATE.format(db=spec.db_filename, bench=spec.name)
    return template.format(n=len(prompts), questions=questions)
