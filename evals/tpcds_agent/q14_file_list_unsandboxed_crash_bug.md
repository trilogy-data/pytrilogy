# Bug: `trilogy file list` escapes the workspace → 144 MB result crashes the agent (q14)

**Status:** PARTIALLY FIXED (2026-06-25) — output is now capped/truncated; workspace
sandboxing (suggested fix #1) is still open.

**Fix applied:** `LocalFileBackend.list` (`trilogy/scripts/file_helpers/backends.py`)
now caps at `LIST_MAX_ENTRIES = 100`, breaking *during* iteration before the
`sorted()` materialization — the giant in-memory walk was the actual OOM/crash cause.
`trilogy file list` (`file.py`) surfaces a truncation notice in both rich and JSON
output. This makes `file list / --recursive` bounded and safe; it no longer crashes the
agent. Tests: `tests/scripts/test_file.py::test_local_backend_list_caps_entries`,
`::test_list_cap_emits_truncation_notice`.

**Still open:** suggested fix #1 (sandbox `file` subcommands to the workspace root so
`..`/abs/`/` can't traverse out) and #3 (harness-layer result-size backstop). The cap
prevents the crash and the host-tree *dump*, but a caller can still probe individual
paths outside the workspace.

---

**Status (original):** OPEN — observed in eval; root cause clear from logs.
**Surfaced by:** TPC-DS q14 enriched eval, run `20260625-150049`. Graded `crashed`
(agent exit 1, no query file produced).
**Severity:** HIGH (tooling). A single tool call can return a multi-hundred-MB payload that
crashes the agent process, losing the whole query attempt — and it leaks the host filesystem
outside the sandboxed workspace.

## Symptom

The q14 agent made 90 `trilogy` calls; the final ones were directory listings climbing **out of
the workspace**:

```
file list /workspace
file list .
file list ..
file list / --recursive      <-- last call before crash
```

`file list / --recursive` walked the entire host filesystem from root. The resulting
`agent_log.q14.jsonl` is **144 MB** (vs ~1–5 MB for a normal query log). The agent process then
exited 1 with no `query14.preql` written — the run scorer recorded
`agent crashed (exit 1, was: missing) — no query file produced`.

Evidence the listing escaped the workspace: the captured tool output contains host install paths
like `\wtfix\trilogy\std\geography.preql`, `\wtfix\trilogy\std\metric.preql` — i.e. the Trilogy
package directory, far outside the eval workspace.

## Root cause

The `trilogy file list` CLI subcommand:

1. **Is not sandboxed to the workspace.** It accepts `..`, absolute paths, and `/`, so the agent
   can traverse arbitrarily up and out of the intended working directory.
2. **Has no output cap on `--recursive`.** `file list / --recursive` enumerates the whole
   filesystem with no depth limit, count limit, or byte cap, producing a payload large enough to
   crash the agent (here 144 MB).

Either property alone is bad; together, one tool call both leaks the host tree and OOM/`exit 1`s
the agent.

## Why the agent went there (context, not the bug)

q14 is the cross-channel "common items" query. The agent appears to have been hunting for model
files / schema and, not finding what it wanted in the workspace, climbed `..` → `/` →
`/ --recursive`. The agent's exploration was misguided, but the **tool must not let a listing
escape the workspace or return an unbounded payload** — that is the framework defect. A
well-behaved tool would have returned a scoped, capped result and the agent would have recovered.

## Suggested fix

1. **Sandbox `file list` (and peer `file` subcommands) to the workspace root.** Resolve the
   requested path against the workspace and reject/clamp anything that escapes it (`..`, absolute
   paths, `/`). Mirror whatever sandbox the `file read`/`file write` subcommands use.
2. **Cap `--recursive` output:** enforce a max entry count and/or max byte size, with a clear
   truncation marker (`... N entries omitted`) so the agent learns the listing was limited rather
   than silently getting a giant or partial dump.
3. **Bound tool-result size at the harness layer** as a backstop, so no single tool result can
   exceed a sane cap and crash the agent regardless of which tool produced it.

## Repro

In an eval workspace:

```
trilogy file list / --recursive
```

Expect (current): traverses the host filesystem, returns a multi-hundred-MB listing including
paths outside the workspace; downstream the agent process crashes (exit 1). Expect (fixed):
rejected or clamped to the workspace with a capped, truncated result.

Artifacts: `evals/tpcds_agent/results/20260625-150049/agent_log.q14.jsonl` (144 MB; the runaway
listing), task in `task.q14.txt`.
