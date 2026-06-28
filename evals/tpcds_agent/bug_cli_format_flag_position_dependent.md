# Handoff: `--format` (and other group options) are position-dependent → confusing "not a valid dialect" error

## Symptom
An agent ran:
```
trilogy run --import raw/all_sales:all_sales --format json select ...
```
and got:
```
Error: 'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, ...
```
`--format json` is real and documented (`trilogy/scripts/agent_info.py:259` — "agentic access will default to --format json"; defined at `trilogy/scripts/trilogy.py:66-77`). The agent did not hallucinate it — it placed it **after** the `run` subcommand, where it is silently swallowed and the value `json` falls through to the `run` command's `DIALECT` positional.

This is the "why is argument *position* load-bearing?" problem: a global flag that works before the subcommand fails confusingly after it.

## Root cause (exact)
`--format`, `--debug`, `--debug-file` are options on the **group** (`cli`, a `LazyGroup`) in `trilogy/scripts/trilogy.py:57-96`. Click only consumes group options *before* the subcommand. To paper over that, `LazyGroup.parse_args` (`trilogy/scripts/click_utils.py:79-82`) calls `_hoist_group_flags` (`click_utils.py:17-55`), which moves group flags found *after* the subcommand back to before it.

But the set of hoisted flags is a **hand-maintained hardcoded dict** (`click_utils.py:11-14`):
```python
_HOIST_FLAGS = {
    "--debug": False,      # bool flag
    "--debug-file": True,  # takes a value
}
```
**`--format` is missing.** So it is not hoisted; the `run` subcommand is loaded with `IGNORE_UNKNOWN` (`click_utils.py:102`, applied at `trilogy.py:51`), which makes it ignore the unknown `--format` token and bind `json` to the `DIALECT` positional → the misleading error. Any group option not listed in `_HOIST_FLAGS` has the same latent bug; the list has already drifted out of sync with the actual group options.

## Desired behavior
`trilogy --format json run ...` and `trilogy run ... --format json` should behave identically — and this should hold for **every** group-level option without anyone remembering to update a list.

## Fix options

### (A) Minimal stopgap — add the missing flag
```python
_HOIST_FLAGS = {
    "--debug": False,
    "--debug-file": True,
    "--format": True,   # takes a value (rich|json)
}
```
Fixes this case; leaves the drift bug in place for the next added option.

### (B) Recommended — derive the hoistable set from the group's actual params (drift-proof)
Stop hardcoding. In `LazyGroup.parse_args`, build the map by introspecting the group's own options so it can never go stale:
```python
def parse_args(self, ctx, args):
    subcommands = set(self._lazy_subcommands) | set(self.commands)
    hoist = {}
    for p in self.get_params(ctx):
        if isinstance(p, click.Option):
            takes_value = not p.is_flag and not p.count and p.nargs == 1
            for opt in (*p.opts, *p.secondary_opts):
                if opt.startswith("--"):
                    hoist[opt] = takes_value
    args = _hoist_group_flags(list(args), subcommands, hoist)
    return super().parse_args(ctx, args)
```
(Make `_hoist_group_flags` take the `hoist` map instead of the module-level `_HOIST_FLAGS`.) This automatically covers `--format`, `--debug`, `--debug-file`, and anything added later. Skip eager/`expose_value=False` options like `--version` if desired (harmless either way).

### (C) Secondary — make the error self-explanatory
`validate_dialect` already exists (`click_utils.py:105`) to catch a dialect that "looks like a misplaced flag or path." Extend it (or the `run` arg handling) so that when the `DIALECT` positional equals a known group-option *value* (e.g. `json`/`rich` after a swallowed `--format`), the message says "`--format` must precede the subcommand (or was placed after a value) — try `trilogy --format json run ...`" instead of "not a valid dialect." Do this even if (B) lands, for any residual misplacement.

## Edge cases for the fixer
- Value-taking options: hoist must also move the **following token** (`--format json`), as the existing code does for `--debug-file`. The `--format=json` form is already handled (the `=` branch).
- Don't hoist tokens after a `--` separator, and don't hoist a flag-looking token that is actually a value of a preceding option in the subcommand's own args. (Current logic scans linearly from just after the subcommand; verify it doesn't misattribute a subcommand option's value.)
- A query body can legitimately contain the text `--format` only after `--`; ensure `--` is respected.
- `run` uses `IGNORE_UNKNOWN`, which is what makes misplacement *silent* rather than a clean error — that's why (C) matters as a backstop.

## Pointers
- `trilogy/scripts/click_utils.py:11-14` — `_HOIST_FLAGS` (the stale list)
- `trilogy/scripts/click_utils.py:17-55` — `_hoist_group_flags`
- `trilogy/scripts/click_utils.py:79-82` — `LazyGroup.parse_args` (hook point for option (B))
- `trilogy/scripts/click_utils.py:102` — `IGNORE_UNKNOWN`
- `trilogy/scripts/click_utils.py:105` — `validate_dialect` (hook point for option (C))
- `trilogy/scripts/trilogy.py:57-96` — group + `--format`/`--debug`/`--debug-file` definitions
- `trilogy/scripts/agent_info.py:259-261` — agent guidance that points agents at `--format`

## Tests
Add CLI tests (CliRunner) asserting equivalence of pre/post placement for each group option:
- `trilogy --format json run <q>` == `trilogy run <q> --format json` (both emit JSON; neither treats `json` as a dialect)
- same for `--debug`, `--debug-file <path>`
- `trilogy run <q> duck_db --format json` still binds `duck_db` as the dialect (value not mis-hoisted)
- a misplaced `--format` with (C) yields the friendly hint, not "not a valid dialect"
