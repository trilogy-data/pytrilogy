# V4 bug brief — Root cause A: filters that consume a row-shape-barrier output

**Owner:** (fixing agent)  **Planner:** v4 discovery (`CONFIG.use_v4_discovery=True`)
**Single highest-leverage v4 correctness bug** — accounts for TPC-H q02/q20 plus
several non-tpc_h failures, and shares a theme with the q18/adhoc fan-outs.

## One-line statement

v4 refuses to apply (or silently drops) a `WHERE`/`HAVING` atom whose inputs
only come into existence **after** a row-shape barrier (aggregate / window /
unnest / rowset). v3 applies these correctly as post-barrier filters.

## Where it lives

`trilogy/core/processing/v4_helper/group_graph.py` → `_inject_conditions()`
(lines ~346-506). Stage 2 of the planner decomposes each WHERE clause into
AND-atoms and places each atom at the furthest-upstream group whose **input row
stream** already contains the atom's row inputs. Two guards in that function
raise on this bug; a third path drops the atom silently.

## Three observed manifestations

1. **"not reachable from any group"** — `group_graph.py:436`
   ```
   ValueError: Could not place condition atom local.size = 15:
   row inputs ['local.size'] not reachable from any group.
   ```
   No candidate group's `_reachable_input(gid)` contains the atom's row inputs.
   Hits: TPC-H **q02** (`size = 15`), **q20** (`part.name like 'forest%'`),
   hackernews adhoc03 (`type = 'story'`), TPC-H q22-shape.

2. **"downstream of d0 barrier"** — `group_graph.py:497`
   ```
   ValueError: Atom local.user_rank < 10 would be injected at
   grp:window:d0:local.user_id, which is downstream of d0 barrier(s)
   ['grp:aggregate:d0:…dedup:local.post_id']; conditions cannot be pushed
   past row-shape changes.
   ```
   A placement was found, but `offending = d0_group_ids & ancestors(chosen)` is
   non-empty so it's rejected. Note the placement is *downstream* of the
   barrier — which is exactly where a filter that **consumes** the barrier's
   output belongs. Hits: `complex/test_window_function_parsing::test_select`
   (`user_rank < 10`), `engine/test_duckdb::test_not_value` (`z = 1` past
   unnest), `modeling/test_complex::test_window_alt` (`filtered = 1` past
   unnest), `modeling/tpc_ds/test_queries::test_one` (`total_returns > avg`
   past rowset), `optimization/test_join_hoist` (`web_cume > store_cume` past
   aggregate).

3. **Silent drop (no error, wrong rows)** — the most dangerous.
   `engine/test_duckdb::test_proper_basic_unnest_handling`: `WHERE
   prime_cubed_plus_one % 7 = 0` over a value derived from an unnested const.
   v4 returns **10 rows (unfiltered)** where v3 returns **5**. The atom never
   reaches the `raise`; trace where it lands (or is discarded) for the
   unnest→basic-derive→filter shape.

## The conceptual gap

`_inject_conditions` correctly forbids pushing a filter **up** past a barrier
(you can't `WHERE avg(x) > 5` before the aggregate exists — that's what
`producer_closures` + the `offending` check defend against). But it conflates
that with the legitimate, common case of a filter that **depends on a barrier's
output** and therefore must live **at or below** the barrier:

- `HAVING avg(x) > 5` → at the aggregate (already handled via `producer_closures`).
- `rank() … < 10`, `unnest(...)→f(x) % 7 = 0`, `cumulative_a > cumulative_b` →
  the atom's row inputs are the barrier's output column (or a basic derived
  from it), so the only valid host is downstream of the barrier. v4 either
  finds no candidate (manifestation 1, when the derived column isn't in any
  group's *input* stream) or finds one and rejects it as "offending"
  (manifestation 2).

The invariant to restore: an atom may sit downstream of a d0 barrier **when
that barrier produces (or transitively derives) the atom's own inputs** — that
is not "pushing past" the barrier, it's consuming it. The `offending` check
should ignore barriers the atom legitimately consumes from; and the candidate
search (`_reachable_input`) needs to see barrier-output / basic-derived-from-
barrier columns as available in the consumer's input stream.

## Minimal reproductions (already wired)

```bash
# generic, self-contained — fastest loop:
.venv/Scripts/python.exe local_scripts/v4_evals/run_parity.py filter_past_unnest
#   expect after fix: [match] filter_past_unnest (v3=5 v4=5)

# TPC-H crashes from this bug:
.venv/Scripts/python.exe local_scripts/v4_evals/tpc_h_v4_compare.py 02 20
#   expect after fix: [match] for both
```

Direct pytest (these must pass with v4 on, and keep passing with v4 off):
```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/engine/test_duckdb.py::test_proper_basic_unnest_handling \
  tests/engine/test_duckdb.py::test_not_value \
  tests/complex/test_window_function_parsing.py::test_select \
  tests/optimization/test_join_hoist.py::test_hoist_preserves_concepts_referenced_via_output_lineage \
  -p no:cacheprovider -q
```

## Verification / guardrails

- Regression-check the whole family with the eval harness:
  `python local_scripts/v4_evals/tpc_h_v4_compare.py` (baseline today: 17/28;
  q02 & q20 should flip to match, ideally without regressing the 17).
- Do **not** fix by silently dropping unplaceable atoms — the codebase's
  standing rule is fix-at-source, no silent guards (the `raise` exists on
  purpose). A correct fix makes the atom *placeable* at the right group.
- v3 is the reference oracle. For any case, compare against v3 output
  (`CONFIG.use_v4_discovery=False`) — it is known-correct (full suite passes on
  `main`).
- Re-run the full v4 sweep after the fix to confirm no new breakage:
  `python local_scripts/v4_suite_sweep.py` (per-file, timeout-guarded).

## Out of scope (separate bugs, don't chase here)

- q10 "Invalid input concepts … nation.id" (missing-parent, root cause B)
- q11 duckdb "must appear in GROUP BY" (invalid SQL, root cause D)
- q18 / adhoc01 / adhoc07 fan-out (wrong join grain) — may *partly* overlap but
  triage separately.
