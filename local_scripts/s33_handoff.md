# Handoff — v4 network discovery cutover (end of s33)

Read `docs/v4_network_discovery_design.md` first: §0 is the s32 shadow work, **§0.1 and
§0.2 are this session**. This file is orientation + commands only; the design doc is the
source of truth.

## State in one line

The network search is **wired into the v4 plan path behind a flag** and the TPC-DS
battery is at **105 passed / 1 failed / 1 xfailed** against a flag-off baseline of
**106 / 1 xfailed**. Exactly one regression: `test_five` (query05). Everything else is
green — `mypy trilogy`, ruff (`E,F,I`), black, and the 15 `test_v4_network_search` tests.

## What landed

| file | change |
|---|---|
| `trilogy/constants.py` | `CONFIG.use_v4_network_search` (nested under `use_v4_discovery`) |
| `v4_helper/source_planning.py` | `_network_bridge_plan()` — the stage-D adapter — plus its call at the top of `plan_source`'s attempt loop |
| `v4_helper/network_search.py` | `join_keys` memoization (two cache fields on `SourceNetwork`) |
| `tests/conftest.py` | `TRILOGY_V4_NETWORK_SEARCH=1` opt-in + per-test restore so it cannot leak |
| `local_scripts/s33_network_burndown.py` | generation sweep over the corpus, failures grouped by signature |

**The adapter's design choice:** the network only *selects* sources; legacy still
*emits*. `_network_bridge_plan` converts a `SourceSolution` into a `BridgePlan` and hands
it to the unchanged `_datasource_nodes_for_bridge` → `_merge_component_sources` →
`_complete_partial_requested` chain, so every §5 carry-over those implement stays in
force and the cutover diff is confined to "who chose the sources". Keep that property.

Two adapter bugs were found and fixed during burndown; both are documented in §0.2. The
second one is the kind to expect more of: `reinject_common_join_keys_v2` runs *inside*
`determine_induced_minimal_nodes`, which the adapter bypasses, so it had to be ported
explicitly. **If a new failure looks like a silent wrong-rows change, first ask what else
lives inside the Steiner helper that this path skips.**

## Commands

```bash
# generation sweep, all 109 TPC-DS queries (fast, no execution)
.venv/Scripts/python.exe local_scripts/s33_network_burndown.py

# the row-level gate: battery WITH the network search
TRILOGY_V4_DISCOVERY=1 TRILOGY_V4_NETWORK_SEARCH=1 \
  .venv/Scripts/python.exe -m pytest tests/modeling/tpc_ds_duckdb/test_queries.py -q -p no:randomly

# baseline for comparison (v4 ladder) — drop the second env var
TRILOGY_V4_DISCOVERY=1 \
  .venv/Scripts/python.exe -m pytest tests/modeling/tpc_ds_duckdb/test_queries.py -q -p no:randomly

# just the failure
... -k test_five

# unit tests for the search (fast; these are the guardrail — see "traps")
.venv/Scripts/python.exe -m pytest tests/core/processing/test_v4_network_search.py -q -p no:randomly

# s32 probes still work: ladder-vs-network shadow, and per-request detail
.venv/Scripts/python.exe local_scripts/s32_network_shadow.py
.venv/Scripts/python.exe local_scripts/s32_solution_detail.py query05.preql s.return_channel_dim_id
```

Repo-wide checks (note the ruff form — see traps):

```bash
.venv/Scripts/python.exe -m ruff check <paths> --select E,F,I
.venv/Scripts/python.exe -m mypy trilogy
.venv/Scripts/python.exe -m black .
```

## Not yet run

join_matrix, gcat + enum_unions, `local_scripts/v4_sql_snapshot.py check`, and a full
repo sweep — all with the flag on. **Do these before trusting the 105/1 number.** The
authored-join suites are the ones most likely to surface new failures, because they are
disproportionately made of the scoped-join shapes §0.1 shows the search handling badly.

## The one failure: query05

Fully diagnosed in §0.2. Short version: the network picks the plain returns family (which
does **not** bind `return_channel_dim_id` — TPC-DS `web_returns` has no site key) plus
the `*_dim_return` family, assigning the FK to the dimension keyed *by* it. No fact-side
source materializes it, the two share only the 3-valued `s.channel`, and the join fans out
~4000×. Legacy's `cheerful` CTE is a 10-branch `UNION ALL` stacking the dim arms **and**
the fact arms that carry a return site, including `WS_WEB_SITE_SK` from `web_sales`.

**Three fix classes are ruled out with evidence. Do not retry them** (§0.2 has the
detail):

1. `unkeyed_joins` as a **cost axis** — launderable; the search bought its way out by
   adding a `catalog_page` scan to manufacture join keys.
2. The same rule as a **connectivity predicate** — wrong semantics; it forbids the
   canonical fact↔fact conformed-dimension blend. Two unit tests caught it.
3. A **new union-construction mechanism** — unnecessary; `get_union_sources` already
   emits the needed hybrid group and assembly already stacks selected groups into one CTE.

Also established: enumeration is **not** the blocker (61,752 exhaustive covers, 24,634
containing the hybrid, the broken cover still wins), and `_reduce` is what discards the
hybrid, because its minimality test is profile-based and cannot see that the hybrid is
the only thing putting the FK on the fact side.

**The remaining hypothesis, UNTESTED:** what separates q05's bad join from the legitimate
blend is not key coverage — it is that `return_channel_dim_id` is a *requested terminal*
that is also its provider's own grain key, where another candidate could have co-located
it with the fact side and was not chosen. That points at `_assign` / terminal provenance
rather than join topology. **Validate any such rule against `BRIDGE_MODEL` and the
twin-scan tests before running the battery** — that check is what killed ruled-out 2 in
under a minute, after a much slower detour.

## Traps

- **`ruff check . --fix` is destructive** with this venv's ruff 0.16 defaults, despite
  what AGENTS.md says. Use `--select E,F,I` on specific paths.
- **Never `git stash` / `checkout` / `reset`** — parallel agents share this tree. A/B by
  copying to the scratchpad.
- **Writing files from Python on Windows:** `pathlib.write_text()` defaults to `cp1252`
  and will raise on `→`, `×`, `—` — *after* truncating the file. This destroyed
  `docs/v4_network_discovery_design.md` this session (recovered from the `pre_cutover`
  commit). Always pass `encoding="utf-8"`, or use the Write/Edit tools.
- `tests/modeling/tpc_ds_duckdb/zquery*.log` and the perf PNGs churn on every battery
  run. That noise in `git diff` is expected, not yours.
- The cost model has now mis-read q05 three times. Prefer a failing query or a unit test
  over reasoning about cost tuples.

## Open items beyond q05

From §0.1, unchanged and untouched:

- **`_connect` fabricates bridges** for `query97-one`/`-two`: the cover
  `{customer.customers, item.items}` has two components and no shared keys, so a
  `catalog_sales` scan gets added as a bridge, restricting the axis. The root gap is that
  the search does not consume the domain map — `subset_join_map` / the ⊑ edges of
  `docs/domain_graph_design.md` are *extra effective traversal keys*, and
  `network_search.py` has zero references to them. It inherits the ≡ half only because
  `request.graph` is already canonicalized. Neither query is in the TPC-DS battery, so
  the 105/1 number does not cover this.
- **Nullability is not modeled** — zero references, though `Modifier.NULLABLE` is on the
  column assignment. `~` is a subset of *values*, `?` speaks to *rows*; only the first is
  in the cost axes.
- Union family selection, enumeration branch-and-bound, derived connectors, and the
  remaining §5 carry-over inventory.
