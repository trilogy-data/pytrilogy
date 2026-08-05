# Handoff: remove parse-time FK key re-assignment

## STATUS: LANDED (2026-08-04, s69). The parse-time rewrite is deleted.

The hypothesis — **the parse-time rewrite of a KEY concept's `keys` from
datasource grain is redundant in v4** — was CONFIRMED, but not in the shape the
proposal predicted. The fact is not redundant; the *write* is. The datasource
declaration already carries it, so it is now derived on demand from
`Environment.fk_derived_keys()` instead of being written back onto the concept.

## What the feature was

`trilogy/parsing/v2/rules/datasource_rules.py::datasource_node`, tail block:
when a datasource bound a `Purpose.KEY` concept that was NOT part of the
datasource's grain, that concept's `keys` were REWRITTEN to the grain
components (unless a grain component already listed it in its own keys). s68
had made this copy-on-write to stop it poisoning shared cached import envs.

## What replaced it

`Environment.fk_derived_keys()` (`core/models/environment.py`) computes the
same map — `address -> frozenset(grain components)` — as a pure function of
`environment.datasources`, cached on
`(datasources.content_version, concepts.content_version)`.

`Concept.effective_keys(environment)` (`core/models/author.py`) is the single
accessor: `keys` when the concept has them, the datasource-implied keys when it
does not. **Every reader that treats `keys` as a functional dependency goes
through it.** The five that mattered (found by instrumentation, see below):

- `Concept.get_select_grain_and_keys` — author-layer select grain/keys
- `parsing/common.py::concept_is_relevant` — the KEY-with-covered-keys drop
- `parsing/common.py::concept_list_to_keys` and `function_to_concept` (x2 sites)
- `core/domain_graph.py::mint_fd_edges` — the DECLARED FD edge
- `core/where_scope_normalization.py::_covered_by_grain`
- `core/models/build.py::Factory.__build_concept` (both construction sites)

and one snapshot site, `parsing/v2/rowset_semantics.py::_rowset_concept`: a
rowset output copies its source's identity into a rowset namespace that has no
datasource, so the derived keys must be baked in there rather than re-derived.

## Evidence (the validation protocol, run as written)

**Step 1 — instrumentation.** The proposal guessed "the tpc corpora may not
trigger it at all". That was wrong and it is the single most important
correction: the corpora exercise **128 distinct rewrites**, dominated by
`catalog_sales` (16), `catalog_returns` (15), `web_sales` (14) — the classic
dimension FK on a fact table (`return_customer.sk` keyed to
`(item.sk, order_number)`). gcat contributes 17.

**Step 2 — disable and diff.** With the rewrite off and nothing replacing it,
**25 of 132** corpus queries changed, mostly much larger SQL (q64 +6080 chars,
q81 +2873, q30 +2530). Three got *smaller* (q80 −564, q70 −79, q54 −38). So the
rewrite is load-bearing, not dead.

**The FD closure does already know the datasource facts.** Probing
`build_fd_closure` against the built environment with the rewrite off: every
non-grain KEY column of every datasource is derivable from its datasource's
grain — 54/54 for q30, 86/86 for q64, 7/7 for tpc-h q17, 29/29 for gcat. What it
cannot do is reach concepts the parse layer *derives* (rowset aliases,
namespaced import copies), because those snapshot `keys` at creation and have no
datasource of their own. That is why the answer is an accessor on the concept
rather than a query against the closure at each use site.

**Step 3 — landing gate.** With the rewrite deleted and every reader routed
through `effective_keys`: **132/132 byte-identical** — both in one process
against the same tree with the flag toggled, and against a `HEAD` worktree
render with `PYTHONHASHSEED=0`.

## Dead ends recorded so they are not re-tried

- **An FD-closure-based reduction inside `concepts_to_build_grain_concepts`**
  (drop a keyless KEY component when the rest of the grain FD-determines it)
  fixes the gcat canary and 15 of the 25 diffs, but is a strict no-op once
  `effective_keys` is in place. It was implemented, measured, and removed.
- **Deriving keys only at build time** (in `Factory`) closes q30/q81/q18 but
  cannot close q17/q64: the author layer computes select grain, domain-graph FD
  edges and rowset identity *before* any build env exists.

## Canaries (all green)

- `tests/modeling/gcat/test_gcat.py::test_should_group` — `vehicle.stage.engine.name`
  still reduces out of the pregrain, and `launches == launch_count` still holds.
- `tests/parsing/test_datasource_fk_derived_keys.py` — new: pins the derivation,
  the grain reduction it drives, and that it tracks a datasource added later.
- `tests/parsing/test_import_env_store.py::test_datasource_fk_binding_never_rewrites_a_concept`
  — the s68 poisoning regression, rewritten: the concept is now never mutated in
  either environment, so the hazard is structurally gone rather than patched.

## Follow-on

This deletes the last known parse-path mutation of author concept objects, so
`docs/handoff_immutable_import_envs.md` no longer has to design around a
copy-on-write carve-out.
