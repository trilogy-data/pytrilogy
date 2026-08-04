# Handoff: immutable import environments + contextual overlays

## STATUS: NOT STARTED (written 2026-08-04, s68)

Audience: an agent starting fresh on restructuring how imported environments
are shared and flattened. Read `docs/handoff_remove_parse_time_fk_rekey.md`
first — that increment deletes one whole mutation class and should land
before or alongside this work.

## Why (evidence from s68)

s68 landed a cross-parse import environment store
(`trilogy/parsing/v2/import_service.py::_IMPORT_ENV_STORE`): parsed import
`Environment`s are shared across top-level parses, validated on reuse by
re-hashing the transitive text closure plus an env-integrity stamp. It works
(q03 fresh parse 23.6ms → 7.0ms; corpus render 30.7s → 17.6s), but every
defect found on the way was the same shape: **leaked mutability through
shared objects**. The integrity stamp, the taint machinery, and a
copy-on-write patch are all detection/repair for an immutability contract
the code does not express. Maintainer direction: lean into immutable base
import envs + contextual overlays instead.

The second motivation is cost. After the store, the parse floor is
`Environment.add_import` flattening: ~47% of parse profile, 57k
`address_with_namespace` calls per 10 q03 parses, residual ~28–32ms on
q64/q77. Aliased imports copy every concept/datasource per edge via
`with_namespace`; an overlay/memoized model removes that.

## Current architecture (read before designing)

`Environment.add_import` (`trilogy/core/models/environment.py`) has two modes:

- **Aliased** (`import x as y`): every concept/datasource is COPIED via
  `with_namespace(alias)` — fresh objects per edge, internal address
  references (keys, grain components, lineage ConceptRefs, pseudonyms)
  REWRITTEN under the alias. Safe but expensive; this is the flatten cost.
- **Bare** (`import x`, alias == DEFAULT_NAMESPACE): objects are inserted
  with ZERO copying — the parent env and the (now process-cached) child env
  share Concept/Datasource objects. Fast but the source of every hazard.

The store treats cached child envs as immutable-by-convention, enforced by:

- closure text re-hash (file edits invalidate transitively),
- integrity stamp `_env_integrity`: child dict `mutations` counters + per-ds
  `(status, len(columns))` — catches the KNOWN in-place writers,
- `_ClosureFrame.tainted` (cycle/depth stubs, dict-resolver text never cached).

### Known mutation surfaces (s68 audit — the complete list found)

1. `parsing/v2/rules/datasource_rules.py` datasource_node key propagation —
   WAS in-place `target_c.keys = ...` on possibly-shared concepts; now
   copy-on-write (`replace(target_c, keys=...)` written to the parent env).
   Pinned by `tests/parsing/test_import_env_store.py::
   test_datasource_key_propagation_does_not_poison_store`. Candidate for
   full removal — see the sibling handoff.
2. `dialect/metadata.py:70-72` — `datasource.status` flips during warehouse
   metadata sync. Runtime state written onto author objects; caught by the
   integrity stamp (entry evicted, re-parsed).
3. `core/query_processor.py:1271-1300` — transient status flip/restore
   around persist processing. Steady-state safe, thread-hostile.
4. `core/models/environment.py::validate_concept.handle_currently_bound_sources`
   — rebinds `datasource.columns` when a persisted concept is redeclared
   with different lineage. Caught by the stamp's column-count.
5. `core/validation/fix.py:162,185` — `concept.keys = ...` in the CLI
   validation-fix flow. One-shot CLI today; NOT stamp-covered (concept-level).
   Accepted risk, must be resolved by this refactor.

Also relevant: `add_concept` re-registration dedup keeps durable objects via
`concept_structural_signature`; rowset/multiselect concepts are exempt
because their lineage embeds the statement's SelectLineage OBJECT matched by
identity against `named_statements` (see s68 memory — this identity coupling
constrains any design that swaps or wraps concept objects).

## The crux the design must solve

A naive read-through overlay (dict-key mapping per alias) does NOT work for
aliased imports: the VALUE objects' internal addresses differ per alias —
`with_namespace` rewrites keys/grain/lineage/pseudonym addresses inside each
concept. Any view must translate addresses on read, everywhere a consumer
follows a reference. That is invasive: build, domain graph, FD closure,
renderer, LSP helpers all read `env.concepts` / concept internals directly.

## Recommended increments

1. **Memoized namespaced projections** (cheap, most of the win, low risk):
   keep flattening semantics, but cache the `with_namespace` product per
   (cached child env entry, alias) on the store entry, invalidated with it.
   `add_import` becomes dict merges of shared immutable copies; re-import of
   the same file+alias reuses identical objects, which also makes the
   `content_version` dedup identity-cheap (no signature compares). The
   sharing risk class is the same one the bare path already has, and the
   audit above is the checklist: land AFTER the FK-rekey removal so surface
   (1) is gone rather than CoW-patched.
2. **Formalize immutability**: freeze cached child envs
   (`Environment.frozen` exists and blocks add_concept/add_datasource/
   add_import); add a debug-flag `__setattr__` tripwire on Concept/
   Datasource for post-registration writes to catch new mutation sites in CI
   rather than by corpus divergence. Move datasource `status` (runtime
   state) OFF the author object — parent-env-scoped state map keyed by
   datasource identifier — which deletes surfaces (2)/(3) and the status
   tuple from both the session stamp and the store integrity stamp.
3. **True overlay reads** only if profiles still justify it after (1):
   bare imports resolved through layered views, aliased imports through
   address-translating views. Expect a long tail of direct readers; measure
   first — increment (1) may leave nothing worth the invasiveness.

## Gates (the s68 set — run ALL per increment)

1. `tests/parsing/test_import_env_store.py` +
   `tests/parsing/test_reparse_content_version.py` (13 tests: reuse
   equivalence, transitive invalidation, integrity eviction, cycle taint,
   dict-resolver bypass, params keying, rekey-poisoning regression, serve
   bundle survival).
2. **Corpus byte-identity, three legs in ONE process**: store off / on-cold /
   on-warm over all 132 `query*.preql` (tpc_ds_duckdb + tpc_h), sha256 per
   query, all legs identical. One-process matters: cross-parse sharing is
   the configuration under test. (~60-line harness; s68 scratchpad
   `gate_corpus_ab.py`.)
3. Full repo suite `-m "not adventureworks_execution"` — the s68 defects
   were caught by funnel_analysis (rowset identity) and gcat
   test_should_group (key poisoning), neither of which the corpus gate can
   see. NEVER two pytest processes concurrently.
4. `ruff check --select E,F,I <changed>`, `mypy trilogy`, `black .`.

## Reference numbers (2026-08-04, quiet box)

q03 fresh parse: 23.6ms (store off) / 7.0ms (on). q64: 56.5 / 27.8ms.
q77: 67.6 / 32.1ms. Corpus render: 30.7s / 17.6s. Rust pest parse of all 17
q03 files: 6.9ms cold, lru_cached warm — the parse cost is Python object
construction, not parsing; that is why increment (1) targets the copy
fan-out and why an on-disk compiled format (".preqlc") was rejected as the
wrong layer. Box is spiky: judge by call counts
(`address_with_namespace`, `model_construct`) first, walls second.
