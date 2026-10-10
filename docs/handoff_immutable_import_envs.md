# Handoff: immutable import environments — open increments

Parsed import `Environment`s are shared across top-level parses through
`trilogy/parsing/v2/import_service.py::_IMPORT_ENV_STORE`, and aliased imports
reuse memoized namespaced projections (`with_namespace` fan-out is zero in
steady state). Cached child envs are immutable only by convention, enforced
after the fact by closure text re-hashing and the `_env_integrity` stamp. Every
defect found building the store was the same shape — **leaked mutability
through shared objects** — so the remaining work makes the contract explicit.
Re-profile before starting; the parse floor has moved since this was written.

## Open increments

**(2) Formalize immutability.**

- Move datasource `status` (runtime state) OFF the author object, into a
  parent-env-scoped state map keyed by datasource identifier. Writers today:
  - `trilogy/dialect/metadata.py:70-72` flips `datasource.status` during
    warehouse metadata sync. Caught by the integrity stamp (entry evicted,
    re-parsed) but it is runtime state written onto author objects.
  - `trilogy/core/query_processor.py` (persist processing) sets
    `ds.status = UNPUBLISHED` and restores it in a `finally`. Steady-state
    safe, thread-hostile.
  This deletes both surfaces and the status tuple from both the session
  build-cache stamp (`_session_build_caches`) and `_env_integrity`.
- Freeze cached child envs (`Environment.frozen` exists and blocks
  `add_concept` / `add_datasource` / `add_import`), and add a debug-flag
  `__setattr__` tripwire on Concept/Datasource for post-registration writes
  so new mutation sites fail in CI rather than by corpus divergence.

**(3) `trilogy/core/validation/fix.py` mutates `concept.keys` in place**
(the `ConceptDeclarationStatement` and `PropertiesDeclarationStatement`
branches, `statement.concept.keys = new_keys` / `concept.keys = new_keys`).
One-shot CLI today and NOT stamp-covered (concept-level), so a fix run inside
a process that shares the store can poison it. Rewrite as copy-on-write
(`replace(concept, keys=...)` written back to the statement), as the
datasource key propagation in `datasource_rules.py` already does.

**Constraint for any design that swaps or wraps concept objects:** rowset and
multiselect concepts are exempt from `add_concept`'s re-registration dedup
because their lineage embeds the statement's SelectLineage OBJECT, matched by
identity against `named_statements`.

## Gates (run ALL per increment)

1. `tests/parsing/test_import_env_store.py` +
   `tests/parsing/test_reparse_content_version.py`.
2. **Corpus SQL identity, three legs in ONE process**: store off / on-cold /
   on-warm over all `query*.preql` (tpc_ds_duckdb + tpc_h), sha256 per query.
   One process matters: cross-parse sharing is the configuration under test.
3. Full repo suite `-m "not adventureworks_execution"` — store defects have
   surfaced in funnel_analysis (rowset identity) and gcat `test_should_group`
   (key poisoning), neither visible to the corpus gate. Never run two pytest
   processes concurrently.
4. `ruff check . --fix`, `mypy trilogy`, `black .`.

Judge cost by call counts (`address_with_namespace`, `model_construct`) first,
wall time second — the box is spiky.
