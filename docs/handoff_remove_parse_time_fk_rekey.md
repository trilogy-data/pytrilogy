# Handoff: remove parse-time FK key re-assignment

## STATUS: PROPOSED by maintainer (2026-08-04, s68). Feature currently CoW-patched, not removed.

Audience: an agent starting fresh. The hypothesis to prove or refute:
**the parse-time rewrite of a KEY concept's `keys` from datasource grain is
redundant in v4** — the build-time concept relationship machinery (FD
closure / domain graph) can derive the same facts from the datasource
declaration itself, so the author-layer mutation can be deleted.

## The feature

`trilogy/parsing/v2/rules/datasource_rules.py::datasource_node`, tail block
("Propagate keys from datasource grain to foreign key concepts", v1
second-pass parity): when a datasource binds a `Purpose.KEY` concept that is
NOT part of the datasource's grain, that concept's `keys` are REWRITTEN to
the grain components (unless any grain component already lists it in its own
keys).

Concrete effect (gcat corpus): `vehicle.stage.engine.name` starts with
`keys={'vehicle.stage.name'}`; parsing `fuel_dashboard.preql` (datasource
`fuel_aggregates` at `grain (launch_tag, vehicle.stage_no, vehicle.name,
vehicle.variant)` binding it as a non-grain column) rewrites its keys to
`{'local.launch_tag', 'vehicle.name', 'vehicle.stage_no', 'vehicle.variant'}`.

s68 found this mutating SHARED concept objects in place (bare imports share
objects with the cross-parse cached child env), silently poisoning later
parses. It is now copy-on-write: if the target concept is the durable env
object, a `dataclasses.replace(target_c, keys=new_keys)` is registered on
the parent env instead. Removal would delete this whole mutation class; the
CoW branch and its regression test
(`tests/parsing/test_import_env_store.py::
test_datasource_key_propagation_does_not_poison_store`) become deletable.

## Who consumes `concept.keys` (inventory starting points)

- **Grain reduction** — `trilogy/core/models/build.py::
  concepts_to_build_grain_concepts` (~lines 341–354): drops a concept from a
  grain when its keys are covered by the other components; three rules
  (property-keys-subset, KEY-with-covered-keys, etc.). This is what the
  canary test asserts (below). BuildConcept keys come straight from author
  keys (`_build_keys(base.keys)` at build.py:3250/3380).
- **FD closure** — `trilogy/core/processing/v4_helper/functional_dependency.py`:
  fact tuples `(address, grain components, keys)` gathered for concepts AND
  — critically — **datasource columns separately** (see the dedup comment at
  ~line 85: a datasource column can carry a different grain than the env's
  concept). So the closure ALREADY ingests datasource-level grain facts
  without needing the author-concept rewrite. This is the strongest
  evidence for the hypothesis; verify the closure actually derives the same
  determinations for the gcat case.
- **Domain graph** — `trilogy/core/domain_graph.py` structural edges read
  key/grain relationships; check `structural_domain_edge`.
- Planner enrichment paths that follow `keys` for property→key joins
  (search `\.keys` under `trilogy/core/processing/`), window/property
  handling in `concept_rules.py` (NumberingWindowItem widening — that site
  mutates freshly minted concepts, unrelated, leave it).
- Author-layer visibility: `explore`/LSP metadata may display keys; a
  removal changes what users see for FK-bound concepts.

## Validation protocol (instrumentation first — do NOT judge by coverage)

Per the project rule (validate dead/removable code by instrumentation + a
corpus sweep, and gate against the CURRENT tree in one process):

1. Instrument the rekey site: log `(datasource, concept address, old keys,
   new keys)` and run the full corpus + modeling suites. This yields the
   complete set of rewrites the corpora exercise. Expect gcat
   (`fuel_dashboard*.preql`, `launch_dashboard.preql`) to dominate; the
   tpc corpora may not trigger it at all (their models declare keys
   explicitly) — an empty tpc log is NOT evidence of removability for gcat.
2. Disable the rewrite (flag, not deletion) and diff generated SQL per-query
   across both corpora in one process, plus run the result-validating
   suites. Every plan change maps to a rewrite from step 1; for each,
   determine whether the FD closure / BuildGrain reduction already knows the
   relationship from the datasource column facts — if not, THAT is the gap
   to close in build-time machinery before removing the parse-time write.
3. Likely landing shape: extend `concepts_to_build_grain_concepts` (or the
   FD closure it should consult) to use datasource-column key/grain facts,
   then delete the parse block + CoW + regression test, keeping a NEW test
   that pins the gcat reduction through the build-time path.

## Canaries

- `tests/modeling/gcat/test_gcat.py::test_should_group` — asserts
  `vehicle.stage.engine.name` reduces OUT of a pregrain
  (`BuildGrain.from_concepts`) AND validates row counts
  (`launches == launch_count`). This is the test that caught the s68
  poisoning; it must pass with the rewrite gone.
- gcat suite broadly (36 tests) — the corpus that actually exercises
  FK-bound datasources at wider grains.
- `tests/parsing/test_import_env_store.py` — the poisoning regression stays
  green during the transition, gets deleted with the feature.

## Gates

Same set as s68 (see `docs/handoff_immutable_import_envs.md`): corpus
byte-identity in one process against the current tree — EXPECT diffs in
step 2 while the flag is off (that is data, not failure); the landing gate
is byte-identity between "rewrite removed + build-time derivation" and the
current tree, 132/132, plus full repo suite, ruff/mypy/black.

## Why this is worth it

Deletes the last known parse-path in-place mutation of shared author
objects, which unblocks the immutable-import-env work (sibling handoff)
without CoW carve-outs; moves a semantic fact from a parse-order-dependent
side effect into the declarative build-time relationship map where v4 keeps
its other grain knowledge; and removes a behavior that is invisible in the
source (a concept's declared keys silently change because a *different
file* declared a datasource).
