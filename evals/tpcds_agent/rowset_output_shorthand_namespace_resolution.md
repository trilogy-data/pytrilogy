# Feature: resolve a rowset output by leaf shorthand (`rs.col`) when unambiguous

**Status:** DONE (2026-06-26). Implemented + tested. Resolution is **parse-time only** —
returns the canonical full-path concept; the short string is never persisted. Two resolvers:
- `ConceptLookup._resolve_rowset_suffix` (`trilogy/parsing/v2/semantic_state.py`) — the
  load-bearing one (select-finalize goes through `ConceptLookup.get`/`require`). Pending-aware
  (consuming select validates before the rowset's COMMIT phase), gated by `RowsetItem` lineage
  over committed + pending concepts so it never collapses import paths.
- `EnvironmentConceptDict._try_resolve_namespace_suffix` (`trilogy/core/models/environment.py`) —
  for direct `env.concepts["rs.col"]` lookups (post-commit), gated by `rowset_namespaces`.

Tests: `tests/test_rowset_output_shorthand.py` (basic/partial/isolation/import-strict/ambiguity/exec);
`tests/test_undefined_concept.py` two prior "suggest don't resolve" negative tests rewritten to
assert resolution. Original design notes below kept for reference.

**Status (original):** OPEN — design agreed (allow the shorthand instead of fighting it with guidance).
**Surfaced by:** the single most expensive recurring agent stumble across the TPC-DS enriched eval
(q02/q05/q11/q23/q64 — 5+ queries, many round-trips each). Repeated AI-guidance examples have not
stopped it; the decision is to **support the natural shorthand** rather than keep correcting agents.
**Severity:** MEDIUM (no crash — burns agent tokens / iterations).

## Problem

A rowset keeps each **unaliased** output at its FULL source path under the rowset namespace:

```trilogy
import raw.all_sales as s;
with base_sales as
  where s.channel in ('WEB','CATALOG')
  select s.date.week_seq, s.date.day_name, sum(s.ext_sales_price) as day_sales;

select base_sales.week_seq, base_sales.day_sales limit 5;
```

`base_sales` actually exposes:
- `base_sales.s.date.week_seq`   ← full source path kept
- `base_sales.s.date.day_name`   ← full source path kept
- `base_sales.day_sales`         ← aliased output, short

So `base_sales.week_seq` raises `Undefined concept: base_sales.week_seq. Suggestions:
['base_sales.s.date.week_seq', ...]`. Agents then guess increasingly wrong forms
(seen in q02: `base_sales.s.sum.s.ext_sales_price`) and loop.

## Desired behavior

Allow `abc.cde` to resolve to a full output address `abc.<...>.cde` **iff exactly one** output
under namespace `abc` matches the shorthand. If two outputs collide on the suffix — e.g.
`abc.s.date.week_seq` AND `abc.s.return_date.week_seq` both reachable as `abc.week_seq` — **raise a
clear ambiguity error** (do NOT silently pick one). Zero matches → existing undefined-concept error.

The match is an **ordered-subsequence on dotted segments**, scoped to the leading namespace:
`abc.week_seq` → segments `[abc, week_seq]`; `abc.s.date.week_seq` → `[abc, s, date, week_seq]`;
`[abc, week_seq]` is a subsequence ⇒ match. Partial paths work too: `abc.date.week_seq` matches
`abc.s.date.week_seq`. The leading segment must be equal (so it only collapses WITHIN a rowset, never
across unrelated namespaces).

## The machinery already exists — reuse it

`trilogy/core/models/environment.py` already has both halves; today they only feed *suggestions*:

- `_is_subsequence(needle, haystack)` (`environment.py:126`) — exactly the match rule above.
- `_find_similar_concepts` (`environment.py:341`) already computes `path_matches` with
  `_is_subsequence(q_segs, candidate_segs)` gated to `len(q_segs) >= 2` (`environment.py:366-376`).
  That candidate set IS the shorthand resolution set — promote it from "suggestion" to "resolution
  when the set has size 1."

Rowset namespaces are registered in `self.named_statements` (`add_rowset`, `environment.py:505`), so
the leading segment can be confirmed to be a rowset before collapsing.

## Suggested implementation

Add a resolver in `Environment.__getitem__` (`environment.py:266`), AFTER `_try_resolve_derived`
(so real derived concepts like `signup_date.year` still win, `:286-288`) and BEFORE the
`fail_on_missing` / `raise_undefined` fallback (`:289-306`):

```python
resolved = self._try_resolve_namespace_suffix(key)
if resolved is not None:
    return resolved
```

```python
def _try_resolve_namespace_suffix(self, key: str) -> Concept | None:
    q_segs = key.split(".")
    if len(q_segs) < 2:
        return None
    head = q_segs[0]
    # Scope to rowset namespaces (start here to bound blast radius; could widen later).
    if head not in self.named_statements:
        return None
    # Candidates = committed + STAGED rowset outputs under `head.` (see gotcha 1).
    candidates = [
        k for k in self._candidate_addresses()
        if k != key and k.startswith(head + ".") and _is_subsequence(q_segs, k.split("."))
    ]
    if len(candidates) == 1:
        return self.data[candidates[0]]      # canonical concept; its .address is the full path
    if len(candidates) > 1:
        raise UndefinedConceptException(
            f"Ambiguous reference {key!r}: matches {sorted(candidates)}. "
            "Qualify the full path to disambiguate.",
            sorted(candidates),
        )
    return None
```

Optionally register the resolved short address as a pseudonym/alias so repeat lookups are O(1) and
build-time sees a stable address.

## Gotchas / edge cases (call these out in the implementation)

1. **Staged/pending outputs.** The first reference to a rowset output can occur BEFORE the parse
   commits the rowset's concepts — this is why `_find_similar_concepts` takes `extra_keys` (the
   staged set, `_pending_by_address` / `ConceptLookup.values()`). The resolver's candidate set must
   include those staged outputs too, or shorthand on a freshly-defined rowset won't resolve. Verify
   whether resolution at the consuming `select` is pre- or post-commit and source candidates
   accordingly.
2. **Aliased outputs are already short** (`base_sales.day_sales`) — unaffected; they won't have a
   longer full-path twin, so no spurious ambiguity.
3. **Leading-segment gate is load-bearing.** Without `k.startswith(head + ".")` a bare leaf could
   collapse into an unrelated deep path. Keep the `len(q_segs) >= 2` + head-equality gates (mirrors
   the existing `path_matches` gating).
4. **Ordering vs derived resolution.** Run AFTER `_try_resolve_derived` so `rs.some_date.year`
   (a real derived) isn't shadowed by a coincidental suffix collapse.
5. **Return the canonical concept** (full-path `.address`) so the build planner keys off the real
   address, not the shorthand string.
6. **Don't recurse.** Mirror the `_resolving` guard used by `_try_resolve_derived` (`:312-320`) if
   the resolver can re-enter `__getitem__`.
7. **Scope decision:** recommend gating to rowset namespaces (`named_statements`) FIRST. Whether to
   extend the same shorthand to import namespaces (`s.week_seq` → `s.date.week_seq`) is a separate,
   riskier call — leave import namespaces requiring full paths unless explicitly wanted.

## Test matrix

- `with r as select s.date.week_seq, sum(x) as m; select r.week_seq` → resolves to `r.s.date.week_seq`.
- Ambiguous: arm projects both `s.date.week_seq` and `s.return_date.week_seq` → `r.week_seq` raises
  ambiguity, listing both full paths.
- Partial path: `r.date.week_seq` → `r.s.date.week_seq`.
- Aliased output unaffected: `r.m` still resolves directly.
- Cross-rowset isolation: `r1.week_seq` and `r2.week_seq` resolve independently (no leakage).
- Zero match: `r.nonexistent` → unchanged `Undefined concept` error with suggestions.
- Non-rowset namespace left strict (if scoped to rowsets): `s.week_seq` still errors (or document if
  intentionally allowed).
- Build-through: a resolved-shorthand query `generate_sql`s and executes (the address is canonical).
```
