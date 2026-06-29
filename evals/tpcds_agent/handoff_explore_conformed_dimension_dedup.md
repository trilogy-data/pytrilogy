# Handoff: `explore` should deduplicate role-playing conformed dimensions (token bloat)

## Summary
`trilogy explore` prints the **full schema of a conformed dimension once per role it plays**, so a model that reuses one dimension across many roles (the TPC-DS norm) produces large, highly-redundant output. Concretely, `explore raw/all_sales.preql --regex date` emits **14,411 chars / ~3,600 tokens** that is mostly **8 identical copies** of the `date_dim` schema. `explore` is the single largest context contributor for the queries we profiled, so collapsing these duplicates is a high-leverage, model-agnostic token win.

## Evidence (TPC-DS q05, run `20260628-194910`)
- `explore` was **60% of q05's carried context** (vs `agent-info` 28%, `file` 6%, query **result sets only ~6%**). The lever is explore output, not result truncation.
- `explore raw/all_sales.preql --regex date` → `count: 110`, across 11 date-role namespaces, of which **8 are full `date_dim` expansions**, all identical in shape:
  `date`, `return_date`, and `{billing,purchasing,ship}_customer.{first_sales_date,first_shipto_date}` (3 customer roles × 2 date FKs + sold + return = 8).
- Each is the same ~12-property date dimension (`day_name`, `day_of_week`, `month_seq`, `quarter`, `week_seq`, `year`, …). The heavy property comments are repeated **8× verbatim** — e.g. the `week_seq` paragraph ("Monotonically-increasing week-of-time… ~53 per year… week_seq + 53") and the `month_seq` comment each appear 8 times.
- This is structural, not a q05 quirk: role-playing conformed dimensions (date, customer, address, item) are the standard TPC-DS shape, so most `explore`/`--regex` calls over `all_sales` pay this tax.

## Root cause / where it lives
- JSON payload: `build_concepts_payload` (`trilogy/scripts/explore.py:511-567`) builds `namespaces` via `_grouped_decls(env, local_items)` (line 564); each namespace renders its keys/properties in full Trilogy declaration syntax.
- The default view already collapses *imported* namespaces to name-only lists (lines 528-561) to avoid exactly this kind of noise — **but `--regex` deliberately bypasses that collapse** (documented at `explore.py:207`), and these date roles surface as in-namespace results, each rendered in full.
- Rich/human path: `_emit_namespace` (`explore.py:324`) / `_emit_local_groups` (`explore.py:231`) have the same per-namespace full render.

## Proposed fix — shape-signature dedup
Detect namespaces whose concept set is **structurally identical** (same role-played dimension) and emit the full schema for **one canonical role**, replacing each identical sibling with a one-line reference.

1. For each rendered namespace, compute a **shape signature**: the sorted tuple of its concept declarations with the namespace prefix stripped (leaf name + datatype + grain + comment). Two namespaces with the same signature are the same conformed dimension in different roles.
2. Group namespaces by signature. For a group of size > 1, pick a canonical member (shortest/most-central address, e.g. `date` over `ship_customer.first_shipto_date`), emit it in full, and emit the others as a reference:
   - JSON: `"ship_customer.first_shipto_date": {"same_shape_as": "date"}` (the agent infers `ship_customer.first_shipto_date.<leaf>` exists for every `date.<leaf>`).
   - Rich: `ship_customer.first_shipto_date  → same shape as date.* (12 properties)`.
3. Apply in **both** the JSON (`build_concepts_payload`) and rich (`_emit_namespace`) paths so behavior matches across `--format`.
4. Add a top-level `"conformed": {"date": ["date", "return_date", "billing_customer.first_sales_date", …]}` map (or similar) so the dedup is explicit and machine-readable, not just inferred.

## Constraints / edge cases
- **Exact-match only.** Collapse only when signatures are byte-identical. If `date` carries an extra `_date_string`/`text_id` that a customer date role lacks, they are NOT identical → render both in full (or show the canonical + a small diff). Do not lossily merge near-identical shapes.
- The reference must let the agent **reach every concept** it could before (`<role>.<leaf>`), so the canonical's leaf list must be complete and the sibling clearly pointed at it.
- Respect `--regex`: still collapse identical shapes *within* the regex result set (that's where the worst duplication shows up); the regex match set defines the candidates.
- Keep an **escape hatch**: `--expand-imports` (and/or a new `--expand-roles`) renders every role in full for the rare case the agent wants the literal per-role dump.
- Internal/`__`-prefixed namespaces stay hidden as today (lines 540, 545).

## Expected impact
- `all_sales --regex date`: ~14.4k → ~3k chars (one full `date_dim` + 7 one-line refs). ~75% reduction on that call.
- Generalizes to every `explore`/`--regex` over a model with role-playing dimensions (date/customer/address across all TPC-DS facts). Compounds because explore output is injected early and re-read across the whole query (the "carry" cost).

## Tests
- `explore raw/all_sales.preql --regex date` (JSON): exactly one full `date`/`return_date`-style schema; the other 7 date roles are `same_shape_as` refs; total size drops materially; every previously-listed `<role>.<leaf>` is still reachable (canonical leaf list complete).
- A model with a genuinely *different*-shaped same-named dimension does NOT collapse (exact-match guard).
- `--expand-imports`/`--expand-roles` restores full per-role output.
- Rich and JSON outputs agree on which roles collapsed.

## Pointers
- `trilogy/scripts/explore.py:511-567` — `build_concepts_payload` (JSON; add signature dedup here)
- `trilogy/scripts/explore.py:324` — `_emit_namespace` (rich path)
- `trilogy/scripts/explore.py:231` — `_emit_local_groups`
- `trilogy/scripts/explore.py:207` — note that `--regex` bypasses the import collapse (why these surface in full)
- `trilogy/scripts/explore.py:528-561` — existing imported-namespace collapse (precedent for the token-saving pattern)
