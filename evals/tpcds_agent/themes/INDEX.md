# Why Trilogy trails raw SQL on TPC-DS — theme index

**Headline (clean full runs, same harness):** raw SQL `sql_bare` **72–74/99** vs Trilogy
`enriched` **60–64/99** — a persistent **~−12** gap, at **~2.2×** the tokens. The eval's core
question ("what does Trilogy buy over raw SQL?") is currently NEGATIVE.

- Trilogy genuinely UNLOCKS 11 queries SQL can't get (complex multi-hop/semantic): q2, q12, q16,
  q29, q31, q45, q57, q58, q67, q97, q98. That value is real.
- But it LOSES 23 the same agent nails in SQL: q10, q11, q14, q25, q30, q35, q36, q37, q38, q46,
  q47, q49, q51, q62, q64, q65, q73, q74, q76, q78, q81, q85, q88. Net +11 −23 = **−12**.

We diagnosed all 23 (one read-only agent each: root cause + canonical-matches-reference check +
a harvested agent-confusion quote). **Every canonical `.preql` matches its reference** — so these
are NOT framework limits (except q51, a real engine bug). They cluster into 3 themes + 1 bug.

## The themes (see per-theme docs)

| theme | # of 23 | queries | dominant fix lever |
|---|---|---|---|
| **[C — business-language → concept mapping](theme_C_business_language_mapping.md)** | **12** | q11,25,37 (surrogate vs business key); q46,73,74,85,88 (role); q64,78 (extended vs per-unit); q36,49 (format/spec) | prompt wording + model-comment disambiguation |
| **[A — null-preservation ("no INNER join")](theme_A_null_preservation.md)** | 5 (+q59,q84) | q10,35,38,62,65 | guidance idiom + the [coalesced-key spike](../handoffs/spike_side_qualified_condition_vs_coalesced_key.md) |
| **[D — Trilogy idioms](theme_D_trilogy_idioms.md)** | 5 | q30,81 (avg-scope); q14 (rollup-timing); q47 (window-vs-filter); q76 (union-count) | guidance / syntax-example fixes + prompt |
| **[engine bug — q51](../handoffs/bug_q51_null_unsafe_multiwindow_reassembly.md)** | 1 | q51 | engine fix |

## The meta-lesson

The agent knows SQL well; it stumbles on **(C) mapping business words to the right concept when
the model over-provides** (surrogate vs business key, current-snapshot vs sale-time role,
extended vs per-unit measure) and **(A) Trilogy's row-preserving joins** (SQL's implicit
inner-join-drops-nulls has no Trilogy equivalent, so a null-key group leaks). Both are recoverable
without engine work: **C** by prompt wording + sharper/cross-referenced model comments; **A** by a
single load-bearing guidance idiom (*"there is no INNER join — add `<key>.id is not null` to drop
unmatched/null-key rows"*). **D** is a grab-bag of real Trilogy idioms that want better
examples/guidance. Closing even half of the 23 flips the funnel positive.

A striking cross-cutting tell (A + several C): **the agent SEES the defect and rationalizes it** —
"the NULL group is valid", "the result looks clean", "a null in the data is fine". The guidance
should give it a stop-and-check heuristic for exactly these moments.

## Fix-priority (by queries recoverable per unit of effort)
1. **C1 surrogate-vs-business key** (q11,17,25,37) + **C2 role** (q46,73,74,85,88): a shared
   model-comment/preamble convention ("codes = `text_id`; joins = surrogate `id`; bare dimension =
   the sale/line role, not the customer's current snapshot") — ~9 queries, one lever.
2. **A null-preservation guidance** — ~5–7 queries, one idiom line.
3. **D avg-scope** — replicate the q30 prompt fix to q81 + update the
   `nested-aggregate-group-average` syntax example (agents copy it verbatim).
4. **q51 engine bug** — handoff.
