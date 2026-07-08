# Theme A — Null-preservation: "there is no INNER join"

**One line:** SQL's implicit inner joins silently drop rows whose FK is NULL or unmatched; Trilogy
joins are ROW-PRESERVING, so those rows survive, and with `order by … nulls first … limit N` the
leaked NULL-key group fills the top of the output and corrupts the result. The fix is a single
explicit `<joined-key>.id is not null`.

## Mechanism

The reference SQL relies on a comma/inner join — e.g. `FROM store_sales, store WHERE
ss_store_sk = s_store_sk` — which drops the ~129k `store_sales` rows with a NULL `ss_store_sk`.
Trilogy has no inner join; the `ss.store.*` path is a row-preserving (LEFT/FULL) enrichment, so
those NULL-store rows survive as an all-NULL group. Under `nulls first` + `limit`, that group
sorts to the top and displaces legitimate rows → silent wrong result.

**The characteristic tell:** the agent RUNS its query, SEES the NULL group at the top, and
**rationalizes it as valid** instead of recognizing a missing intersection filter.

## Representative agent-confusion quote (q10)

> "I notice there's a NULL group at the end which represents customers whose demographics have
> NULL values. The question asks for 'only including customers that have such a current
> customer-demographics record' — NULL values in the demographics fields means some customers
> have demographics but with unknown values. That's fine, the NULL group is valid."

The prompt literally said *"only including customers that have such a record"*; the agent read the
null group as "has a record, values unknown" rather than "no record → must be filtered." Same
pattern verbatim elsewhere: q65 *"store_name: has some nulls (shown first due to nulls first
ordering)"* → *"The result looks clean."*; q35 *"Everything looks correct."* right after its own
stats reported `nulls: 41` on every demographics column.

## Representative error / diff signature (q65)

- No exception; both sides return 100 rows. Enriched top-100 is **entirely** `store_name = NULL`,
  revenue ~0.5–5; reference top-100 is `store_name = 'able' …`, revenue ~17–33. **100/100 rows
  differ.** One-line fix `and ss.store.id is not null` → byte-matches the reference.

## Affected queries (5 of the 23; + 2 supporting)

| query | the missing filter | effect |
|---|---|---|
| q10 | `customer.demographics.id is not null` | +1 all-NULL demographics group → 7 rows vs ref 6 |
| q35 | `customer.demographics.id is not null` | NULL-current-cdemo group displaces 1 row past LIMIT 100 |
| q62 | `warehouse.id is not null` (agent used `warehouse.name is not null`) | dropped a valid-FK/NULL-name warehouse group the ref keeps |
| q65 | `store.id is not null` | 129k NULL-store rows fill the entire top-100 |
| q38 | count a non-null key over the NULL-safe INTERSECT (see also Theme D) | `count(last_name)` skips the all-NULL group → 0 vs 107 |
| q59 (supp.) | intersection via a non-key anchor attr, NOT the coalesced join key | 18 next_year-only rows leak; see the spike |
| q84 (supp.) | `is not null` on the demographic join key | NULL=NULL cross-join → 61k rows vs 16 |

Note q62 is the inverse flavor: the agent DID add a not-null but on the wrong column — a nullable
DISPLAY attribute (`warehouse.name`) instead of the FK/`id`. "Recorded/present" means the *key*
is present.

## Fix lever — guidance (not engine; the engine is behaving as designed)

Add a load-bearing idiom to the always-loaded reference and/or the `existence-anti-join` /
`scoped-join` examples:

> **There is no INNER join in Trilogy — every join keeps all rows.** A SQL query that inner-joins a
> dimension (dropping rows whose FK is null or unmatched) must be reproduced by adding an explicit
> `<joined-key>.id is not null`. "Only customers/stores/items that HAVE a <record>", "where <X> is
> recorded/known" all mean `<key>.id is not null` — filter the KEY, not a display attribute.
> **Heuristic:** if a NULL-key group appears at the top of a `nulls first` result, you are almost
> certainly missing this filter — do not accept it as "present but unknown."

The harder case — enforcing intersection on a coalescing subset/union JOIN key, where a
side-qualified `<side>.<key> is not null` is a no-op — is the separate engine spike:
[`spike_side_qualified_condition_vs_coalesced_key.md`](../handoffs/spike_side_qualified_condition_vs_coalesced_key.md)
(q59, q84). Landing that lets the guidance say the intuitive thing (`where <side>.<key> is not
null`) instead of the current non-obvious "filter a non-key attribute of the anchor side."

**Estimated recovery:** ~5 of the 23 directly (q10, q35, q62, q65, q38), plus q59/q84 once the
spike lands.

## Prompt-audit outcome (2026-07-08) — made the null-exclusion explicit in the question

The reference drops null-key rows as a SIDE EFFECT of its SQL inner join; several prompts never
stated it. Audited all Theme A prompts and made the condition explicit so both legs are told it:

| q | reference drops | prompt before | edit applied |
|---|---|---|---|
| q35 | null `c_current_cdemo_sk` | **silent** | added "include only customers who have such a record (exclude any with no linked demographic-profile record — no catch-all null group)" |
| q65 | null `ss_store_sk` | **silent** | "For store sales in 1998 **that were made at a recorded store (exclude any sale with no associated store)**, …" |
| q10 | null `c_current_cdemo_sk` | had the clause; agent misread null attrs as "record present" | sharpened: "a demographic record must be linked … customers with no linked record — which would otherwise appear as a single all-null demographics group — must be excluded" |
| q62 | *reverse*: ref KEEPS valid-key/null-name rows | said "recorded" (ambiguous) | clarified "recorded = identifier present; a linked entry with a blank name still counts — do not exclude a row merely because the name is null" |

This is a QUESTION-side fix (makes the intended exclusion explicit); the GUIDANCE-side idiom
above is still the durable lever, since the exclusion recurs and won't be spelled out in every
future prompt.
