# `explore` conformed-dimension dedup — options & before/after comparison

Investigation of [`handoff_explore_conformed_dimension_dedup.md`](handoff_explore_conformed_dimension_dedup.md).
The handoff proposed a **structural shape-signature** dedup. After testing that against
alternatives on the real data, the recommendation is to key on the **root source file each
namespace was parsed from** (its import lineage), guarded by a leaf-shape check. This is both
the most *correct* identity and the most token-efficient (~79% on the worst case).

All numbers below are measured on the run `20260628-194910` workspace:

```
trilogy --format json explore raw/all_sales.preql --regex date
```

(compact `json.dumps`, no indent; the CLI pretty-prints so absolute byte savings are larger.)

---

## 1. The problem (confirmed)

`explore … --regex date` emits **11 namespaces**, of which **8 are full, byte-identical
copies of the same date dimension** in different roles, all parsed from `raw/date.preql`:

```
date, return_date,
billing_customer.{first_sales_date, first_shipto_date},
purchasing_customer.{first_sales_date, first_shipto_date},
ship_customer.{first_sales_date, first_shipto_date}
```

Each is the same ~12-property date dimension; the heavy comments (`week_seq`, `month_seq`, the
`enum<…>` lists) repeat verbatim 8×. Plus 3 customer FK-stubs
(`billing_/purchasing_/ship_customer`) identical to each other, all from `raw/customer.preql`.

This is structural to TPC-DS: role-playing conformed dimensions (date, customer, address,
item) are the norm, so nearly every `--regex` / `--expand-imports` call over a fact pays this
tax. `explore` was ~60 % of q05's carried context.

---

## 2. The real question: how do we decide two namespaces are "the same dimension"?

I tested four candidate identities on the real data. **Three are wrong** — and the failures
are instructive, because they each fail in a different way.

### Signal A — naive string shape-signature (strip the namespace prefix from rendered text)

The literal reading of the handoff ("strip the prefix off each rendered declaration").
**Fragile — false *positives* via comment corruption.** Stripping the namespace as a substring
eats the namespace word wherever it appears, including inside comment prose:

```
date.id … # Surrogate key … a calendar date.
  strip "date." →  # Surrogate key … a calendar         ← "date." eaten from "date."
return_date.id … # … a calendar date.
  strip "return_date." →  # … a calendar date.          ← untouched
```

`date` and `return_date` now look *different* for the wrong reason. Any dimension whose name is
also an English word in its own comments (date, time, item, store, …) is affected. Unsafe.

### Signal B — structural shape-signature from concept objects (handoff proposal, done correctly)

Build the signature from `concept` fields — `(relative leaf, purpose, datatype, keys, description)` —
operating on `concept.address`, never on strings. Fixes the comment corruption, but now
**false *negatives*:** the 8 identical date roles split into **4 groups**:

```
[date, return_date]
[billing_customer.first_sales_date, billing_customer.first_shipto_date]
[ship_customer.first_sales_date,    ship_customer.first_shipto_date]
[purchasing_customer.first_sales_date, purchasing_customer.first_shipto_date]
```

The only differing field is the `id` key's own grain coupling — `billing_customer.id` vs
`ship_customer.id` — the **parent grain** the FK attaches to. Real lineage, nothing to do with
the date schema. Including `keys` over-splits; excluding them blindly is unsafe in general.

### Signal C — physical source table (`datasource.safe_address`)

All 8 date roles' datasources resolve to physical table `date_dim`. Immune to A and B, but
**wrong identity** for two reasons (both raised in review):

1. **Same physical table, different models.** The same `date_dim` table can be declared in two
   different `.preql` files with genuinely different labels/comments (a "sales calendar" model
   vs a "fiscal calendar" model). Physical address would *wrongly merge* them and emit one
   model's descriptions for both.
2. **Multi-datasource namespaces.** A namespace fed by more than one datasource has no single
   physical address; keying on `safe_address` is ill-defined there.

Physical address is a *proxy* for "same source" that happens to coincide here. It isn't the
thing we mean.

### Signal D — root source file from import lineage ✅ recommended

The thing we actually mean by "the same dimension imported into N roles" is **"parsed from the
same source file."** `import date as return_date` and `import date as first_sales_date` both
resolve to `raw/date.preql`; that file *is* the dimension's identity. Recovered from the parse:

```
date                                  <- raw/date.preql
return_date                           <- raw/date.preql
billing_customer.first_sales_date     <- raw/date.preql
ship_customer.first_shipto_date       <- raw/date.preql        (…all 8 → raw/date.preql)
billing_customer / purchasing / ship  <- raw/customer.preql
```

This is correct precisely where Signal C is wrong: two models over the same table live in two
files → two paths → **not merged**; a multi-datasource namespace still has exactly one source
file. And it's immune to A (no string surgery) and B (doesn't look at keys).

**Recommendation: key on the root source file path, guarded by a leaf-shape check.** Group
namespaces by source file; within a group, sub-group by a leaf-only signature
`sorted((rel_leaf, purpose, datatype, description))` so a role that adds/retypes/re-comments a
local property (e.g. `import date as foo; property foo.id.extra …`) splits off and renders in
full. Source-file is the identity; the leaf guard makes the collapse provably lossless.

---

## 3. Why the source path isn't already available (and the minimal fix)

The path **is** determined during parsing — it's just discarded for nested imports.
`Environment.add_import` (`environment.py:762`) copies the sub-environment's concepts,
datasources, merges, functions, and types up to the parent, but **not** `source.imports`. So
after parsing `all_sales`, `env.imports` holds only the 11 *top-level* imports; the fact that
`billing_customer.first_sales_date` came from `raw/date.preql` (recorded in customer's
sub-environment as `Import.input_path`) is dropped.

Two ways to retain it:

- **Reuse `env.imports` (propagate nested).** Rejected: `render.py` keys rendering decisions on
  `concept.namespace in env.imports` (`render.py:409,783`), so adding nested namespace keys to
  that dict changes renderer behavior. Too much blast radius for a token optimization.
- **Add a dedicated `namespace_source: dict[str, Path]` map** ✅ — namespace → root source file,
  populated in `add_import` (record the direct alias's `input_path`, then merge
  `source.namespace_source` re-prefixed under the alias). Nothing else reads it, so blast
  radius is zero. A prototype that reconstructs exactly this map by re-walking the import
  statements produces the correct grouping (all 8 date roles → `raw/date.preql`).

The map also benefits the existing `explore --show imports` listing, which is currently
top-level-only.

---

## 4. Measured impact

Same `--regex date` payload, all strategies:

| strategy | chars | reduction | full schemas kept | correctness |
|---|---|---|---|---|
| baseline (today) | 12,428 | — | 11 | — |
| A — naive string strip | n/a | — | — | ❌ false-merge/split via comments |
| B — structural sig incl. keys | 6,370 | 48.7 % | **5** (4× date + 1 customer) | ✅ lossless, leaves 4 redundant date copies |
| C — physical source table | 2,621 | 78.9 % | 2 | ⚠️ over-merges 2 models on 1 table |
| **D — root source file + leaf guard** | **2,670** | **78.5 %** | **2** (1× date + 1 customer) | ✅ lossless **and** correct identity |

D and C save the same (~79 %); D costs a few chars for the longer source label and is the only
one that's *both* maximally collapsed *and* semantically correct. B — the handoff approach —
saves only half as much because it keeps one full date schema per parent grain.

Signal D across dimensions (same workspace):

| call | baseline | deduped | reduction |
|---|---|---|---|
| `--regex date` | 12,428 | 2,670 | **78.5 %** |
| `--regex customer` | 23,519 | 8,650 | **63.2 %** |
| `--regex address` | 4,859 | 1,777 | **63.4 %** |
| `--expand-imports` (no filter) | 33,781 | 15,154 | **55.1 %** |

---

## 5. Before / after (the `date` family)

### Before — 8 full copies (excerpt: 2 of 8 shown)

```json
"date": [
  { "keys": ["date.id int; # Surrogate key uniquely identifying a calendar date."] },
  { "grain": "date.id",
    "properties": [
      "_date_string string; # Raw date value as string (intermediate stage for cast to date).",
      "date date; # Actual calendar date value (cast from _date_string).",
      "day_name enum<string>['Sunday', … 'Saturday']?; # Spelled-out day name.",
      "week_seq int::week; # Monotonically-increasing week-of-time sequence number. ~53 per year …",
      "month_seq int; # Monotonically-increasing month sequence across the calendar …",
      "year int::year; # Four-digit calendar year (1900-2100)."  /* …12 props… */ ] }
],
"return_date": [
  { "keys": ["return_date.id int; # Surrogate key uniquely identifying a calendar date."] },
  { "grain": "return_date.id",
    "properties": [ /* …the exact same 12 props, verbatim… */ ] }
],
/* …6 more identical copies across the 3 customer roles… */
```

### After — one canonical + one-line refs + a machine-readable map keyed by source file

```json
"date": [ /* …the single full schema, exactly as above… */ ],
"return_date":                        { "same_as": "date" },
"billing_customer.first_sales_date":  { "same_as": "date" },
"billing_customer.first_shipto_date": { "same_as": "date" },
"purchasing_customer.first_sales_date":  { "same_as": "date" },
"purchasing_customer.first_shipto_date": { "same_as": "date" },
"ship_customer.first_sales_date":     { "same_as": "date" },
"ship_customer.first_shipto_date":    { "same_as": "date" },

"conformed": {
  "date":          { "source": "date.preql",     "roles": ["return_date", "billing_customer.first_sales_date", …7] },
  "ship_customer": { "source": "customer.preql", "roles": ["billing_customer", "purchasing_customer"] }
}
```

Every concept reachable before is still reachable: `billing_customer.first_sales_date.year`
exists because the role is declared same-shape as `date`, whose full leaf list is present. The
`conformed` block makes the grouping explicit, machine-readable, and names the **source file**
(the dimension's true identity, and a hint for the agent that these roles share a definition).

---

## 6. Proposed implementation

1. **Core (one small, isolated change):** add `Environment.namespace_source: dict[str, Path]`,
   populated in `add_import` — `self.namespace_source[alias] = imp_stm.input_path or path`, plus
   merge `source.namespace_source` re-prefixed under `alias`. Additive; no existing reader.
2. **JSON** — in `build_concepts_payload` (`explore.py:511`), after namespaces are built:
   group by `namespace_source[ns]`; within a group sub-group by the leaf-only signature
   (built from `concept.address` + existing `_compact_datatype` / `_concept_description` so the
   signature can't drift from the display); collapse sub-groups > 1 to a canonical
   (shallowest depth → shortest → lexical; prefer a role whose leaf matches the file's base
   name, e.g. `date` for `date.preql`) plus `{"same_as": canonical}` siblings; add the
   top-level `conformed` map.
3. **Rich** — mirror in `_emit_namespace` / `_emit_local_groups` (`explore.py:324` / `:231`):
   `ship_customer.first_shipto_date  → same shape as date.* (12 properties, from date.preql)`.
4. **Escape hatch** — `--expand-imports` (and/or a new `--expand-roles`) restores the literal
   per-role dump.

---

## 7. Edge cases / guarantees

- **Correct identity.** Two models over the same physical table (different files) never merge;
  a multi-datasource namespace still has one source file. (Both are where physical-address —
  Signal C — fails.)
- **Lossless.** Collapse requires same source file **and** identical leaf-only signature; a
  role that adds/drops/retypes/re-comments a property splits off and renders full.
- **Reachability.** The canonical's leaf list is complete, so `<role>.<leaf>` is inferable for
  every previously-listed concept.
- **Parent grain isn't lost for querying.** `first_sales_date` attaches to `customer` grain vs
  `date`'s self grain — a join fact the planner resolves, not a schema fact the agent reads.
- **Internal/`__` namespaces** stay hidden exactly as today.
- **`--regex` still collapses within the match set** — the candidate set is just the filtered
  namespaces.
- **Rich and JSON agree** on which roles collapsed (same transform, two renderers).

---

## 8. Open questions

1. **Keep inline refs *and* the `conformed` map?** Mildly redundant; both together is still
   78.5 % and keeps each role discoverable while scanning `namespaces` *and* gives a
   machine-readable summary. Recommend keeping both.
2. **Canonical naming.** Current pick gives `ship_customer` for the customer group (no role is
   literally `customer`); preferring the leaf that matches the file base name is cosmetic.
3. **Cross-call memo.** `explore` is re-read across a session; a deterministic canonical lets
   the agent cache "X is date" once. The pick above is deterministic.
