# `explore` conformed-dimension dedup — before/after JSON (for intelligibility review)

Implemented and live on this branch (default-on; `--expand-roles` restores the old dump).
This shows the **actual** JSON the agent receives, before vs after.

Command: `trilogy --format json explore raw/all_sales.preql --regex <X>`
(pretty-printed, as the CLI emits; workspace `20260628-194910`).

## Versioned payload (internal, toggleable per render type)

The explore payload now carries a **`"version"`** and the shape is selected by an internal
per-render-type config (no CLI flag):

- **json v1** — every namespace in full (role-played dimensions repeat). The old behavior.
- **json v2** — conformed dimensions collapse into one combined-key entry. **Default.**
- **rich v1** — the text path (no dedup yet; v2 mirror is future work).

Flip the default in `_LATEST_RENDER_VERSION` (`explore.py`); pin a shape in tests/callers with
`render_version_override("json", 1)`. `build_concepts_payload(..., version=N)` takes an explicit
override. `--expand-roles` still forces the full per-role dump regardless of version.

## Format: namespace-list key (no `same_as`, no separate map)

Under v2, role-played namespaces that share a schema collapse into **one entry whose key lists
every sharing namespace, canonical first**, with the schema rendered once. The grouping *is* the
key — no pointer to chase, no second map to cross-reference. The body keeps the canonical's
prefix (a derived concept like `birth_date` embeds its namespace in its lineage expression, so
the body can't be made fully prefix-free); the canonical is listed first so that prefix matches
the leading key, and any `<role>.<leaf>` is reached by substituting any listed namespace.

## Size summary

| call | before chars* (v1) | after chars* (v2) | reduction | namespaces → entries |
|---|---|---|---|---|
| `--regex date` | 14,592 | 2,415 | **83.4 %** | 11 → 2 entries (1 lists 8 roles, 1 lists 3) |
| `--regex customer` | 27,989 | 8,188 | **70.7 %** | 21 → 6 entries |
| `--regex address` | 6,046 | 1,742 | **71.2 %** | 7 → 2 entries |

\* pretty-printed CLI bytes. `--show groups` default differs only by the new `"version"` line
(dedup only touches the expanded/`--regex` paths). `--expand-roles` reproduces the old per-role
output exactly except for that one added `"version": 2` line.

**Verified lossless:** every `<role>.<leaf>` reachable before is still reachable after
(date 110/110, customer 210/210, address 55/55).

---

## Case: `--regex date`

### BEFORE — 8 byte-identical date schemas (showing 1 of 8; the other 7 are verbatim repeats)

```json
"date": [
  { "keys": ["date.id int; # Surrogate key uniquely identifying a calendar date."] },
  { "grain": "date.id",
    "properties": [
      "_date_string string; # Raw date value as string (intermediate stage for cast to date).",
      "date date; # Actual calendar date value (cast from _date_string).",
      "day_name enum<string>['Sunday', … 'Saturday']?; # Spelled-out day name.",
      "day_of_month int::day?; # Day-of-month (1-31).",
      "day_of_week enum<int>[0, 1, 2, 3, 4, 5, 6]; # Day-of-week index (0 = Sunday … 6 = Saturday).",
      "month_of_year enum<int>[1, … 12]; # Month of the year (1 = January … 12 = December).",
      "month_seq int; # Monotonically-increasing month sequence across the calendar (month-of-time).",
      "quarter enum<int>[1, 2, 3, 4]; # Calendar quarter (1-4) within the year.",
      "quarter_name string?; # Quarter label combining year and quarter (e.g. '1998Q3').",
      "text_id string; # Stable business identifier for the calendar date (16-char alphanumeric).",
      "week_seq int::week; # Monotonically-increasing week-of-time sequence number. ~53 per year …",
      "year int::year; # Four-digit calendar year (1900-2100)."
    ] }
],
"return_date": [ /* …same 14 lines, prefix return_date.* … */ ],
"billing_customer.first_sales_date":  [ /* …same 14 lines… */ ],
"billing_customer.first_shipto_date": [ /* …same 14 lines… */ ],
"purchasing_customer.first_sales_date":  [ /* …same 14 lines… */ ],
"purchasing_customer.first_shipto_date": [ /* …same 14 lines… */ ],
"ship_customer.first_sales_date":     [ /* …same 14 lines… */ ],
"ship_customer.first_shipto_date":    [ /* …same 14 lines… */ ]
```

### AFTER — this is the **complete** payload

```json
{
  "type": "concepts",
  "version": 2,
  "count": 110,
  "namespaces": {
    "date, billing_customer.first_sales_date, billing_customer.first_shipto_date, purchasing_customer.first_sales_date, purchasing_customer.first_shipto_date, return_date, ship_customer.first_sales_date, ship_customer.first_shipto_date": [
      { "keys": ["date.id int; # Surrogate key uniquely identifying a calendar date."] },
      { "grain": "date.id",
        "properties": [
          "_date_string string; # Raw date value as string (intermediate stage for cast to date).",
          "date date; # Actual calendar date value (cast from _date_string).",
          "day_name enum<string>['Sunday', … 'Saturday']?; # Spelled-out day name.",
          "day_of_month int::day?; # Day-of-month (1-31).",
          "day_of_week enum<int>[0, 1, 2, 3, 4, 5, 6]; # Day-of-week index (0 = Sunday … 6 = Saturday).",
          "month_of_year enum<int>[1, … 12]; # Month of the year (1 = January … 12 = December).",
          "month_seq int; # Monotonically-increasing month sequence across the calendar (month-of-time).",
          "quarter enum<int>[1, 2, 3, 4]; # Calendar quarter (1-4) within the year.",
          "quarter_name string?; # Quarter label combining year and quarter (e.g. '1998Q3').",
          "text_id string; # Stable business identifier for the calendar date (16-char alphanumeric).",
          "week_seq int::week; # Monotonically-increasing week-of-time sequence number. ~53 per year …",
          "year int::year; # Four-digit calendar year (1900-2100)."
        ] }
    ],
    "ship_customer, billing_customer, purchasing_customer": [
      { "grain": "ship_customer.id",
        "properties": [
          "auto ship_customer.birth_date <- concat(ship_customer.birth_year::string, '/', …)::date; # Derived calendar birth date …",
          "last_review_date string; # FK to date for customer's last review (stored as string; should be cast to date)."
        ] }
    ]
  }
}
```

**How the agent reads it:** the key `"date, billing_customer.first_sales_date, …, return_date,
…"` says these 8 namespaces share one schema; the declarations are shown for `date` (the first
listed). To query `return_date.year`, substitute `return_date` for `date` → `return_date.year`.

---

## Case: `--regex address` (full after-payload)

```json
"namespaces": {
  "bill_address, billing_customer.address, purchasing_customer.address, ship_customer.address": [
    { "keys": ["bill_address.id int; # …"] },
    { "grain": "bill_address.id",
      "properties": ["city string; # …", "country string; # …", "state string::us_state; # …", "zip string; # …", … ] }
  ],
  "billing_customer.household_demographic.income_band, purchasing_customer.household_demographic.income_band, ship_customer.household_demographic.income_band": [
    { … income_band schema once … }
  ]
}
```

---

## What changed in code

- **`Environment.namespace_source: dict[str, Path]`** (`environment.py`) — namespace → source
  file, populated in `add_import` (direct alias + nested aliases re-prefixed). Propagated through
  `duplicate()` and the lazy-load path. Nothing else reads it → no renderer impact. (Also fixes
  `explore --show imports`, previously top-level-only, and a self-import iteration bug found en
  route.)
- **Render versioning** (`explore.py`) — `_LATEST_RENDER_VERSION` / `_RENDER_VERSION` per render
  type, `render_version()` accessor, `render_version_override()` context manager. Internal, no
  CLI flag; default `json=2`, `rich=1`. The payload emits its `version`.
- **`_dedup_conformed` / `_conformed_signature` / `_pick_canonical`** (`explore.py`) — the v2
  transform: group by `(source file, leaf-only signature)`; collapse groups > 1 to a single
  combined-key entry. Gated on `version >= 2 and not expand_roles`.
- **`--expand-roles`** flag — user escape hatch; forces the full per-role dump at any version.
- Rich/human (`--format` text) path is **not yet** deduped (registered as `rich v1`).

Tests: `tests/scripts/test_explore*.py` (38, incl. 5 new version/dedup tests) + import/env/
duplicate suites (302 total in the touched areas) green; mypy/ruff/black clean.

---

## Open questions

1. **Delimiter.** Currently `", "` (comma-space). Namespaces never contain commas, so it's
   unambiguous; an agent splits on `", "`. Prefer something else (newline-in-key, ` | `, a
   `roles:` array field instead of a delimited key)?
2. **Teach the convention in `agent-info`?** The combined key is self-evident in-context, but a
   one-liner ("a comma-listed namespace key means those namespaces share the shown schema;
   substitute any for the first") would remove all doubt. Lean yes.
3. **Canonical/body prefix under a filter.** `--regex date` makes `ship_customer` (a 2-property
   *date-filtered* stub) the customer canonical/body prefix — smallest-address wins. Fine, or
   prefer the most-complete role?
4. **Mirror the rich/text path now** or after sign-off on this JSON shape?
