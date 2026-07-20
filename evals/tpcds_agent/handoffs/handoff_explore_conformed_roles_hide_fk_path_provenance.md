# Handoff — conformed-role collapsing obscures distinct semantic bindings

**Status:** RESOLVED 2026-07-20. Combined conformed entries now carry a
`roles` map with structural provenance (`{"direct": true}` /
`{"via": "customer"}`, plus `description` when authored) whenever the group is
path-heterogeneous or described; homogeneous alias groups stay bare to keep
the token savings. The `agent-info` conformed-dimension guidance was rewritten
around the q96 example to state the names are NOT interchangeable and give the
direct-vs-via decision rule. Validated: `repeat_query.py --query-id 96
--repeats 10 --scale-factor 1 --category ingest` → 10/10 pass (all reps chose
the bare binding, returning 874; run `repeat_q96_20260720-003804_ingest`).
Originally discovered from the ingest-versus-enriched delta on TPC-DS q96 in
run `20260717-173332_ingest`.

## Summary

Automated ingest correctly discovers two distinct semantic bindings from a
fact to the same conformed dimension type. `explore` then collapses them into a
shared schema entry without clearly preserving that they are separate roles.

For ingest q96, exploration of `raw/store_sales.preql` showed:

```json
"household_demographics, customer.household_demographics": {
  "concepts": [
    {
      "keys": [
        "household_demographics.demo_sk bigint;"
      ]
    },
    {
      "grain": "household_demographics.demo_sk",
      "properties": [
        "dep_count enum<bigint>[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];"
      ]
    }
  ]
}
```

This accurately says both bindings expose the same dimension shape. It does
not emphasize that the two names are separate semantic bindings and can
therefore resolve to different dimension members for the same fact.

The agent selected the customer-qualified binding:

```trilogy
ss.customer.household_demographics.dep_count = 7
```

and returned 832 rows. The required bare fact binding:

```trilogy
ss.household_demographics.dep_count = 7
```

returns the reference value, 874.

Both raw-SQL legs passed, but their physical joins are only evidence from this
particular generated model. Physical layout is immaterial to the semantic
contract: both bindings could be backed by one table, separate tables, a view,
or no directly corresponding physical relation at all. The public distinction
is the binding name/path (`household_demographics` versus
`customer.household_demographics`), not its storage implementation.

## Cross-mode evidence

Run family: `evals/tpcds_agent/results/20260717-173332_*`

| Mode | Status | Relationship used |
|---|---|---|
| `sql_bare` | pass | `store_sales.ss_hdemo_sk -> household_demographics.hd_demo_sk` |
| `sql_schema` | pass | `store_sales.ss_hdemo_sk -> household_demographics.hd_demo_sk` |
| `ingest` | fail | `store_sales -> customer -> household_demographics` |
| `enriched` | pass | curated `pos_household_demographic` role |

Artifacts:

- Ingest candidate: `results/20260717-173332_ingest/workspace/query96.preql`
- Ingest trajectory: `results/20260717-173332_ingest/agent_log.q96.{jsonl,conversation.txt}`
- Generated fact model: `results/20260717-173332_ingest/workspace/raw/store_sales.preql`
- Generated customer model: `results/20260717-173332_ingest/workspace/raw/customer.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query96.sql`

## Information available without comments

The generated semantic model already contains the minimum factual distinction:
two separately addressable bindings with different names.

The generated storage mapping for the bare binding happens to be:

```trilogy
ss_hdemo_sk: ?household_demographics.demo_sk,
```

The generated storage mapping for the customer-qualified binding happens to be:

```trilogy
# store_sales.preql
ss_customer_sk: ~?customer.customer_sk,

# customer.preql
c_current_hdemo_sk: ?household_demographics.demo_sk,
```

These physical columns explain how this specific ingest produced the bindings,
but they must not become the user-facing definition of the roles. A semantic
model is allowed to abstract, merge, or replace that physical topology.

No hand-authored description is necessary to state neutrally that the two
names are distinct bindings and are not interchangeable merely because their
schemas conform. Ingest cannot safely infer which role a business phrase
intends, and `explore` must not fabricate that meaning.

## Proposed output

Keep the compact conformed schema, but represent each binding explicitly in a
neutral `roles` map:

```json
"household_demographics, customer.household_demographics": {
  "roles": {
    "household_demographics": {
      "binding": "household_demographics"
    },
    "customer.household_demographics": {
      "binding": "customer.household_demographics"
    }
  },
  "concepts": ["...shared schema unchanged..."]
}
```

Exact field names can differ. The important behavior is that schema
deduplication must not visually turn multiple bindings into one apparently
interchangeable namespace. Prefer structured role identity over generated
prose: it is deterministic, compact, and does not claim semantics ingest
cannot know.

## Important distinction

Conformed schema does not imply interchangeable semantic roles.

These bindings share the same concept schema, so schema deduplication is
appropriate. They do not necessarily identify the same dimension member on a
given fact row. Combining their schema without preserving binding identity
makes the stronger and incorrect suggestion that either name is an equivalent
way to answer the same question.

## Implementation direction

`trilogy/scripts/explore.py` currently deduplicates conformed namespaces and
can attach a `roles` map from import-line descriptions. Extend the role entry
construction so descriptions are optional: every collapsed binding should
still receive a neutral role entry keyed by its semantic address.

Do not generate or persist comments into ingested `.preql` files. The feature
belongs in schema discovery/presentation and should work for any automatically
ingested database.

For example:

```json
"roles": {
  "household_demographics": {"binding": "household_demographics"},
  "customer.household_demographics": {
    "binding": "customer.household_demographics"
  }
}
```

Do not require, prioritize, or expose physical source columns as the role
definition. Physical lineage may exist in a separate diagnostic surface, but
it is not the semantic differentiator proposed here.

## Regression tests

Create a small semantic model with two names for the same conformed schema:

```text
dimension
customer.dimension
```

Assert:

1. The conformed dimension schema remains deduplicated.
2. Both accessible role names remain visible.
3. Both bindings receive separate role entries even without descriptions.
4. The role entries preserve their full semantic addresses.
5. No physical-layout claim is required to distinguish them.
6. No semantic description or inferred temporal claim is fabricated.
7. Existing hand-authored import descriptions remain preserved and
   can coexist with structural provenance.
8. JSON v2 and the rich renderer expose equivalent path information.

Add a fixture-level assertion for generated TPC-DS `store_sales.preql` that
distinguishes `household_demographics` from
`customer.household_demographics`.

## Acceptance criteria

- An agent exploring an automatically ingested fact can see that multiple names
  are distinct bindings to the same conformed schema.
- Output is derived solely from known semantic binding identity.
- No hand-authored comments or guessed business semantics are required.
- Physical layout is neither required nor presented as semantic meaning.
- Conformed-schema token savings are retained.
- q96's bare `household_demographics` binding is visibly distinct from
  `customer.household_demographics`.
