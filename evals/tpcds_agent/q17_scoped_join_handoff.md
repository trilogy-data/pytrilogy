# Q17 scoped property join handoff

## Original problem

The last enriched run failed q17:

`evals/tpcds_agent/results/20260709-105517_enriched/workspace/query17.preql`

The agent authored scoped joins like:

```preql
union join ss.ticket_number = sr.ticket_number
union join ss.item.sk = sr.item.sk
union join ss.customer.id = sr.billing_customer.id
union join ss.item.sk = cs.item.sk
union join ss.customer.id = cs.billing_customer.id
```

The generated SQL originally aligned the fact tables only on item/ticket/item.
The customer-id scoped join did not participate in the actual fact/source
connection path.

## Why this is tricky

`customer.id` is not physically present on the fact tables. It has to be
projected through each side's local customer FK:

```text
ss.SS_CUSTOMER_SK -> customer.C_CUSTOMER_SK -> customer.C_CUSTOMER_ID
sr.SR_CUSTOMER_SK -> customer.C_CUSTOMER_SK -> customer.C_CUSTOMER_ID
cs.CS_BILL_CUSTOMER_SK -> customer.C_CUSTOMER_SK -> customer.C_CUSTOMER_ID
```

The intended semantic shape is:

```sql
ss_branch = ss left join customer ss_c on ss.customer_sk = ss_c.sk
sr_branch = sr left join customer sr_c on sr.customer_sk = sr_c.sk

ss_branch
full/union join sr_branch
  on item/ticket/customer_id
```

The bad shape is:

```sql
ss joined sr on item/ticket
then customer dimensions joined downstream
```

That downstream customer relationship is not equivalent to using customer ID as
part of the union-join alignment key.

## Spike findings

`BuildEnvironment.scoped_join_key_groups` is available during planning, so the
information exists.

Relevant locations:

- `trilogy/core/models/build_environment.py`: `scoped_join_key_groups`
- `trilogy/core/query_processor.py`: query join clauses are collected and passed
  into materialization
- `trilogy/core/processing/node_generators/node_merge_node.py`:
  `resolve_weak_components` / `inject_property_key_terminals`

A spike that injected all scoped join group endpoints into
`resolve_weak_components(...)` made q17 generate customer joins, but it was too
broad and was reverted.

The bad spike shape was roughly:

```py
for canonical, members in environment.scoped_join_key_groups.items():
    add canonical and all members to all_concepts
```

Do not reintroduce this globally.

## Why the spike was wrong

`environment.scoped_join_key_groups` includes global model merges, not just
query-local `union join` operands.

That broke canonical-collision tests. Example:

`tests/discovery/test_canonical_collision_merge.py::test_canonical_collision_s1_arm_sources_own_physical_column`

The model has:

```preql
merge d1 into ~s1;
merge d2 into ~s2;
```

The query asks only for:

```preql
select s1, m1;
```

The broad spike dragged `s2/d2` into discovery anyway, causing discovery to try
to source unrelated scoped groups together.

This canary must remain green.

## Better implementation direction

Make scoped-join expansion input-driven, similar to
`inject_property_key_terminals(...)`.

Guidelines:

- Only expand scoped groups relevant to concepts already in the active
  query/discovery scope.
- Distinguish query-local scoped joins from global model merges if possible.
- For property scoped joins such as:

  ```preql
  ss.customer.id = sr.billing_customer.id
  ```

  preserve/project the property into each branch before union alignment.
- Only rewrite/expand to key terminals when uniqueness/FDs prove it is
  equivalent.
- Do not add existence filtering. `union join` should connect on these fields
  using full/union semantics; filtering to rows existing on all sides is the
  query author's responsibility.

## Needed regression coverage

Add a small regression modeled on q17:

- Two or three fact sources.
- Each fact has a local FK to a shared dimension.
- The scoped join is authored on a unique property of that dimension, not on the
  physical FK.
- The generated SQL must include the property projection/path as part of the
  alignment, not merely a downstream nullable dimension predicate.

Also run:

```powershell
.venv\Scripts\python.exe -m pytest tests/discovery/test_canonical_collision_merge.py -q
```

At minimum, keep this specific canary green:

```powershell
.venv\Scripts\python.exe -m pytest tests/discovery/test_canonical_collision_merge.py::test_canonical_collision_s1_arm_sources_own_physical_column -q
```
