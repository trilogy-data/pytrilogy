# High-token connectivity audit: 20260715 enriched run

**Run:** `results/20260715-033011_enriched`  
**Comparison:** `results/20260714_200120_enriched`

## Outcome

The enriched score moved from 84/99 to 82/99 and total tokens increased from
29.68M to 32.69M. The connectivity audit found one concrete framework defect:
q64's reuse of a joined rowset through two slices. q29 and q84 do not reproduce
as framework connectivity failures.

## q29

The final candidate declares two explicit joins:

```trilogy
subset join ss.customer.sk = cs.billing_customer.sk
subset join ss.item.sk = cs.item.sk
```

They compile as a composite relationship. The wrong result (15 rows versus 1)
comes from two authored semantic omissions:

1. it never requires `ss.return_customer.sk = ss.customer.sk`;
2. it joins catalog facts before isolating the store-return aggregate, fanning
   the store quantities across matching catalog lines.

The only rejected q29 attempt was an actionable domain diagnostic for
`return_date.month_of_year <= 12`, which is tautological for the month enum.
There was no disconnect, generated-SQL error, or framework exception.

**Classification:** agent/query-idiom failure, not connectivity framework bug.

## q84

The two disconnects used this shape:

```trilogy
where c.current_demographics.sk = ss.return_customer_demographic.sk
select ...;
```

In Trilogy, equality in `where` filters an existing connected graph; it does not
declare a cross-model relationship. The error correctly identifies two
subgraphs. The eventual form:

```trilogy
subset join c.current_demographics.sk = ss.return_customer_demographic.sk
```

generates and executes in 0.179 seconds. It returns 15 rows because only customer
code and name remain at the output grain. Keeping the requested ticket/item
fields hidden preserves the `(customer, ticket, item)` grain and returns 16 rows,
matching the reference row count.

**Classification:** join-syntax guidance plus hidden-grain guidance, not a
resolver defect. The renamed `current_*` paths resolve correctly.

## Whole high-token set

Queries above 500k tokens in the new run were:

`q5, q17, q23, q29, q54, q64, q66, q71, q72, q74, q75, q77, q83, q84, q93, q94`.

Actual failing tool results, excluding `agent-info` documentation text:

| Query | Framework-looking signal | Observed driver |
|---|---|---|
| q5 | None | Seven actionable authoring errors around imports, grouping, and join syntax |
| q17 | None | One top-level statement-order parse error |
| q23 | None in this run | Two parse errors; persistent sink still merits its existing bug audit |
| q29 | None | Tautological enum-bound diagnostic; final semantic fanout |
| q54 | None | Five authored syntax/reference errors, including SQL `GROUP BY` |
| q64 | **Yes** | Valid joined-rowset reuse disconnects or emits a >600s plan |
| q66 | None | Comment text accidentally parsed as a projection |
| q71 | None | No failed tool result; silent/high-context investigation remains warranted |
| q72 | None | Malformed join declaration |
| q74 | None | SQL `GROUP BY` authoring error |
| q75 | None in tool errors | Silent/high-context investigation remains warranted |
| q77 | None | No failed tool result; silent/high-context investigation remains warranted |
| q83 | None | Two statement-boundary parse errors |
| q84 | No | Equality predicate used instead of join; final hidden-grain loss |
| q93 | None | Four authored reference/grouping/expression errors |
| q94 | None in tool errors | Silent/high-context investigation remains warranted |

This table does not clear the silent sinks. Per the eval methodology, q71, q75,
q77, and q94 require result/SQL comparison because absence of an exception is
not evidence of correct framework behavior. It does show that the explicit
connectivity errors in this run are confined to q64 and q84, and only q64 fails
after a valid join declaration.

## Recommended follow-up order

1. Fix or independently hand off q64 using
   `bug_q64_reused_joined_rowset_self_pair_disconnect_timeout.md`.
2. Add an agent-info example stating that cross-model equality requires
   `subset join`/`union join`; a `where a.key = b.key` predicate does not connect
   models.
3. Add an agent-info example showing hidden grain fields for “one row per X but
   display only Y” questions; q84 is a clean motivating case.
4. Probe q71, q75, q77, and q94 for silent wrong-result/codegen issues before
   attributing their token load to agent behavior.

## Follow-up: q71/q75/q77/q94

Completed in `audit_20260715_silent_sinks_q71_q75_q77_q94.md`. Direct replay
found no silent framework miscompilation. q71, q75, and q77 score correctly and
execute in under one second; their churn comes from repeated large exploration
payloads. q94 consistently executes the authored grain, but the agent alternated
between counting 40 qualifying lines and returning 32 per-order rows instead of
counting 32 distinct qualifying orders.
