# Handoff: lint for `filtered-aggregate = 0` (empty group is NULL, never 0) (q16)

**Status: doc fix LANDED; optional lint/hint remains. From
`results/20260810-211903_enriched_aggregates/agent_log.q16.jsonl` — this
single misconception drove most of a 2.1M-token, 38-turn debugging loop.**

## The trap

```trilogy
auto returned_lines_per_order <- sum(cs.row_counter ? cs.is_returned = true) by cs.order_number;

with nr as
where returned_lines_per_order = 0     # matches NOTHING
select cs.order_number as order_id;
```

An aggregate over a group with no qualifying rows is NULL (SQL semantics), so
`= 0` selects zero orders. The count spelled the same way
(`count(cs.order_number ? cs.is_returned = true) by cs.order_number = 0`)
returned the correct 65,661 in the same session — `count` of an empty set is
0, `sum` is NULL — which made the inconsistency look like an engine bug to the
agent and fueled the probe loop.

## Already done

The agent language reference (`trilogy/ai/constants.py`, RULE_PROMPT "Not
SQL" list) now carries: "An aggregate over a group with no qualifying rows is
NULL, never 0 ... test emptiness with `count(key ? cond) by k = 0` or wrap:
`coalesce(sum(x ? cond), 0)`."

## Remaining task (optional, small)

A targeted authoring-time hint: when a comparison `<agg> = 0` (or `<= 0`)
appears in `where`/`having` and `<agg>` is a NULL-on-empty aggregate (`sum`,
`avg`, `min`, `max` — NOT `count`/`count_distinct`) with an inline filter or
explicit `by` grain, emit a warning event:

> `sum(...)` is NULL (not 0) for groups where no rows match its filter;
> `= 0` will not match those groups. Use `coalesce(<agg>, 0) = 0` or a
> `count(...) = 0` test if you mean "no qualifying rows".

Notes:

- Warning, never an error — `= 0` on a filtered sum is legitimate when the
  author means "rows summing to zero".
- In agent mode (`TRILOGY_AGENT_MODE=1`) warnings surface prominently; that is
  the audience. Check how existing scope warnings
  (`TRILOGY_AGENT_SCOPE_WARNINGS`) are plumbed and reuse that channel.
- Cheap detection point: post-parse over the AST of the condition trees; no
  planner involvement needed.
- Test: warning fires for `where (sum(x ? c) by k) = 0`; does NOT fire for
  `count`, does NOT fire for `coalesce(sum(...), 0) = 0`, does not alter exit
  code.
