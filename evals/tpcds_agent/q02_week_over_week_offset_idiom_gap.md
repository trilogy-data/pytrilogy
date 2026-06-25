# q02 week-over-week offset: NOT a planner bug — idiom/grammar gap

**Status:** NOT A BUG (corrected). An earlier draft claimed `sum(x) by (key + N)` was a
silently-wrong "grain collapse." That diagnosis was **wrong** — the engine behaves correctly.
This note records the correct analysis and the *real* gaps that drove q02's ~92-message thrash.

## The corrected understanding

`sum(x) by (week_seq + 53)` groups rows by the value of `week_seq + 53`. `+53` is a bijection
on integers, so this is the **same partition** as `by week_seq`: each bucket still holds exactly
one week's rows, relabeled. The bucket containing week 5270's rows is labeled `5323` but still
aggregates **week 5270's data**. Joined back to a `week_seq`-grained output via the functional
dependency `week_seq → week_seq+53`, it returns to the same week. So:

```trilogy
auto sun_same <- sum(... ? day_of_week = 0) by all_sales.date.week_seq;
auto sun_next <- sum(... ? day_of_week = 0) by (all_sales.date.week_seq + 53);
where all_sales.date.week_seq = 5270
select all_sales.date.week_seq, sun_same, sun_next;
-- (5270, 11457085.88, 11457085.88)  -- CORRECT: both are week 5270's own day-0 sum
```

`sun_next == sun_same` is right. Grouping by a transformed key **relabels** buckets; it does not
**fetch** another bucket's rows. Neither `+53` nor `-53` would ever shift the value, because the
join always returns through the bijection to the same week.

## What actually shifts a value (the real gap)

To make "week 5270's row show week 5323's value", you need a **lookup**, not a regroup: build a
per-week table `T = sum(x) by week_seq`, then match `this_row.(week_seq + 53) = T.week_seq` —
a derived value on one side against the base key on the other. In Trilogy that's a self-join on
an arithmetic/offset key:

```trilogy
join a.week_seq + 53 = b.week_seq   -- parse error: join_group is bare-identifier equality only
```

This is rejected by the grammar (both `.lark` and `.pest`: `join_group: IDENTIFIER "=" ...`).
That genuine limitation — **no derived/offset-key join** — is the core q02 gap. Workaround the
agent never found: materialize the offset as a rowset column (`week_seq + 53 as next_week_seq`)
and join `a.next_week_seq = b.week_seq` (bare-identifier on both sides).

## The other friction (the NULLs)

The agent first tried `lead(sun, 53) over (order by week_seq)` with `where week_seq in <2001
weeks>`. WHERE restricts the partition to 2001's ~53 rows before the window evaluates (standard
SQL), so `lead(53)` falls off the end → null everywhere. There is no clean idiom to compute the
window over the full series while filtering the output to 2001, and a module-level
`auto next <- lead(...)` concept does not materialize independently of the outer WHERE. Friction,
but standard semantics — not a correctness bug.

## Takeaway for q02 token burn

Real framework gaps (worth addressing), none a planner correctness bug:
1. **No derived/offset-key join** (grammar) — the legitimate blocker; needs the rowset-column
   workaround or grammar support. This is the same family as the join-arithmetic limitation in
   `project_root_outer_source_key_no_coalesce`-adjacent notes.
2. **Window-over-pre-filtered-rows** has no ergonomic idiom (compute window on full series, then
   filter output).
3. **Guidance gap:** nothing tells an agent that `by (key + N)` relabels rather than shifts — a
   docs/error nicety, not a bug.

Lesson (revised): a large eval token-burn usually points at a real gap, but verify the *mechanism*
before calling it a planner correctness bug — here the engine was correct and the gap was
grammar/idiom.
