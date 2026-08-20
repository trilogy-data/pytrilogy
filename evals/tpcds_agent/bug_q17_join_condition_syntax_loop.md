# q17 token sink: missing `;` after a post-select join is misdiagnosed as Syntax [225] "Expected a join condition"

**FIXED 2026-08-20.** All three error-message recommendations plus the query-guide nudge
landed; the file is kept one commit carrying this stamp (it was never committed) so the
deletion leaves a record. What changed:

- The end-of-input 202 probe now runs BEFORE the 225 branch in both backends
  (`pest_backend.py`, `lark_backend.py`). All 16 enriched shapes now report
  "Missing closing semicolon?" instead of "Expected a join condition".
- New Syntax [230] + `detect_join_comma_group` (`errors.py`) for the ingest leg's
  comma-between-join-groups spelling; the caret points AT the comma and the message leads
  with "Join groups are separated by `and`, not commas".
- `detect_clause_after_join`'s docstring no longer claims a join may only be followed by
  another join or `select`.
- `trilogy/ai/syntax_examples.py` scoped-join example gained an example that ends directly
  after a join clause, a `;`-terminator note, and a comma-is-not-a-separator note.

Regression guard: `tests/complex/test_join_missing_key_error.py` (5 unterminated post-select
shapes x2 backends -> 202, 3 comma shapes x2 backends -> 230, plus the malformed-key and
select-list-comma cases that must NOT be stolen). Every 202/230 case was confirmed to report
225 on both backends before the change.

**Verified 2026-08-20** against run `20260820-031800_{enriched,ingest}_deepseek_deepseek-v4-flash`
and HEAD `c40ef023b`. Every quoted error reproduces byte-identically from a workspace copy
(session scratchpad `probe_q17/`), and the minimal repro below reproduces on BOTH parser
backends with no workspace at all.

## Symptom

q17 burned 1.47M raw tokens (enriched) / 1.51M (ingest); both legs ultimately PASSED (23 rows).
Cache-adjusted: enriched 95,330 fresh tokens over 26 LLM turns, ingest 93,265 over 25 - the raw
figure is mostly DeepSeek reasoning-replay accounting (same mechanism as `bug_q29_cross_leg_sink.md`),
but unlike q29 there is a real engine-side loop underneath: **20 firings of Syntax [225]**
(16 enriched, 4 ingest), and on the enriched leg **every single one was a false diagnosis**.

## Enriched leg: 16/16 firings were valid Trilogy missing only the final `;`

The agent's first `answer_765177085.preql` (two stacked `union join` clauses after the select
list, ending `limit 100;`) ran cleanly on its first write. All 16 failures were short
verification probes with the same join spellings, written without a terminating semicolon.

Failing-spelling inventory (all drew the identical [225] text; full bodies in
`agent_log.q17.jsonl` events noted):

| shape | example probe (event) | actual defect |
|---|---|---|
| two stacked `union join a=b` / `union join c=d` lines after select list | probe_intersection (24), probe_a (37), probe_e/f/g (56/59/62), probe_n (85), probe_p (90) | missing `;` only |
| single clause with `and`-separated groups `union join a=b and c=d` | probe_min (34), probe_b/c/d (42/45/47), probe_o predecessor shapes | missing `;` only |
| single bare `union join a=b` / `subset join a=b` | probe_h (67), probe_j (75), probe_l (80) | missing `;` only |
| stacked joins inside a `rowset` body | probe_distinct (31) | missing `;` only |

Proof of the discriminator, from the same log: probes i/k/m/o/q/s use byte-identical join
spellings but happen to end `order by ... limit 100;` - all parsed and ran. Every 225 probe
ends `...where ss.sale_date.year = 2001<EOF>` with no `;`. Appending `;` to any failing body
makes it parse and execute (verified against the copied warehouse).

The error the agent saw 16 times:

```
Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a
key equality - write `subset join a.key = b.key` ... Location:
    count(ss.quantity) as n1, ??? union join ss.customer.sk = cs...
```

The join condition was present and well-formed every time. The message names the wrong missing
piece (a `=` condition that is not missing), the caret points at the join clause rather than the
end of input, and nothing mentions the terminator - so the agent mutated join spellings
(stacked vs `and`-chained, union vs subset, moved the `where`, re-read
`agent-info syntax example scoped-join` twice) instead of adding one character. This is the
definition of a non-actionable message: 16 attempts, zero convergence from the message itself.

## Minimal repro (no workspace needed)

```python
from trilogy.parsing.v2.pest_backend import parse_pest   # lark_backend.parse_lark identical
parse_pest("select\n  ss.item.id,\n  count(ss.quantity) as n1\n"
           "union join ss.customer.sk = cs.billing_customer.sk\n")
# -> Syntax [225] "Expected a join condition"  (WRONG: the condition is fine)
# The same text + ";" parses on both backends.
# The same text with the join moved BEFORE `select` reports Syntax [202]
# "Missing closing semicolon?" (RIGHT), so only the post-select join position misfires.
```

Monkeypatching `detect_join_missing_key` to return None makes the identical body report
Syntax [202] - the correct, actionable error - proving the ordering fix below is sufficient.

## Root cause (parser-message defect)

`trilogy/parsing/v2/errors.py:405` `detect_join_missing_key`: after any parse failure it grabs
the last `subset|union join` in the statement and, as its only false-positive guard
(errors.py:417-420), checks for a `select` BETWEEN the join and the failure position - a guard
written for the legacy pre-select join position. For a join placed after the select list
(the position `agent-info` and error 226 call "preferred, SQL-like"; grammar
`trilogy/scripts/dependency/src/trilogy.pest:175` allows `join_clause*` after `select_list`),
the `select` is before the join, the failure position is EOF, the guard can never fire, and a
plain missing terminator is claimed to be a malformed join key.

Ordering makes it stick: both backends run the 225 detector BEFORE the end-of-input 202 probe.

- pest: `trilogy/parsing/v2/pest_backend.py:358-363` (emit 225) precedes the
  `text[pos:].strip() == "" and _pest_parses(text + ";")` check at pest_backend.py:381-385.
- lark: `trilogy/parsing/v2/lark_backend.py:220-223` (emit 225) precedes the feed-`;` 202
  check at lark_backend.py:238-246.
- Message text: `trilogy/parsing/v2/errors.py:109-116`.

## Ingest leg: 4 firings, a different spelling - comma between join groups

Bodies WERE semicolon-terminated; the agent separated two join groups with a comma
(select-list habit):

```
subset join matched.customer_sk = sr.customer.customer_sk,
    matched.item_sk = sr.item.item_sk;
```

Grammar requires `and` between groups (trilogy.pest:188). Replacing the `,` with `and` makes the
identical body parse and execute (verified). Here the 225 text does contain the cure
("separate independent joins with `and`"), but it is the third sentence, the first sentence
("needs a key equality") is false for this body, and the caret points at the START of the join
clause instead of the offending comma. The agent burned 4 attempts (events 29/32/35/37,
including one `rowset` -> `with` detour chasing the wrong dimension) before recovering by
dropping the comma into stacked clauses. Sibling precedent: `detect_align_missing_and`
(errors.py:516, code 221) exists for exactly this shape in `align` clauses; join groups have no
comma detector.

## Ingest `3 disconnected subgraphs`: correct behavior, not a bug

Reproduced (probe1, event 23): selecting `ss.*`, `sr.*`, `cs.*` raw concepts with no join.
In the ingest model each fact module imports its OWN dimension copies
(`root/store_sales.preql` `import customer as customer;` etc.), so `ss.customer.customer_sk`,
`sr.customer.customer_sk`, `cs.bill_customer.customer_sk` are genuinely three disconnected
concept trees. The message says "Are you missing a join or merge statement to relate them?" and
adding `union join ss.customer.customer_sk = sr.customer.customer_sk = cs.bill_customer.customer_sk
and ss.item.item_sk = ...` connects them and returns rows (verified). Actionable and correct;
the agent recovered from it in one step.

## Recommendations (do not fix in this file's session)

**Error-message fix (the high-leverage one):**

1. Before emitting 225 (or inside `detect_join_missing_key`), run the same end-of-input probe
   the 202 path already uses: if `text[pos:].strip() == ""` and the text parses with `";"`
   appended, report 202, not 225. Equivalently: hoist the existing 202 check above the
   225 branch in both backends - it is EOF-gated and append-probe-confirmed, so it cannot mask
   mid-stream failures. Either way, all 16 enriched firings become the correct
   "Missing closing semicolon?".
2. Comma-between-join-groups: when the join clause up to the failure position parses in
   isolation (reuse the 226 `select 1 as trilogy_join_probe <clause>;` probe) and the failing
   character is `,`, point the caret at the comma and lead with "use `and` between join
   groups, not a comma" - the join-group sibling of `detect_align_missing_and` (221).
3. Optional hygiene in the same pass: `detect_clause_after_join`'s docstring
   (errors.py:376-381, "A join may only be followed by another join or `select`") predates the
   post-select join position and contradicts trilogy.pest:175.

**Query-guide fix (minor):** the guide content is not the cause - the agent's first answer,
written straight from the `scoped-join` example, ran cleanly. One nudge: every guide example
ends `order by ... limit 100;`, so the statement terminator is never visually separable from
the limit clause; the moment the agent wrote a probe without an order-by it lost the `;`. A
single line in `trilogy/ai/syntax_examples.py`'s scoped-join example ("the select is still one
statement - it must end with `;` even without order/limit") plus one example ending directly
after a join clause would close that gap. No multi-fact-correlation content is missing.

**Harness note:** raw-vs-fresh accounting again inflated the sink (97.1% prompt cache hits on
both legs); same recommendation as `bug_q29_cross_leg_sink.md` - the >500k detector should
report fresh tokens alongside raw.
