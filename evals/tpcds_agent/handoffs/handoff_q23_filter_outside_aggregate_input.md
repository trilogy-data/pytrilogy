# q23 — RESOLVED: NOT A BUG (doc fix, not engine fix)

**Disposition:** ❌ withdrawn as an engine bug. Owner ruling: `?` filters the **immediately-prior
expression**, uniformly — there is NO inline where/having distinction in the language.

## Correct semantics (owner-confirmed)
`?` creates a filter on the prior expression. That single rule covers both spellings:
- `sum(x ? cond) by k` — `?` filters `x` (the aggregate's INPUT) → windowed sum.
- `(sum(x) by k) ? cond` — `?` filters the aggregate EXPRESSION `sum(x) by k` → the (lifetime) result,
  re-expanded to the filter's grain. Returning the lifetime value is CORRECT BY DESIGN.

So `sum(x) by customer ? date.year in (...)` returning **282421.67 (lifetime)** is right; the windowed
**236266.51** requires `sum(x ? date.year in (...)) by customer`. The agent picked the wrong spelling.

## Why this was mis-filed
The AI guidance previously described `?` as a pre-aggregate/input filter:
- `ai/constants.py:127` "INLINE filter `x ? cond` <- filters one expression's **input**"
- `ai/syntax_examples.py:107` "`?` is a per-aggregate filter (a WHERE that applies to ONE aggregate)"
Those phrasings implied `(sum(x) by k) ? cond` should windowize. They misled both the DeepSeek agent
(wrong-spelling authoring error, ~1.08M token sink) and this investigation. **Owner has updated the
docs.** No `trilogy/` change required.

## Action
- None in the engine.
- Class = agent authoring error caused by misleading guidance (same bucket as q75/q80/q64), now
  addressed by the doc update.
- If a guard is wanted, it's a docs/example test, not an engine test.
