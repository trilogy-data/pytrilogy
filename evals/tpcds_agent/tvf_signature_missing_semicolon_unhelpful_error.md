# Bug: missing `;` after a TVF `-> (...)` signature gives a cryptic "expected metadata, PURPOSE, or data_type" error (q05)

**Status:** FIXED 2026-06-25 — friendly Syntax [222] now surfaced (error-message clarity only;
the `;`-less syntax itself is still required to be terminated). Supporting the unterminated form is
deferred.
**Fix:** `detect_missing_signature_semicolon` in `trilogy/parsing/v2/errors.py` (+ ERROR_CODES[222]),
wired into both pest and lark backends, each gated by a `;`-insertion reparse probe.
Tests: `tests/complex/test_missing_signature_semicolon_error.py`.

---

**Status (original):** OPEN — confirmed, deterministic. Low severity (error-message clarity).
**Surfaced by:** TPC-DS q05 enriched eval — agent wrote a `union(...) -> (...)` definition without
a trailing `;` and got an opaque error pointing at the signature.
**Severity:** Low. The input is genuinely missing a `;`, but the error names internal grammar
tokens and gives no hint that a statement terminator is missing.

## Symptom

A named TVF (`with x as union(...) -> (cols)`) **without a trailing semicolon** before the
consuming `select`:

```trilogy
with u as union(
  (where all_sales.date.year=2001 select all_sales.channel as channel, sum(all_sales.net_profit) as np),
  (where all_sales.date.year=2002 select all_sales.channel as channel, sum(all_sales.return_amount) as np)
) -> (channel, np)            -- (!) no semicolon here
select u.channel, sum(u.np) as s limit 10;
```

fails with:

```
InvalidSyntaxException ... ) -> (channel, np)
                                            ^--- expected metadata, PURPOSE, or data_type
```

Adding `;` after `-> (channel, np)` fixes it. Confirmed by toggling the semicolon alone (all else
equal): WITHOUT → error, WITH → builds.

## Why it's confusing

After the signature column `np`, the grammar can accept an optional type/metadata annotation
(`np: int`, a `# ...` purpose, etc.), so when the next token is the keyword `select` (because the
statement was never terminated) the parser reports "expected metadata, PURPOSE, or data_type"
instead of "expected `;`". An agent reads this as "my signature columns need types," not "I forgot
a semicolon."

## Suggested fix

In `trilogy/parsing/v2/errors.py`, when a TVF/select definition is followed by a statement-starting
keyword (`select`, `with`, `import`, ...) where a `;` or column annotation was expected, surface a
targeted hint: *"missing `;` — terminate the `union(...) -> (...)` definition with a semicolon
before the next statement."* Mirrors the existing GROUP BY / FROM / join-condition hints.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-164230/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
body = '''import raw.all_sales as all_sales;
with u as union(
 (where all_sales.date.year=2001 select all_sales.channel as channel, sum(all_sales.net_profit) as np),
 (where all_sales.date.year=2002 select all_sales.channel as channel, sum(all_sales.return_amount) as np)
) -> (channel, np)
select u.channel, sum(u.np) as s limit 10;'''
eng.generate_sql(body)   # ERR: expected metadata, PURPOSE, or data_type
```
