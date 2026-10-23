# Formal semantics (Lean 4)

A small, dependency-free Lean 4 model of the Trilogy rules that decide
*which rows* a query returns: condition logic, WHERE / staged `then where` /
HAVING, aggregates over partitions, the "no intrinsic rows" set semantics,
grain, and `~` padding with the grain pin. Every theorem is checked; there is
no `sorry`.

```bash
cd formal
lake build        # elan installs the pinned toolchain from lean-toolchain
```

## How it is used

The model is the oracle for the fuzzer's `law` family
(`local_scripts/fuzzer/laws.py`). A theorem that two query shapes agree
becomes a case pairing two Trilogy programs. The runner compiles both and
compares rows (`FuzzCase.twin_trilogy`), so no hand-written SQL oracle is
needed, and the random datasets (`--random-datasets`) widen every law for
free. A theorem that two shapes *disagree*, or that two rules conflict, records
a design decision or a bug.

| Module | Rules | Laws exercised |
|---|---|---|
| `Logic.lean` | Kleene `and`/`or`, two-valued `not` | commutativity, De Morgan, double negation, excluded middle, `true` identity |
| `Agg.lean` | filters, `x ? p`, per-key partitions | count split over `not`, inclusion-exclusion, filtered agg = WHERE, grand/rollup totals, `max = -min(-x)` |
| `Staged.lean` | flat WHERE, `then where`, HAVING | row stage folds, staged = inline filter (2 and 3 stages), flat WHERE aggregate = membership, HAVING = outer WHERE |
| `Rows.lean` | distinct tuples, grain | keyed rowset keeps rows, filter pushdown through a rowset, `distinct = group`, FD extension, window = `by`, sibling aggregates |
| `Domain.lean` | `~` padding, grain pin | materialization invariance, pin vs stored column, pin is projection-invariant, `sum(coalesce)` |

## What it found

- **`not (...)` was Kleene while `not p` is two-valued**
  (`Logic.mixed_negation_breaks_de_morgan`). `where not (p or q)` dropped a row
  with an UNKNOWN `p` that `where not p and not q` kept, so parentheses changed
  the answer. Compound negation now renders `coalesce(..., false) = false`, like
  atomic negation.
- **A bare boolean beside `and` blocked conjunct splitting.** In
  `where active and sum(x ? active and y > 0) by k > 0`, `decompose_condition`
  refused to split a conjunction with a bare `active` operand. The atom then
  landed in an invalid `HAVING`, and `where true and p` answered differently
  from `where p` (`Logic.and_true_left`). The parser now normalizes operands as
  a lone `where active` already was.
- **A pinned value shared a bucket with a comparison at its reads' grain.**
  `select customer_id, status, undelivered` gave the order-less customer NULL
  status, while `select customer_id, status` gave `'in-transit'`
  (`Pin.projection_invariant`). The sibling kept the shared bucket off the
  padded rows. Pinned BASICs now bucket apart.
- **A WHERE over a named derivation in a rowset with a `subset join` could
  not be placed.** `where has_amount = true` raised "could only be placed
  after the aggregates", while its inline spelling `nullable_amount is not
  null` planned (`Pin.named_eq_inline`). The joined rowset counted as a
  row-shape barrier the atom's rows crossed, though it is a side input. This
  was latent on main: TPC-DS q29's bare `is_returned` dodged it only because
  the conjunct-split bug above kept the whole WHERE in one piece.
- **`docs/modeling_concepts.md` contradicted `docs/grain_pin.md`** on the
  padded row of a CASE with an ELSE (`Domain.pin_contradicts_stored_reading`:
  no one value satisfies both). The engine follows the pin, and the
  modeling doc is corrected.
- **Staged-where collisions are necessary, not conservative**
  (`Staged.same_address_two_stages_differ`). One address cannot carry both
  stages' values, which justifies `_validate_staged_where`.

## Limits

The model is a reference semantics, not a refinement proof of the planner.
Populations are lists, and conditions and aggregates are functions over them.
Joins appear only through padding and fan-out. Windows, ROLLUP subtotal rows
and unions are modelled at the level the laws need.
