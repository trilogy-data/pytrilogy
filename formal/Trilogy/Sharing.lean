/-!
# Sharing: a rendering evaluates its operand once

A cast or a clamped substring is rendered as SQL text around its operand.
A rendering that names the operand `k` times copies it `k` times, so an
operand that is itself such a rendering grows `k`-fold per level, and an
operand that draws a value (`random`) is drawn `k` times. Bound once, a
rendering adds its own text per level and every use reads one draw.

A rendering is modelled by its own characters and the number of places it
names its operand; a volatile operand by a stream of draws, one per
evaluation.

Laws:
- `inline_size`, `inline_nested_ge` (exact, induction): inlined, `n`
  levels hold at least `k ^ n` copies of the innermost operand.
- `bound_size`, `bound_nested` (exact, induction): bound, `n` levels are the
  operand plus each level's own text and binding, linear in `n`.
- `bound_le_inline`: binding is no larger once the copies it saves outweigh
  its own text.
- `bind_draws_once` (exact): bound, every use reads the same draw.
- `correlated_per_row` (exact): a binding that reads a column of its row is
  evaluated per row, so a volatile operand is still drawn per row.
- `flattened_is_inline` (exact): substituting each bound level back into the
  next (SQLite's query flattener) is inlining.

Counterexamples, one per bug fixed in 0.3.476:
- `sqlite_instant_blowup`: SQLite's datetime read named its operand 756
  times in ~120 kB of its own text, so a read of a read's text was over
  50 MB (77 MB measured); bound, it is under 10 kB.
- `clickhouse_instant_blowup`: ClickHouse's datetime write of a read named
  the operand 29 times; three levels were 2.1 MB.
- `inline_redraws`: SQLite's float write named its operand 42 times, so a
  random operand's digit count, layout and printed value came from
  different draws.
- `hoisted_draw_is_shared`: SQLite evaluates a subquery with no column once
  per query, so an operand with no column is left inline.
- `flattened_program_blowup`: without an OFFSET SQLite flattened the bound
  levels back into one expression, so the compiled datetime read was 1,866
  instructions where bound it is 291, and ran 12 times slower.
-/

namespace Trilogy.Sharing

/-! ## Size -/

/-- A rendering: `own` characters of its own and `uses` places naming the
operand. -/
structure Rendering where
  own : Nat
  uses : Nat

/-- Inlined: each use copies the operand's `s` characters. -/
def inline (r : Rendering) (s : Nat) : Nat := r.own + r.uses * s

/-- Bound: the operand once and `c` characters to bind it. -/
def bound (r : Rendering) (c s : Nat) : Nat := r.own + c + s

theorem inline_size (r : Rendering) (s : Nat) : inline r s = r.own + r.uses * s := rfl

theorem bound_size (r : Rendering) (c s : Nat) : bound r c s = r.own + c + s := rfl

theorem bound_le_inline (r : Rendering) (c s : Nat) (h : c + s ≤ r.uses * s) :
    bound r c s ≤ inline r s := by
  unfold bound inline; omega

/-- `n` levels of a rendering around an operand of `s` characters. -/
def inlineNested (r : Rendering) (s : Nat) : Nat → Nat
  | 0 => s
  | n + 1 => inline r (inlineNested r s n)

def boundNested (r : Rendering) (c s : Nat) : Nat → Nat
  | 0 => s
  | n + 1 => bound r c (boundNested r c s n)

theorem inline_nested_ge (r : Rendering) (s : Nat) :
    ∀ n, r.uses ^ n * s ≤ inlineNested r s n
  | 0 => by simp [inlineNested]
  | n + 1 => by
    have ih := inline_nested_ge r s n
    have : r.uses * (r.uses ^ n * s) ≤ r.uses * inlineNested r s n :=
      Nat.mul_le_mul_left _ ih
    simp only [inlineNested, inline, Nat.pow_succ]
    rw [Nat.mul_comm (r.uses ^ n) r.uses, Nat.mul_assoc]
    omega

theorem bound_nested (r : Rendering) (c s : Nat) :
    ∀ n, boundNested r c s n = s + n * (r.own + c)
  | 0 => by simp [boundNested]
  | n + 1 => by
    simp only [boundNested, bound, bound_nested r c s n, Nat.succ_mul]
    omega

theorem sqlite_instant_blowup :
    inlineNested ⟨120000, 756⟩ 7 2 > 50000000 ∧
      boundNested ⟨4600, 1⟩ 120 7 2 < 10000 := by decide

theorem clickhouse_instant_blowup : inlineNested ⟨2000, 29⟩ 7 3 > 1500000 := by decide

/-! ## Draws -/

/-- The values `k` uses read when the `i`-th evaluation draws `draw i`. -/
def inlineReads (k : Nat) (draw : Nat → Int) : List Int := (List.range k).map draw

def boundReads (k : Nat) (draw : Nat → Int) : List Int := List.replicate k (draw 0)

theorem bind_draws_once (k : Nat) (draw : Nat → Int) :
    ∀ x ∈ boundReads k draw, x = draw 0 := by
  intro x hx
  exact List.eq_of_mem_replicate hx

theorem inline_redraws :
    inlineReads 42 Int.ofNat ≠ boundReads 42 Int.ofNat ∧
      (inlineReads 42 Int.ofNat).eraseDups.length = 42 := by decide

/-- Per row `r`, the draw a binding reads: a correlated one draws per row, an
uncorrelated one is hoisted and drawn once for the query. -/
def correlated (rows : Nat) (draw : Nat → Int) : List Int := (List.range rows).map draw

def hoisted (rows : Nat) (draw : Nat → Int) : List Int := List.replicate rows (draw 0)

theorem correlated_per_row (rows : Nat) (draw : Nat → Int) :
    correlated rows draw = inlineReads rows draw := rfl

theorem hoisted_draw_is_shared : hoisted 3 Int.ofNat ≠ correlated 3 Int.ofNat := by
  decide

/-! ## Flattening -/

/-- Levels of a binding, innermost first: each level's own text and how
many times it names the level before it. -/
def levelsBound : List Rendering → Nat → Nat
  | [], s => s
  | r :: rs, s => levelsBound rs (bound r 1 s)

/-- The flattener substitutes each level into the next. -/
def levelsFlattened : List Rendering → Nat → Nat
  | [], s => s
  | r :: rs, s => levelsFlattened rs (inline r s)

theorem flattened_is_inline (r : Rendering) (s : Nat) :
    levelsFlattened [r] s = inline r s := rfl

/-- The SQLite datetime read's five levels (the trim, the month, the day,
the zone, the body), each naming the level before it up to eight times. -/
def instantLevels : List Rendering := [⟨40, 1⟩, ⟨60, 6⟩, ⟨90, 6⟩, ⟨70, 4⟩, ⟨900, 8⟩]

theorem flattened_program_blowup :
    levelsFlattened instantLevels 7 > 6 * levelsBound instantLevels 7 := by decide

end Trilogy.Sharing
