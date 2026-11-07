/-!
# Declared types hold their values

The planner answers a query from any datasource that binds its concepts,
including one it built itself (`persist`), whose columns are created with each
concept's declared type. A copy reads back the live value only if the declared
type holds every value the expression takes (`Functions.int_typed_quotient_breaks_materialization`
was the first case). This module states which type each operator's result
needs and where Trilogy's typing was narrower:

- a sum or difference holds in the join of its operands' types, and a product
  needs the sum of their scales (`add_preserved`, `mul_preserved`). Trilogy
  merged a decimal beside a float to the decimal, so `n * f` stored at the
  decimal's scale (`decimal_beside_float_rounds`), and kept a product at its
  operands' scale (`product_scale_rounds`).
- no decimal scale holds a third (`third_has_no_scale`), so a quotient of
  decimals is the double the engine computes, as a quotient of integers is.
- a remainder has its dividend's fraction (`rem_preserved`); Trilogy typed every
  `%` INTEGER (`fractional_rem_is_not_int`). The value itself dropped the
  fraction on SQLite (`%` of the integer parts) and on ClickHouse (`modulo`
  of a decimal's unscaled integer; `integer_rem_drops_fraction`);
  `x - m * trunc(x / m)` is the remainder (`rem_by_trunc`).
- `greatest` / `least` return one of their arguments, so they need the join of
  all of them (`pick_preserved`); Trilogy typed them by the first
  (`first_argument_type_drops`).
- a 4-byte float keeps 24 bits of mantissa, so a column of that width does not
  hold the double the engine computed (`single_misses_double`). DuckDB's and
  MySQL's `FLOAT` and Presto's `REAL` are 4 bytes; Trilogy's float now persists
  as a double everywhere.

A date stored as text is a separate shape of the same law; it is
`Scalars.stamp_text_misses_date`.
-/

namespace Trilogy.Types

/-- A decimal value `n / 10 ^ k`. -/
structure Dec where
  n : Int
  k : Nat
deriving DecidableEq

inductive Ty where
  | int
  | dec (scale : Nat)
  | flt
deriving DecidableEq

def pow10 (k : Nat) : Int := (10 : Int) ^ k

/-- A column of the type stores the value exactly. Precision (the count of
digits) is not modelled: exceeding it is an error, never another answer. A
double stands for every finite decimal: its rounding is far below any scale a
decimal column keeps. -/
def holds : Ty → Dec → Bool
  | .int, v => v.n % pow10 v.k == 0
  | .dec s, v => (v.n * pow10 s) % pow10 v.k == 0
  | .flt, _ => true

def add (a b : Dec) : Dec := ⟨a.n * pow10 b.k + b.n * pow10 a.k, a.k + b.k⟩

def mul (a b : Dec) : Dec := ⟨a.n * b.n, a.k + b.k⟩

/-- The remainder of a value by an integer, truncated as SQL's `%` is. -/
def rem (a : Dec) (m : Int) : Dec := ⟨a.n.tmod (m * pow10 a.k), a.k⟩

/-- The larger of two values. -/
def greatest (a b : Dec) : Dec := if a.n * pow10 b.k ≤ b.n * pow10 a.k then b else a

/-- The type a sum, difference, `coalesce` or `greatest` needs. -/
def join : Ty → Ty → Ty
  | .flt, _ => .flt
  | _, .flt => .flt
  | .dec s, .dec t => .dec (max s t)
  | .dec s, .int => .dec s
  | .int, .dec s => .dec s
  | .int, .int => .int

/-- The type a product needs: the scales add. -/
def mulTy : Ty → Ty → Ty
  | .flt, _ => .flt
  | _, .flt => .flt
  | .dec s, .dec t => .dec (s + t)
  | .dec s, .int => .dec s
  | .int, .dec s => .dec s
  | .int, .int => .int

/-- Trilogy's old merge: a decimal won over a float. -/
def oldJoin : Ty → Ty → Ty
  | .dec s, .flt => .dec s
  | .flt, .dec s => .dec s
  | a, b => join a b

def tys : List Ty := [.int, .dec 1, .dec 2, .flt]

/-- Integers, tenths, hundredths and a thousandth, of both signs. -/
def vals : List Dec := [⟨-3, 0⟩, ⟨5, 0⟩, ⟨25, 1⟩, ⟨-7, 1⟩, ⟨125, 2⟩, ⟨-1, 2⟩, ⟨1, 3⟩]

/-- Every pair of values the operand types hold gives a result the result
type holds. -/
def preserves (op : Dec → Dec → Dec) (ty : Ty → Ty → Ty) : Bool :=
  tys.all fun a => tys.all fun b => vals.all fun x => vals.all fun y =>
    !(holds a x && holds b y) || holds (ty a b) (op x y)

theorem add_preserved : preserves add join = true := by decide

theorem mul_preserved : preserves mul mulTy = true := by decide

theorem pick_preserved : preserves greatest join = true := by decide

theorem rem_preserved :
    (tys.all fun a => vals.all fun x => ([2, 3, -4] : List Int).all fun m =>
      !holds a x || holds (join a .int) (rem x m)) = true := by
  decide

/-- `7.25 * 2.3333`: the product needs six places; the old merge typed it
`numeric(_, 4)`. -/
theorem decimal_beside_float_rounds :
    holds (oldJoin (.dec 4) .flt) (mul ⟨725, 2⟩ ⟨23333, 4⟩) = false ∧
      holds (mulTy (.dec 4) .flt) (mul ⟨725, 2⟩ ⟨23333, 4⟩) = true := by
  decide

/-- `0.25 * 0.25 = 0.0625` does not fit two places. -/
theorem product_scale_rounds :
    holds (join (.dec 2) (.dec 2)) (mul ⟨25, 2⟩ ⟨25, 2⟩) = false ∧
      holds (mulTy (.dec 2) (.dec 2)) (mul ⟨25, 2⟩ ⟨25, 2⟩) = true := by
  decide

/-- A third has no finite decimal expansion: at scale `s` it would need `3`
to divide `10 ^ s`. -/
theorem third_has_no_scale :
    ((List.range 8).all fun s => pow10 s % 3 != 0) = true := by
  decide

theorem fractional_rem_is_not_int : holds .int (rem ⟨25, 1⟩ 2) = false := by decide

/-- `greatest(i, f)` with `i = 3` and `f = 3.5` typed INTEGER. -/
theorem first_argument_type_drops :
    holds .int (greatest ⟨3, 0⟩ ⟨35, 1⟩) = false ∧
      holds (join .int .flt) (greatest ⟨3, 0⟩ ⟨35, 1⟩) = true := by
  decide

/-! ## Float width

A binary float with a `p`-bit mantissa holds an integer exactly when its odd
part is below `2 ^ p`. -/

def oddPart : Nat → Nat → Nat
  | 0, n => n
  | fuel + 1, n => if n % 2 = 0 ∧ n ≠ 0 then oddPart fuel (n / 2) else n

def representable (p n : Nat) : Bool := decide (oddPart 64 n < 2 ^ p)

/-- `2 ^ 24 + 1` is a double and not a single: a 4-byte column rounds it. -/
theorem single_misses_double :
    representable 24 16777217 = false ∧ representable 53 16777217 = true := by
  decide

/-! ## A remainder keeps the fraction on every engine -/

/-- SQLite's `%` casts both sides to integers first. -/
def intRem (a : Dec) (m : Int) : Dec := ⟨(a.n.tdiv (pow10 a.k)).tmod m * pow10 a.k, a.k⟩

/-- ClickHouse's `modulo` of a decimal divides its unscaled integer by `m`. -/
def unscaledRem (a : Dec) (m : Int) : Dec := ⟨a.n.tmod m, a.k⟩

/-- `7.5 % 3` is `1.5`; SQLite answered `1` and ClickHouse `0`. -/
theorem integer_rem_drops_fraction :
    intRem ⟨75, 1⟩ 3 ≠ rem ⟨75, 1⟩ 3 ∧ unscaledRem ⟨75, 1⟩ 3 ≠ rem ⟨75, 1⟩ 3 := by
  decide

/-- The ClickHouse rendering `x - m * trunc(x / m)`: a decimal quotient is
truncated toward zero, so `trunc` of it is `tdiv` of the scaled integers. -/
def remByTrunc (a : Dec) (m : Int) : Dec :=
  ⟨a.n - m * pow10 a.k * a.n.tdiv (m * pow10 a.k), a.k⟩

theorem rem_by_trunc (a : Dec) (m : Int) : remByTrunc a m = rem a m := by
  unfold remByTrunc rem
  rw [Int.tmod_def]

end Trilogy.Types
