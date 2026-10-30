/-!
# Scalar functions: one meaning on every engine

A Trilogy function is rendered per dialect, so a query means the same thing on
every engine only if each rendering computes the reference value. DuckDB is the
reference. The laws below are what a user can rely on whatever the engine, and
each failed on at least one dialect:

- `/` is true division (`div_mul_cancel`). Trilogy typed an integer quotient
  INTEGER, so DuckDB computed `7 / 2 = 3.5` but a datasource built from it
  stored `4`, and the planner answered from that copy
  (`int_typed_quotient_breaks_materialization`). SQLite, Postgres, SQL Server
  and Presto truncate an integer quotient (`trunc_div_loses_remainder`), so
  `sum(x) / count(x)` was not `avg(x)` there (`trunc_ratio_is_not_avg`).
- a cast to an integer rounds as `round` does, half away from zero
  (`cast_is_round`). DuckDB and Postgres round a double half to even, SQLite
  and SQL Server truncate (`half_even_cast_is_not_round`, `trunc_cast_is_not_round`).
- `date_diff(a, b, part)` counts the part's boundaries between `a` and `b`, so
  it adds along a chain and is zero exactly when both truncate to the same part
  (`diff_add`, `diff_eq_zero_iff`). SQLite, Postgres and MySQL counted whole
  elapsed units, which does neither (`elapsed_not_additive`,
  `elapsed_zero_across_a_boundary`).
- `day_of_week` is 1 on Sunday through 7 on Saturday. Dialects read an ISO day
  or a `DATEFIRST`-relative day and shift it (`sunday_first_from_iso`,
  `datefirst_independent`); BigQuery subtracted one
  (`bigquery_minus_one_disagrees`).
-/

namespace Trilogy.Functions

/-! ## Division -/

/-- A fraction `n / d` with `d > 0`. Two fractions are one value when they
cross-multiply equal. -/
structure Frac where
  n : Int
  d : Int
  pos : 0 < d

def Frac.eqv (p q : Frac) : Prop := p.n * q.d = q.n * p.d

instance (p q : Frac) : Decidable (p.eqv q) := inferInstanceAs (Decidable (_ = _))

def ofInt (i : Int) : Frac := ⟨i, 1, by decide⟩

/-- True division of integers, for a positive divisor. -/
def divide (x y : Int) (h : 0 < y) : Frac := ⟨x, y, h⟩

/-- Multiplying back by the divisor gives the dividend. -/
theorem div_mul_cancel (x y : Int) (h : 0 < y) :
    (⟨(divide x y h).n * y, (divide x y h).d, h⟩ : Frac).eqv (ofInt x) := by
  simp [Frac.eqv, divide, ofInt]

/-- Truncating division, as SQLite, Postgres, SQL Server and Presto divide two
integers. -/
def truncDivide (x y : Int) : Int := x.tdiv y

theorem trunc_div_loses_remainder : truncDivide 7 2 * 2 ≠ 7 := by decide

/-- Round half away from zero: `round(x, 0)` on every engine. -/
def roundHalfAway (p : Frac) : Int :=
  if 0 ≤ p.n then (2 * p.n + p.d) / (2 * p.d) else -((2 * -p.n + p.d) / (2 * p.d))

/-- A value stored in a column of the expression's declared type. An INTEGER
column rounds what it is given. -/
inductive Ty where
  | integer
  | double

def store : Ty → Frac → Frac
  | .integer, p => ofInt (roundHalfAway p)
  | .double, p => p

/-- Reading a materialized copy answers as the live expression only when the
declared type holds the value. The old typing declared an integer quotient
INTEGER: the copy of `7 / 2` read `4`. -/
theorem int_typed_quotient_breaks_materialization :
    ¬ (store .integer (divide 7 2 (by decide))).eqv (divide 7 2 (by decide)) ∧
      (store .double (divide 7 2 (by decide))).eqv (divide 7 2 (by decide)) := by
  decide

/-- With true division the ratio of a sum to a count is the mean; truncating it
is not. -/
def avgFrac (xs : List Int) (h : 0 < (xs.length : Int)) : Frac := ⟨xs.foldr (· + ·) 0, xs.length, h⟩

theorem trunc_ratio_is_not_avg :
    ¬ (ofInt (truncDivide ([1, 2].foldr (· + ·) 0) 2)).eqv (avgFrac [1, 2] (by decide)) := by
  decide

/-! ## Casting to an integer -/

/-- Round half to even: DuckDB's and Postgres' cast of a double. -/
def roundHalfEven (p : Frac) : Int :=
  let fl := p.n / p.d
  let twice := 2 * (p.n - fl * p.d)
  if twice < p.d then fl
  else if twice > p.d then fl + 1
  else if fl % 2 = 0 then fl else fl + 1

/-- The cast every dialect now renders: `round` first, then the cast. -/
def castInt (p : Frac) : Int := roundHalfAway p

theorem cast_is_round (p : Frac) : castInt p = roundHalfAway p := rfl

/-- `2.5::int` was `2` from a DOUBLE and `3` from a DECIMAL on DuckDB, while
`round(2.5, 0)` is `3`. -/
theorem half_even_cast_is_not_round : roundHalfEven ⟨5, 2, by decide⟩ ≠ roundHalfAway ⟨5, 2, by decide⟩ := by
  decide

/-- SQLite and SQL Server truncate: `3.7::int` was `3`. -/
theorem trunc_cast_is_not_round : (37 : Int).tdiv 10 ≠ roundHalfAway ⟨37, 10, by decide⟩ := by
  decide

/-- Half-away rounding is odd: `round(-x) = -round(x)`, so a sign flip in a
model never moves a value to a different integer. -/
theorem round_neg (n d : Int) (h : 0 < d) (hn : n ≠ 0) :
    roundHalfAway ⟨-n, d, h⟩ = -roundHalfAway ⟨n, d, h⟩ := by
  unfold roundHalfAway
  by_cases p : 0 ≤ n
  · have : ¬ 0 ≤ -n := by omega
    simp only [p, this, ↓reduceIte, Int.neg_neg]
  · have : 0 ≤ -n := by omega
    simp only [p, this, ↓reduceIte, Int.neg_neg]

/-! ## `date_diff` counts boundaries

Instants are integers in the smallest unit; a part is a width `w > 0` in that
unit (an hour is 3600 seconds, a day 24 hours). The part an instant falls in is
its floor division by `w`. Calendar months and years are boundaries too, of
uneven width, and every dialect already counted them by their fields. -/

def part (w t : Int) : Int := t / w

def truncTo (w t : Int) : Int := part w t * w

/-- The reference: boundaries crossed from `a` to `b`. -/
def diff (w a b : Int) : Int := part w b - part w a

theorem diff_add (w a b c : Int) : diff w a b + diff w b c = diff w a c := by
  unfold diff; omega

theorem diff_antisymm (w a b : Int) : diff w a b = -diff w b a := by
  unfold diff; omega

theorem diff_eq_zero_iff (w a b : Int) (h : 0 < w) :
    diff w a b = 0 ↔ truncTo w a = truncTo w b := by
  unfold diff truncTo part
  constructor
  · intro e; rw [show b / w = a / w by omega]
  · intro e
    have := Int.eq_of_mul_eq_mul_right (Int.ne_of_gt h) e
    omega

/-- Truncating both sides first changes nothing. -/
theorem diff_trunc (w a b : Int) (h : 0 < w) : diff w (truncTo w a) (truncTo w b) = diff w a b := by
  unfold diff truncTo part
  rw [Int.mul_ediv_cancel _ (Int.ne_of_gt h), Int.mul_ediv_cancel _ (Int.ne_of_gt h)]

/-- What SQLite (`julianday` differences), Postgres (interval fields) and MySQL
(`TIMESTAMPDIFF`) computed: whole units elapsed. -/
def elapsed (w a b : Int) : Int := (b - a).tdiv w

/-- 23:00 to 01:00 to 23:00, in hours with day parts. -/
theorem elapsed_not_additive :
    elapsed 24 23 25 + elapsed 24 25 47 ≠ elapsed 24 23 47 ∧
      diff 24 23 25 + diff 24 25 47 = diff 24 23 47 := by
  decide

/-- 23:00 to 01:00 crosses midnight: one day apart, but no whole day elapsed. -/
theorem elapsed_zero_across_a_boundary :
    elapsed 24 23 25 = 0 ∧ truncTo 24 23 ≠ truncTo 24 25 ∧ diff 24 23 25 = 1 := by
  decide

/-- DuckDB's `week` is whole 7-day spans of the day difference, truncated toward
zero, which keeps it antisymmetric; unlike the other parts it does not add. -/
def weekDiff (a b : Int) : Int := (diff 1 a b).tdiv 7

theorem week_antisymm (a b : Int) : weekDiff a b = -weekDiff b a := by
  unfold weekDiff
  rw [diff_antisymm 1 a b, Int.neg_tdiv]

theorem week_not_additive : weekDiff 0 4 + weekDiff 4 8 ≠ weekDiff 0 8 := by decide

/-! ## Day of the week

Days are counted from 1970-01-01, a Thursday. -/

/-- The reference: 1 on Sunday through 7 on Saturday. -/
def dow (d : Int) : Int := (d + 4) % 7 + 1

/-- ISO: 1 on Monday through 7 on Sunday (Presto, Snowflake `DAYOFWEEKISO`,
ClickHouse `toDayOfWeek`). -/
def isoDow (d : Int) : Int := (d + 3) % 7 + 1

theorem sunday_first_from_iso (d : Int) : isoDow d % 7 + 1 = dow d := by
  unfold isoDow dow; omega

/-- SQL Server's `DATEPART(weekday, x)` counts from `@@DATEFIRST`, an ISO day. -/
def sqlServerWeekday (first d : Int) : Int := (isoDow d - first + 7) % 7 + 1

theorem datefirst_independent (first d : Int) :
    ((sqlServerWeekday first d + first - 2) % 7 + 1) % 7 + 1 = dow d := by
  unfold sqlServerWeekday isoDow dow; omega

/-- BigQuery's `DAYOFWEEK` is already 1 on Sunday; subtracting one numbered
Sunday 0, and its `date_part(x, day_of_week)` did not. -/
theorem bigquery_minus_one_disagrees : dow 3 - 1 ≠ dow 3 := by decide

end Trilogy.Functions
