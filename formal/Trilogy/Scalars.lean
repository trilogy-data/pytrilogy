/-!
# Scalar functions II: calendar offsets, extremes and logarithms

`Functions.lean` holds division, casts, `date_diff` and `day_of_week` to one
meaning on every engine. This module adds the next set of scalar functions,
each of which answered differently on at least one dialect:

- `date_add(d, month, n)` is the same day `n` months on, clamped to the target
  month's last day (`addMonths`). Then `date_diff(d, date_add(d, month, n),
  month) = n` (`add_months_diff`), the day is a real day of its month
  (`add_months_valid`), and a day that fits is kept (`add_months_keeps_day`).
  SQLite's `+n months` modifier lets the day overflow into the next month, so
  Jan 31 + 1 month was Mar 2 and two months apart (`overflow_breaks_diff`).
  The SQLite rendering now builds the first of the target month and adds the
  clamped day (`first_plus_clamped_day_exact`).
- a whole-day offset of a DATE is a DATE. Trilogy typed every `date_add` DATE,
  but DuckDB and Postgres returned a timestamp and SQLite a datetime string, so
  on SQLite `date_add(d, day, 1) = '2020-02-01'::date` was false: text
  comparison does not coerce (`stamp_text_misses_date`). A value rendered in
  its declared type compares the same under either rule (`typed_eq_agrees`).
- `greatest` / `least` are over the arguments that are present, NULL only when
  all are (DuckDB, Postgres, SQL Server). SQLite, BigQuery, Snowflake, MySQL
  and Presto return NULL when any argument is (`strict_drops_present`); each
  argument falling back to the others restores the reference
  (`fallback_greatest_exact`, `fallback_least_exact`).
- `log(x, b)` is the log of `x` to base `b`. The base rendering was
  `log(x, b)`, which SQLite, Postgres, Snowflake and MySQL read base first, so
  `log(100)` was `0.5` there (`base_first_misreads`).
-/

namespace Trilogy.Scalars

/-! ## Month offsets -/

/-- A calendar date: a year, a month in `1..12` and a day in `1..31`. -/
structure Date where
  y : Int
  m : Int
  d : Int
deriving DecidableEq, Repr

def leap (y : Int) : Bool := (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

def monthLen (y m : Int) : Int :=
  if m = 2 then (if leap y then 29 else 28)
  else if m = 4 ∨ m = 6 ∨ m = 9 ∨ m = 11 then 30
  else 31

/-- Months since year 0; `date_diff(a, b, month)` is the difference of these
(`Functions.diff` counts calendar boundaries by their fields). -/
def monthIndex (t : Date) : Int := t.y * 12 + t.m - 1

def monthDiff (a b : Date) : Int := monthIndex b - monthIndex a

/-- The reference: the same day `n` months on, clamped to the month's length. -/
def addMonths (t : Date) (n : Int) : Date :=
  let a := monthIndex t + n
  ⟨a / 12, a % 12 + 1, min t.d (monthLen (a / 12) (a % 12 + 1))⟩

theorem add_months_diff (t : Date) (n : Int) : monthDiff t (addMonths t n) = n := by
  show (monthIndex t + n) / 12 * 12 + ((monthIndex t + n) % 12 + 1) - 1 - monthIndex t = n
  omega

theorem add_months_month (t : Date) (n : Int) :
    1 ≤ (addMonths t n).m ∧ (addMonths t n).m ≤ 12 := by
  show 1 ≤ (monthIndex t + n) % 12 + 1 ∧ (monthIndex t + n) % 12 + 1 ≤ 12
  omega

theorem add_months_valid (t : Date) (n : Int) :
    (addMonths t n).d ≤ monthLen (addMonths t n).y (addMonths t n).m := by
  show min t.d (monthLen ((monthIndex t + n) / 12) ((monthIndex t + n) % 12 + 1)) ≤
    monthLen ((monthIndex t + n) / 12) ((monthIndex t + n) % 12 + 1)
  omega

theorem add_months_keeps_day (t : Date) (n : Int)
    (h : t.d ≤ monthLen (addMonths t n).y (addMonths t n).m) : (addMonths t n).d = t.d := by
  have h' : t.d ≤ monthLen ((monthIndex t + n) / 12) ((monthIndex t + n) % 12 + 1) := h
  show min t.d (monthLen ((monthIndex t + n) / 12) ((monthIndex t + n) % 12 + 1)) = t.d
  omega

/-- What the SQLite rendering computes: the first of the target month
(`'start of month', '+n months'`), plus the clamped day less one. -/
def firstPlusClampedDay (t : Date) (n : Int) : Date :=
  let a := monthIndex t + n
  ⟨a / 12, a % 12 + 1, 1 + (min t.d (monthLen (a / 12) (a % 12 + 1)) - 1)⟩

theorem first_plus_clamped_day_exact (t : Date) (n : Int) :
    firstPlusClampedDay t n = addMonths t n := by
  simp only [firstPlusClampedDay, addMonths]
  congr 1 <;> omega

/-- SQLite's `+n months` modifier: the day is kept and overflows into the
following month. -/
def overflowMonths (t : Date) (n : Int) : Date :=
  let a := monthIndex t + n
  let len := monthLen (a / 12) (a % 12 + 1)
  if t.d ≤ len then ⟨a / 12, a % 12 + 1, t.d⟩
  else ⟨(a + 1) / 12, (a + 1) % 12 + 1, t.d - len⟩

/-- Jan 31 2020 plus one month: Feb 29 by the reference, Mar 2 on SQLite,
which `date_diff` then counts as two months. -/
theorem overflow_breaks_diff :
    addMonths ⟨2020, 1, 31⟩ 1 = ⟨2020, 2, 29⟩ ∧
      overflowMonths ⟨2020, 1, 31⟩ 1 = ⟨2020, 3, 2⟩ ∧
      monthDiff ⟨2020, 1, 31⟩ (overflowMonths ⟨2020, 1, 31⟩ 1) = 2 := by
  decide

/-- A quarter and a year are three and twelve months: Feb 29 2020 plus a year
is Feb 28 2021. -/
theorem leap_day_plus_year : addMonths ⟨2020, 2, 29⟩ 12 = ⟨2021, 2, 28⟩ := by decide

/-! ## A date offset stays a date

A value is a date (a day number) or a timestamp (a day and a second of it). -/

inductive Val where
  | date (day : Int)
  | stamp (day sec : Int)
deriving DecidableEq

/-- The instant a value denotes: a date is its midnight. -/
def Val.instant : Val → Int × Int
  | .date d => (d, 0)
  | .stamp d s => (d, s)

/-- DuckDB and Postgres coerce a date to a timestamp to compare them. -/
def coerceEq (a b : Val) : Bool := decide (a.instant = b.instant)

/-- SQLite stores both as text and compares the text: equal only when the
shapes are. -/
def textEq (a b : Val) : Bool := decide (a = b)

/-- The value a whole-day offset of a date renders as: a date, its declared
type (`get_date_offset_output`). -/
def offsetDate (d n : Int) : Val := .date (d + n)

theorem typed_eq_agrees (d n e : Int) :
    textEq (offsetDate d n) (.date e) = coerceEq (offsetDate d n) (.date e) := by
  by_cases h : d + n = e <;> simp [textEq, coerceEq, offsetDate, Val.instant, h]

/-- The old SQLite rendering returned `datetime(...)`: midnight of the right
day, which no date equals as text. -/
theorem stamp_text_misses_date :
    textEq (.stamp 1 0) (.date 1) = false ∧ coerceEq (.stamp 1 0) (.date 1) = true := by
  decide

/-! ## `greatest` and `least` over the present arguments

An argument is `none` (NULL) or a value. -/

def pick (f : Nat → Nat → Nat) : Option Nat → Option Nat → Option Nat
  | none, v => v
  | some a, none => some a
  | some a, some b => some (f a b)

/-- The reference: the extreme of the present arguments, NULL only when every
argument is. -/
def extreme (f : Nat → Nat → Nat) (xs : List (Option Nat)) : Option Nat :=
  xs.foldl (pick f) none

/-- SQLite's multi-argument `max`/`min`, and BigQuery's, Snowflake's, MySQL's
and Presto's `greatest`/`least`: NULL when any argument is. -/
def strict (f : Nat → Nat → Nat) (xs : List (Option Nat)) : Option Nat :=
  if xs.all Option.isSome then extreme f xs else none

def coalesce : List (Option Nat) → Option Nat
  | [] => none
  | some v :: _ => some v
  | none :: rest => coalesce rest

/-- The rendering: each argument falls back to the others in order. -/
def fallback3 (f : Nat → Nat → Nat) (a b c : Option Nat) : Option Nat :=
  strict f [coalesce [a, b, c], coalesce [b, a, c], coalesce [c, a, b]]

/-- NULL and three values: every pattern of NULLs and every order of the
present values among three arguments. -/
def args : List (Option Nat) := [none, some 0, some 1, some 2]

def fallbackExact (f : Nat → Nat → Nat) : Bool :=
  args.all fun a => args.all fun b => args.all fun c =>
    decide (fallback3 f a b c = extreme f [a, b, c])

theorem fallback_greatest_exact : fallbackExact max = true := by decide

theorem fallback_least_exact : fallbackExact min = true := by decide

theorem strict_drops_present :
    strict max [some 1, none, some 3] = none ∧ extreme max [some 1, none, some 3] = some 3 := by
  decide

/-! ## Logarithms

The integer part of a logarithm stands in for the real one: the argument
order is what went wrong, not the precision. Sixty-four divisions reach every
value below `2 ^ 64`. -/

def ilog (b : Nat) : Nat → Nat → Nat
  | 0, _ => 0
  | fuel + 1, x => if 2 ≤ b ∧ b ≤ x then ilog b fuel (x / b) + 1 else 0

/-- The reference: `log(x, b)` is the log of `x` to base `b`. -/
def log (x b : Nat) : Nat := ilog b 64 x

theorem log_inverts_power :
    (List.range 8).all (fun n => decide (log (2 ^ n) 2 = n)) = true := by decide

theorem log_ten :
    (List.range 5).all (fun n => decide (log (10 ^ n) 10 = n)) = true := by decide

/-- SQLite, Postgres, Snowflake and MySQL read `log(a, b)` as the log of `b`
to base `a`: the base rendering `log(100, 10)` was the log of 10 to base 100. -/
def baseFirst (x b : Nat) : Nat := log b x

theorem base_first_misreads : log 100 10 = 2 ∧ baseFirst 100 10 = 0 := by decide

end Trilogy.Scalars
