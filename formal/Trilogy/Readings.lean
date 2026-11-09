import Trilogy.Functions

/-!
# Readings: what a string cast to a number or a date means

A string cast to a number reads, between whitespace, a signed decimal with an
optional exponent (`trilogy.core.casts.NUMBER_PATTERN`); an integer rounds that
value half away from zero and a decimal rescales it the same way, and a float
also reads `inf`, `infinity` and `nan`. A string cast to a date reads a year,
then a month and a day of one or two digits split by `-` or `/` (DuckDB's
reading, which TPC-DS's `'1980/5/3'` birth dates rely on), naming a real day,
with an optional time of day; the guard compares the zero-padded day. Every
other string has no reading and casts to NULL, on every dialect.

Laws:
- `glob_number_exact` (bounded): SQLite has no regular expressions; the
  conjunction of GLOBs it renders (`trilogy.dialect.sqlite.is_number`) accepts
  exactly the strings the pattern does.
- `read_number_exact`: the value read is the numeral's, sign, fraction and
  exponent included, and an integer target rounds it half away.
- `truncate_then_round_exact` (bounded): ClickHouse reads a number at 18
  decimal places, dropping the digits past them, then rounds; when the scale
  kept is at least one digit finer than the target, truncation cannot move a
  value across a half-away tie, so the read is exact.
- `readback_exact` (bounded): a calendar that rolls an overflowing day into
  the next month (SQLite's `date()`, ClickHouse's `toDate32`) gives a written
  day back unchanged iff the day is real, so `date(d) = d` is the guard.
- `read_null_introducing`: a non-null string can read as NULL, so a cast from
  a string does not keep `is not null`, a persisted copy declares it `?`, and
  a set of reads can hold NULL for membership.

Counterexamples, one per bug fixed in 0.3.473:
- `sqlite_reads_prefix`: SQLite read the longest numeric prefix: `'abc'` as
  0 and `'1e3'::int` as 1.
- `sqlite_truncates_fraction`: `'2.5'::int` was 2 on SQLite and 3 on DuckDB.
- `clickhouse_refuses_space`: ClickHouse's `CAST` read `' 12 '` and `'1.5'`
  as an integer as NULL.
- `duckdb_raises`: DuckDB raised on `'abc'::int`, where SQLite and ClickHouse
  answer.
- `rolled_day_is_not_the_day`: `'2020-02-30'::date` was March 1st on SQLite
  and ClickHouse, and `'2020-13-01'` was 1970-01-01 on ClickHouse
  (`clickhouse_month_thirteen_is_epoch`).
- `sqlite_julian_day`: SQLite's `date('1.5')` is a Julian day number.
- `sqlite_text_infinity_is_zero`: a folded infinite float rendered
  `CAST('inf' AS REAL)`, which SQLite reads as 0; it renders `9e999` there.
- `null_free_claim_drops_match`: a set of reads was claimed NULL-free, so
  `x in S` rendered as plain equality and dropped a NULL probe that matches
  the NULL member by identity.

A read past the target's range (0.3.474): a numeral whose rounded value an
integer type or a `numeric(p, s)` cannot hold has no reading and is NULL, as
DuckDB's `TRY_CAST` gives. Laws:
- `saturate_in_range`: clamping is the identity on the range, so a guard
  that checks the range keeps every value the target holds.
- `digit_compare_exact` (bounded): a whole numeral with no leading zero is
  at most the bound iff it has fewer digits, or as many and is not later in
  digit order (SQLite's text check of the 64-bit bound).
- `round_can_leave_range`: a value inside the range can round out of it
  (`9.995` at two places is `10.00`), so the guard reads the rounded value.

Counterexamples:
- `saturation_is_not_the_value`: SQLite's CAST saturated
  `'99999999999999999999'::int` to 2^63 - 1, where DuckDB is NULL.
- `clickhouse_overflow_raises`: ClickHouse's `accurateCastOrNull` from a
  decimal raised past Int64, failing the whole query.
- `decimal_precision_unchecked`: SQLite and ClickHouse kept
  `'12345678901.25'::numeric(12,2)`, which has 11 whole digits.
-/

namespace Trilogy.Readings

open Trilogy.Functions

/-! ## The number grammar -/

/-- A character class: a digit, the punctuation the grammar names, a space,
or anything else. -/
inductive C where
  | d (n : Nat)
  | dot
  | e
  | plus
  | minus
  | sp
  | x
  deriving DecidableEq, Repr

def C.isDigit : C → Bool
  | .d _ => true
  | _ => false

def C.isSign : C → Bool
  | .plus | .minus => true
  | _ => false

def trim (s : List C) : List C :=
  ((s.dropWhile (· == .sp)).reverse.dropWhile (· == .sp)).reverse

def allDigits (s : List C) : Bool := s.all C.isDigit

/-- `[0-9]+([.][0-9]*)?|[.][0-9]+`. -/
def mantissa (s : List C) : Bool :=
  match s.span C.isDigit with
  | ([], .dot :: frac) => !frac.isEmpty && allDigits frac
  | ([], _) => false
  | (_, []) => true
  | (_, .dot :: frac) => allDigits frac
  | _ => false

/-- `[+-]?[0-9]+`. -/
def exponent : List C → Bool
  | c :: rest => if c.isSign then !rest.isEmpty && allDigits rest
      else allDigits (c :: rest)
  | [] => false

def unsigned (s : List C) : Bool :=
  match s.span (· != .e) with
  | (m, []) => mantissa m
  | (m, _ :: ex) => mantissa m && exponent ex

/-- `NUMBER_PATTERN` over the trimmed string. -/
def accepts (s : List C) : Bool :=
  match trim s with
  | c :: rest => if c.isSign then unsigned rest else unsigned (c :: rest)
  | [] => false

/-! SQLite's GLOB guard: `rest = lower(ltrim(t, '+-'))`. -/

def ltrimSigns (s : List C) : List C := s.dropWhile C.isSign

/-- `*a*b*`: `a` occurs, then `b` after it. -/
def occursThen (a b : C → Bool) : List C → Bool
  | [] => false
  | c :: rest => (a c && rest.any b) || occursThen a b rest

/-- `*[^e][+-]*`: a sign right after a character that is not `e`. -/
def signAfterNonE : List C → Bool
  | a :: b :: rest => (a != .e && b.isSign) || signAfterNonE (b :: rest)
  | _ => false

def lastTwo (s : List C) : List C := s.drop (s.length - 2)

def globNumber (t : List C) : Bool :=
  let r := ltrimSigns t
  !(match t with | a :: b :: _ => a.isSign && b.isSign | _ => false)
    && r.all (fun c => c != .sp && c != .x)
    && !occursThen (· == .e) (· == .e) r
    && !occursThen (· == .dot) (· == .dot) r
    && !occursThen (· == .e) (· == .dot) r
    && !signAfterNonE r
    && (match r with
        | .d _ :: _ => true
        | .dot :: .d _ :: _ => true
        | _ => false)
    && r.getLast? != some .e
    && !(match lastTwo r with | [.e, s] => s.isSign | _ => false)

/-- Every string over a small alphabet, up to a length. -/
def strings : Nat → List (List C)
  | 0 => [[]]
  | n + 1 => [] :: ((strings n).flatMap fun s =>
      [.d 1, .dot, .e, .plus, .minus, .x].map (· :: s))

set_option maxRecDepth 100000 in
/-- Bounded: every string of up to five characters from `1 . e + - x`.
`globNumber` reads the trimmed string, as the rendering does. -/
theorem glob_number_exact :
    (strings 5).all (fun s => globNumber (trim s) == accepts s) = true := by
  decide

/-- Spaces inside a numeral are not trimmed away. -/
theorem inner_space_refused : accepts [.d 1, .sp, .d 2] = false := by decide

/-! ## The value read -/

def digitsValue (s : List C) : Int :=
  s.foldl (fun acc c => match c with | .d n => acc * 10 + n | _ => acc) 0

/-- A numeral as `n / 10^k`: the digits with the point removed, scaled by the
fraction's length and then by the exponent. -/
def numeral (sign : Int) (whole frac : List C) (ex : Int) : Frac :=
  let n := sign * digitsValue (whole ++ frac) * 10 ^ (if 0 ≤ ex then ex.toNat else 0)
  ⟨n, 10 ^ (frac.length + (if ex < 0 then (-ex).toNat else 0)), Int.pow_pos (by decide)⟩

/-- The integer a numeral reads as: its value rounded half away. -/
def readInt (sign : Int) (whole frac : List C) (ex : Int) : Int :=
  roundHalfAway (numeral sign whole frac ex)

/-- `'-1.5e2'` is `-150`; `'2.5'` is 3 and `'-2.5'` is -3; `'.5'` is 1;
`'1e3'` is 1000; `'1e-3'` is 0. -/
theorem read_number_exact :
    readInt (-1) [.d 1] [.d 5] 2 = -150 ∧ readInt 1 [.d 2] [.d 5] 0 = 3 ∧
      readInt (-1) [.d 2] [.d 5] 0 = -3 ∧ readInt 1 [] [.d 5] 0 = 1 ∧
      readInt 1 [.d 1] [] 3 = 1000 ∧ readInt 1 [.d 1] [] (-3) = 0 := by
  decide

/-- SQLite's CAST reads the longest numeric prefix; with no digit first it
reads 0. -/
def sqlitePrefixInt : List C → Int
  | s => digitsValue (s.takeWhile C.isDigit)

theorem sqlite_reads_prefix :
    accepts [.x] = false ∧ sqlitePrefixInt [.x] = 0 ∧
      accepts [.d 1, .e, .d 3] = true ∧ sqlitePrefixInt [.d 1, .e, .d 3] = 1 ∧
      readInt 1 [.d 1] [] 3 = 1000 := by
  decide

/-- SQLite's integer CAST of a fraction truncates. -/
theorem sqlite_truncates_fraction :
    readInt 1 [.d 2] [.d 5] 0 = 3 ∧ (25 : Int).tdiv 10 = 2 := by decide

/-- An engine's answer to a cast: a value, NULL, or a raised error. -/
inductive Out where
  | val (v : Int)
  | null
  | error
  deriving DecidableEq

def reference (s : List C) (v : Int) : Out := if accepts s then .val v else .null

/-- ClickHouse's `CAST(x AS Nullable(Int64))` reads only bare digits. -/
def clickhouseCast (s : List C) (v : Int) : Out :=
  if !s.isEmpty && allDigits s then .val v else .null

theorem clickhouse_refuses_space :
    reference [.sp, .d 1, .d 2, .sp] 12 = .val 12 ∧
      clickhouseCast [.sp, .d 1, .d 2, .sp] 12 = .null ∧
      reference [.d 1, .dot, .d 5] 2 = .val 2 ∧
      clickhouseCast [.d 1, .dot, .d 5] 2 = .null := by
  decide

/-- DuckDB's CAST raises where there is no reading. -/
def duckdbCast (s : List C) (v : Int) : Out := if accepts s then .val v else .error

theorem duckdb_raises : reference [.x] 0 = .null ∧ duckdbCast [.x] 0 = .error := by decide

/-- SQLite's CAST of the text `'inf'`: no digit first, so 0. -/
theorem sqlite_text_infinity_is_zero : sqlitePrefixInt [.x, .x, .x] = 0 := by decide

/-! ## Truncation then rounding -/

/-- Truncate `m / 10^k` to `K` decimal places. -/
def truncTo (m : Int) (k K : Nat) : Int := m.tdiv (10 ^ (k - K))

def ratio (n : Int) (k : Nat) : Frac := ⟨n, 10 ^ k, Int.pow_pos (by decide)⟩

set_option maxRecDepth 100000 in
/-- Bounded: numerators in `[-300, 300]` at three places, truncated to one or
two places, then rounded to an integer: the same as rounding the exact value.
A tie needs the digit after the kept ones to be exactly 5 with nothing after,
which truncation keeps; a value past a tie stays at or past it. -/
theorem truncate_then_round_exact :
    ∀ a : Nat, a < 601 → ∀ K : Nat, K < 2 →
      roundHalfAway (ratio (truncTo ((a : Int) - 300) 3 (K + 1)) (K + 1)) =
        roundHalfAway (ratio ((a : Int) - 300) 3) := by
  decide

/-- Truncating to the target's own scale is not enough: `0.5` truncates to 0
at no places, where it rounds to 1. -/
theorem truncate_to_target_misrounds :
    roundHalfAway (ratio (truncTo 5 1 0) 0) = 0 ∧ roundHalfAway (ratio 5 1) = 1 := by
  decide

/-! ## Dates -/

def leap (y : Nat) : Bool := y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)

def monthDays (y m : Nat) : Nat :=
  if m == 2 then (if leap y then 29 else 28)
  else if m == 4 || m == 6 || m == 9 || m == 11 then 30 else 31

def valid (y m d : Nat) : Bool := 1 ≤ m && m ≤ 12 && 1 ≤ d && d ≤ monthDays y m

/-- SQLite's `date()` and ClickHouse's `toDate32` take any day up to 31 and
roll the overflow into the next month. -/
def roll (y m d : Nat) : Nat × Nat × Nat :=
  if d ≤ monthDays y m then (y, m, d)
  else if m == 12 then (y + 1, 1, d - monthDays y m)
  else (y, m + 1, d - monthDays y m)

/-- Bounded: three years, every month, days 1 to 31. -/
theorem readback_exact :
    ∀ y : Nat, y < 3 → ∀ m : Nat, m < 12 → ∀ d : Nat, d < 31 →
      (roll (2019 + y) (m + 1) (d + 1) == (2019 + y, m + 1, d + 1)) =
        valid (2019 + y) (m + 1) (d + 1) := by
  decide

theorem rolled_day_is_not_the_day :
    valid 2020 2 30 = false ∧ roll 2020 2 30 = (2020, 3, 1) ∧
      valid 2019 2 29 = false ∧ roll 2019 2 29 = (2019, 3, 1) := by
  decide

/-- ClickHouse's CAST of a month it cannot read is the epoch, not NULL. -/
def clickhouseDate (y m d : Nat) : Nat × Nat × Nat :=
  if 1 ≤ m && m ≤ 12 then roll y m d else (1970, 1, 1)

theorem clickhouse_month_thirteen_is_epoch :
    valid 2020 13 1 = false ∧ clickhouseDate 2020 13 1 = (1970, 1, 1) := by decide

/-- `YYYY-MM-DD`: four digits, a dash, two, a dash, two. -/
def isoShape (s : List C) : Bool :=
  s.length == 10 && (List.range 10).all fun i =>
    if i == 4 || i == 7 then s[i]? == some .minus else (s[i]?.map C.isDigit) == some true

/-- SQLite's `date()` reads a number as a Julian day; the guard refuses what
is not `YYYY-MM-DD`, though it is a number. -/
theorem sqlite_julian_day :
    accepts [.d 1, .dot, .d 5] = true ∧ isoShape [.d 1, .dot, .d 5] = false ∧
      isoShape [.d 2, .d 0, .d 2, .d 0, .minus, .d 0, .d 1, .minus, .d 0, .d 5] = true := by
  decide

/-! ## A read is NULL-introducing -/

/-- A cast from a non-null string can be NULL. -/
theorem read_null_introducing : ∃ s : List C, s ≠ [] ∧ reference s 0 = .null :=
  ⟨[.x], by decide, by decide⟩

/-- Membership by identity: a NULL probe matches a NULL member. -/
def member (x : Option Int) (S : List (Option Int)) : Bool := S.contains x

/-- Plain equality, the rendering over a set claimed NULL-free. -/
def memberEq (x : Option Int) (S : List (Option Int)) : Bool :=
  S.any fun y => match x, y with | some a, some b => a == b | _, _ => false

theorem null_free_claim_drops_match :
    member none [some 12, none] = true ∧ memberEq none [some 12, none] = false := by
  decide

/-- Over a set with no NULL member, equality is identity. -/
theorem null_free_eq_exact (x : Option Int) (S : List Int) :
    memberEq x (S.map some) = member x (S.map some) := by
  cases x <;> induction S <;> simp_all [memberEq, member] <;> rfl

/-! ## A read past the target's range -/

def inRange (lo hi v : Int) : Bool := lo ≤ v && v ≤ hi

/-- The reference: the value read, if the target holds it. -/
def readIn (lo hi : Int) (s : List C) (v : Int) : Out :=
  if accepts s && inRange lo hi v then .val v else .null

def saturate (lo hi v : Int) : Int := max lo (min hi v)

theorem saturate_in_range (lo hi v : Int) (h : inRange lo hi v = true) :
    saturate lo hi v = v := by
  simp only [inRange, Bool.and_eq_true, decide_eq_true_eq] at h
  simp only [saturate]; omega

def int64Lo : Int := -(2 ^ 63)
def int64Hi : Int := 2 ^ 63 - 1

theorem saturation_is_not_the_value :
    saturate int64Lo int64Hi (10 ^ 20) = int64Hi ∧
      readIn int64Lo int64Hi [.d 1] (10 ^ 20) = .null := by
  decide

/-- ClickHouse's `accurateCastOrNull(decimal, 'Int64')` raises out of range. -/
def clickhouseAccurate (v : Int) : Out := if inRange int64Lo int64Hi v then .val v else .error

theorem clickhouse_overflow_raises :
    clickhouseAccurate (10 ^ 19) = .error ∧ readIn int64Lo int64Hi [.d 1] (10 ^ 19) = .null := by
  decide

/-- A `numeric(p, s)` holds a value scaled by `10^s` with fewer than
`10^p` units. -/
def fitsPrecision (p : Nat) (n : Int) : Bool := n.natAbs < 10 ^ p

theorem decimal_precision_unchecked : fitsPrecision 12 1234567890125 = false := by decide

/-- `9.995` rounded to two places is `10.00`, past `numeric(3, 2)`, though
`9.995` itself has one whole digit. -/
theorem round_can_leave_range :
    (9995 : Int).natAbs < 10 * 1000 ∧
      fitsPrecision 3 (roundHalfAway ⟨9995, 10, by decide⟩) = false := by
  decide

/-! SQLite's text check of the bound. -/

def digitsOf (n : Nat) : List Nat := (Nat.toDigits 10 n).map fun c => c.toNat - 48

def lexLe : List Nat → List Nat → Bool
  | [], _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs => a < b || (a == b && lexLe as bs)

def textLe (n bound : Nat) : Bool :=
  (digitsOf n).length < (digitsOf bound).length ||
    ((digitsOf n).length == (digitsOf bound).length && lexLe (digitsOf n) (digitsOf bound))

set_option maxRecDepth 100000 in
/-- Bounded: every numeral below 2000 against the bound 1207. -/
theorem digit_compare_exact : ∀ n : Nat, n < 2000 → textLe n 1207 = decide (n ≤ 1207) := by
  decide

end Trilogy.Readings
