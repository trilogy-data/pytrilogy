import Trilogy.Readings

/-!
# Writings: what a value cast to a string is

A value cast to `string` has one text on every engine, and the text reads back
as the value (`trilogy.core.casts`, `float_to_text`, `decimal_to_text`,
`instant_to_text`):

- a `numeric(p, s)` is its sign, its whole digits and exactly `s` fraction
  digits, so the text names the declared scale as well as the value;
- a float is its shortest round-tripping digits, laid out as Python's `repr`:
  fixed with at least one fraction digit when the decimal exponent `e` of the
  first digit is in `[-4, 16)`, else `d.ddd` then `e`, a sign and at least two
  exponent digits;
- a datetime is `YYYY-MM-DD HH:MM:SS` then its fraction with no trailing zero
  (none when the fraction is zero); a timestamp is its UTC instant's datetime
  text followed by `+00:00`.

A float is modelled by its shortest digits and their exponent: two doubles are
equal iff their shortest digits and exponents are, so a writing that keeps both
keeps the value.

Laws:
- `decimal_read_write` (bounded): a decimal's text reads back as its value,
  and its fraction has exactly the scale's digits (`decimal_scale_written`).
- `layout_read_exact` (bounded): `repr`'s layout reads back as the digits and
  exponent it laid out, in either notation; `fixed_iff` says which.
- `round15_exact` (bounded): rounding digits that already fit in 15 places
  to 15 keeps them, so a printf at 15 digits is the shortest form of every
  value that has one of 15 digits or fewer.
- `fraction_read_write`, `fraction_precision_free` (bounded): the fraction
  with its trailing zeros dropped reads back, and is the same text whatever
  precision the engine holds the instant at.

Counterexamples, one per bug fixed in 0.3.475:
- `sqlite_fifteen_digits`: SQLite's CAST writes a REAL at 15 significant
  digits, so `0.1 + 0.2` (`0.30000000000000004`) was written `'0.3'`, which
  reads back as another double.
- `clickhouse_drops_point`: ClickHouse wrote the float 1.0 as `'1'` and
  `1e20` as twenty-one digits, where DuckDB writes `'1.0'` and `'1e+20'`.
- `scale_dropped`: SQLite and ClickHouse wrote a `numeric(12,2)` 3.00 as
  `'3.0'` and `'3'`.
- `clickhouse_pads_fraction`: ClickHouse wrote a `DateTime64(3)` instant
  12:00:00.25 as `'12:00:00.250'` and a whole second as `'.000'`.
- `session_zone_written`: DuckDB wrote a timestamp in the session's zone
  (`+01` in Europe/Paris), so one instant had a text per session.
- `clickhouse_decimal_quotient`: ClickHouse divided a decimal at the
  dividend's scale, so `1 / 3` of a `numeric(12,4)` was 0.3333, where the
  declared type of the quotient is a double.
-/

namespace Trilogy.Writings

open Trilogy.Readings (digitsOf)

/-! ## Digits -/

def valueOf (ds : List Nat) : Nat := ds.foldl (fun acc d => acc * 10 + d) 0

def zeros (n : Nat) : List Nat := List.replicate n 0

/-- Characters of a written text: a digit, `-`, `+`, `.` or `e`. -/
inductive W where
  | d (n : Nat)
  | minus
  | plus
  | point
  | e
  deriving DecidableEq, Repr

def digitText (ds : List Nat) : List W := ds.map W.d

def digitsOfText : List W → Option (List Nat)
  | [] => some []
  | .d n :: rest => (digitsOfText rest).map (n :: ·)
  | _ :: _ => none

/-! ## A decimal keeps its declared scale -/

/-- `u` units of `10^-s`, at least one whole digit, exactly `s` fraction digits. -/
def writeDecimal (u : Int) (s : Nat) : List W :=
  let ds := digitsOf u.natAbs
  let padded := zeros (s + 1 - ds.length) ++ ds
  let whole := padded.take (padded.length - s)
  let frac := padded.drop (padded.length - s)
  (if u < 0 then [W.minus] else []) ++ digitText whole ++
    (if s = 0 then [] else W.point :: digitText frac)

/-- The units and scale a decimal text names. -/
def readDecimal (t : List W) : Option (Int × Nat) :=
  let (sign, body) := match t with
    | .minus :: rest => ((-1 : Int), rest)
    | _ => (1, t)
  match body.span (· != W.point) with
  | (whole, []) => (digitsOfText whole).map fun w => (sign * valueOf w, 0)
  | (whole, _ :: frac) => do
    let w ← digitsOfText whole
    let f ← digitsOfText frac
    pure (sign * valueOf (w ++ f), f.length)

set_option maxRecDepth 100000 in
theorem decimal_read_write :
    ∀ u : Nat, u < 400 → ∀ s : Nat, s < 4 →
      readDecimal (writeDecimal ((u : Int) - 200) s) = some ((u : Int) - 200, s) := by
  decide

theorem decimal_scale_written :
    writeDecimal 300 2 = [.d 3, .point, .d 0, .d 0] ∧
    writeDecimal (-5) 2 = [.minus, .d 0, .point, .d 0, .d 5] := by decide

/-- SQLite's `CAST(REAL AS TEXT)` and ClickHouse's `toString` drop trailing
fraction zeros (SQLite keeps one). -/
def stripZeros (t : List W) : List W :=
  match (t.reverse.dropWhile (· == W.d 0)).reverse with
  | t' => if t'.getLast? == some W.point then t' ++ [W.d 0] else t'

def clickhouseStrip (t : List W) : List W :=
  let t' := (t.reverse.dropWhile (· == W.d 0)).reverse
  if t'.getLast? == some W.point then t'.dropLast else t'

theorem scale_dropped :
    stripZeros (writeDecimal 300 2) = [.d 3, .point, .d 0] ∧
    clickhouseStrip (writeDecimal 300 2) = [.d 3] ∧
    readDecimal (clickhouseStrip (writeDecimal 300 2)) ≠
      readDecimal (writeDecimal 300 2) := by decide

/-! ## A float is its shortest digits, laid out as `repr` -/

def expText (x : Int) : List W :=
  let n := x.natAbs
  (if x < 0 then W.minus else W.plus) :: digitText ((if n < 10 then [0] else []) ++ digitsOf n)

/-- `repr`'s layout of significant digits `ds` (first nonzero, last nonzero)
whose first digit is at decimal exponent `x`. -/
def layout (ds : List Nat) (x : Int) : List W :=
  if -4 ≤ x ∧ x < 16 then
    if x ≥ (ds.length : Int) - 1 then
      digitText (ds ++ zeros (x.toNat + 1 - ds.length)) ++ [W.point, W.d 0]
    else if 0 ≤ x then
      digitText (ds.take (x.toNat + 1)) ++ W.point :: digitText (ds.drop (x.toNat + 1))
    else
      [W.d 0, W.point] ++ digitText (zeros ((-x).toNat - 1) ++ ds)
  else
    digitText (ds.take 1) ++
      (if ds.length > 1 then W.point :: digitText (ds.drop 1) else []) ++
      W.e :: expText x

def trimZeros (ds : List Nat) : List Nat :=
  ((ds.dropWhile (· == 0)).reverse.dropWhile (· == 0)).reverse

/-- The digits and exponent a laid-out text names. -/
def readLayout (t : List W) : Option (List Nat × Int) :=
  match t.span (· != W.e) with
  | (mant, []) =>
    match mant.span (· != W.point) with
    | (whole, _ :: frac) => do
      let w ← digitsOfText whole
      let f ← digitsOfText frac
      let all := w ++ f
      let lead := (all.takeWhile (· == 0)).length
      pure (trimZeros all, (w.length : Int) - 1 - lead)
    | _ => none
  | (mant, _ :: sign :: ex) => do
    let m ← digitsOfText (mant.filter (· != W.point))
    let x ← digitsOfText ex
    let v : Int := valueOf x
    pure (trimZeros m, if sign == W.minus then -v else v)
  | _ => none

/-- Significant digit strings of up to three digits, first and last nonzero. -/
def sigs : List (List Nat) :=
  let nz := [1, 5, 9]
  (nz.map fun a => [a]) ++ (nz.flatMap fun a => nz.map fun b => [a, b]) ++
    (nz.flatMap fun a => [0, 7].flatMap fun b => nz.map fun c => [a, b, c])

set_option maxRecDepth 100000 in
theorem layout_read_exact :
    ∀ ds ∈ sigs, ∀ x : Nat, x < 140 →
      readLayout (layout ds ((x : Int) - 120)) = some (ds, (x : Int) - 120) := by
  decide

def isFixed (t : List W) : Bool := !t.contains W.e

theorem fixed_iff :
    ∀ x : Nat, x < 40 → isFixed (layout [1, 5] ((x : Int) - 20)) =
      decide (-4 ≤ (x : Int) - 20 ∧ (x : Int) - 20 < 16) := by
  decide

theorem layout_examples :
    layout [1] 0 = [.d 1, .point, .d 0] ∧
    layout [1] 20 = [.d 1, .e, .plus, .d 2, .d 0] ∧
    layout [1] (-5) = [.d 1, .e, .minus, .d 0, .d 5] ∧
    layout [1] 15 = digitText (1 :: zeros 15) ++ [.point, .d 0] ∧
    layout [1, 2, 5] 1 = [.d 1, .d 2, .point, .d 5] := by decide

/-- Digits rounded half up to `p` places, then trailing zeros dropped: the
digits of printf's `%.{p-1}e`. A carry past the first digit is left out of
the model (the inputs below have none). -/
def roundTo (p : Nat) (ds : List Nat) : List Nat :=
  if ds.length ≤ p then ds
  else
    let head := valueOf (ds.take p) + (if ds.getD p 0 ≥ 5 then 1 else 0)
    trimZeros (digitsOf head)

theorem round15_exact :
    ∀ ds ∈ sigs, roundTo 15 ds = ds := by decide

/-- `0.1 + 0.2` is the double whose shortest digits are 30000000000000004. -/
def pointThree : List Nat := [3] ++ zeros 15 ++ [4]

theorem sqlite_fifteen_digits :
    roundTo 15 pointThree = [3] ∧ layout (roundTo 15 pointThree) (-1) = [.d 0, .point, .d 3] ∧
    readLayout (layout (roundTo 15 pointThree) (-1)) ≠ some (pointThree, -1) := by decide

/-- ClickHouse's `toString`: fixed below 1e21 with no `.0`. -/
def clickhouseLayout (ds : List Nat) (x : Int) : List W :=
  if x ≥ (ds.length : Int) - 1 ∧ x < 21 then
    digitText (ds ++ zeros (x.toNat + 1 - ds.length))
  else layout ds x

theorem clickhouse_drops_point :
    clickhouseLayout [1] 0 = [.d 1] ∧ layout [1] 0 = [.d 1, .point, .d 0] ∧
    clickhouseLayout [1] 20 ≠ layout [1] 20 := by decide

/-! ## An instant's fraction -/

/-- The fraction of `μ` microseconds with no trailing zero (none for 0). -/
def fractionText (μ : Nat) : List W :=
  if μ = 0 then [] else
    W.point :: digitText (((zeros (6 - (digitsOf μ).length) ++ digitsOf μ).reverse.dropWhile
      (· == 0)).reverse)

def readFraction : List W → Option Nat
  | [] => some 0
  | .point :: ds => (digitsOfText ds).map fun f => valueOf (f ++ zeros (6 - f.length))
  | _ => none

set_option maxRecDepth 100000 in
theorem fraction_read_write :
    ∀ k : Nat, k < 1000 → readFraction (fractionText (k * 997 % 1000000)) =
      some (k * 997 % 1000000) := by
  decide

/-- An engine holding `k` fraction digits (ClickHouse's 3) writes them all. -/
def paddedFraction (k μ : Nat) : List W :=
  let ds := digitsOf (μ / 10 ^ (6 - k))
  W.point :: digitText (zeros (k - ds.length) ++ ds)

set_option maxRecDepth 100000 in
/-- At milliseconds the stripped text is the microsecond one. -/
theorem fraction_precision_free :
    ∀ ms : Nat, ms < 1000 →
      fractionText (ms * 1000) = clickhouseStrip (paddedFraction 3 (ms * 1000)) := by
  decide

theorem clickhouse_pads_fraction :
    paddedFraction 3 250000 = [.point, .d 2, .d 5, .d 0] ∧
    fractionText 250000 = [.point, .d 2, .d 5] ∧
    paddedFraction 3 0 = [.point, .d 0, .d 0, .d 0] ∧ fractionText 0 = [] := by decide

/-! ## A timestamp is written at UTC -/

/-- The hour written for a UTC hour `h` in a session `z` hours east. -/
def sessionHour (h z : Int) : Int := (h + z) % 24

theorem session_zone_written : sessionHour 1 0 ≠ sessionHour 1 1 := by decide

/-! ## A decimal quotient is a double -/

/-- ClickHouse's Decimal quotient at the dividend's scale `s`: truncated. -/
def clickhouseQuotient (a b : Int) (s : Nat) : Int := (a * 10 ^ s).tdiv b

theorem clickhouse_decimal_quotient :
    clickhouseQuotient 1 3 4 = 3333 ∧ (3333 : Int) * 3 ≠ 1 * 10 ^ 4 := by decide

end Trilogy.Writings
