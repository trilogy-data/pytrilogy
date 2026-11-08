import Trilogy.Logic
import Trilogy.Functions

/-!
# Casts and folded constants: one value whether the engine or the parser computes it

A cast and a literal expression mean one value. The parser folds some literal
expressions before any engine sees them (`trilogy/core/models/build.py`,
`trilogy/parsing/v2/rules/function_rules.py`), and each dialect renders the
rest; both must give the engine reference (DuckDB).

Laws:
- `rescale_exact`: a decimal cast to a smaller scale rounds half away from
  zero, as `round` does; ClickHouse's rendering (round to the scale, then
  cast) keeps it, since the cast of an already-scaled value is exact.
- `text_bool_exact`: a string reads as a bool by one table of spellings
  (`trilogy.core.casts.text_to_bool`); the CASE rendered on SQLite, MySQL,
  SQL Server, BigQuery and Presto is that table.
- `ne_zero_exact`: a number is true iff it is not zero.
- `domain_guard_exact`: `sqrt` and `log` are NULL outside their domain and
  the function inside it; `domain_null_introducing`: so a non-null argument
  does not prove a non-null result.
- `fold_agrees`: a folded comparison of two literals is the Kleene comparison;
  a NULL operand is left to the engine, so `null = null` stays UNKNOWN.
- `drop_false_arms_exact`: dropping the CASE arms that fold false keeps the
  value, and a CASE with no arm left and no ELSE is NULL (`no_arm_is_null`).

Counterexamples, one per bug fixed in 0.3.472:
- `clickhouse_rescale_truncates`: `1.005::numeric(12,2)` was `1.00` on
  ClickHouse; SQLite's `NUMERIC` kept every digit (`sqlite_keeps_digits`).
- `sqlite_true_is_false`: SQLite's `cast('true' as INTEGER)` is `0`;
  `bigquery_refuses_t`: BigQuery refuses `'t'` and `'yes'`; the literal fold
  read `'t'` and `'1'` as false (`capitalize_fold_misreads_t`).
- `sqlite_int_bool_keeps_value`: `2::bool` was `2` on SQLite, which `= true`
  does not match.
- `bool_text_is_digit`: a bool cast to a string was `'1'` on SQLite, MySQL
  and SQL Server.
- `nan_is_not_null`: `sqrt(-1)` was `nan` on DuckDB and ClickHouse and NULL on
  SQLite, so `sqrt(x) is null` split; `log(0)` was `-inf`.
- `null_eq_null_folded_true`, `literal_substitution_changes`: `null = null`
  folded to TRUE and `1 = null` to FALSE, where a column holding the same
  values compares UNKNOWN.
- `empty_case_rendered`: a CASE whose every arm folded false rendered
  `CASE END`, which no engine parses.
-/

namespace Trilogy.Casts

open Trilogy.B3

/-! ## Decimal scale -/

/-- A decimal `m / 10^k` cast to `s ≤ k` digits: half away from zero. -/
def rescale (m : Int) (k s : Nat) : Int :=
  Functions.roundHalfAway ⟨m, 10 ^ (k - s), by
    have : 0 < (10 : Int) ^ (k - s) := Int.pow_pos (by decide)
    exact this⟩

/-- ClickHouse's bare cast truncates the dropped digits. -/
def truncRescale (m : Int) (k s : Nat) : Int := m.tdiv (10 ^ (k - s))

/-- The rendering: `round(x, s)` (half away for a decimal), then the cast of
the rounded value, which drops only zeros. -/
def clickhouseRendered (m : Int) (k s : Nat) : Int :=
  (rescale m k s * 10 ^ (k - s)).tdiv (10 ^ (k - s))

theorem rescale_exact (m : Int) (k s : Nat) :
    clickhouseRendered m k s = rescale m k s := by
  unfold clickhouseRendered
  have : (10 : Int) ^ (k - s) ≠ 0 := Int.ne_of_gt (Int.pow_pos (by decide))
  exact Int.mul_tdiv_cancel _ this

/-- `1.005::numeric(12,2)`: `1.01`, but `1.00` truncated. -/
theorem clickhouse_rescale_truncates :
    rescale 1005 3 2 = 101 ∧ truncRescale 1005 3 2 = 100 := by decide

/-- `-2.505::numeric(12,2)` is `-2.51`: half AWAY, not half up. -/
theorem rescale_negative : rescale (-2505) 3 2 = -251 := by decide

/-- SQLite's `NUMERIC(12,2)` is a REAL and keeps `1.005`; `round(x, 2)` is
rendered instead. -/
theorem sqlite_keeps_digits : (1005 : Int) ≠ rescale 1005 3 2 * 10 := by decide

/-! ## Strings and numbers as bools -/

def trueText : List String := ["true", "t", "1", "yes", "y"]
def falseText : List String := ["false", "f", "0", "no", "n"]

/-- The reference reading of a (lower-cased) string; `none` has no reading. -/
def textToBool (s : String) : Option Bool :=
  if s ∈ trueText then some true else if s ∈ falseText then some false else none

/-- The rendered `CASE WHEN lower(s) IN (...) THEN true WHEN ... END`. -/
def renderedCase (s : String) : Option Bool :=
  if trueText.contains s then some true
  else if falseText.contains s then some false else none

theorem text_bool_exact (s : String) : renderedCase s = textToBool s := by
  unfold renderedCase textToBool
  simp only [List.contains_iff_mem]

/-- SQLite reads a string as a number by its leading digits: 'true' is 0. -/
def sqliteNumeric (s : String) : Int := if s = "1" then 1 else 0

theorem sqlite_true_is_false :
    textToBool "true" = some true ∧ sqliteNumeric "true" = 0 := by decide

/-- BigQuery's (and, for `yes`, Presto's) cast reads only these. -/
def bigqueryText (s : String) : Option Bool :=
  if s = "true" then some true else if s = "false" then some false else none

theorem bigquery_refuses_t :
    textToBool "t" = some true ∧ bigqueryText "t" = none := by decide

/-- The old literal fold: `value.capitalize() == "True"`. -/
def capitalizeFold (s : String) : Bool := s = "true"

theorem capitalize_fold_misreads_t :
    textToBool "1" = some true ∧ capitalizeFold "1" = false := by decide

/-- A number as a bool: true iff it is not zero. -/
def numToBool (n : Int) : Bool := n != 0

/-- The rendered `(x <> 0)`. -/
theorem ne_zero_exact (n : Int) : numToBool n = !(n == 0) := by
  unfold numToBool; cases h : n == 0 <;> simp_all [bne]

/-- SQLite's `cast(2 as INTEGER)` stays `2`, which `= 1` (true) misses. -/
theorem sqlite_int_bool_keeps_value : numToBool 2 = true ∧ (2 : Int) ≠ 1 := by decide

def boolText (b : Bool) : String := if b then "true" else "false"

/-- A TINYINT bool cast to text. -/
def digitText (b : Bool) : String := if b then "1" else "0"

theorem bool_text_is_digit : boolText true ≠ digitText true := by decide

/-! ## Domains -/

/-- An engine's answer outside a function's domain. -/
inductive Out where
  | val (v : Int)
  | nan
  | null
  deriving DecidableEq

/-- `CASE WHEN x >= 0 THEN x END` under the function: NULL outside. -/
def guarded (dom : Int → Bool) (f : Int → Int) (x : Int) : Out :=
  if dom x then .val (f x) else .null

theorem domain_guard_exact (dom : Int → Bool) (f : Int → Int) (x : Int) :
    (guarded dom f x = .null ↔ dom x = false) ∧
      (dom x = true → guarded dom f x = .val (f x)) := by
  unfold guarded; cases dom x <;> simp

def sqrtDom (x : Int) : Bool := 0 ≤ x
def logDom (x : Int) : Bool := 0 < x

/-- A non-null argument can give a NULL result, so `sqrt` and `log` are
NULL-introducing and `sqrt(x) is not null` is not implied by `x is not null`. -/
theorem domain_null_introducing (f : Int → Int) :
    guarded sqrtDom f (-1) = .null ∧ guarded logDom f 0 = .null := by
  simp [guarded, sqrtDom, logDom]

/-- DuckDB's unguarded `sqrt(-1)`. -/
def duckdbSqrt (f : Int → Int) (x : Int) : Out := if 0 ≤ x then .val (f x) else .nan

theorem nan_is_not_null (f : Int → Int) :
    duckdbSqrt f (-1) ≠ guarded sqrtDom f (-1) := by
  simp [duckdbSqrt, guarded, sqrtDom]

/-! ## Folded comparisons -/

/-- The Kleene comparison of two values, `none` for NULL. -/
def eq3 : Option Int → Option Int → B3
  | some a, some b => if a = b then t else f
  | _, _ => u

/-- The fold before the fix: Python's `==`, where `None == None`. -/
def oldFold (a b : Option Int) : B3 := if a = b then t else f

/-- The fold now: two values compare; a NULL operand is not folded. -/
def fold (a b : Option Int) : Option B3 :=
  match a, b with
  | some x, some y => some (if x = y then t else f)
  | _, _ => none

theorem fold_agrees (a b : Option Int) (r : B3) (h : fold a b = some r) : r = eq3 a b := by
  cases a <;> cases b <;> simp_all [fold, eq3]

theorem fold_null_unknown (a : Option Int) : fold a none = none ∧ fold none a = none := by
  cases a <;> simp [fold]

theorem null_eq_null_folded_true : oldFold none none = t ∧ eq3 none none = u := by decide

/-- `1 = null` folded FALSE; the column `x = null` with `x = 1` is UNKNOWN,
and a `select` shows NULL for it. -/
theorem literal_substitution_changes : oldFold (some 1) none = f ∧ eq3 (some 1) none = u := by
  decide

/-! ## Folded CASE arms -/

/-- A searched CASE: the first arm whose condition holds, else the ELSE. -/
def caseEval {α : Type} : List (B3 × α) → Option α → Option α
  | [], e => e
  | (c, v) :: rest, e => if c = t then some v else caseEval rest e

theorem drop_false_arms_exact {α : Type} (arms : List (B3 × α)) (e : Option α) :
    caseEval (arms.filter fun a => a.1 ≠ f) e = caseEval arms e := by
  induction arms with
  | nil => rfl
  | cons a rest ih =>
    obtain ⟨c, v⟩ := a
    cases c <;> simp_all [caseEval]

/-- No arm left and no ELSE: NULL, which the fold now renders. -/
theorem no_arm_is_null {α : Type} (arms : List (B3 × α)) (h : ∀ a ∈ arms, a.1 = f) :
    caseEval arms none = none := by
  induction arms with
  | nil => rfl
  | cons a rest ih =>
    obtain ⟨c, v⟩ := a
    have hc : c = f := h _ (List.mem_cons_self ..)
    subst hc
    simp only [caseEval, reduceCtorEq, ↓reduceIte]
    exact ih fun b hb => h b (List.mem_cons_of_mem _ hb)

/-- The empty CASE the renderer emitted had no value at all. -/
theorem empty_case_rendered : caseEval ([] : List (B3 × Nat)) none = none := rfl

end Trilogy.Casts
