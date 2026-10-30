import Trilogy.Logic

/-!
# Datasource assertions: the build check is exact

A datasource's `assert` clauses decide whether a build replaces the asset. An
assertion compares aggregates of the build with `prior(...)` aggregates of the
asset it replaces, per `by` group. The evaluator reads each side with one
query, outer-joins the two result sets on the group in Python, and checks each
group by substituting its values into the condition and running that as a
constant query. The verdict must be the documented rule
(`docs/datasource_assertions.md`, "Semantics at the edges") applied to every
group of either side (`check_exact`). That holds only if three steps are exact:

- every name in the condition is replaced by the group's own value. A `by`
  key the condition names was left in place, so the constant query read it
  from whatever row was in scope: one group was judged by another's threshold
  (`unsubstituted_key_reads_another_group`), passing a build that broke it.
- each value survives its round trip through a literal. A `Decimal` term was
  converted to a double, so two sums a cent apart compared equal
  (`lossy_literal_passes_a_change`, `decimal_as_double_merges_cents`) and
  `sum(x) = prior(sum(x))` passed a changed build. Rendered values also
  failed to parse at all: a double printed as `1e+17` or `1e-05` (the
  grammar has no exponent form) and a string holding a quote.
- the constant query's answer is read as the engine encodes TRUE. The check
  tested `is True`, and SQLite and MySQL return `1`, so every assertion failed
  there (`is_true_rejects_sqlite_true`, `truthy_exact`).

Groups are matched by identity, not SQL `=`, which is what GROUP BY does with a
NULL key; an `=` join would report the NULL group vanished from a build that
kept it (`sql_eq_join_vanishes_null_group`).
-/

namespace Trilogy.Assert

open Trilogy.B3

variable {K V : Type} [DecidableEq K]

/-- One side's result: per group, its term value, `none` where it is NULL. -/
abbrev Side (K V : Type) := List (K × Option V)

def groups (s : Side K V) : List K := s.map Prod.fst

/-- The documented rule for one group. The outer `Option` is the group's
presence on a side, the inner one a NULL term. -/
def judge (φ : K → V → V → B3) (g : K) : Option (Option V) → Option (Option V) → Bool
  | none, some _ => false
  | some _, none => true
  | none, none => true
  | some none, some _ => false
  | some (some _), some none => true
  | some (some n), some (some o) => isT (φ g n o)

/-- The assertion holds when every group of either side passes. -/
def Spec (φ : K → V → V → B3) (new old : Side K V) : Prop :=
  ∀ g, g ∈ groups new ∨ g ∈ groups old → judge φ g (new.lookup g) (old.lookup g) = true

/-- The evaluator's group order: the build's groups, then the vanished ones. -/
def order (new old : Side K V) : List K :=
  groups new ++ (groups old).filter fun g => decide (g ∉ groups new)

/-- What the evaluator decides. The constant query sees `key g` where the
condition names the group and `lit v` for each value's literal round trip. -/
def check (key : K → K) (lit : V → V) (φ : K → V → V → B3) (new old : Side K V) : Bool :=
  (order new old).all fun g =>
    judge (fun g n o => φ (key g) (lit n) (lit o)) g (new.lookup g) (old.lookup g)

theorem mem_order (new old : Side K V) (g : K) :
    g ∈ order new old ↔ g ∈ groups new ∨ g ∈ groups old := by
  unfold order
  by_cases h : g ∈ groups new <;> simp [h]

/-- With every name substituted and every literal exact, the check is the rule. -/
theorem check_exact (φ : K → V → V → B3) (new old : Side K V) :
    check id id φ new old = true ↔ Spec φ new old := by
  unfold check Spec
  rw [List.all_eq_true]
  exact forall_congr' fun g => imp_congr_left (mem_order new old g)

omit [DecidableEq K] in
/-- The edge rules: a vanished group fails, a group with no history passes. -/
theorem vanished_fails_new_passes (φ : K → V → V → B3) (g : K) (v : Option V) :
    judge φ g none (some v) = false ∧ judge φ g (some v) none = true := by
  constructor <;> rfl

/-- The threshold depends on the group (`case when source = 'A' then 80 else 5`).
The constant query read the key from group 0, so group 1's 10 rows were held to
80 and failed, and group 0's 10 rows were held to 5 and passed. -/
theorem unsubstituted_key_reads_another_group :
    let φ : Nat → Nat → Nat → B3 := fun g n _ =>
      if (if g = 0 then 80 else 5) ≤ n then t else f
    let fromRow0 : Nat → Nat := fun _ => 0
    let fromRow1 : Nat → Nat := fun _ => 1
    (check fromRow0 id φ [(1, some 10)] [(1, some 0)] = false ∧
      Spec φ [(1, some 10)] [(1, some 0)]) ∧
    (check fromRow1 id φ [(0, some 10)] [(0, some 0)] = true ∧
      ¬ Spec φ [(0, some 10)] [(0, some 0)]) := by
  refine ⟨⟨by decide, ?_⟩, ⟨by decide, ?_⟩⟩
  · intro g hg
    simp [groups] at hg
    subst hg
    decide
  · intro h
    have := h 0 (Or.inl (by simp [groups]))
    revert this
    decide

/-- Any literal that maps two values to one passes an unchanged-total check on
a build that changed it. -/
theorem lossy_literal_passes_a_change [DecidableEq V] (lit : V → V) (g : K) (n o : V)
    (same : lit n = lit o) (changed : n ≠ o) :
    let φ : K → V → V → B3 := fun _ a b => if a = b then t else f
    check id lit φ [(g, some n)] [(g, some o)] = true ∧
      ¬ Spec φ [(g, some n)] [(g, some o)] := by
  refine ⟨?_, fun h => ?_⟩
  · simp [check, order, groups, judge, same, isT]
  · have := h g (Or.inl (by simp [groups]))
    simp [judge, changed, isT] at this

/-- Between `2^52` and `2^53` a double is a whole number, so a `numeric(38,2)`
sum near `5e15`, counted here in cents, rounds to the unit and loses its cents. -/
def asDouble (cents : Int) : Int := (cents + 50) / 100 * 100

theorem decimal_as_double_merges_cents :
    (2 : Int) ^ 52 ≤ 5000000000000000 ∧ (5000000000000000 : Int) < 2 ^ 53 ∧
      asDouble 500000000000000001 = asDouble 500000000000000000 := by
  decide

/-! ## Reading the constant query's answer -/

/-- A boolean cell as a driver returns it. -/
inductive Cell where
  | bool (b : Bool)
  | int (i : Int)
  | null
  deriving DecidableEq

/-- DuckDB, Postgres and BigQuery return booleans. -/
def nativeBool : B3 → Cell
  | t => .bool true
  | f => .bool false
  | u => .null

/-- SQLite and MySQL return `1`/`0`. -/
def intBool : B3 → Cell
  | t => .int 1
  | f => .int 0
  | u => .null

/-- Shipped: `row[i] is True`. -/
def isTrueCell (c : Cell) : Bool := c == .bool true

/-- Fixed: `row[i] is not None and bool(row[i])`. -/
def truthy : Cell → Bool
  | .bool b => b
  | .int i => i != 0
  | .null => false

theorem is_true_rejects_sqlite_true : isTrueCell (intBool t) = false := by decide

theorem truthy_exact (p : B3) : truthy (nativeBool p) = isT p ∧ truthy (intBool p) = isT p := by
  cases p <;> decide

/-! ## Matching groups -/

/-- A join of the two sides on SQL `=`: a NULL key matches nothing. -/
def lookupEq (s : Side (Option Nat) V) (g : Option Nat) : Option (Option V) :=
  match g with
  | none => none
  | some _ => s.lookup g

/-- One NULL-keyed group, unchanged between the builds. The identity join the
evaluator uses passes it; an `=` join finds no build row for it and fails it
as vanished. -/
theorem sql_eq_join_vanishes_null_group :
    let φ : Option Nat → Nat → Nat → B3 := fun _ n o => if o ≤ n then t else f
    let s : Side (Option Nat) Nat := [(none, some 5)]
    Spec φ s s ∧ judge φ none (lookupEq s none) (s.lookup none) = false := by
  refine ⟨?_, by decide⟩
  intro g hg
  simp [groups] at hg
  subst hg
  decide

end Trilogy.Assert
