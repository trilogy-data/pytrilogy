/-!
# Condition logic

`and`/`or` are Kleene (SQL three-valued). `not` is TWO-valued: an UNKNOWN
operand negates to TRUE (`fnot` renders `coalesce(p, false) = false`), so a
negation never yields UNKNOWN. A WHERE keeps a row iff its condition `holds`.

`mixed_negation_breaks_de_morgan` is the bug the fuzzer's `de_morgan_*` laws
found: compound `not (...)` was Kleene while atomic `not p` was two-valued.
-/

namespace Trilogy

inductive B3 where
  | t | f | u
  deriving DecidableEq, Repr

namespace B3

def and3 : B3 → B3 → B3
  | f, _ => f
  | _, f => f
  | t, t => t
  | _, _ => u

def or3 : B3 → B3 → B3
  | t, _ => t
  | _, t => t
  | f, f => f
  | _, _ => u

/-- Trilogy's `not`: `coalesce(p, false) = false`. -/
def not2 : B3 → B3
  | t => f
  | _ => t

/-- SQL's `not`, which `not (...)` rendered as before the fix. -/
def notK : B3 → B3
  | t => f
  | f => t
  | u => u

def holds (p : B3) : Prop := p = t

instance (p : B3) : Decidable (holds p) := inferInstanceAs (Decidable (p = t))

/-- `holds` as a filter predicate. -/
def isT : B3 → Bool
  | t => true
  | _ => false

end B3

open B3

theorem Logic.and_comm (p q : B3) : and3 p q = and3 q p := by
  cases p <;> cases q <;> rfl

theorem Logic.and_true_left (p : B3) : and3 t p = p := by
  cases p <;> rfl

theorem Logic.not_and (p q : B3) : not2 (and3 p q) = or3 (not2 p) (not2 q) := by
  cases p <;> cases q <;> rfl

theorem Logic.not_or (p q : B3) : not2 (or3 p q) = and3 (not2 p) (not2 q) := by
  cases p <;> cases q <;> rfl

/-- As a WHERE; as a projected value `not not p` is `coalesce(p, false)`. -/
theorem Logic.not_not_holds (p : B3) : holds (not2 (not2 p)) ↔ holds p := by
  cases p <;> decide

theorem Logic.not_not_not_value : ∃ p, not2 (not2 p) ≠ p := ⟨u, by decide⟩

theorem Logic.or_not_self_holds (p : B3) : holds (or3 p (not2 p)) := by
  cases p <;> decide

theorem Logic.and_not_self_never_holds (p : B3) : ¬ holds (and3 p (not2 p)) := by
  cases p <;> decide

/-- `not (p)` and `not p` are one operator; parentheses are transparent. -/
theorem Logic.not_paren_atom (p q : B3) :
    and3 (not2 p) (not2 q) = not2 (or3 p q) := (Logic.not_or p q).symm

/-- Under SQL logic the excluded middle needs a known operand. -/
theorem Logic.kleene_or_not_self_holds_iff (p : B3) :
    holds (or3 p (notK p)) ↔ p ≠ u := by
  cases p <;> decide

/-- Kleene `not` on compounds beside two-valued `not` on atoms: De Morgan
fails on an UNKNOWN operand. -/
theorem Logic.mixed_negation_breaks_de_morgan :
    ∃ p q, holds (notK (or3 p q)) ≠ holds (and3 (not2 p) (not2 q)) :=
  ⟨u, f, by decide⟩

theorem Logic.not2_two_valued (p : B3) : not2 p ≠ u := by
  cases p <;> decide

theorem Logic.holds_split (p : B3) : holds p ∨ holds (not2 p) := by
  cases p <;> decide

theorem Logic.holds_exclusive (p : B3) : ¬ (holds p ∧ holds (not2 p)) := by
  cases p <;> decide

end Trilogy
