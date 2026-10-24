import Trilogy.Logic

/-!
# Null rejection: which conditions force a column non-null

An outer join pads the rows it could not match with NULL. A WHERE that can
never hold on such a row makes the outer join equal to a stricter one
(`upgrade_by_proof`), and a source that holds only the non-NULL values of a
column then answers alone. The planner finds such columns syntactically
(`gather_non_null_proofs`, `concepts_implied_non_null`); `proofs` is that walk.

`proofs_sound`: a column in `proofs c` is non-NULL on every row `c` keeps. It
needs each function the walk descends into to be strict, i.e. NULL on a NULL
argument. The walk's soundness rests on four rules:

- descend only into strict functions. `concat` skips a NULL argument, so a
  filter over `concat(x, 'z')` keeps rows with a NULL `x`
  (`skip_null_keeps_null`).
- membership is identity, not `=`. `x in (1, null)` keeps a NULL `x`
  (`mem_null_member_keeps_null`), and `x not in (1, 3)` does too
  (`not_mem_keeps_null`). An `in` proves its operand only over a NULL-free set.
- two-valued `not` keeps a NULL row that its operand rejected
  (`not_keeps_null`), so the walk does not descend into it.
- a proof forces a side of a join only through a column read off that side.
  A value computed after the join from a padded column need not be NULL on
  the padded row. `ret is not null` there is `false`, so `flag = false` keeps
  exactly the padded rows (`derived_flag_breaks_upgrade`). A derived column
  is judged by its lineage instead (`proofs_subst`).

`total_sound` is the dual the nullability inference relies on: an expression
whose columns are non-NULL is non-NULL when every function is total. `x % 0`
(`mod_zero_not_total`) and a missing map key are NULL from non-NULL inputs.
-/

namespace Trilogy.NullReject

open Trilogy.B3

abbrev Val := Option Int
abbrev Row := Nat → Val

/-- A binary scalar function and what the planner claims about it. -/
structure Fn where
  ap : Val → Val → Val
  strict : Bool
  total : Bool

def Fn.Strict (g : Fn) : Prop :=
  g.strict = true → ∀ a b, (a = none ∨ b = none) → g.ap a b = none

def Fn.Total (g : Fn) : Prop :=
  g.total = true → ∀ a b, a ≠ none → b ≠ none → g.ap a b ≠ none

inductive Expr where
  | col (i : Nat)
  | lit (v : Int)
  | app (g : Fn) (a b : Expr)
  | coalesce (a : Expr) (d : Int)

def Expr.eval (r : Row) : Expr → Val
  | col i => r i
  | lit v => some v
  | app g a b => g.ap (a.eval r) (b.eval r)
  | coalesce a d => (a.eval r).or (some d)

/-- Every function in `e` is as strict and total as claimed. -/
def Expr.Sound : Expr → Prop
  | col _ | lit _ => True
  | app g a b => g.Strict ∧ g.Total ∧ a.Sound ∧ b.Sound
  | coalesce a _ => a.Sound

/-- `concepts_implied_non_null`: columns a non-NULL `e` proves non-NULL. -/
def Expr.implied : Expr → List Nat
  | col i => [i]
  | lit _ => []
  | app g a b => if g.strict then a.implied ++ b.implied else []
  | coalesce _ _ => []

theorem implied_sound (r : Row) :
    ∀ e : Expr, e.Sound → ∀ i ∈ e.implied, r i = none → e.eval r = none
  | .col _, _, i, h, hr => by simp [Expr.implied] at h; subst h; exact hr
  | .lit _, _, _, h, _ => by simp [Expr.implied] at h
  | .app g a b, ⟨hg, _, ha, hb⟩, i, h, hr => by
    simp only [Expr.implied] at h
    split at h
    · rename_i hs
      simp only [Expr.eval]
      rcases List.mem_append.mp h with h | h
      · exact hg hs _ _ (.inl (implied_sound r a ha i h hr))
      · exact hg hs _ _ (.inr (implied_sound r b hb i h hr))
    · simp at h
  | .coalesce _ _, _, _, h, _ => by simp [Expr.implied] at h

/-- The nullability inference: `e` may be NULL. -/
def Expr.nullable (nullCol : Nat → Bool) : Expr → Bool
  | col i => nullCol i
  | lit _ => false
  | app g a b => !g.total || a.nullable nullCol || b.nullable nullCol
  | coalesce _ _ => false

theorem total_sound (nullCol : Nat → Bool) (r : Row)
    (hr : ∀ i, nullCol i = false → r i ≠ none) :
    ∀ e : Expr, e.Sound → e.nullable nullCol = false → e.eval r ≠ none
  | .col i, _, h => hr i h
  | .lit _, _, _ => by simp [Expr.eval]
  | .app g a b, ⟨_, hg, ha, hb⟩, h => by
    simp only [Expr.nullable, Bool.or_eq_false_iff, Bool.not_eq_false'] at h
    exact hg h.1.1 _ _ (total_sound nullCol r hr a ha h.1.2)
      (total_sound nullCol r hr b hb h.2)
  | .coalesce _ _, _, _ => by simp [Expr.eval]

inductive Cmp where
  | eq | ne | lt | le

def Cmp.ev : Cmp → Int → Int → Bool
  | eq, a, b => a == b
  | ne, a, b => a != b
  | lt, a, b => decide (a < b)
  | le, a, b => decide (a ≤ b)

def ofBool : Bool → B3
  | true => t
  | false => f

inductive Cond where
  | cmp (op : Cmp) (a b : Expr)
  | isNull (a : Expr)
  | isNotNull (a : Expr)
  /-- `a in s`, matched by identity: a NULL probe matches a NULL member. -/
  | mem (a : Expr) (s : List Val)
  | notMem (a : Expr) (s : List Val)
  | and (p q : Cond)
  | or (p q : Cond)
  | not (p : Cond)

def Cond.eval (r : Row) : Cond → B3
  | cmp op a b =>
    match a.eval r, b.eval r with
    | some x, some y => ofBool (op.ev x y)
    | _, _ => u
  | isNull a => ofBool (a.eval r).isNone
  | isNotNull a => ofBool (a.eval r).isSome
  | mem a s => ofBool (s.contains (a.eval r))
  | notMem a s => ofBool (!s.contains (a.eval r))
  | and p q => and3 (p.eval r) (q.eval r)
  | or p q => or3 (p.eval r) (q.eval r)
  | not p => not2 (p.eval r)

def Cond.Sound : Cond → Prop
  | cmp _ a b => a.Sound ∧ b.Sound
  | isNull a | isNotNull a | mem a _ | notMem a _ => a.Sound
  | and p q | or p q => p.Sound ∧ q.Sound
  | not p => p.Sound

/-- `gather_non_null_proofs`: columns `c` forces non-NULL on the rows it
keeps. `coalesce(a, d) op k` proves `a` when the default fails the test. -/
def Cond.proofs : Cond → List Nat
  | cmp op (.coalesce a d) (.lit k) =>
    if op.ev d k then [] else a.implied
  | cmp _ a b => a.implied ++ b.implied
  | isNull _ => []
  | isNotNull a => a.implied
  | mem a s => if s.contains none then [] else a.implied
  | notMem _ _ => []
  | and p q => p.proofs ++ q.proofs
  | or p q => p.proofs.filter (· ∈ q.proofs)
  | not _ => []

private theorem holds_ofBool {b : Bool} : holds (ofBool b) ↔ b = true := by
  cases b <;> decide

private theorem cmp_null (r : Row) (op : Cmp) (a b : Expr)
    (h : a.eval r = none ∨ b.eval r = none) : ¬ holds ((Cond.cmp op a b).eval r) := by
  simp only [Cond.eval]
  cases ha : a.eval r <;> cases hb : b.eval r <;> simp_all [holds]

private theorem cmp_implied (r : Row) (op : Cmp) (a b : Expr) (ha : a.Sound)
    (hb : b.Sound) (i : Nat) (h : i ∈ a.implied ++ b.implied) (hr : r i = none) :
    ¬ holds ((Cond.cmp op a b).eval r) := by
  apply cmp_null
  rcases List.mem_append.mp h with h | h
  · exact .inl (implied_sound r a ha i h hr)
  · exact .inr (implied_sound r b hb i h hr)

private theorem and3_holds {p q : B3} : holds (and3 p q) ↔ holds p ∧ holds q := by
  cases p <;> cases q <;> decide

private theorem or3_holds {p q : B3} : holds (or3 p q) ↔ holds p ∨ holds q := by
  cases p <;> cases q <;> decide

theorem proofs_sound (r : Row) :
    ∀ c : Cond, c.Sound → ∀ i ∈ c.proofs, r i = none → ¬ holds (c.eval r)
  | .cmp op a b, ⟨ha, hb⟩, i, h, hr => by
    match a, b, ha, hb, h with
    | .coalesce a' d, .lit k, ha, _, h =>
      simp only [Cond.proofs] at h
      split at h
      · simp at h
      · rename_i hd
        have := implied_sound r a' ha i h hr
        simp only [Cond.eval, Expr.eval, this, Option.none_or]
        rw [holds_ofBool]; simpa using hd
    | .col _, _, ha, hb, h | .lit _, _, ha, hb, h | .app _ _ _, _, ha, hb, h
    | .coalesce _ _, .col _, ha, hb, h | .coalesce _ _, .app _ _ _, ha, hb, h
    | .coalesce _ _, .coalesce _ _, ha, hb, h =>
      exact cmp_implied r op _ _ ha hb i h hr
  | .isNull _, _, _, h, _ => by simp [Cond.proofs] at h
  | .isNotNull a, ha, i, h, hr => by
    simp only [Cond.eval, implied_sound r a ha i h hr, holds_ofBool]; decide
  | .mem a s, ha, i, h, hr => by
    simp only [Cond.proofs] at h
    split at h
    · simp at h
    · rename_i hs
      simp only [Cond.eval, implied_sound r a ha i h hr, holds_ofBool]
      simpa using hs
  | .notMem _ _, _, _, h, _ => by simp [Cond.proofs] at h
  | .and p q, ⟨hp, hq⟩, i, h, hr => by
    simp only [Cond.eval, and3_holds]
    rintro ⟨h1, h2⟩
    rcases List.mem_append.mp h with h | h
    · exact proofs_sound r p hp i h hr h1
    · exact proofs_sound r q hq i h hr h2
  | .or p q, ⟨hp, hq⟩, i, h, hr => by
    simp only [Cond.proofs, List.mem_filter, decide_eq_true_eq] at h
    simp only [Cond.eval, or3_holds, not_or]
    exact ⟨proofs_sound r p hp i h.1 hr, proofs_sound r q hq i h.2 hr⟩
  | .not _, _, _, h, _ => by simp [Cond.proofs] at h

/-- Membership is total, so `not (a in s)` is `a not in s`: the parser
emits the complement, which keeps the set an existence input. -/
theorem not_mem_is_complement (r : Row) (a : Expr) (s : List Val) :
    (Cond.not (.mem a s)).eval r = (Cond.notMem a s).eval r := by
  simp only [Cond.eval]
  cases s.contains (a.eval r) <;> rfl

/-- A COUNT padded to 0 reads as `coalesce(count, 0)`: `= 0` holds on the
padded row and proves nothing, `= 1` proves the count's group present. -/
theorem zero_filled_count :
    (Cond.cmp .eq (.coalesce (.col 0) 0) (.lit 0)).proofs = [] ∧
      (Cond.cmp .eq (.coalesce (.col 0) 0) (.lit 1)).proofs = [0] := by
  decide

/-! ## Joins -/

/-- A joined row: the left row and the matched right row, or `none` where
the outer join padded. A consumer reads left column `i` at `2i` and right
column `i` at `2i+1`. -/
structure JRow where
  l : Row
  r : Option Row

def JRow.view (j : JRow) : Row := fun n =>
  if n % 2 = 0 then j.l (n / 2) else j.r.bind (· (n / 2))

def leftJoin (on : Row → Row → Bool) (L R : List Row) : List JRow :=
  L.flatMap fun l =>
    match R.filter (on l) with
    | [] => [⟨l, none⟩]
    | ms => ms.map fun r => ⟨l, some r⟩

def innerJoin (on : Row → Row → Bool) (L R : List Row) : List JRow :=
  L.flatMap fun l => (R.filter (on l)).map fun r => ⟨l, some r⟩

def keep (c : Cond) (js : List JRow) : List JRow :=
  js.filter fun j => isT (c.eval j.view)

private theorem isT_false {p : B3} (h : ¬ holds p) : isT p = false := by
  cases p <;> simp_all [holds, isT]

/-- A WHERE that rejects a NULL in a column read off the right side makes
the left join equal the inner join. -/
theorem upgrade_sound (on : Row → Row → Bool) (L R : List Row) (c : Cond) (i : Nat)
    (hc : ∀ v : Row, v (2 * i + 1) = none → ¬ holds (c.eval v)) :
    keep c (leftJoin on L R) = keep c (innerJoin on L R) := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [keep, leftJoin, innerJoin, List.flatMap_cons, List.filter_append] at ih ⊢
    rw [ih]
    congr 1
    split
    · rename_i hm
      rw [hm]
      have hpad : (JRow.view ⟨l, none⟩) (2 * i + 1) = none := by
        simp [JRow.view, Nat.add_mod]
      simp [isT_false (hc _ hpad)]
    · rfl

theorem upgrade_by_proof (on : Row → Row → Bool) (L R : List Row) (c : Cond)
    (hc : c.Sound) (i : Nat) (h : 2 * i + 1 ∈ c.proofs) :
    keep c (leftJoin on L R) = keep c (innerJoin on L R) :=
  upgrade_sound on L R c i fun v hv => proofs_sound v c hc _ h hv

/-- Substituting a derived column's lineage for the column. -/
def Expr.subst (k : Nat) (e : Expr) : Expr → Expr
  | col i => if i = k then e else col i
  | lit v => lit v
  | app g a b => app g (subst k e a) (subst k e b)
  | coalesce a d => coalesce (subst k e a) d

def setCol (r : Row) (k : Nat) (v : Val) : Row := fun i => if i = k then v else r i

/-- A consumer that computes column `k` from `e` reads the same value as
the substituted expression, so its proofs are judged on the lineage. -/
theorem proofs_subst (r : Row) (k : Nat) (e x : Expr) :
    x.eval (setCol r k (e.eval r)) = (Expr.subst k e x).eval r := by
  induction x with
  | col i => by_cases h : i = k <;> simp [Expr.subst, Expr.eval, h, setCol]
  | lit _ => rfl
  | app g a b iha ihb => simp [Expr.subst, Expr.eval, iha, ihb]
  | coalesce a d ih => simp [Expr.subst, Expr.eval, ih]

/-! ## Where the rules break -/

/-- `concat` skips a NULL argument. -/
def skipNull : Fn where
  ap a b := match a, b with
    | some x, some y => some (x + y)
    | some x, none | none, some x => some x
    | none, none => none
  strict := true
  total := true

/-- Claiming `concat` strict: `concat(x, 1) = 1` keeps the row with NULL `x`. -/
theorem skip_null_keeps_null :
    let c := Cond.cmp .eq (.app skipNull (.col 0) (.lit 1)) (.lit 1)
    0 ∈ c.proofs ∧ holds (c.eval fun _ => none) := by
  decide

theorem mem_null_member_keeps_null :
    holds ((Cond.mem (.col 0) [some 1, none]).eval fun _ => none) := by decide

theorem not_mem_keeps_null :
    holds ((Cond.notMem (.col 0) [some 1, some 3]).eval fun _ => none) := by decide

theorem not_keeps_null :
    holds ((Cond.not (.cmp .eq (.col 0) (.lit 1))).eval fun _ => none) := by decide

/-- The flag `ret is not null`, computed after the join. -/
def presence : Fn where
  ap a _ := some (if a.isSome then 1 else 0)
  strict := false
  total := true

/-- Treating a derived flag as a column of the right side: `flag = 0` holds
on the padded row, so the inner join loses it. Its lineage proves nothing
(`proofs` is empty), which is the answer `proofs_subst` reads off. -/
theorem derived_flag_breaks_upgrade :
    let flag := Expr.app presence (.col 1) (.lit 0)
    let c := Cond.cmp .eq flag (.lit 0)
    c.proofs = [] ∧
      keep c (leftJoin (fun _ _ => false) [fun _ => some 0] [fun _ => some 0]) ≠
        keep c (innerJoin (fun _ _ => false) [fun _ => some 0] [fun _ => some 0]) := by
  refine ⟨rfl, ?_⟩
  simp [keep, leftJoin, innerJoin, Cond.eval, Expr.eval, presence, JRow.view, isT, ofBool,
    Cmp.ev]

def modFn : Fn where
  ap a b := match a, b with
    | some x, some y => if y = 0 then none else some (x % y)
    | _, _ => none
  strict := true
  total := true

/-- `x % 0` is NULL from non-NULL inputs, so `%` is not total. -/
theorem mod_zero_not_total : ¬ modFn.Total := by
  intro h
  exact h rfl (some 1) (some 0) (by simp) (by simp) (by simp [modFn])

end Trilogy.NullReject
