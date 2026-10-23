import Trilogy.Agg

/-!
# WHERE, staged `then where`, HAVING

A condition may be cross-row: it reads the population it is evaluated over
(`sum(z) by x > 5` does). A flat WHERE evaluates every conjunct over the full
population; stage `n` of a chain is evaluated over the rows passing stages
`1..n-1` (docs/staged_where_design.md).
-/

namespace Trilogy

open B3

variable {α : Type}

/-- A condition sees the population it gates. -/
abbrev Cond (α : Type) := List α → α → B3

def gate (c : Cond α) (pop : List α) : List α := pop.filter (fun r => isT (c pop r))

def flat (cs : List (Cond α)) (pop : List α) : List α :=
  pop.filter (fun r => cs.all (fun c => isT (c pop r)))

def staged : List (Cond α) → List α → List α
  | [], pop => pop
  | c :: cs, pop => staged cs (gate c pop)

def rowLocal (c : Cond α) : Prop := ∀ p₁ p₂ r, c p₁ r = c p₂ r

theorem isT_and3 (a b : B3) : isT (and3 a b) = (isT a && isT b) := by
  cases a <;> cases b <;> rfl

theorem filter_filter' (p q : α → Bool) (l : List α) :
    (l.filter p).filter q = l.filter (fun r => p r && q r) := by
  induction l with
  | nil => rfl
  | cons a l ih => by_cases hp : p a <;> by_cases hq : q a <;> simp_all

theorem Staged.single_stage_eq_flat (c : Cond α) (pop : List α) :
    staged [c] pop = flat [c] pop := by
  simp [staged, gate, flat]

/-- A stage with no cross-row computation is an ordinary conjunct. -/
theorem Staged.row_stage_folds (c₁ c₂ : Cond α) (h : rowLocal c₂) (pop : List α) :
    staged [c₁, c₂] pop = flat [c₁, c₂] pop := by
  simp only [staged, gate, flat, List.all_cons, List.all_nil, Bool.and_true]
  rw [filter_filter']
  congr 1; funext r; rw [h _ pop r]

theorem Staged.stage_idempotent (c : Cond α) (h : rowLocal c) (pop : List α) :
    staged [c, c] pop = flat [c] pop := by
  rw [Staged.row_stage_folds c c h]
  simp [flat]

/-- The inline-filter spelling: the later condition read over the earlier
stage's rows. -/
def inline (c₁ g : Cond α) : Cond α := fun pop r => g (gate c₁ pop) r

/-- `where a then where g` ≡ `where a and g(? a)`. -/
theorem Staged.staged_eq_inline_filter (c₁ g : Cond α) (pop : List α) :
    staged [c₁, g] pop = flat [c₁, inline c₁ g] pop := by
  simp only [staged, gate, flat, inline, List.all_cons, List.all_nil, Bool.and_true]
  rw [filter_filter']

/-- Three stages: the last stage sees the conjunction of the earlier ones. -/
theorem Staged.chain_eq_inline_filter (c₁ c₂ g : Cond α) (h₂ : rowLocal c₂)
    (pop : List α) :
    staged [c₁, c₂, g] pop =
      flat [c₁, c₂, inline (fun p r => and3 (c₁ p r) (c₂ p r)) g] pop := by
  simp only [staged, gate, flat, inline, List.all_cons, List.all_nil, Bool.and_true]
  have hc : ∀ r, (isT (c₁ pop r) && isT (c₂ (pop.filter (fun r => isT (c₁ pop r))) r))
      = isT (and3 (c₁ pop r) (c₂ pop r)) := by
    intro r; rw [h₂ _ pop r, isT_and3]
  have hpop : ((pop.filter fun r => isT (c₁ pop r)).filter fun r =>
      isT (c₂ (pop.filter fun r => isT (c₁ pop r)) r)) =
      pop.filter (fun r => isT (and3 (c₁ pop r) (c₂ pop r))) := by
    rw [filter_filter']; exact List.filter_congr (fun r _ => hc r)
  rw [hpop, filter_filter']
  apply List.filter_congr; intro r _; rw [isT_and3, Bool.and_assoc]

/-! ### One address, one value

`sum(z) by x` gating stage 1 and the same spelling gating stage 2 name two
values over two populations; with one value per address the chain has no
consistent reading, so `_validate_staged_where` rejects it. -/

structure Ev where
  x : Nat
  f : Bool
  z : Int
  deriving DecidableEq, Repr

def sumByX (pop : List Ev) (r : Ev) : Int :=
  ((pop.filter (fun e => e.x == r.x)).map Ev.z).sum

def witness : List Ev := [⟨1, true, 1⟩, ⟨1, false, 10⟩]

theorem Staged.same_address_two_stages_differ :
    sumByX witness ⟨1, true, 1⟩ ≠ sumByX (witness.filter (·.f)) ⟨1, true, 1⟩ := by
  decide

/-! ### Flat WHERE aggregates and HAVING -/

/-- A flat WHERE aggregate reads the pristine population: the gate is the
row predicate over rows qualifying on the full population, a membership in
a rowset computed without the sibling conjunct. -/
theorem Where.flat_agg_atom_eq_membership (c g : Cond α) (pop : List α) :
    flat [c, g] pop = (gate g pop).filter (fun r => isT (c pop r)) := by
  simp only [flat, gate, List.all_cons, List.all_nil, Bool.and_true]
  rw [filter_filter']
  congr 1; funext r; exact Bool.and_comm _ _

/-- HAVING is a row predicate over the grouped rows: a WHERE over the
aggregated rowset. -/
theorem Where.having_eq_outer_where {β : Type} (grouped : List β) (p : β → B3) :
    grouped.filter (fun g => isT (p g)) = gate (fun _ g => p g) grouped :=
  rfl

end Trilogy
