import Trilogy.Cost

/-!
# Stacking a partition family

A family of `complete where` arms unions into one source (`Sourcing.lean`).
For a table at its row key that union is the table, row for row
(`Sourcing.union_perm_of_partition`). For SUMMARIES it is subtler: each arm
stores one row per grain value of the rows it keeps, so stacking the arms
stores one row per grain value of the whole table only when every grain
value's rows lie in ONE arm, which holds when the grain determines the arm
(`Stack.group_in_one_arm`). Then each stacked row is that grain value's
complete aggregate (`Stack.stacked_summary_sum_exact`) and no grain value
repeats (`Stack.stacked_keys_nodup`).

An arm whose discriminator the grain does not determine breaks this: a
species summary per city stores `Acer` once per city
(`Stack.unbound_arm_repeats_grain`). A bound discriminator is the binding's
own claim that the grain determines it (the FK rule); an unbound one has to
follow from declared keys.

The section after that covers value-set arms. `x in S` is the disjunction of
`x = a` over `S`, which the cell checker already models as an arm
(`Stack.in_is_eq_disjunction`), so `Sourcing.checker_exact` covers families
that mix `=` and `in` arms. A search that picks one arm per VALUE cannot
take them: it can pick `x = b` for `b` and `x in (b, c)` for `c`, and count
`b` twice (`Stack.per_value_choice_double_counts`).
-/

namespace Trilogy
namespace Stack

variable {α κ : Type}

/-- **Each grain value's rows lie in one arm** when the arm is a predicate
`q` on the grain: the arm keeps the whole group of `k` or none of it. -/
theorem Stack.group_in_one_arm [DecidableEq κ] (g : α → κ) (q : κ → Bool)
    (rows : List α) (k : κ) :
    (rows.filter (fun r => q (g r))).filter (fun r => g r == k) =
      if q k then rows.filter (fun r => g r == k) else [] := by
  rw [List.filter_filter]
  by_cases hq : q k = true
  · simp only [hq, ↓reduceIte]
    congr 1
    funext r
    by_cases hr : g r = k
    · simp [hr, hq]
    · simp [hr]
  · simp only [hq]
    apply List.filter_eq_nil_iff.mpr
    intro r _ h
    simp only [Bool.and_eq_true, beq_iff_eq] at h
    obtain ⟨h1, h2⟩ := h
    rw [h1] at h2
    exact hq h2

/-- The stacked row for `k` is `k`'s complete aggregate. -/
theorem Stack.stacked_summary_sum_exact [DecidableEq κ] (g : α → κ) (q : κ → Bool)
    (v : α → Int) (rows : List α) (k : κ) (hk : q k = true) :
    sumBy v ((rows.filter (fun r => q (g r))).filter (fun r => g r == k)) =
      sumBy v (rows.filter (fun r => g r == k)) := by
  rw [Stack.group_in_one_arm g q rows k, hk]
  rfl

/-- **No grain value repeats.** An arm and its complement, both predicates
on the grain, store disjoint grain values. -/
theorem Stack.stacked_keys_nodup [DecidableEq κ] (g : α → κ) (q : κ → Bool)
    (rows : List α) :
    (Cost.stored (fun r => q (g r)) g rows ++
      Cost.stored (fun r => !q (g r)) g rows).Nodup := by
  rw [List.nodup_append]
  refine ⟨nodup_dedup _, nodup_dedup _, ?_⟩
  intro a ha b hb hab
  subst hab
  obtain ⟨r, hr, rfl⟩ := List.mem_map.mp ((mem_dedup _ _).mp ha)
  obtain ⟨s, hs, hgs⟩ := List.mem_map.mp ((mem_dedup _ _).mp hb)
  have h1 := (List.mem_filter.mp hr).2
  have h2 := (List.mem_filter.mp hs).2
  rw [hgs, h1] at h2
  simp at h2

/-- **The bug.** Rows are `(city, species)`. Arms `city = 0` and its
complement, stored at the species grain: species 5 is stored by both. -/
theorem Stack.unbound_arm_repeats_grain :
    let rows := [((0 : Nat), (5 : Nat)), (1, 5)]
    ¬ (Cost.stored (fun r => r.1 == 0) Prod.snd rows ++
      Cost.stored (fun r => !(r.1 == 0)) Prod.snd rows).Nodup := by
  decide

/-! ### Value-set arms -/

/-- `x in S` is the arm of `x = a` atoms over `S`, so the cell checker's
exactness theorem covers families with value-set arms. -/
theorem Stack.in_is_eq_disjunction (S : List Int) (v : Val) :
    Cost.inSet S v = (S.map (fun a => Atom.mk Op.eq a)).any (fun a => a.eval v) := by
  cases v with
  | none => simp [Cost.inSet, Atom.eval]
  | some x =>
    rw [Bool.eq_iff_iff]
    simp [Cost.inSet, Atom.eval, List.any_map, Function.comp_def, List.any_eq_true]

/-- A point arm and a value-set arm partition `{0, 1, 2}`. -/
theorem Stack.value_set_family_partitions :
    ∀ v ∈ [some 0, some 1, some 2], hits [Cost.inSet [0], Cost.inSet [1, 2]] v = 1 := by
  decide

/-- Choosing an arm per value: `x = 1` for 1 and `x in (1, 2)` for 2. Both
claim 1, so the union counts its rows twice. -/
theorem Stack.per_value_choice_double_counts :
    hits [Cost.inSet [1], Cost.inSet [1, 2]] (some 1) = 2 := by decide

end Stack
end Trilogy
