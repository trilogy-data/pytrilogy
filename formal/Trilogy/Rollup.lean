import Trilogy.Agg

/-!
# Aggregate resolution from a summary table

A summary datasource stores one aggregate value per group of its grain `g`.
A query asks for that aggregate at a target grain under a WHERE. Both the
filter and the target group are read off the summary's own grain whenever
their columns are grain keys or properties of them. Then each is a predicate
`q : κ → Bool` on the summary key: `q k := p k && t k == τ`, one target group
`τ` at a time. The rolled-up answer re-aggregates the summary rows `q` keeps;
the direct answer aggregates the base rows `q ∘ g` keeps.

The planner's rule is the satisfiability theorem below: when the aggregate
re-aggregates (`sum`, `count`) and every filter and target column is
evaluable on the summary's grain, the summary answers. The target grain may
be empty (a grand total) and the aggregate may be a component of a larger
demand (`count(x) by k` inside a window): neither changes `q`.

`none` is NULL. SQL `SUM` skips NULLs and is NULL over no non-NULL input;
`COUNT` is never NULL.
-/

namespace Trilogy
namespace Rollup

variable {α κ : Type} [DecidableEq κ]

/-- SQL `SUM`. -/
def sqlSum (xs : List (Option Int)) : Option Int :=
  if xs.all (·.isNone) then none else some (xs.map (·.getD 0)).sum

/-- SQL `COUNT(x)`: the non-NULL inputs. -/
def sqlCount (xs : List (Option Int)) : Int := xs.countP (·.isSome)

def coalesce0 : Option Int → Int
  | none => 0
  | some x => x

/-- The groups a GROUP BY emits: keys holding at least one row. -/
def groups (g : α → κ) (ks : List κ) (rows : List α) : List κ :=
  ks.filter (fun k => !(slice g k rows).isEmpty)

/-- The summary column `agg` stores, read under the key predicate `q`. -/
def stored (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) (q : κ → Bool) : List (Option Int) :=
  ((groups g ks rows).filter q).map (fun k => agg (slice g k rows))

def keep (g : α → κ) (q : κ → Bool) (rows : List α) : List α :=
  rows.filter (fun r => q (g r))

def sumAgg (v : α → Option Int) (s : List α) : Option Int := sqlSum (s.map v)

def countAgg (v : α → Option Int) (s : List α) : Option Int :=
  some (sqlCount (s.map v))

theorem all_none_iff (xs : List (Option Int)) :
    xs.all (·.isNone) = true ↔ ∀ x ∈ xs, x = none := by
  simp [List.all_eq_true, Option.isNone_iff_eq_none]

theorem sqlSum_none_iff (xs : List (Option Int)) :
    sqlSum xs = none ↔ ∀ x ∈ xs, x = none := by
  unfold sqlSum
  split
  · next h => exact ⟨fun _ => (all_none_iff xs).mp h, fun _ => rfl⟩
  · next h => exact ⟨fun e => absurd e (by simp), fun hn => absurd ((all_none_iff xs).mpr hn) h⟩

theorem tot_all_none (xs : List (Option Int)) (h : ∀ x ∈ xs, x = none) :
    (xs.map (·.getD 0)).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons]
    rw [h x (List.mem_cons_self ..), ih (fun y hy => h y (List.mem_cons_of_mem _ hy))]
    rfl

theorem coalesce0_sqlSum (xs : List (Option Int)) :
    coalesce0 (sqlSum xs) = (xs.map (·.getD 0)).sum := by
  unfold sqlSum
  split
  · next h => rw [tot_all_none xs ((all_none_iff xs).mp h)]; rfl
  · rfl

theorem sqlSum_eq_of (xs ys : List (Option Int))
    (hn : (∀ x ∈ xs, x = none) ↔ (∀ y ∈ ys, y = none))
    (ht : (xs.map (·.getD 0)).sum = (ys.map (·.getD 0)).sum) :
    sqlSum xs = sqlSum ys := by
  have e : xs.all (·.isNone) = ys.all (·.isNone) :=
    Bool.eq_iff_iff.mpr (by rw [all_none_iff, all_none_iff]; exact hn)
  unfold sqlSum
  rw [e, ht]

theorem slice_keep (g : α → κ) (q : κ → Bool) (k : κ) (rows : List α) :
    slice g k (keep g q rows) = if q k then slice g k rows else [] := by
  induction rows with
  | nil => simp [slice, keep]
  | cons r rs ih =>
    simp only [slice, keep] at *
    by_cases hk : g r = k
    · subst hk
      by_cases hq : q (g r) <;> simp [hq, ih]
    · by_cases hq : q (g r) <;> simp [hq, hk, ih]

/-- Every stored value, unfiltered. -/
def storedAll (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) : List (Option Int) :=
  (groups g ks rows).map (fun k => agg (slice g k rows))

/-- Reading the summary under `q` is the summary of the rows `q` keeps: the
filter can go on the summary scan. -/
theorem stored_keep (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) (q : κ → Bool) :
    stored g ks rows agg q = storedAll g ks (keep g q rows) agg := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
    simp only [stored, storedAll, groups, List.filter_cons, slice_keep] at *
    by_cases hq : q k <;> by_cases he : (slice g k rows).isEmpty <;> simp_all

theorem sum_groups_eq_sum (g : α → κ) (rows : List α) (f : κ → Int)
    (hf : ∀ k, slice g k rows = [] → f k = 0) (ks : List κ) :
    ((groups g ks rows).map f).sum = (ks.map f).sum := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
    by_cases he : slice g k rows = []
    · simp [groups, he, hf k he] at *; exact ih
    · simp [groups, he] at *; rw [ih]

theorem mem_slice_self (g : α → κ) (r : α) (rows : List α) (hr : r ∈ rows) :
    r ∈ slice g (g r) rows := by
  simp [slice, hr]

theorem slice_sub (g : α → κ) (k : κ) (rows : List α) {r : α}
    (hr : r ∈ slice g k rows) : r ∈ rows := (List.mem_filter.mp hr).1

theorem mem_groups (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (r : α) (hr : r ∈ rows) :
    g r ∈ groups g ks rows := by
  refine List.mem_filter.mpr ⟨hcov r hr, ?_⟩
  have hm := mem_slice_self g r rows hr
  cases hs : slice g (g r) rows with
  | nil => rw [hs] at hm; cases hm
  | cons _ _ => rfl

theorem stored_all_none_iff (g : α → κ) (ks : List κ) (rows : List α)
    (v : α → Option Int) (hcov : ∀ r ∈ rows, g r ∈ ks) :
    (∀ x ∈ storedAll g ks rows (sumAgg v), x = none) ↔
      (∀ x ∈ rows.map v, x = none) := by
  simp only [storedAll, List.mem_map, forall_exists_index, and_imp,
    forall_apply_eq_imp_iff₂, sumAgg]
  constructor
  · intro h r hr
    have := (sqlSum_none_iff _).mp (h (g r) (mem_groups g ks rows hcov r hr))
    exact this (v r) (List.mem_map_of_mem (mem_slice_self g r rows hr))
  · intro h k _
    apply (sqlSum_none_iff _).mpr
    intro x hx
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hx
    exact h r (slice_sub g k rows hr)

theorem sumBy_map_getD (v : α → Option Int) (rows : List α) :
    sumBy (fun r => (v r).getD 0) rows = ((rows.map v).map (·.getD 0)).sum := by
  simp [sumBy, Function.comp_def]

/-- The summary's group totals, re-summed, are the base rows' total. -/
theorem tot_stored_sum (g : α → κ) (ks : List κ) (hnd : ks.Nodup) (rows : List α)
    (v : α → Option Int) (hcov : ∀ r ∈ rows, g r ∈ ks) :
    ((storedAll g ks rows (sumAgg v)).map (·.getD 0)).sum =
      ((rows.map v).map (·.getD 0)).sum := by
  have hrw : ∀ k, (sumAgg v (slice g k rows)).getD 0 =
      sumBy (fun r => (v r).getD 0) (slice g k rows) := by
    intro k
    rw [sumBy_map_getD, ← coalesce0_sqlSum]
    unfold sumAgg
    cases sqlSum ((slice g k rows).map v) <;> rfl
  simp only [storedAll, List.map_map, Function.comp_def, hrw]
  rw [sum_groups_eq_sum g rows _ (fun k he => by simp [he, sumBy]) ks,
    Agg.sum_partition g _ ks hnd rows hcov, sumBy_map_getD]
  simp [Function.comp_def]

/-- **SUM rolls up.** Re-summing a summary's per-group sums under any
predicate on its grain is the filtered sum over the base rows, NULLs and the
empty answer included. -/
theorem Rollup.sum_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) :
    sqlSum (stored g ks rows (sumAgg v) q) = sumAgg v (keep g q rows) := by
  have hcov' : ∀ r ∈ keep g q rows, g r ∈ ks :=
    fun r hr => hcov r (List.mem_filter.mp hr).1
  rw [stored_keep]
  exact sqlSum_eq_of _ _ (stored_all_none_iff g ks _ v hcov')
    (tot_stored_sum g ks hnd _ v hcov')

theorem count_as_sumBy (v : α → Option Int) (rows : List α) :
    sqlCount (rows.map v) = sumBy (fun r => if (v r).isSome then 1 else 0) rows := by
  simp only [sqlCount, sumBy, List.countP_map]
  induction rows with
  | nil => simp
  | cons r rs ih =>
    simp only [List.countP_cons, List.map_cons, List.sum_cons]
    by_cases h : (v r).isSome <;> simp [h, Function.comp_apply] at ih ⊢ <;> omega

/-- **COUNT rolls up through `coalesce(sum(n), 0)`.** -/
theorem Rollup.count_coalesce (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) :
    coalesce0 (sqlSum (stored g ks rows (countAgg v) q)) =
      sqlCount ((keep g q rows).map v) := by
  have hcov' : ∀ r ∈ keep g q rows, g r ∈ ks :=
    fun r hr => hcov r (List.mem_filter.mp hr).1
  rw [stored_keep, coalesce0_sqlSum]
  simp only [storedAll, countAgg, List.map_map, Function.comp_def, Option.getD_some]
  rw [sum_groups_eq_sum _ _ _ (fun k he => by simp [he, sqlCount]) ks]
  simp only [count_as_sumBy]
  exact Agg.sum_partition g _ ks hnd _ hcov'

theorem storedAll_nil_iff (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (agg : List α → Option Int) :
    storedAll g ks rows agg = [] ↔ rows = [] := by
  constructor
  · intro h
    cases rows with
    | nil => rfl
    | cons r rs =>
      exfalso
      have hg := mem_groups g ks (r :: rs) hcov r (List.mem_cons_self ..)
      have hm : agg (slice g (g r) (r :: rs)) ∈ storedAll g ks (r :: rs) agg :=
        List.mem_map_of_mem hg
      rw [h] at hm
      cases hm
  · intro h
    subst h
    simp [storedAll, groups, slice]

/-- A target group the base rows reach gets the exact count from a bare
`sum(n)`: GROUP BY never emits an empty group. -/
theorem Rollup.count_group (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) (hne : keep g q rows ≠ []) :
    sqlSum (stored g ks rows (countAgg v) q) =
      some (sqlCount ((keep g q rows).map v)) := by
  have hcov' : ∀ r ∈ keep g q rows, g r ∈ ks :=
    fun r hr => hcov r (List.mem_filter.mp hr).1
  have hc := Rollup.count_coalesce g ks hnd rows hcov v q
  rw [stored_keep] at hc ⊢
  have hs : storedAll g ks (keep g q rows) (countAgg v) ≠ [] :=
    fun e => hne ((storedAll_nil_iff g ks _ hcov' _).mp e)
  have hn : sqlSum (storedAll g ks (keep g q rows) (countAgg v)) ≠ none := by
    intro e
    obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil _ hs
    have := (sqlSum_none_iff _).mp e x hx
    simp only [storedAll, countAgg, List.mem_map] at hx
    obtain ⟨_, _, rfl⟩ := hx
    cases this
  revert hc hn
  cases sqlSum (storedAll g ks (keep g q rows) (countAgg v)) <;> simp [coalesce0]

/-- A grand total under Trilogy's rule that a group formed from no rows is
no row (`HAVING count(*) > 0`): present only when it read a row. -/
def total {β : Type} (read : List β) (x : Option Int) : Option (Option Int) :=
  if read = [] then none else some x

/-- **A COUNT grand total rolls up**, row presence included, once the re-sum
carries the same no-rows guard as the direct aggregate. -/
theorem Rollup.grand_total_count (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) :
    total (stored g ks rows (countAgg v) q) (sqlSum (stored g ks rows (countAgg v) q)) =
      total (keep g q rows) (countAgg v (keep g q rows)) := by
  have hcov' : ∀ r ∈ keep g q rows, g r ∈ ks :=
    fun r hr => hcov r (List.mem_filter.mp hr).1
  by_cases hk : keep g q rows = []
  · have hs : stored g ks rows (countAgg v) q = [] := by
      rw [stored_keep]; exact (storedAll_nil_iff g ks _ hcov' _).mpr hk
    simp [total, hs, hk]
  · have hs : stored g ks rows (countAgg v) q ≠ [] := by
      rw [stored_keep]; exact fun e => hk ((storedAll_nil_iff g ks _ hcov' _).mp e)
    simp [total, hs, hk, Rollup.count_group g ks hnd rows hcov v q hk, countAgg]

theorem Rollup.grand_total_sum (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) :
    total (stored g ks rows (sumAgg v) q) (sqlSum (stored g ks rows (sumAgg v) q)) =
      total (keep g q rows) (sumAgg v (keep g q rows)) := by
  have hcov' : ∀ r ∈ keep g q rows, g r ∈ ks :=
    fun r hr => hcov r (List.mem_filter.mp hr).1
  have he : stored g ks rows (sumAgg v) q = [] ↔ keep g q rows = [] := by
    rw [stored_keep]; exact storedAll_nil_iff g ks _ hcov' _
  simp only [total, he, Rollup.sum_exact g ks hnd rows hcov v q]

/-- Without the guard, a grand total that matches no row differs: COUNT reads
no row, a bare `sum(n)` over the summary reads one NULL. -/
theorem Rollup.bare_sum_of_count_null :
    sqlSum (stored (fun r : Nat × Nat => r.1) [1] [(1, 7)] (countAgg (fun r => some r.2))
        (fun _ => false)) = none ∧
      total (keep (fun r : Nat × Nat => r.1) (fun _ => false) [(1, 7)]) (some 0) = none := by
  decide

/-- A filter on a column the summary's grain does not determine cannot be
read off the summary: no key predicate gives the count of `d = 1`. -/
theorem Rollup.finer_filter_unservable :
    ¬ ∃ q : Nat → Bool,
      coalesce0 (sqlSum (stored (fun r : Nat × Nat => r.1) [1] [(1, 1), (1, 2)]
          (countAgg (fun r => some r.2)) q)) =
        sqlCount (([(1, 1), (1, 2)].filter (fun r : Nat × Nat => r.2 == 1)).map
          (fun r => some r.2)) := by
  rintro ⟨q, h⟩
  cases hq : q 1 <;> simp_all [stored, groups, slice, countAgg, sqlSum, sqlCount,
    coalesce0]

/-- COUNT DISTINCT does not re-aggregate: one value in two groups. -/
theorem Rollup.count_distinct_not_additive :
    let rows : List (Nat × Nat) := [(1, 5), (2, 5)]
    let distinct (s : List (Nat × Nat)) : Nat := (s.map (·.2)).eraseDups.length
    ((([1, 2] : List Nat).map (fun k => distinct (slice (·.1) k rows))).sum) ≠
      distinct rows := by
  decide

end Rollup
end Trilogy
