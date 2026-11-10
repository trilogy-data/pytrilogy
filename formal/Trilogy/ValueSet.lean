import Trilogy.Rows
import Trilogy.Cover
import Trilogy.Cost

/-!
# Aggregates of a column stored at its own grain

A table that declares `grain (cls)` and binds `cls` as a complete column holds
every value of `cls` once, however `cls` is defined (derived from a row
property, or a property of a key the table does not store). Every row of a
complete column is some base value, and every base value is some row: the two
lists have the same members.

A **repeat-blind** aggregate (`min`, `max`, so `bool_and`/`bool_or`, and
`count_distinct`) is a function of its input's set of values
(`value_set_min_exact`, `value_set_max_exact`, `value_set_count_distinct_exact`),
and so is a scalar of the column alone (`SameValues.map`),
so any list with the base rows' members answers it. That is the read the
planner falls back to when no table binds the column's row identity: the
column's own values are its identity (`concept_graph._reads_value_set`). A
table at a grain that determines the column holds those members under any
predicate on that grain (`grain_column_values`), which generalizes
`Rollup.count_distinct_grain_column_exact` to every repeat-blind aggregate
(`grain_column_min_exact`).

A **repeat-sensitive** aggregate (`count`, `sum`) is not: one row per class
does not count the rows of each class (`value_set_misses_repeats`). An
aggregate stored at the table's grain needs no such reading. `sum(n)` over
`n <- count(id) by cls` reads `n` once per `cls` (`SelectGrain.ofValue`), which
is one table row each, so it needs only the grain key `cls` read from the same
table (the binding-only walk follows the graph's edges, so it reaches `cls`).

`count(cls)` counts the `id` rows with a non-null class. The table stores
`count(id) by cls`, and re-summing it over the non-null classes is exact
(`count_column_from_row_count`), but re-summing every class counts the NULL
class's rows too (`count_column_counts_null_group`). The planner does not
rewrite a `count` of a column into a re-sum of another column's count, so
`count(cls)` with the row table hidden is refused.

`none` is NULL.
-/

namespace Trilogy
namespace ValueSet

open Rollup

variable {α κ : Type} [DecidableEq κ]

/-- Two inputs holding the same values, each any number of times. -/
def SameValues (xs ys : List (Option Int)) : Prop := ∀ a, a ∈ xs ↔ a ∈ ys

theorem SameValues.symm {xs ys : List (Option Int)} (h : SameValues xs ys) :
    SameValues ys xs := fun a => (h a).symm

/-- A scalar of the column alone (`cls = 'x'`) maps equal sets to equal sets,
so `bool_or(cls = 'x')` reads `cls`'s values too. -/
theorem SameValues.map {xs ys : List (Option Int)} (h : SameValues xs ys)
    (f : Option Int → Option Int) : SameValues (xs.map f) (ys.map f) := by
  intro a
  constructor <;> intro ha <;> obtain ⟨b, hb, rfl⟩ := List.mem_map.mp ha
  · exact List.mem_map_of_mem ((h b).mp hb)
  · exact List.mem_map_of_mem ((h b).mpr hb)

/-- **MIN reads a set of values.** -/
theorem value_set_min_exact (xs ys : List (Option Int)) (h : SameValues xs ys) :
    sqlMin xs = sqlMin ys :=
  sqlMin_eq_of xs ys (fun a ha => ⟨a, (h _).mp ha, Int.le_refl a⟩)
    (fun b hb => ⟨b, (h _).mpr hb, Int.le_refl b⟩)

/-- **MAX reads a set of values**, as the negated `MIN`. -/
theorem value_set_max_exact (xs ys : List (Option Int)) (h : SameValues xs ys) :
    sqlMax xs = sqlMax ys := by
  unfold sqlMax
  rw [value_set_min_exact _ _ (h.map negOpt)]

theorem distinctCount_dedup (l : List Int) :
    Cover.distinctCount l = (dedup l).length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    have hm := mem_dedup x xs
    by_cases hx : x ∈ xs
    · simp [Cover.distinctCount, dedup, hx, hm.mpr hx, ih]
    · have hn : x ∉ dedup xs := fun h => hx (hm.mp h)
      simp [Cover.distinctCount, dedup, hx, hn, ih, Nat.add_comm]

theorem distinctCount_eq (l m : List Int) (h : ∀ a, a ∈ l ↔ a ∈ m) :
    Cover.distinctCount l = Cover.distinctCount m := by
  rw [distinctCount_dedup, distinctCount_dedup]
  apply Nat.le_antisymm
  · exact Cost.nodup_length_le _ _ (nodup_dedup l) fun a ha =>
      (mem_dedup a m).mpr ((h a).mp ((mem_dedup a l).mp ha))
  · exact Cost.nodup_length_le _ _ (nodup_dedup m) fun a ha =>
      (mem_dedup a l).mpr ((h a).mpr ((mem_dedup a m).mp ha))

/-- **COUNT DISTINCT reads a set of values.** -/
theorem value_set_count_distinct_exact (xs ys : List (Option Int))
    (h : SameValues xs ys) :
    Cover.sqlCountDistinct xs = Cover.sqlCountDistinct ys := by
  unfold Cover.sqlCountDistinct
  congr 1
  apply distinctCount_eq
  intro a
  simp only [List.mem_filterMap, id_eq, exists_eq_right]
  exact h (some a)

/-- A COUNT and a SUM read repeats: three rows over two classes count 3 and
sum to 4, one row per class counts 2 and sums to 3. -/
theorem value_set_misses_repeats :
    let rows : List (Option Int) := [some 1, some 2, some 2]
    let classes : List (Option Int) := [some 1, some 2]
    SameValues rows classes ∧ sqlCount rows ≠ sqlCount classes ∧
      sqlSum rows ≠ sqlSum classes := by
  refine ⟨fun a => ?_, by decide, by decide⟩
  simp only [List.mem_cons, List.not_mem_nil, or_false]
  constructor
  · rintro (h | h | h) <;> simp [h]
  · rintro (h | h) <;> simp [h]

/-- **A column its table's grain determines holds the base rows' values**
under any predicate on that grain: each grain value the table stores is a
group of base rows, and each base row lands in one. -/
theorem grain_column_values (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (f : κ → Option Int) (q : κ → Bool) :
    SameValues ((keep g q rows).map (fun r => f (g r)))
      (((groups g ks rows).filter q).map f) :=
  fun a => Rollup.count_distinct_grain_column_exact g ks rows hcov f q a

theorem grain_column_min_exact (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (f : κ → Option Int) (q : κ → Bool) :
    sqlMin ((keep g q rows).map (fun r => f (g r))) =
      sqlMin (((groups g ks rows).filter q).map f) :=
  value_set_min_exact _ _ (grain_column_values g ks rows hcov f q)

theorem grain_column_max_exact (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (f : κ → Option Int) (q : κ → Bool) :
    sqlMax ((keep g q rows).map (fun r => f (g r))) =
      sqlMax (((groups g ks rows).filter q).map f) :=
  value_set_max_exact _ _ (grain_column_values g ks rows hcov f q)

theorem grain_column_count_distinct_exact (g : α → κ) (ks : List κ)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (f : κ → Option Int)
    (q : κ → Bool) :
    Cover.sqlCountDistinct ((keep g q rows).map (fun r => f (g r))) =
      Cover.sqlCountDistinct (((groups g ks rows).filter q).map f) :=
  value_set_count_distinct_exact _ _ (grain_column_values g ks rows hcov f q)

/-- **`count(cls)` is the re-sum of `count(id) by cls` over the non-null
classes.** -/
theorem count_column_from_row_count (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (f : κ → Option Int)
    (q : κ → Bool) :
    sqlCount ((keep g q rows).map (fun r => f (g r))) =
      Rollup.coalesce0 (sqlSum (Rollup.stored g ks rows (countAgg (fun _ => some 1))
        (fun k => q k && (f k).isSome))) := by
  rw [Rollup.count_coalesce g ks hnd rows hcov]
  clear hcov
  simp only [sqlCount, keep]
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    cases hq : q (g r) <;> cases hf : f (g r) <;>
      simp_all

/-- Re-summing every class's row count counts the NULL class too: two rows,
one classed, count 1, and the classes' counts sum to 2. -/
theorem count_column_counts_null_group :
    let rows : List Nat := [0, 1]
    let f : Nat → Option Int := fun k => if k = 0 then some 7 else none
    sqlCount ((keep id (fun _ => true) rows).map (fun r => f r)) = 1 ∧
      Rollup.coalesce0 (sqlSum (Rollup.stored id [0, 1] rows (countAgg (fun _ => some 1))
        (fun _ => true))) = 2 := by
  decide

end ValueSet
end Trilogy
