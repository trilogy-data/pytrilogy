import Trilogy.Agg

/-!
# No intrinsic rows; grain

A select's rows are the DISTINCT tuples of its requested concepts. A rowset
that carries a row key keeps every source row; one that drops it collapses
duplicates (`docs/modeling_concepts.md`; memory: a row is the tuple).
-/

namespace Trilogy

variable {α β κ π : Type}

def dedup [DecidableEq α] : List α → List α
  | [] => []
  | a :: l => if a ∈ dedup l then dedup l else a :: dedup l

theorem mem_dedup [DecidableEq α] (a : α) (l : List α) : a ∈ dedup l ↔ a ∈ l := by
  induction l with
  | nil => simp [dedup]
  | cons b l ih =>
    by_cases h : b ∈ dedup l
    · simp only [dedup, h, ite_true, ih, List.mem_cons]
      constructor
      · exact Or.inr
      · rintro (rfl | hm)
        · exact ih.mp h
        · exact hm
    · simp [dedup, h, ih]

theorem dedup_of_nodup [DecidableEq α] (l : List α) (h : l.Nodup) : dedup l = l := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    rw [List.nodup_cons] at h
    have : a ∉ dedup l := fun hm => h.1 ((mem_dedup a l).mp hm)
    simp only [dedup, ih h.2, h.1, ↓reduceIte]

theorem nodup_dedup [DecidableEq α] (l : List α) : (dedup l).Nodup := by
  induction l with
  | nil => exact List.nodup_nil
  | cons a l ih =>
    by_cases h : a ∈ dedup l
    · simp [dedup, h, ih]
    · simp only [dedup, h, ite_false]; exact List.nodup_cons.mpr ⟨h, ih⟩

/-- A rowset whose projection is injective on its rows (it carries the row
key) keeps every row, so aggregating through it is aggregating directly. -/
theorem Rows.keyed_projection_preserves_multiplicity [DecidableEq β]
    (f : α → β) (rows : List α) (h : (rows.map f).Nodup) :
    dedup (rows.map f) = rows.map f :=
  dedup_of_nodup _ h

/-- Without the key, equal tuples collapse: the sum through the rowset is a
sum of distinct values. -/
theorem Rows.unkeyed_projection_dedups :
    ((dedup [((1 : Nat), (5 : Int)), (1, 5)]).map Prod.snd).sum ≠
      ([((1 : Nat), (5 : Int)), (1, 5)].map Prod.snd).sum := by decide

theorem Rows.filter_commutes_with_keyed_projection (f : α → β) (p : β → Bool)
    (rows : List α) : (rows.map f).filter p = (rows.filter (p ∘ f)).map f :=
  List.filter_map ..

/-- `distinct(x)` is sugar for `group(x) by x`: both are the distinct values. -/
def distinctOf [DecidableEq β] (x : α → β) (rows : List α) := dedup (rows.map x)
def groupBySelf [DecidableEq β] (x : α → β) (rows : List α) := dedup (rows.map x)

theorem Rows.distinct_eq_group [DecidableEq β] (x : α → β) (rows : List α) :
    distinctOf x rows = groupBySelf x rows := rfl

/-- `count(key)` counts distinct values; staging the keys through a rowset
first does not change it. -/
theorem Rows.count_key_eq_count_distinct [DecidableEq κ] (key : α → κ)
    (rows : List α) :
    (dedup (dedup (rows.map key))).length = (dedup (rows.map key)).length := by
  rw [dedup_of_nodup _ (nodup_dedup _)]

/-- Grouping by a key plus a property OF that key selects the same slice:
an FD property neither splits a group nor changes its aggregates. -/
theorem Grain.fd_extension_preserves_aggregates [DecidableEq κ] [DecidableEq π]
    (key : α → κ) (prop : κ → π) (v : α → Int) (k : κ) (rows : List α) :
    sumBy v (slice (fun r => (key r, prop (key r))) (k, prop k) rows) =
      sumBy v (slice key k rows) := by
  unfold slice
  congr 2
  funext r
  by_cases h : key r = k <;> simp [h]

/-- A partitioned window total is the aggregate `by` the partition, read
back on each row. -/
theorem Grain.window_partition_eq_by [DecidableEq κ] (key : α → κ) (v : α → Int)
    (rows : List α) (r : α) :
    sumBy v (rows.filter (fun r' => decide (key r' = key r))) =
      sumBy v (slice key (key r) rows) := rfl

/-- Each fact aggregates over its own slice, so a sibling aggregate over
another fact cannot change it ... -/
theorem Grain.sibling_aggregate_independent [DecidableEq κ] (keyS : α → κ)
    (keyR : β → κ) (vS : α → Int) (vR : β → Int) (sales : List α)
    (returns : List β) (k : κ) :
    (sumBy vS (slice keyS k sales), sumBy vR (slice keyR k returns)).1 =
      sumBy vS (slice keyS k sales) := rfl

/-- ... whereas joining the facts on the key before aggregating (the chasm
trap) multiplies one sum by the other side's fanout. -/
theorem Grain.chasm_join_fans_out :
    let sales : List (Nat × Int) := [(1, 10)]
    let returns : List (Nat × Int) := [(1, 1), (1, 2)]
    ((sales.flatMap (fun s => (returns.filter (·.1 == s.1)).map (fun _ => s.2))).sum)
      ≠ (sales.map Prod.snd).sum := by decide

end Trilogy
