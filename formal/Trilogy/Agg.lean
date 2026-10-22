import Trilogy.Logic

/-!
# Rows, filters and aggregates

A population is a list of rows; a WHERE keeps the rows whose condition holds,
and `x ? p` aggregates over the rows where `p` holds. Grouping by a key sums
each key's slice.
-/

namespace Trilogy

open B3

variable {α κ : Type}

def sumBy (v : α → Int) (rows : List α) : Int := (rows.map v).sum

def countWhere (p : α → B3) (rows : List α) : Nat :=
  rows.countP (fun r => decide (holds (p r)))

def slice [DecidableEq κ] (key : α → κ) (k : κ) (rows : List α) : List α :=
  rows.filter (fun r => decide (key r = k))

theorem sumBy_cons (v : α → Int) (r : α) (rs : List α) :
    sumBy v (r :: rs) = v r + sumBy v rs := by
  simp [sumBy]

theorem sum_map_add (ks : List κ) (f g : κ → Int) :
    (ks.map (fun k => f k + g k)).sum = (ks.map f).sum + (ks.map g).sum := by
  induction ks with
  | nil => simp
  | cons k ks ih => simp [ih]; omega

theorem sum_map_zero (ks : List κ) : (ks.map (fun _ => (0 : Int))).sum = 0 := by
  induction ks with
  | nil => rfl
  | cons k ks ih => simp [ih]

theorem sum_indicator [DecidableEq κ] (ks : List κ) (k : κ) (x : Int)
    (hnd : ks.Nodup) (hk : k ∈ ks) :
    (ks.map (fun k' => if k = k' then x else 0)).sum = x := by
  induction ks with
  | nil => cases hk
  | cons k' ks ih =>
    rw [List.nodup_cons] at hnd
    by_cases h : k = k'
    · subst h
      have : ∀ k'' ∈ ks, (if k = k'' then x else 0) = 0 := by
        intro k'' hm
        have : k ≠ k'' := fun e => hnd.1 (e ▸ hm)
        simp [this]
      simp [List.map_congr_left this, sum_map_zero]
    · have hm : k ∈ ks := by
        cases hk with
        | head => exact absurd rfl h
        | tail _ hm => exact hm
      simp [h, ih hnd.2 hm]

/-- Per-key sums over a covering key list add up to the grand sum: the
grand-total row, a `sum` over a per-group rowset, a count of counts. -/
theorem Agg.sum_partition [DecidableEq κ] (key : α → κ) (v : α → Int)
    (ks : List κ) (hnd : ks.Nodup) :
    ∀ rows : List α, (∀ r ∈ rows, key r ∈ ks) →
      (ks.map (fun k => sumBy v (slice key k rows))).sum = sumBy v rows := by
  intro rows
  induction rows with
  | nil => intro _; simp [slice, sumBy, sum_map_zero]
  | cons r rs ih =>
    intro hcov
    have step : ∀ k, sumBy v (slice key k (r :: rs)) =
        (if key r = k then v r else 0) + sumBy v (slice key k rs) := by
      intro k
      by_cases h : key r = k <;> simp [slice, sumBy, h]
    simp only [step, sum_map_add]
    rw [sum_indicator ks (key r) (v r) hnd (hcov r (List.mem_cons_self ..)),
      ih (fun r' hm => hcov r' (List.mem_cons_of_mem _ hm)), sumBy_cons]

theorem Agg.count_eq_sum_group_counts [DecidableEq κ] (key : α → κ)
    (ks : List κ) (hnd : ks.Nodup) (rows : List α)
    (hcov : ∀ r ∈ rows, key r ∈ ks) :
    (ks.map (fun k => sumBy (fun _ => 1) (slice key k rows))).sum =
      sumBy (fun _ => 1) rows :=
  Agg.sum_partition key _ ks hnd rows hcov

theorem Agg.rollup_total_eq_global [DecidableEq κ] (key : α → κ) (v : α → Int)
    (ks : List κ) (hnd : ks.Nodup) (rows : List α)
    (hcov : ∀ r ∈ rows, key r ∈ ks) :
    (ks.map (fun k => sumBy v (slice key k rows))).sum = sumBy v rows :=
  Agg.sum_partition key v ks hnd rows hcov

/-- ROLLUP's leaf rows: each group's slice, unchanged by the subtotal pass. -/
theorem Agg.rollup_leaves_eq_group_by [DecidableEq κ] (key : α → κ)
    (v : α → Int) (k : κ) (rows : List α) :
    sumBy v (slice key k rows) = sumBy v (rows.filter (fun r => decide (key r = k))) :=
  rfl

/-- Two-valued `not` sends every row to exactly one side. -/
theorem Agg.count_split_not (p : α → B3) (rows : List α) :
    countWhere p rows + countWhere (fun r => not2 (p r)) rows = rows.length := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    simp only [countWhere, List.countP_cons, List.length_cons] at *
    have e : ∀ b : B3, (if decide (holds b) = true then 1 else 0) +
        (if decide (holds (not2 b)) = true then 1 else 0) = 1 := by
      intro b; cases b <;> rfl
    have := e (p r)
    omega

theorem Agg.count_or_inclusion_exclusion (p q : α → B3) (rows : List α) :
    countWhere (fun r => or3 (p r) (q r)) rows + countWhere (fun r => and3 (p r) (q r)) rows
      = countWhere p rows + countWhere q rows := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    simp only [countWhere, List.countP_cons] at *
    have e : ∀ a b : B3, (if decide (holds (or3 a b)) = true then 1 else 0) +
        (if decide (holds (and3 a b)) = true then 1 else 0) =
        (if decide (holds a) = true then 1 else 0) +
        (if decide (holds b) = true then 1 else 0) := by
      intro a b; cases a <;> cases b <;> rfl
    have := e (p r) (q r)
    omega

/-- `sum(x ? p)` is the WHERE-`p` sum; HAVING `is not null` only drops the
groups the WHERE never produces. -/
theorem Agg.filtered_agg_eq_where (p : α → B3) (v : α → Int) (rows : List α) :
    sumBy v (rows.filter (fun r => decide (holds (p r)))) =
      sumBy (fun r => if holds (p r) then v r else 0) rows := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : holds (p r) <;> simp_all [sumBy]

theorem max_neg_min (xs : List Int) (a : Int) :
    xs.foldr max a = - (xs.map (fun x => -x)).foldr min (-a) := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

/-- `max(x)` is `-min(-x)` (over a non-empty group, seeded by its first row). -/
theorem Agg.max_eq_neg_min_neg (x : Int) (xs : List Int) :
    xs.foldr max x = - (xs.map (fun y => -y)).foldr min (-x) :=
  max_neg_min xs x

end Trilogy
