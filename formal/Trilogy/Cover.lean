import Trilogy.Rollup

/-!
# When a count summary covers a query

`Rollup.lean` proves that re-aggregating a summary under a key predicate is
exact. This module names what the planner must recognize as a key predicate
or a target of the key before it reads the summary, one rule per shape of
query. A summary stores `count(x)` per group of its grain `g`; everything
below is a function of the summary key `k`, so each reduces to
`Rollup.count_coalesce` or `Rollup.reaggregate_partial_count`.

1. **Value, not spelling** (`target_tuple_count`, `duplicate_target_slice`).
   A target group is any function of the key: a rename (`species as s`), a
   scalar of a stored column (`plant_year::int`, `plant_date.year` against a
   stored `year(plant_date)`), a tuple, a column listed twice.
2. **A key predicate inside the aggregate** (`filtered_count_exact`,
   `case_count_exact`). `count(x ? c)` with `c` a function of the key is the
   re-sum of the groups `c` keeps, written `sum(case when c then n end)` when
   it shares a scan with other aggregates. `count(case when c then x end)` is
   the same aggregate (`filtered` is that CASE row by row). A column of `c`
   joined onto the summary through a functional lookup is a function of the
   key too (`joined_case_exact`).
3. **A distinct count of the counted key** (`count_distinct_of_key_exact`).
   On rows where the counted key is unique, `count_distinct(x)` is `count(x)`.
4. **Siblings at other grains** (`ratio_parts_exact`). A total `count(x) by *`
   beside a derived dimension is two independent re-sums of one summary.
5. **Existence** (`exists_by_value`). With no aggregate at all, the values of
   a key function over the kept rows are its values over the summary's
   groups, since GROUP BY emits only groups that hold a row.
6. **A relation keyed on the grain** (`keyed_relation_count`). Counting by a
   value `t` the key relates to (an ecoregion of a species' range, a biome of
   it) is the re-sum of the groups related to `t`: the relation is a key
   predicate. Joining the summary to relation PAIRS and re-summing counts a
   group once per pair, so the joined pairs must be distinct per `(k, t)`
   (`collapsed_fanout_double_counts`, `distinct_pairs_resum_exact`,
   `repeated_element_counts_once`).
7. **A scalar over an aggregate's own grain** (`case_on_aggregate_rows`).
   `sum(case when bucket = 'Native' then count(x) by bucket end)` reads
   `bucket` on the aggregate's rows; reading it from the finer table it is a
   property of fans each group out (`finer_grain_read_fans_out`). This one is
   the tree plan's rule, summary or not.

What each rule needs of the summary is the same: the counted key is the
summary's own aggregate argument (`Rollup.stored_column_answers_its_own_argument`),
every row lands in one group (`Rollup.fanout_partial_double_counts`), and a
condition or target finer than the key is refused
(`Rollup.finer_filter_unservable`).
-/

namespace Trilogy
namespace Cover

open Rollup

variable {α κ τ : Type} [DecidableEq κ] [DecidableEq τ]

/-! ## 1. Value, not spelling -/

/-- **A tuple of key functions is a target.** Any list of columns that are
functions of the summary key, renamed, computed or repeated, groups the
summary as it groups the rows. -/
theorem target_tuple_count (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (h₁ h₂ : κ → τ) (t : τ × τ)
    (hne : slice (fun r => (h₁ (g r), h₂ (g r))) t rows ≠ []) :
    sqlSum (stored g ks rows (countAgg v) (fun k => decide ((h₁ k, h₂ k) = t))) =
      countAgg v (slice (fun r => (h₁ (g r), h₂ (g r))) t rows) :=
  Rollup.reaggregate_partial_count g ks hnd rows hcov v (fun k => (h₁ k, h₂ k)) t hne

omit [DecidableEq κ] in
/-- A column listed twice adds no group: the pair `(h, h)` keeps a row at
`(a, b)` only when `a = b`, and then keeps the rows `h` keeps at `a`. -/
theorem duplicate_target_slice (g : α → κ) (h : κ → τ) (a b : τ) (rows : List α) :
    slice (fun r => (h (g r), h (g r))) (a, b) rows =
      if a = b then slice (fun r => h (g r)) a rows else [] := by
  by_cases hab : a = b
  · subst hab
    simp [slice]
  · simp only [hab, ite_false, slice]
    apply List.filter_eq_nil_iff.mpr
    intro r _
    simp only [Prod.mk.injEq, decide_eq_true_eq, not_and]
    intro ha hb
    exact hab (ha ▸ hb)

omit [DecidableEq κ] in
/-- Two spellings of one value answer alike: grouping depends on the target
only as a function of the key. -/
theorem target_by_value (g : α → κ) (h₁ h₂ : κ → τ) (heq : ∀ k, h₁ k = h₂ k)
    (t : τ) (rows : List α) :
    slice (fun r => h₁ (g r)) t rows = slice (fun r => h₂ (g r)) t rows := by
  simp [slice, heq]

/-! ## 2. A key predicate inside the aggregate -/

/-- `x ? c`: the value where `c` holds, NULL elsewhere. -/
def filtered (v : α → Option Int) (c : α → Bool) : α → Option Int :=
  fun r => if c r then v r else none

theorem count_filtered (v : α → Option Int) (c : α → Bool) (rows : List α) :
    sqlCount (rows.map (filtered v c)) = sqlCount ((rows.filter c).map v) := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    by_cases hc : c r <;>
      simp_all [sqlCount, filtered, List.countP_cons]

omit [DecidableEq κ] in
theorem keep_filter (g : α → κ) (q c : κ → Bool) (rows : List α) :
    (keep g q rows).filter (fun r => c (g r)) = keep g (fun k => q k && c k) rows := by
  simp [keep, List.filter_filter, Bool.and_comm]

/-- **A filtered count rolls up.** `count(x ? c)` with `c` a function of the
summary key is the re-sum of the groups `q` and `c` both keep. -/
theorem filtered_count_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q c : κ → Bool) :
    coalesce0 (sqlSum (stored g ks rows (countAgg v) (fun k => q k && c k))) =
      sqlCount ((keep g q rows).map (filtered v (fun r => c (g r)))) := by
  rw [count_filtered, keep_filter]
  exact Rollup.count_coalesce g ks hnd rows hcov v _

/-- The summary read under `q`, each group's count kept where `c` holds:
`sum(case when c then n end)`, the form a filtered count takes when it
shares a scan with other aggregates. -/
def storedCase (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) (q c : κ → Bool) : List (Option Int) :=
  ((groups g ks rows).filter q).map (fun k => if c k then agg (slice g k rows) else none)

omit [DecidableEq κ] in
theorem sum_getD_case (xs : List κ) (f : κ → Option Int) (c : κ → Bool) :
    ((xs.map (fun k => if c k then f k else none)).map (·.getD 0)).sum =
      (((xs.filter c).map f).map (·.getD 0)).sum := by
  induction xs with
  | nil => rfl
  | cons k ks ih => by_cases hc : c k <;> simp_all

theorem case_count_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q c : κ → Bool) :
    coalesce0 (sqlSum (storedCase g ks rows (countAgg v) q c)) =
      sqlCount ((keep g q rows).map (filtered v (fun r => c (g r)))) := by
  rw [← filtered_count_exact g ks hnd rows hcov v q c, coalesce0_sqlSum,
    coalesce0_sqlSum, storedCase, sum_getD_case, stored, List.filter_filter]
  simp only [Bool.and_comm]

/-! ### Any re-aggregable function, at the partial grain `G ∪ cols(c)`

Rule 2 holds for `sum`, `min` and `max` too: a summary storing `agg(x)` at a
grain `g` that determines the condition's columns answers `agg(x ? c)` as
`reagg(case when c then agg(x) end)` over its rows. The planner matches the
filtered aggregate to the stored one (`BuildConcept.filtered_rollup`) and
renders that CASE where it re-aggregates the stored column. `c` is a function
of the summary key, so each lemma below is the rule over the summary's
groups. -/

/-- `case when c then f end`, the shape both sides of the rule share: a
per-group value, or a per-row one under `filtered`. -/
def kept (c : κ → Bool) (f : κ → Option Int) : κ → Option Int :=
  fun k => if c k then f k else none

omit [DecidableEq κ] in
theorem all_none_kept (xs : List κ) (f : κ → Option Int) (c : κ → Bool) :
    (xs.map (kept c f)).all (·.isNone) = ((xs.filter c).map f).all (·.isNone) := by
  induction xs with
  | nil => rfl
  | cons k ks ih => by_cases hc : c k <;> simp_all [kept]

omit [DecidableEq κ] in
/-- SUM skips the NULLs a CASE leaves. -/
theorem sqlSum_kept (xs : List κ) (f : κ → Option Int) (c : κ → Bool) :
    sqlSum (xs.map (kept c f)) = sqlSum ((xs.filter c).map f) := by
  have hs := sum_getD_case xs f c
  unfold sqlSum
  rw [all_none_kept]
  unfold kept
  rw [hs]

omit [DecidableEq κ] in
/-- So does MIN. -/
theorem sqlMin_kept (xs : List κ) (f : κ → Option Int) (c : κ → Bool) :
    sqlMin (xs.map (kept c f)) = sqlMin ((xs.filter c).map f) := by
  induction xs with
  | nil => rfl
  | cons k ks ih =>
    by_cases hc : c k
    · simp only [List.map_cons, List.filter_cons, hc, ↓reduceIte, sqlMin,
        List.foldr_cons, kept] at *
      rw [ih]
    · simp only [List.map_cons, List.filter_cons, hc, sqlMin, List.foldr_cons,
        kept, Bool.false_eq_true, ↓reduceIte] at *
      rw [← ih]; rfl

omit [DecidableEq κ] in
theorem sqlMax_kept (xs : List κ) (f : κ → Option Int) (c : κ → Bool) :
    sqlMax (xs.map (kept c f)) = sqlMax ((xs.filter c).map f) := by
  have h : (xs.map (kept c f)).map negOpt = xs.map (kept c (fun k => negOpt (f k))) := by
    simp only [List.map_map]
    congr 1; funext k; by_cases hc : c k <;> simp [kept, hc, negOpt]
  simp only [sqlMax, h, sqlMin_kept, List.map_map, Function.comp_def]

theorem storedCase_kept (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) (q c : κ → Bool) :
    storedCase g ks rows agg q c =
      ((groups g ks rows).filter q).map (kept c (fun k => agg (slice g k rows))) := rfl

theorem stored_and (g : α → κ) (ks : List κ) (rows : List α)
    (agg : List α → Option Int) (q c : κ → Bool) :
    (((groups g ks rows).filter q).filter c).map (fun k => agg (slice g k rows)) =
      stored g ks rows agg (fun k => q k && c k) := by
  simp [stored, List.filter_filter, Bool.and_comm]

/-- **A filtered SUM rolls up**: `sum(case when c then sum(x) by G, C end)`. -/
theorem filtered_sum_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q c : κ → Bool) :
    sqlSum (storedCase g ks rows (sumAgg v) q c) =
      sumAgg (filtered v (fun r => c (g r))) (keep g q rows) := by
  have hrow : (keep g q rows).map (filtered v (fun r => c (g r))) =
      (keep g q rows).map (kept (fun r => c (g r)) v) := rfl
  rw [storedCase_kept, sqlSum_kept, stored_and, Rollup.sum_exact g ks hnd rows hcov]
  unfold sumAgg
  rw [hrow, sqlSum_kept, keep_filter]

/-- **A filtered MIN rolls up**: `min(case when c then min(x) by G, C end)`. -/
theorem filtered_min_exact (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int) (q c : κ → Bool) :
    sqlMin (storedCase g ks rows (minAgg v) q c) =
      minAgg (filtered v (fun r => c (g r))) (keep g q rows) := by
  have hrow : (keep g q rows).map (filtered v (fun r => c (g r))) =
      (keep g q rows).map (kept (fun r => c (g r)) v) := rfl
  rw [storedCase_kept, sqlMin_kept, stored_and, Rollup.min_exact g ks rows hcov]
  unfold minAgg
  rw [hrow, sqlMin_kept, keep_filter]

/-- **A filtered MAX rolls up**: `max(case when c then max(x) by G, C end)`. -/
theorem filtered_max_exact (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int) (q c : κ → Bool) :
    sqlMax (storedCase g ks rows (maxAgg v) q c) =
      maxAgg (filtered v (fun r => c (g r))) (keep g q rows) := by
  have hrow : (keep g q rows).map (filtered v (fun r => c (g r))) =
      (keep g q rows).map (kept (fun r => c (g r)) v) := rfl
  rw [storedCase_kept, sqlMax_kept, stored_and, Rollup.max_exact g ks rows hcov]
  unfold maxAgg
  rw [hrow, sqlMax_kept, keep_filter]

/-- The re-sum of a filtered count needs its `coalesce`: a group whose rows
all fail `c` counts 0, and `sum` over only NULL cases is NULL. -/
theorem uncoalesced_case_count_null :
    sqlSum (storedCase (fun r : Nat × Nat => r.1) [1] [(1, 7)]
        (countAgg (fun r => some r.2)) (fun _ => true) (fun _ => false)) = none ∧
      sqlCount ([(1, 7)].map (filtered (fun r : Nat × Nat => some r.2)
        (fun _ => false))) = 0 := by
  decide

/-- The summary's grain must determine the condition's columns. Keyed on
`r.1` alone, a group holding rows on both sides of `c` (`r.2 = 1`) has no
single value of `c`: reading `c` off any one of its rows keeps both rows or
neither, never the one row `count(x ? c)` counts. This is why the matcher
requires the summary to bind every column `c` reads. -/
theorem partial_off_condition_misreads :
    let rows : List (Nat × Nat) := [(1, 1), (1, 2)]
    sqlCount (rows.map (filtered (fun r => some r.2) (fun r => r.2 == 1))) = 1 ∧
      ∀ r ∈ rows, coalesce0 (sqlSum (storedCase (fun r : Nat × Nat => r.1) [1] rows
        (countAgg (fun r => some r.2)) (fun _ => true) (fun _ => r.2 == 1))) ≠ 1 := by
  decide

/-! ### A condition joined onto the summary's rows

`c` may read a column the summary does not bind: `bucket` of `species`, from
a dimension `dim : μ → δ` keyed on a part `m : κ → μ` of the summary key.
Joined on that part, each summary row meets one dimension row, so the CASE
reads `c' (dim (m k))`, a function of the key, and rule 2 holds as it is
(`joined_case_exact`, `joined_case_count_exact`). Two plans break it: a
lookup that is not functional fans each summary row out
(`joined_fanout_double_counts`), and a re-sum below the CASE at a grouping
that does not determine `c` mixes rows on both sides of it
(`case_above_coarser_grouping_misreads`). A re-sum at a grouping that does
determine it is exact (`resum_then_case_exact`). The planner states the
lookup as an obligation of the summary it reads (`ObligationKind.JOINED`): an
in-cover functional chain off the summary's rows to a full binder of each
column of `c`, and the re-aggregation after it. -/

/-- The CASE reading a joined column: on summary row `k`, `c' (dim (m k))`. -/
def joinedCond {μ δ : Type} (m : κ → μ) (dim : μ → δ) (c' : δ → Bool) : κ → Bool :=
  fun k => c' (dim (m k))

/-- **A joined condition rolls up**: `sum(case when c' then sum(x) end)` over
the summary joined to its dimension. -/
theorem joined_case_exact {μ δ : Type} (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q : κ → Bool) (m : κ → μ) (dim : μ → δ) (c' : δ → Bool) :
    sqlSum (storedCase g ks rows (sumAgg v) q (joinedCond m dim c')) =
      sumAgg (filtered v (fun r => c' (dim (m (g r))))) (keep g q rows) :=
  filtered_sum_exact g ks hnd rows hcov v q _

theorem joined_case_count_exact {μ δ : Type} (g : α → κ) (ks : List κ)
    (hnd : ks.Nodup) (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) (m : κ → μ) (dim : μ → δ) (c' : δ → Bool) :
    coalesce0 (sqlSum (storedCase g ks rows (countAgg v) q (joinedCond m dim c'))) =
      sqlCount ((keep g q rows).map (filtered v (fun r => c' (dim (m (g r)))))) :=
  case_count_exact g ks hnd rows hcov v q _

omit [DecidableEq κ] in
/-- **A re-sum below the CASE is exact at a grouping that determines `c`.**
Re-summing the summary's partials at `h ∘ g` (`species` kept, `plant_year`
summed away) yields the groups of `h ∘ g`, so a CASE over a condition of
`h` is rule 2 at that grain. -/
theorem resum_then_case_exact {ν : Type} [DecidableEq ν] (g : α → κ) (h : κ → ν)
    (ns : List ν) (hnd : ns.Nodup) (rows : List α) (hcov : ∀ r ∈ rows, h (g r) ∈ ns)
    (v : α → Option Int) (q c : ν → Bool) :
    sqlSum (storedCase (fun r => h (g r)) ns rows (sumAgg v) q c) =
      sumAgg (filtered v (fun r => c (h (g r)))) (keep (fun r => h (g r)) q rows) :=
  filtered_sum_exact (fun r => h (g r)) ns hnd rows hcov v q c

/-- A non-functional lookup fans a summary row out. One tree, key 1, kept by
`c`; the dimension holds two rows for its key (one per region), so the joined
summary row is counted twice. -/
theorem joined_fanout_double_counts :
    let rows : List (Nat × Nat) := [(1, 7)]
    let joined : List (Nat × Bool) := [(1, true), (1, true)]
    sqlCount (rows.map (filtered (fun r => some r.2) (fun _ => true))) = 1 ∧
      coalesce0 (sqlSum (joined.map (fun p => if p.2 then some 1 else none))) = 2 := by
  decide

/-- A re-sum below the CASE at a grouping that does not determine `c` loses
it. Summary partials at `(city, species)` are `(1, 1) ↦ 1` and `(1, 2) ↦ 1`,
`c` holds for species 1 only, so the answer is 1. Re-summed at `city` first,
the one group holds 2 and a CASE over any one value of `c` keeps 2 or 0. -/
theorem case_above_coarser_grouping_misreads :
    let partials : List ((Nat × Nat) × Int) := [((1, 1), 1), ((1, 2), 1)]
    let c : Nat × Nat → Bool := fun k => k.2 == 1
    (partials.filter (fun p => c p.1)).map (·.2) = [1] ∧
      ∀ p ∈ partials, (if c p.1 then (partials.map (·.2)).sum else 0) ≠ 1 := by
  decide

/-! ## 3. A distinct count of the counted key -/

/-- The number of distinct values. -/
def distinctCount : List Int → Nat
  | [] => 0
  | x :: xs => (if x ∈ xs then 0 else 1) + distinctCount xs

/-- SQL `COUNT(DISTINCT x)`: the distinct non-NULL inputs. -/
def sqlCountDistinct (xs : List (Option Int)) : Int := distinctCount (xs.filterMap id)

theorem distinctCount_nodup (l : List Int) (h : l.Nodup) : distinctCount l = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.nodup_cons] at h
    simp [distinctCount, h.1, ih h.2, Nat.add_comm]

theorem filterMap_length (xs : List (Option Int)) :
    ((xs.filterMap id).length : Int) = sqlCount xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih => cases x <;> simp_all [sqlCount] <;> omega

/-- **A distinct count of the counted key rolls up.** Where every kept row
carries its own value of the counted key (`tree_id` on rows at its grain),
`count_distinct(x)` is the summary's re-summed `count(x)`. -/
theorem count_distinct_of_key_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q : κ → Bool) (hkey : ((rows.map v).filterMap id).Nodup) :
    coalesce0 (sqlSum (stored g ks rows (countAgg v) q)) =
      sqlCountDistinct ((keep g q rows).map v) := by
  have hsub : (((keep g q rows).map v).filterMap id).Sublist ((rows.map v).filterMap id) :=
    ((List.filter_sublist).map v).filterMap id
  rw [Rollup.count_coalesce g ks hnd rows hcov v q, sqlCountDistinct,
    distinctCount_nodup _ (hkey.sublist hsub), filterMap_length]

/-- A key read twice, as a join that fans a row out leaves it, is no key:
two copies of one tree count once distinct and twice summed. -/
theorem fanned_key_counts_twice :
    let rows : List (Nat × Option Int) := [(1, some 7), (1, some 7)]
    sqlCountDistinct (rows.map (·.2)) = 1 ∧ sqlCount (rows.map (·.2)) = 2 := by
  decide

/-! ## 4. Siblings at other grains -/

/-- **A ratio over the total rolls up in both parts.** The per-group count at
any target of the key and the grand total beside it are each a re-sum of the
same summary, so neither rules out the other. -/
theorem ratio_parts_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q : κ → Bool) (h : κ → τ) (t : τ)
    (hne : keep g (fun k => q k && decide (h k = t)) rows ≠ []) :
    sqlSum (stored g ks rows (countAgg v) (fun k => q k && decide (h k = t))) =
        some (sqlCount ((keep g (fun k => q k && decide (h k = t)) rows).map v)) ∧
      total (stored g ks rows (countAgg v) q) (sqlSum (stored g ks rows (countAgg v) q)) =
        total (keep g q rows) (countAgg v (keep g q rows)) :=
  ⟨Rollup.count_group g ks hnd rows hcov v _ hne,
    Rollup.grand_total_count g ks hnd rows hcov v q⟩

/-! ## 5. Existence -/

omit [DecidableEq τ] in
/-- **A complete summary proves which values exist.** The values of a key
function over the rows `q` keeps are its values over the summary's groups
`q` keeps: a selection of keys and their properties, with no aggregate, reads
the summary. -/
theorem exists_by_value (g : α → κ) (ks : List κ) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (q : κ → Bool) (h : κ → τ) (t : τ) :
    t ∈ (keep g q rows).map (fun r => h (g r)) ↔
      t ∈ ((groups g ks rows).filter q).map h := by
  constructor
  · intro ht
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp ht
    have hk := List.mem_filter.mp hr
    exact List.mem_map.mpr ⟨g r,
      List.mem_filter.mpr ⟨mem_groups g ks rows hcov r hk.1, hk.2⟩, rfl⟩
  · intro ht
    obtain ⟨k, hk, rfl⟩ := List.mem_map.mp ht
    have ⟨hg, hq⟩ := List.mem_filter.mp hk
    obtain ⟨r, hr⟩ := List.exists_mem_of_ne_nil _
      (slice_ne_nil_of_mem_groups g ks rows k hg)
    have ⟨hrows, hgr⟩ := List.mem_filter.mp hr
    have hgr : g r = k := of_decide_eq_true hgr
    exact List.mem_map.mpr ⟨r, List.mem_filter.mpr ⟨hrows, hgr ▸ hq⟩, by rw [hgr]⟩

/-! ## 6. A relation keyed on the grain -/

omit [DecidableEq τ] in
/-- **A count by a related value rolls up.** Counting the rows whose key
relates to `t` (`e ∈ native_ecoregions k`, `∃ e ∈ native_ecoregions k,
biome e = t`) is the re-sum of the groups related to `t`. -/
theorem keyed_relation_count (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (q : κ → Bool) (R : κ → τ → Bool) (t : τ) :
    coalesce0 (sqlSum (stored g ks rows (countAgg v) (fun k => q k && R k t))) =
      sqlCount (((keep g q rows).filter (fun r => R (g r) t)).map v) := by
  rw [keep_filter g q (fun k => R k t)]
  exact Rollup.count_coalesce g ks hnd rows hcov v _

/-- Re-summing a summary joined to relation pairs counts a group once per
pair. A species of two trees ranging over two forest ecoregions has 2 trees in
the forest biome; summed over its (species, ecoregion) pairs it has 4. -/
theorem collapsed_fanout_double_counts :
    let pairs : List (Nat × Nat) := [(1, 1), (1, 2)]
    let biome : Nat → Nat := fun _ => 0
    let n : Nat → Int := fun _ => 2
    ((pairs.filter (fun p => biome p.2 = 0)).map (fun p => n p.1)).sum = 4 ∧
      ((([1] : List Nat).filter (fun k => pairs.any (fun p => p.1 = k && biome p.2 = 0))).map
        n).sum = 2 := by
  decide

/-! ### The pairs a relation is joined on

A count by a related value is read from the summary by joining it to the
relation's `(key, value)` pairs and re-summing. Over the DISTINCT pairs that
is the re-sum `keyed_relation_count` proves exact
(`distinct_pairs_resum_exact`, `relation_resum_count_exact`); over the pairs an
unnest lists, a list repeating a value counts its key once per repeat
(`repeated_element_counts_once`). The tree rows are held to the same rule: a
row lands in a group once, however often its list names it. -/

/-- A list with each value once. -/
def dedup : List τ → List τ
  | [] => []
  | x :: xs => if x ∈ xs then dedup xs else x :: dedup xs

omit [DecidableEq κ] in
theorem count_dedup (l : List τ) (t : τ) :
    (dedup l).count t = if t ∈ l then 1 else 0 := by
  induction l with
  | nil => simp [dedup]
  | cons x xs ih =>
    by_cases hx : x ∈ xs
    · simp only [dedup, hx, ↓reduceIte, ih, List.mem_cons]
      by_cases ht : t = x
      · subst ht; simp [hx]
      · simp [ht]
    · simp only [dedup, hx, ↓reduceIte, List.count_cons, ih, List.mem_cons]
      by_cases ht : t = x
      · subst ht; simp [hx]
      · have : (x == t) = false := by simp [Ne.symm ht]
        simp [ht, this]

/-- The DISTINCT `(key, value)` pairs of a relation listed per key. -/
def distinctPairs (ks : List κ) (rel : κ → List τ) : List (κ × τ) :=
  ks.flatMap (fun k => (dedup (rel k)).map (fun t => (k, t)))

/-- The summary's per-group values re-summed over the pairs naming `t`. -/
def pairResum (f : κ → Int) (ps : List (κ × τ)) (t : τ) : Int :=
  ((ps.filter (fun p => decide (p.2 = t))).map (fun p => f p.1)).sum

omit [DecidableEq κ] in
theorem pairResum_one (f : κ → Int) (k : κ) (l : List τ) (t : τ) :
    pairResum f (l.map (fun u => (k, u))) t = (l.count t : Int) * f k := by
  induction l with
  | nil => simp [pairResum]
  | cons u us ih =>
    unfold pairResum at *
    by_cases hu : u = t
    · subst hu
      simp [ih]
      rw [Int.add_mul, Int.one_mul, Int.add_comm]
    · simp only [List.map_cons, List.filter_cons]
      simp [hu, ih]

omit [DecidableEq κ] in
theorem pairResum_append (f : κ → Int) (a b : List (κ × τ)) (t : τ) :
    pairResum f (a ++ b) t = pairResum f a t + pairResum f b t := by
  simp [pairResum, List.filter_append]

omit [DecidableEq κ] in
/-- **Distinct pairs re-sum exactly.** Summing a per-key value over the
DISTINCT pairs naming `t` is summing it over the keys whose list names `t`,
each once. -/
theorem distinct_pairs_resum_exact (f : κ → Int) (ks : List κ) (rel : κ → List τ)
    (t : τ) :
    pairResum f (distinctPairs ks rel) t =
      ((ks.filter (fun k => decide (t ∈ rel k))).map f).sum := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
    simp only [distinctPairs, List.flatMap_cons] at *
    rw [pairResum_append, ih, pairResum_one, count_dedup]
    by_cases ht : t ∈ rel k <;> simp [ht]

/-- **A count by a related value is the summary over distinct pairs.** The
summary's per-group counts, joined to the DISTINCT `(key, value)` pairs and
re-summed for `t`, count the rows whose key's list names `t`, each once. -/
theorem relation_resum_count_exact (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (rel : κ → List τ) (t : τ) :
    pairResum (fun k => sqlCount ((slice g k rows).map v))
        (distinctPairs (groups g ks rows) rel) t =
      sqlCount ((rows.filter (fun r => decide (t ∈ rel (g r)))).map v) := by
  have h := keyed_relation_count g ks hnd rows hcov v (fun _ => true)
    (fun k t => decide (t ∈ rel k)) t
  have hall : rows.filter (fun _ => true) = rows := List.filter_eq_self.mpr (by simp)
  simp only [Bool.true_and, keep, hall] at h
  rw [distinct_pairs_resum_exact, ← h, coalesce0_sqlSum, stored]
  simp [List.map_map, Function.comp_def, countAgg]

/-- Re-summing over the pairs the expansion lists, not the distinct ones,
counts a key once per repeat: a species of one tree listing ecoregion 1 twice
is one tree in ecoregion 1, and two over its expanded pairs. -/
theorem repeated_element_counts_once :
    let rel : Nat → List Nat := fun _ => [1, 1]
    let expanded := ([7] : List Nat).flatMap (fun k => (rel k).map (fun t => (k, t)))
    pairResum (fun _ => 1) expanded 1 = 2 ∧
      pairResum (fun _ => 1) (distinctPairs [7] rel) 1 = 1 := by
  decide

/-! ## 7. A scalar over an aggregate's own grain -/

/-- **A scalar of the grain is read on the aggregate's rows.** An aggregate
`count(x) by k` is a summary at `k`, so `sum(case when c(k) then count(x) by k
end)` is `case_count_exact` over its own groups, whatever table `k` is a
property of. -/
theorem case_on_aggregate_rows (g : α → κ) (ks : List κ) (hnd : ks.Nodup)
    (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int)
    (c : κ → Bool) :
    coalesce0 (sqlSum (storedCase g ks rows (countAgg v) (fun _ => true) c)) =
      sqlCount ((keep g (fun _ => true) rows).map (filtered v (fun r => c (g r)))) :=
  case_count_exact g ks hnd rows hcov v _ c

/-- Reading the grain column from a finer table joined back on it counts each
group once per finer row. `bucket` is a property of `species`: the per-bucket
count `[(N, 4)]` joined to `[(s1, N), (s2, N)]` sums to 8; on its own rows,
to 4. -/
theorem finer_grain_read_fans_out :
    let agg : List (Nat × Int) := [(0, 4)]
    let finer : List (Nat × Nat) := [(1, 0), (2, 0)]
    let joined := agg.flatMap (fun a => (finer.filter (·.2 = a.1)).map (fun _ => a))
    ((joined.filter (·.1 = 0)).map (·.2)).sum = 8 ∧
      ((agg.filter (·.1 = 0)).map (·.2)).sum = 4 := by
  decide

end Cover
end Trilogy
