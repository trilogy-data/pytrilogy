import Trilogy.Rows
import Trilogy.Rollup
import Trilogy.Sourcing

/-!
# Which source is cheapest

A datasource is a set of base rows an arm `c` keeps (`complete where c`, or
`true` for a complete table) stored at a grain `g` (the row key for a table,
the group key for a summary). It holds one row per distinct grain value of
the rows it keeps. It ANSWERS a query when the query's WHERE implies its arm
(`Sourcing.partial_is_full_under_implication`) and every filter and target
column is a function of its grain (`Rollup.sum_exact`); every answering source
gives the same rows (`Cost.partial_summary_sum_exact`).

So the choice among answering sources is a cost decision, and one order is
data-independent: a source whose arm implies another's and whose grain is
coarser holds no more rows on ANY data (`Cost.dominated_le`). The planner must
never read a source an answering source in scope dominates. Two answering
sources neither dominates (a filtered row table beside a complete summary) can
be cheaper either way (`Cost.partial_and_summary_incomparable`), so between
them any choice is sound.

A source that does NOT answer must not be read alone: a partial summary
under a WHERE that does not imply its arm loses the rows outside it
(`Cost.unimplied_partial_summary_loses_rows`). A `partial datasource` with no
`complete where` has an arm no WHERE implies. A `~` key is padding, not an
arm: the source claims the keys it lacks have no rows.

`none` is NULL.
-/

namespace Trilogy
namespace Cost

variable {α κ τ : Type}

/-- The rows a source stores: one per distinct grain value its arm keeps. -/
def stored [DecidableEq κ] (c : α → Bool) (g : α → κ) (rows : List α) : List κ :=
  dedup ((rows.filter c).map g)

theorem nodup_length_le [DecidableEq τ] (l₁ l₂ : List τ) (h₁ : l₁.Nodup)
    (hs : ∀ x ∈ l₁, x ∈ l₂) : l₁.length ≤ l₂.length := by
  induction l₁ generalizing l₂ with
  | nil => simp
  | cons a t ih =>
    rw [List.nodup_cons] at h₁
    have ha : a ∈ l₂ := hs a (List.mem_cons_self ..)
    have ht : ∀ x ∈ t, x ∈ l₂.erase a := by
      intro x hx
      have hne : x ≠ a := fun e => h₁.1 (e ▸ hx)
      exact (List.mem_erase_of_ne hne).mpr (hs x (List.mem_cons_of_mem a hx))
    have := ih (l₂.erase a) h₁.2 ht
    rw [List.length_erase_of_mem ha] at this
    have hpos : 0 < l₂.length := List.length_pos_of_mem ha
    simp only [List.length_cons]
    omega

/-- **Dominance.** A source whose arm implies `c` and whose grain is a
function `h` of `g` stores no more rows than `(c, g)`, on any data. -/
theorem Cost.dominated_le [DecidableEq κ] [DecidableEq τ] (c c' : α → Bool)
    (g : α → κ) (h : κ → τ) (rows : List α) (himp : ∀ r, c' r = true → c r = true) :
    (stored c' (h ∘ g) rows).length ≤ (stored c g rows).length := by
  unfold stored
  calc (dedup ((rows.filter c').map (h ∘ g))).length
      ≤ ((dedup ((rows.filter c).map g)).map h).length := by
        apply nodup_length_le _ _ (nodup_dedup _)
        intro x hx
        obtain ⟨r, hr, rfl⟩ := List.mem_map.mp ((mem_dedup _ _).mp hx)
        have hr' := List.mem_filter.mp hr
        refine List.mem_map.mpr ⟨g r, (mem_dedup _ _).mpr ?_, rfl⟩
        exact List.mem_map.mpr
          ⟨r, List.mem_filter.mpr ⟨hr'.1, himp r hr'.2⟩, rfl⟩
    _ = (dedup ((rows.filter c).map g)).length := List.length_map ..

/-- A narrower arm at the same grain stores no more rows. -/
theorem Cost.narrower_arm_le [DecidableEq κ] (c c' : α → Bool) (g : α → κ)
    (rows : List α) (himp : ∀ r, c' r = true → c r = true) :
    (stored c' g rows).length ≤ (stored c g rows).length :=
  Cost.dominated_le c c' g id rows himp

/-- A coarser summary of the same rows stores no more rows. -/
theorem Cost.coarser_grain_le [DecidableEq κ] [DecidableEq τ] (c : α → Bool)
    (g : α → κ) (h : κ → τ) (rows : List α) :
    (stored c (h ∘ g) rows).length ≤ (stored c g rows).length :=
  Cost.dominated_le c c g h rows (fun _ hr => hr)

/-- A summary of a filtered table is no larger than the table itself, which
is stored at its row key. -/
theorem Cost.summary_le_rows [DecidableEq α] [DecidableEq κ] (c : α → Bool)
    (g : α → κ) (rows : List α) :
    (stored c g rows).length ≤ (stored c id rows).length :=
  Cost.coarser_grain_le c id g rows

/-- Each dominance step can be strict: a narrower arm, a coarser grain. -/
theorem Cost.narrower_arm_strict :
    (stored (fun (r : Nat × Nat) => r.1 == 0) id [(0, 1), (1, 1)]).length <
      (stored (fun _ => true) id [(0, 1), (1, 1)]).length := by decide

theorem Cost.coarser_grain_strict :
    (stored (fun _ => true) Prod.fst [((0 : Nat), (1 : Nat)), (0, 2)]).length <
      (stored (fun _ => true) id [((0 : Nat), (1 : Nat)), (0, 2)]).length := by decide

/-- **No order between a filtered table and a complete summary.** Rows are
`(city, species)`; the partial table keeps city 0 at the row grain, the
summary keeps every city at the species grain. Either can be smaller. -/
theorem Cost.partial_and_summary_incomparable :
    let arm := fun (r : Nat × Nat) => r.1 == 0
    let few := [((0 : Nat), (0 : Nat)), (1, 1), (2, 2)]
    let many := [((0 : Nat), (0 : Nat)), (0, 0), (0, 0), (1, 0)]
    (stored arm id few).length < (stored (fun _ => true) Prod.snd few).length ∧
      (stored (fun _ => true) Prod.snd many).length <
        (stored arm id (many.zipIdx.map (fun (r, i) => (r.1, r.2 + 10 * i)))).length := by
  decide

open Rollup in
/-- **A partial summary answers exactly under an implying WHERE.** A summary
built over the rows `c` keeps, re-summed under a key predicate `q` whose rows
all satisfy `c`, is the filtered sum over every base row. -/
theorem Cost.partial_summary_sum_exact [DecidableEq κ] (c : α → Bool) (g : α → κ)
    (ks : List κ) (hnd : ks.Nodup) (rows : List α) (hcov : ∀ r ∈ rows, g r ∈ ks)
    (v : α → Option Int) (q : κ → Bool) (himp : ∀ r, q (g r) = true → c r = true) :
    sqlSum (Rollup.stored g ks (rows.filter c) (sumAgg v) q) =
      sumAgg v (keep g q rows) := by
  rw [Rollup.sum_exact g ks hnd _ (fun r hr => hcov r (List.mem_filter.mp hr).1)]
  congr 1
  simp only [keep, List.filter_filter]
  congr 1
  funext r
  by_cases hq : q (g r) = true
  · simp [hq, himp r hq]
  · simp [hq]

open Rollup in
/-- The same for `count`. -/
theorem Cost.partial_summary_count_exact [DecidableEq κ] (c : α → Bool)
    (g : α → κ) (ks : List κ) (hnd : ks.Nodup) (rows : List α)
    (hcov : ∀ r ∈ rows, g r ∈ ks) (v : α → Option Int) (q : κ → Bool)
    (himp : ∀ r, q (g r) = true → c r = true) :
    sqlSum (Rollup.stored g ks (rows.filter c) (countAgg v) q) =
      (if (keep g q rows).isEmpty then none else countAgg v (keep g q rows)) := by
  have hkeep : keep g q (rows.filter c) = keep g q rows := by
    simp only [keep, List.filter_filter]
    congr 1
    funext r
    by_cases hq : q (g r) = true
    · simp [hq, himp r hq]
    · simp [hq]
  by_cases he : (keep g q rows).isEmpty = true
  · have hnil : keep g q rows = [] := List.isEmpty_iff.mp he
    rw [Rollup.stored_keep, hkeep, hnil]
    simp [Rollup.storedAll, Rollup.groups, sqlSum, slice]
  · have hne : keep g q (rows.filter c) ≠ [] := by
      rw [hkeep]; exact fun e => he (List.isEmpty_iff.mpr e)
    rw [Rollup.count_group g ks hnd _ (fun r hr => hcov r (List.mem_filter.mp hr).1)
      v q hne, hkeep]
    simp [he, countAgg]

/-- **The bug.** A summary of city 0's rows, read as complete for an
unfiltered count, drops the other city: 2 trees, not 3. -/
theorem Cost.unimplied_partial_summary_loses_rows :
    let rows := [((0 : Nat), (5 : Nat)), (0, 6), (1, 7)]
    ((rows.filter (fun r => r.1 == 0)).length : Int) ≠ rows.length := by decide

/-! ### Implication by value sets

A `complete where x in S` arm is implied by a WHERE that pins `x` to values
in `S`: `x = a` with `a ∈ S`, `x in Q` with `Q ⊆ S`, or a disjunction of such.
The planner's check compared atoms by syntax, so `x = 'A'` never read an
`x in ('A', 'B')` source. Membership is two-valued over SQL comparisons: a
NULL `x` is in no literal set, even one that lists NULL. -/

/-- SQL `x in S` over non-NULL literals, and the `x = a` it generalizes. -/
def inSet (S : List Int) : Val → Bool
  | none => false
  | some a => S.contains a

/-- SQL `x in (..., NULL)`: the NULL member never matches. -/
def inList (S : List Val) : Val → Bool
  | none => false
  | some a => S.contains (some a)

theorem Cost.value_set_implies (Q S : List Int) (hsub : ∀ a ∈ Q, a ∈ S) (v : Val)
    (hq : inSet Q v = true) : inSet S v = true := by
  cases v with
  | none => simp [inSet] at hq
  | some a =>
    simp only [inSet, List.contains_iff_mem] at *
    exact hsub a hq

/-- A disjunction of pins is the pin to the union of their values. -/
theorem Cost.disjunction_is_union (Q R : List Int) (v : Val) :
    (inSet Q v || inSet R v) = inSet (Q ++ R) v := by
  cases v <;> simp [inSet]

/-- A NULL in the arm's list does not make `x is null` imply it. -/
theorem Cost.null_member_matches_nothing :
    isNull none = true ∧ inList [none, some 1] none = false := by decide

end Cost
end Trilogy
