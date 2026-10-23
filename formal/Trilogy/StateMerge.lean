import Trilogy.Logic

/-!
# State merge: folding partition deltas into a snapshot

A partitioned fan-out has N workers each rebuild the slices they own and
publish a delta that speaks only for those slices. A coordinator folds the
deltas into a base (`merge_snapshots`). It must not matter which order the
files are listed in, and replaying a delta must change nothing, because there
is no lock: the merge is the only coordination.

A slice set is a partial map from slice id to its claim. A scoped delta
overlays the base. Disjoint overlays commute and an overlay is idempotent
(`disjoint_overlays_commute`, `overlay_idem`), but two claims on one slice,
such as a retried worker or a whole-table probe beside a scoped one, do not
commute (`overlapping_order_matters`). The fold therefore keeps the newest
claim, which is a semilattice join (`newest_comm`, `newest_assoc`,
`newest_idem`), so any listing order gives one answer.

The base also carries a count of stale slices over the whole table. A scoped
delta moves it slice by slice (`count_adjusts`), which is exact only when the
base's list speaks for every slice it counts. Over a list that does not, a
slice the count already holds reads as new (`unlisted_slice_counts_twice`).
Two scoped records have that problem, so their merge drops the count. A delta
trimmed to a payload cap loses a rebuild the merge never hears of
(`trimmed_delta_loses_a_rebuild`), so a scoped list is never trimmed. A
trimmed whole-table probe overlays rather than replaces, which keeps base
slices its own count disagrees with; `count_adjusts` then needs the count
recounted from the list, or a slice is subtracted twice.

A claim is `true` for a stale slice.
-/

namespace Trilogy

abbrev Slices := Nat → Option Bool

/-- A scoped delta speaks for its slices and leaves the rest to the base. -/
def overlay (d b : Slices) : Slices := fun i => (d i).or (b i)

theorem StateMerge.overlay_idem (d b : Slices) :
    overlay d (overlay d b) = overlay d b := by
  funext i; simp only [overlay]; cases d i <;> rfl

theorem StateMerge.disjoint_overlays_commute (d₁ d₂ b : Slices)
    (h : ∀ i, d₁ i = none ∨ d₂ i = none) :
    overlay d₁ (overlay d₂ b) = overlay d₂ (overlay d₁ b) := by
  funext i; simp only [overlay]
  rcases h i with h | h <;> simp [h]

/-- A slice claimed stale by one delta and fresh by another. -/
theorem StateMerge.overlapping_order_matters :
    ∃ d₁ d₂ b : Slices, overlay d₁ (overlay d₂ b) ≠ overlay d₂ (overlay d₁ b) :=
  ⟨fun _ => some true, fun _ => some false, fun _ => none, fun h => by
    simpa [overlay] using congrFun h 0⟩

/-- The later of two claims, each ranked by `(time, content)`. -/
def newest : Option Nat → Option Nat → Option Nat
  | some a, some b => some (max a b)
  | a, none => a
  | none, b => b

theorem StateMerge.newest_comm (a b : Option Nat) : newest a b = newest b a := by
  cases a <;> cases b <;> simp [newest, Nat.max_comm]

theorem StateMerge.newest_assoc (a b c : Option Nat) :
    newest (newest a b) c = newest a (newest b c) := by
  cases a <;> cases b <;> cases c <;> simp [newest, Nat.max_assoc]

theorem StateMerge.newest_idem (a : Option Nat) : newest a a = a := by
  cases a <;> simp [newest]

def isStale (s : Slices) (i : Nat) : Int := if s i = some true then 1 else 0

/-- The stale count over a table's slices `ids`. -/
def staleCount (s : Slices) (ids : List Nat) : Int := (ids.map (isStale s)).sum

/-- What a delta's slice moves the count by. -/
def adjustment (d b : Slices) (i : Nat) : Int :=
  if (d i).isSome then isStale d i - isStale b i else 0

private theorem adjust_val (x y : Option Bool) :
    (if x.or y = some true then (1 : Int) else 0) =
      (if y = some true then 1 else 0) +
        if x.isSome then (if x = some true then 1 else 0) -
          (if y = some true then 1 else 0) else 0 := by
  rcases x with _ | ⟨_ | _⟩ <;> rcases y with _ | ⟨_ | _⟩ <;> decide

private theorem adjust_one (d b : Slices) (i : Nat) :
    isStale (overlay d b) i = isStale b i + adjustment d b i :=
  adjust_val (d i) (b i)

/-- Adjusting the base's count by each delta slice gives the merged count. -/
theorem StateMerge.count_adjusts (d b : Slices) (ids : List Nat) :
    staleCount (overlay d b) ids =
      staleCount b ids + (ids.map (adjustment d b)).sum := by
  induction ids with
  | nil => rfl
  | cons i is ih =>
    simp only [staleCount, List.map_cons, List.sum_cons] at ih ⊢
    rw [adjust_one, ih]
    omega

/-- The base counts slice 0 but lists only slice 1, as a scoped record or a
trimmed one does. A delta for slice 0 then reads as new, and the total of a
one-slice change grows from 2 to 3. -/
theorem StateMerge.unlisted_slice_counts_twice :
    let listed : List Nat := [1]
    let counted : Nat := 2
    let delta : List Nat := [0]
    counted + (delta.filter (· ∉ listed)).length ≠ ([0, 1] : List Nat).length := by
  decide

/-- Trimming a delta that rebuilt slices 0 and 1 to one slice leaves slice 1
stale in the merge. -/
theorem StateMerge.trimmed_delta_loses_a_rebuild :
    let base : Slices := fun _ => some true
    let rebuilt : Slices := fun i => if i ≤ 1 then some false else none
    let trimmed : Slices := fun i => if i = 0 then some false else none
    overlay trimmed base 1 ≠ overlay rebuilt base 1 := by
  decide

end Trilogy
