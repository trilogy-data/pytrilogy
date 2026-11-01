import Trilogy.Binding

/-!
# Filtering rows by the elements of an unnested list

`unnest(xs)` repeats its parent row once per element. A parent row whose list
is empty or NULL reads its element as NULL (the padding the outer row stream
gives it), so a WHERE on the element keeps a parent row iff some element of
its list, or that NULL, passes.

- `kept_iff`: the reference. A parent row is kept iff a padded element passes.
- `or_keeps_disjunct`: `where e > k or city = 'B'` keeps every row
  `where city = 'B'` does, element or not. Reading the rows through the bare
  expansion drops a row whose list is empty (`bare_expansion_drops_padded`).
- `agg_reads_each_row_once`: an aggregate over the kept rows reads each once.
  Summing the joined expansion instead counts a row once per passing element
  (`expansion_sum_overcounts`), including a list that repeats one value: an
  element pinned by the WHERE is no row identity (`pinned_element_repeats`).
  A filter keeps a sub-multiset of the expansion (`filter_never_adds_rows`);
  joined to a copy of itself on the element, each repeat meets each
  (`self_join_squares`).
- `coarse_pair_overcounts`, `keyless_pair_keeps_every_row`: the elements pair
  with the rows they expand on the rows' identity. Paired on the aggregate's
  grouping key, every row of a group meets every passing element; paired on
  nothing, every row meets every element.
- `key_table_pads_a_row`: with the element merged into a key, joining the key's
  own table beside the expansion preserving both sides adds a row for a member
  no list names, whose outputs are all NULL.
-/

namespace Trilogy
namespace Unnest

structure Parent where
  key : Nat
  grp : Nat
  val : Int
  elems : List Int
  deriving DecidableEq, Repr

/-- The elements a parent row reads, NULL for an empty list. -/
def Parent.padded (p : Parent) : List (Option Int) :=
  if p.elems = [] then [none] else p.elems.map some

abbrev Pred := Option Int → Parent → Bool

/-- The reference: a parent row is kept iff a padded element passes. -/
def kept (rows : List Parent) (P : Pred) : List Parent :=
  rows.filter fun p => p.padded.any (P · p)

/-- The padded expansion: one row per (parent, element). -/
def expand (rows : List Parent) : List (Parent × Option Int) :=
  rows.flatMap fun p => p.padded.map (p, ·)

/-- The bare expansion: an empty list yields no row. -/
def bare (rows : List Parent) : List (Parent × Option Int) :=
  rows.flatMap fun p => p.elems.map fun e => (p, some e)

theorem kept_iff (rows : List Parent) (P : Pred) (p : Parent) :
    p ∈ kept rows P ↔ p ∈ rows ∧ ∃ e ∈ p.padded, P e p = true := by
  simp [kept, List.any_eq_true]

theorem mem_expand (rows : List Parent) (p : Parent) (e : Option Int) :
    (p, e) ∈ expand rows ↔ p ∈ rows ∧ e ∈ p.padded := by
  simp [expand]

/-- Filtering the padded expansion and keeping its parents is the reference. -/
theorem filter_exists (rows : List Parent) (P : Pred) (p : Parent) :
    p ∈ kept rows P ↔ ∃ e, (p, e) ∈ (expand rows).filter fun x => P x.2 x.1 := by
  simp only [kept_iff, List.mem_filter, mem_expand]
  constructor
  · rintro ⟨hp, e, he, hP⟩
    exact ⟨e, ⟨hp, he⟩, hP⟩
  · rintro ⟨e, ⟨hp, he⟩, hP⟩
    exact ⟨hp, e, he, hP⟩

theorem padded_ne_nil (p : Parent) : p.padded ≠ [] := by
  unfold Parent.padded
  split
  · simp
  · simpa using ‹¬p.elems = []›

/-- A disjunct over the parent alone keeps the row whatever its list holds. -/
theorem or_keeps_disjunct (rows : List Parent) (A : Pred) (B : Parent → Bool)
    (p : Parent) (hp : p ∈ rows) (hB : B p = true) :
    p ∈ kept rows (fun e q => A e q || B q) := by
  rw [kept_iff]
  obtain ⟨e, he⟩ := List.exists_mem_of_ne_nil _ (padded_ne_nil p)
  exact ⟨hp, e, he, by simp [hB]⟩

def bareKept (rows : List Parent) (P : Pred) : List Parent :=
  rows.filter fun p => (p.elems.map some).any (P · p)

def tree0 : Parent := ⟨0, 0, 1, []⟩
def tree1 : Parent := ⟨1, 0, 2, [3]⟩
def tree2 : Parent := ⟨2, 0, 4, [3, 3]⟩
def tree3 : Parent := ⟨3, 0, 8, [1, 2]⟩

/-- `where e > 1 or grp = 0` over a tree with no list: kept, but the bare
expansion never sees it. -/
theorem bare_expansion_drops_padded :
    let P : Pred := fun e q => (match e with | some v => decide (v > 1) | none => false)
      || decide (q.grp = 0)
    tree0 ∈ kept [tree0] P ∧ tree0 ∉ bareKept [tree0] P := by
  decide

/-! ## Aggregating the kept rows -/

def sumVal (rows : List Parent) : Int := (rows.map (·.val)).sum

/-- An aggregate over the kept rows reads each row once: the reference sum is
the sum over the distinct parents of the filtered expansion. -/
theorem agg_reads_each_row_once (rows : List Parent) (P : Pred) :
    sumVal (kept rows P) = sumVal (rows.filter fun p => p.padded.any (P · p)) :=
  rfl

/-- The sum over the joined expansion counts a row once per passing element. -/
def expansionSum (rows : List Parent) (P : Pred) : Int :=
  (((expand rows).filter fun x => P x.2 x.1).map (·.1.val)).sum

theorem expansion_sum_overcounts :
    let P : Pred := fun e _ => e = some 3
    sumVal (kept [tree1, tree2] P) = 6 ∧ expansionSum [tree1, tree2] P = 10 := by
  decide

/-- A list repeating the pinned value yields two rows equal on (row, element):
the pair is no row identity, so a pin cannot stand in for the ordinal. -/
theorem pinned_element_repeats :
    ((expand [tree2]).filter fun x => x.2 = some 3) = [(tree2, some 3), (tree2, some 3)] := by
  decide

/-- A filter keeps a sub-multiset of the expansion: a repeated element stays
as often as the list repeats it, never more. -/
theorem filter_never_adds_rows (rows : List Parent) (P : Pred) :
    ((expand rows).filter fun x => P x.2 x.1).Sublist (expand rows) :=
  List.filter_sublist

/-- The expansion joined to a copy of itself filtered on the element pairs
each repeat with each: two `(tree2, 3)` rows come back as four. Distinct on
its rows, the copy keeps two. -/
theorem self_join_squares :
    let hits := (expand [tree2]).filter fun x => x.2 = some 3
    ((expand [tree2]).flatMap fun x => (hits.filter fun y => y = x).map fun _ => x).length = 4 ∧
      ((expand [tree2]).flatMap fun x => (hits.eraseDups.filter fun y => y = x).map fun _ => x).length = 2 := by
  decide

/-! ## Pairing the elements with the rows -/

/-- Rows paired with the passing elements on `on`, as a join does. -/
def pairedSum (rows : List Parent) (P : Pred) (on : Parent → Parent → Bool) : Int :=
  let hits := (expand rows).filter fun x => P x.2 x.1
  ((rows.flatMap fun r => (hits.filter fun x => on r x.1).map fun _ => r).map (·.val)).sum

theorem identity_pair_counts_each_match :
    let P : Pred := fun e _ => e = some 3
    pairedSum [tree1, tree3] P (fun r s => r.key = s.key) = sumVal (kept [tree1, tree3] P) := by
  decide

/-- Paired on the grouping key, a row with no passing element meets its
group's passing ones. -/
theorem coarse_pair_overcounts :
    let P : Pred := fun e _ => e = some 3
    sumVal (kept [tree1, tree3] P) = 2 ∧
      pairedSum [tree1, tree3] P (fun r s => r.grp = s.grp) = 10 := by
  decide

theorem keyless_pair_keeps_every_row :
    let P : Pred := fun e _ => e = some 1
    sumVal (kept [tree1, tree3] P) = 8 ∧
      pairedSum [tree1, tree3] P (fun _ _ => true) = 10 := by
  decide

/-! ## The key an element is merged into -/

/-- The expansion joined to the key's table preserving both sides: a member no
list names comes back with no row (`none`). -/
def fullWithKeys (rows : List Parent) (keys : List Int) : List (Option Parent × Int) :=
  ((expand rows).filterMap fun x => x.2.map fun e => (some x.1, e)) ++
    (keys.filter fun k => !(expand rows).any fun x => x.2 = some k).map (none, ·)

theorem key_table_pads_a_row :
    (fullWithKeys [tree1] [2, 3]).filter (fun x => x.2 > 1) = [(some tree1, 3), (none, 2)] := by
  decide

end Unnest
end Trilogy
