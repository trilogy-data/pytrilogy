/-!
# Filters: a `?` as a select target

`x ? c` is a value: `x` on the rows `c` holds on, NULL on the others
(`filt`). Written inside an aggregate (`count(id ? c)`) only its non-NULL
rows are read; written as a select target (`select city, id ? c as f`) the
value itself is shown, and three questions the aggregate never asks come up.

- **What determines it.** When `c` and `x` are functions of a key, so is
  `x ? c` (`filtered_value_on_its_keys`): `species ? tree_form = 'broadleaf'`
  is one value per species. The oracle family writes only such conditions.
- **What its rejected rows are.** One NULL: grouped with the select's other
  keys, the rows `c` rejects in a group are a single row of it
  (`rejected_rows_share_one_null`), however many keys `x` held there. A
  filtered key is therefore no identity of the rows it was read off, and the
  column it was read off is no key of the groups a window numbers
  (`source_column_splits_null_group`).
- **What a select of filtered values alone returns.** The rows the condition
  keeps (`alone_keeps_passing_rows`): a NULL there is a row nothing would
  keep, whatever the condition reads. A membership rendered as a CASE showed
  it (`case_alone_shows_rejected`).

A value the condition reads is built before the filtered column:

- a set or an aggregate in the condition is no finer than the column it
  gates (`value_is_no_finer_than_its_reader`): widening the rows it is read
  off by any column, the gated one included, changes nothing it holds;
- a total gated by a total is a scalar of the one row both sit on
  (`total_gated_by_total_is_scalar`), with nothing to collapse;
- a partial grouped by a reader of its own roll-up cannot feed that roll-up
  (`partial_grouped_by_its_reader`): the three have no build order, though
  the value would be exact (`min_of_group_mins`);
- a WHERE applied beneath an aggregate that only a set's condition reads has
  filtered the set's rows, not the statement's
  (`gate_on_a_set_filters_no_row`).

A filter's rows pair with the select's other streams on its grain, so they
must be one per grain value there: a column it carries below its grain fans
the pairing out (`carried_column_fans_out`), and a collapse past a key the
pairing reads crosses every value of it (`collapse_drops_paired_key`).
-/

namespace Trilogy.Filters

/-- `x ? c` on one row. -/
def filt {ρ α : Type} (c : ρ → Bool) (x : ρ → α) (r : ρ) : Option α :=
  if c r then some (x r) else none

/-! ## What determines a filtered value -/

/-- A condition and a value that are functions of a key make `x ? c` one:
two rows of one species agree on `species ? tree_form = 'broadleaf'`. -/
theorem filtered_value_on_its_keys {ρ κ α : Type} (key : ρ → κ)
    (c' : κ → Bool) (x' : κ → α) (r s : ρ) (h : key r = key s) :
    filt (c' ∘ key) (x' ∘ key) r = filt (c' ∘ key) (x' ∘ key) s := by
  simp [filt, h]

/-- Without that, one key shows two values: `species ? tag = 'x'` is Acer on
one Acer tree and NULL on another (`filtered_value_off_its_keys_splits`), and
what a select of it beside `species` shows is undecided. -/
theorem filtered_value_off_its_keys_splits :
    let trees : List (Nat × Bool) := [(0, true), (0, false)]
    (trees.map (filt (·.2) (·.1))).eraseDups.length = 2 := by decide

/-! ## The rows a filter rejects -/

/-- `select city, tree_id ? height > 2 as f`: city A holds four trees, two
of them rejected. The select's rows are the distinct `(city, f)`: three for
A, the two rejected trees one NULL between them. Read as rows unique on
`tree_id`, because `f` is `tree_id` where it is anything, they came back
four (`rejected_rows_share_one_null`). -/
theorem rejected_rows_share_one_null :
    let trees : List (Nat × Nat × Bool) :=
      [(0, 10, true), (0, 11, false), (0, 12, true), (0, 13, false)]
    let shown := trees.map fun r => (r.1, filt (·.2.2) (·.2.1) r)
    shown.length = 4 ∧ shown.eraseDups.length = 3 := by decide

/-- `select tag, height ? c as f, row_number() over (order by tag, f)`: the
window numbers the groups `(tag, f)`. Grouped by the column `f` was read off
too, the rejected rows stay apart by height and the NULL group is numbered
once per height (`source_column_splits_null_group`). -/
theorem source_column_splits_null_group :
    let trees : List (Nat × Nat × Bool) := [(0, 7, false), (0, 9, false), (0, 3, true)]
    let groups := (trees.map fun r => (r.1, filt (·.2.2) (·.2.1) r)).eraseDups
    let finer := (trees.map fun r => (r.1, filt (·.2.2) (·.2.1) r, r.2.1)).eraseDups
    groups.length = 2 ∧ finer.length = 3 := by decide

/-! ## Filtered values alone -/

/-- The non-NULL part of a filtered column is the value over the rows the
condition keeps: a select of filtered values alone is a WHERE. -/
theorem alone_keeps_passing_rows {ρ α : Type} (c : ρ → Bool) (x : ρ → α)
    (rows : List ρ) :
    (rows.map (filt c x)).filterMap id = (rows.filter c).map x := by
  induction rows with
  | nil => rfl
  | cons r rest ih =>
    by_cases h : c r <;> simp [filt, h] <;> simpa [filt] using ih

/-- `select city ? not (city in (city ? city = 'B')) as f`: the condition
keeps city A. Rendered as a CASE over every city, because a membership was
never moved into the WHERE, the select showed B's row as a NULL
(`case_alone_shows_rejected`). -/
theorem case_alone_shows_rejected :
    let cities : List Nat := [0, 1]
    let set := cities.filter (· == 1)
    let c : Nat → Bool := fun k => !set.contains k
    (cities.filter c).map id = [0] ∧ cities.map (filt c id) = [some 0, none] := by
  decide

/-! ## A value the condition reads -/

/-- A set read off rows widened by any column holds what it holds off the
rows: `select tree_id, city ? city in (city ? height > 2) as f` builds the
set in a select whose grain names `f`, and `f` is no key of it. Planned at
that grain the set asked for `f`, which asks for the set. -/
theorem value_is_no_finer_than_its_reader {ρ κ β : Type} (c : ρ → Bool)
    (key : ρ → κ) (g : ρ → β) (rows : List ρ) :
    (((rows.map fun r => (r, g r)).filter fun p => c p.1).map fun p => key p.1) =
      (rows.filter c).map key := by
  induction rows with
  | nil => rfl
  | cons r rest ih =>
    by_cases h : c r <;> simp [h] <;> simpa using ih

/-- `select tag, (count(tree_id) by *) ? min(height) by * <= 5 as f`: both
sides of the `?` sit on the one row of the total, so the value is a scalar
of that row. Collapsed as a value some row of a group passes, it was
rendered as a `max` over an aggregate, which no engine accepts. -/
theorem total_gated_by_total_is_scalar :
    let heights : List Nat := [3, 5, 7, 2]
    let total := heights.length
    let gate := heights.foldl min 100 ≤ 5
    let scalar : Option Nat := if gate then some total else none
    scalar = some 4 ∧ (heights.map fun _ => scalar).eraseDups = [scalar] := by
  decide

/-- A minimum is the minimum of its groups' minimums, whatever groups them. -/
theorem min_of_group_mins :
    let rows : List (Nat × Nat) := [(0, 3), (0, 7), (1, 5), (1, 2)]
    let groupMin (k : Nat) := ((rows.filter (·.1 == k)).map (·.2)).foldl min 100
    min (groupMin 0) (groupMin 1) = (rows.map (·.2)).foldl min 100 := by decide

/-- Whether a build order exists: some node has no pending input, repeatedly. -/
def buildable (edges : List (Nat × Nat)) : List Nat → Nat → Bool
  | [], _ => true
  | _, 0 => false
  | nodes, fuel + 1 =>
    match nodes.find? fun n => !(edges.any fun e => e.2 == n && nodes.contains e.1) with
    | none => false
    | some n => buildable edges (nodes.filter (· != n)) fuel

/-- `select species, tree_id ? min(height) by tree_form < 10 as f,
min(height)`: the select's minimum is grouped by `f` (0 feeds 2 below is
"`f` groups it"), and `f` reads the minimum by form (1 feeds 0). Electing the
select's minimum as a partial of the one by form adds 2 feeds 1 and leaves no
order to build them in (`partial_grouped_by_its_reader`); read off the rows
the three build as form, `f`, select. -/
theorem partial_grouped_by_its_reader :
    let lineage : List (Nat × Nat) := [(1, 0), (0, 2)]
    buildable lineage [0, 1, 2] 3 = true ∧
      buildable ((2, 1) :: lineage) [0, 1, 2] 3 = false := by decide

/-- `select tree_id, species ? species in (species ? count(tree_id) by
species >= 3) as f where sum(height) by city >= 1`: the WHERE's sum joins the
rows the set's count reads. The select's own rows are read beside that count,
not through it, so a gate hosted there alone leaves them whole: three trees
for the two the WHERE keeps (`gate_on_a_set_filters_no_row`; the planner
raised, finding no host among the outputs). -/
theorem gate_on_a_set_filters_no_row :
    let trees : List (Nat × Nat) := [(0, 10), (0, 11), (1, 12)]
    let gate : Nat × Nat → Bool := fun r => r.1 == 0
    let setInput := trees.filter gate
    setInput.length = 2 ∧ trees.length = 3 ∧ (trees.filter gate).map (·.2) = [10, 11] := by
  decide

/-! ## Two streams of one select's rows -/

/-- A filter's rows, as a pairing reads them: one per value of its grain.
Rows of a stream are `(species, column)` here. -/
def pairOn (left right : List (Nat × Nat)) : List (Nat × Nat × Nat) :=
  left.flatMap fun l => (right.filter (·.1 == l.1)).map fun r => (l.1, l.2, r.2)

/-- `select city, height > 2 as big, tree_form ? tree_form in (<set>) as f`:
the filter by `species` carried `city` per tree, so its rows were trees, and
the per-tree `big` paired with them on `species` alone. Two Acer trees, one
tall, became four rows, and city B showed a short Acer it does not have
(`carried_column_fans_out`). Deduplicated to its grain first, the filter is
one row per species and the pairing a lookup. -/
theorem carried_column_fans_out :
    -- (species, tall) per tree; the filter's rows (species, city) per tree
    let big : List (Nat × Nat) := [(0, 0), (0, 1)]
    let filtered : List (Nat × Nat) := [(0, 0), (0, 1)]
    (pairOn big filtered).length = 4 ∧
      (pairOn big (filtered.map fun r => (r.1, 0)).eraseDups).length = 2 := by
  decide

/-- `select city, tree_form, city ? sum(height) by * is null as f`: the
filter's rows were collapsed to `city`, the filtered value's keys, past the
select's other key. Paired with a value by `tree_form` on that key it no
longer holds, every city met every form (`collapse_drops_paired_key`): six
rows for four `(city, tree_form)` pairs. -/
theorem collapse_drops_paired_key :
    let pairs : List (Nat × Nat) := [(0, 0), (0, 2), (1, 0), (1, 1)]
    let cities := (pairs.map (·.1)).eraseDups
    let forms := (pairs.map (·.2)).eraseDups
    (cities.flatMap fun c => forms.map fun f => (c, f)).length = 6 ∧
      pairs.length = 4 := by
  decide

end Trilogy.Filters
