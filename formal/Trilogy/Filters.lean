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
pairing reads crosses every value of it (`collapse_drops_paired_key`). Its
keys are columns of the rows it filters: moved to a scan of their own, they
leave it nothing to pair on (`filter_keys_stay_on_its_rows`). And a WHERE
hosted only beneath an aggregate its condition reads has filtered that
aggregate's rows and not the filter's own; both must pass it
(`gate_on_a_condition_filters_no_row`).
A filter its keys decide is a property of them, shown once per value like
any other (`decided_filter_repeats_per_key`); only one reading finer than its
keys is a value per key. A filter whose content and condition an aggregate
answers is still a value of the select's rows: read off the aggregate alone
it holds no other key of them (`filter_over_aggregate_keeps_its_rows`),
and it meets a WHERE gate on the gate's whole grain
(`filter_meets_gate_on_its_grain`). A filtered key is no identity of the
rows (`filtered_key_identifies_no_row`).

A set's rows are the ones its condition kept, and an aggregate only a set
reads holds the set's rows: neither supplies the select's keys
(`set_holds_no_select_keys`, `set_input_supplies_no_output`) nor is the
parent of a value beside them (`set_input_is_no_row_parent`). A WHERE
aggregate only such an aggregate reads still filters the select's rows
(`gate_beside_set_filters_rows`).

A WHERE reaches a filter's rows through its row parents, not through the
value its condition reads: hosted only on that value, the filter's rows go
unfiltered (`gate_reaches_a_filter_alone`), and hosted only on a set's input
it filters the set, not the statement (`gate_hosted_on_set_input_alone`). A
filter whose content is a gated aggregate holds groups that passed already
(`hosted_content_passed_gate`). A total in a condition is one value on every
row and splits no key (`total_splits_no_key`); an aggregate in a condition
meets it on every key of its own grain (`condition_aggregates_meet_per_row`);
a key regrafted beside a paired one keeps it (`regraft_keeps_paired_key`,
open);
and a chain of partials grouped by a reader of its head has no build order
(`partial_chain_reads_itself`).
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


/-- `select tree_form, city ? city = 'A' as f where species in (<set>)`: the
kept trees are `(form, city)` = (broadleaf, A), (none, A), (linden, B), three
rows. The WHERE's set moved `tree_form` to a scan of the species table, so the
filter's rows held `f` alone and met the forms on nothing: every form beside
every `f`, six rows (`filter_keys_stay_on_its_rows`). -/
theorem filter_keys_stay_on_its_rows :
    let kept : List (Nat × Nat) := [(0, 0), (2, 0), (1, 1)]
    let f : Nat × Nat → Option Nat := filt (·.2 == 0) (·.2)
    let shown := (kept.map fun r => (r.1, f r)).eraseDups
    let forms := (kept.map (·.1)).eraseDups
    let values := (kept.map f).eraseDups
    shown.length = 3 ∧ (forms.flatMap fun k => values.map fun v => (k, v)).length = 6 := by
  decide

/-- `select height ? count(tree_id) by city < 10 as f where min(height) by
species < 4`: the WHERE keeps the species whose minimum is under 4. Hosted
only on the count, it filtered the rows the count reads, while the heights
shown were read beside it off every tree: Quercus's 5 came back
(`gate_on_a_condition_filters_no_row`). Trees are `(species, height)`. -/
theorem gate_on_a_condition_filters_no_row :
    let trees : List (Nat × Nat) := [(0, 3), (0, 7), (1, 5), (3, 2)]
    let speciesMin (s : Nat) := ((trees.filter (·.1 == s)).map (·.2)).foldl min 100
    let kept := trees.filter fun r => speciesMin r.1 < 4
    (kept.map (·.2)) = [3, 7, 2] ∧ (trees.map (·.2)).contains 5 = true := by
  decide

/-- `select fc, tree_form ? species in (<set>) as f`: Acer and Quercus are
both broadleaf, and neither is in the set. The select's rows are the distinct
`(fc, f)`, one; grouped by the hidden `species` that `f` is keyed by, the row
showed once per species (`decided_filter_repeats_per_key`). Rows are
`(species, fc, f)`. -/
theorem decided_filter_repeats_per_key :
    let rows : List (Nat × Nat × Option Nat) := [(0, 0, none), (1, 0, none)]
    (rows.map fun r => r.2).eraseDups.length = 1 ∧ rows.eraseDups.length = 2 := by
  decide

/-- `select city, tree_form, city ? sum(height) by city > 2 as f where
max(height) by tree_form is not null`: the kept trees' `(city, form)` pairs
are (A, broadleaf), (B, broadleaf), (B, linden). The filter was read off the
city sum alone, so its rows held cities and no form, and met the WHERE's
forms on nothing: A beside linden, four rows for three
(`filter_over_aggregate_keeps_its_rows`). Read off the rows beside the sum,
it is one value per pair. -/
theorem filter_over_aggregate_keeps_its_rows :
    let kept : List (Nat × Nat) := [(0, 0), (1, 0), (1, 1)]
    let cities := (kept.map (·.1)).eraseDups
    let forms := (kept.map (·.2)).eraseDups
    (cities.flatMap fun c => forms.map fun f => (c, f)).length = 4 ∧
      kept.eraseDups.length = 3 := by
  decide

/-! ## What a set's own rows are not

A set holds the rows its own condition kept, and an aggregate only a set's
condition reads holds the rows that set reads. Neither is a stream of the
select's rows: not a supplier of its keys, not a parent of a value computed
beside them. -/

/-- `select city, species, (count(tree_id) by city) ? city in (city ?
count(tree_id) by species > 5) as v0`: no species has six trees, so the set's
rows are empty while the select's are its five `(city, species)` pairs. The
keys were read off the set's rows, the one group holding both, and the
select returned nothing (`set_holds_no_select_keys`, 87783). -/
theorem set_holds_no_select_keys :
    -- (city, species) per tree; species counts 3, 1, 1, 1
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let count (s : Nat) := (trees.filter (·.2 == s)).length
    (trees.filter fun r => count r.2 > 5).length = 0 ∧ trees.eraseDups.length = 5 := by
  decide

/-- `select city, tree_form, (min(height) by city) ? city in (city ?
max(height) by tree_form is null) as v0 where tag in (<set>)`: the kept trees'
`(city, form)` pairs are (A, broadleaf), (A, none), (B, linden). `tree_form`
was read off the set's `by tree_form` aggregate, the forms of every kept tree,
and met the cities on nothing: six rows for three
(`set_input_supplies_no_output`, 83978). -/
theorem set_input_supplies_no_output :
    let kept : List (Nat × Nat) := [(0, 0), (0, 2), (1, 1)]
    let cities := (kept.map (·.1)).eraseDups
    let forms := (kept.map (·.2)).eraseDups
    (cities.flatMap fun c => forms.map fun f => (c, f)).length = 6 ∧
      kept.eraseDups.length = 3 := by
  decide

/-- `select coalesce(tree_form, 'none') as form0, tag ? city in (city ?
count(tree_id) by city, tree_form < 10) as v0`: `form0` was computed off the
set's `by city, tree_form` aggregate and paired with the tags on `city`, so
B's linden met its y tree: six rows for four. Off the trees it is one value
per tree (`set_input_is_no_row_parent`, 98603). Trees are `(city, form,
tag)`. -/
theorem set_input_is_no_row_parent :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 0), (0, 0, 1), (0, 0, 0), (0, 2, 0), (1, 0, 1), (1, 1, 0)]
    let forms := (trees.map fun r => (r.1, r.2.1)).eraseDups
    let tags := (trees.map fun r => (r.1, r.2.2)).eraseDups
    let paired := (forms.flatMap fun f =>
      (tags.filter (·.1 == f.1)).map fun t => (f.2, t.2)).eraseDups
    paired.length = 6 ∧ (trees.map fun r => (r.2.1, r.2.2)).eraseDups.length = 4 := by
  decide

/-- `select city, tag, (max(height) by city) ? tag = 'x' as v0 where
min(height) by city, species < 6`: the gate keeps A's Acers and Quercus and
B's Tilia. The filter's rows held `(city, tag)` alone, so FINAL met the gate
on `city` and kept B's y tree, whose Acer failed it
(`filter_meets_gate_on_its_grain`, 85660). Trees are `(city, species, tag)`;
kept `(city, species)` pairs. -/
theorem filter_meets_gate_on_its_grain :
    let trees : List (Nat × Nat × Nat) := [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 1), (1, 3, 0)]
    let kept : List (Nat × Nat) := [(0, 0), (0, 1), (1, 3)]
    ((trees.filter fun r => kept.contains (r.1, r.2.1)).map
        fun r => (r.1, r.2.2)).eraseDups.length = 3 ∧
      ((trees.filter fun r => kept.any (·.1 == r.1)).map
        fun r => (r.1, r.2.2)).eraseDups.length = 4 := by
  decide

/-- `select species, tag, tree_id ? count(tree_id) by species < 0 as v0 where
species in (species ? species = 'Acer')`: the condition holds nowhere, so
`v0` is NULL on all three Acers and the rows are two. The GROUP BY was
dropped as if the membership's set, one row per species, were the rows read
(`filtered_key_identifies_no_row`, 89848). Rows are `(tag, v0)`. -/
theorem filtered_key_identifies_no_row :
    let acers : List (Nat × Option Nat) := [(0, none), (1, none), (1, none)]
    acers.length = 3 ∧ acers.eraseDups.length = 2 := by
  decide

/-- `select city, city ? city in (city ? avg(height) by city > 1) as v0 where
tag in (<set>) and count(tree_id) by city <= 3`: the count reaches only the
set's average, under a WHERE that also reads a set. Hosted there alone it
filtered the set's rows; the select's must pass it too, and only B has at
most three trees (`gate_beside_set_filters_rows`, 82889: no host was found,
and the planner raised). Trees are `(city, tag)`. -/
theorem gate_beside_set_filters_rows :
    let trees : List (Nat × Nat) := [(0, 0), (0, 1), (0, 0), (0, 0), (1, 1), (1, 0)]
    let count (c : Nat) := (trees.filter (·.1 == c)).length
    let kept := trees.filter fun r => r.2 == 0 && count r.1 ≤ 3
    (kept.map (·.1)).eraseDups = [1] := by
  decide

/-! ## Where a WHERE reaches a filter's rows (0.3.537)

A filter's rows are its row parents', whatever value its condition reads:
a WHERE hosted only on that value, or only on a set's input, has filtered
neither. A total its condition reads is one value on every row, so it splits
no key. A filter whose content is a gated aggregate holds that aggregate's
groups, which passed the WHERE already. -/

/-- `select species ? avg(height) by * < 10 as f where avg(height) by city,
species != 2`: the WHERE keeps A's Acer and Quercus and B's Acer. Hosted only
on the total, it left the species `f` shows unfiltered: Tilia (average 2) and
Ulmus (no average) came back (`gate_reaches_a_filter_alone`, 2922). Rows are
`(species, average of its city and species)`. -/
theorem gate_reaches_a_filter_alone :
    let pairs : List (Nat × Option Nat) :=
      [(0, some 3), (1, some 5), (2, none), (0, some 7), (3, some 2)]
    let kept := pairs.filter fun p => p.2 != none && p.2 != some 2
    (kept.map (·.1)).eraseDups = [0, 1] ∧ (pairs.map (·.1)).eraseDups.length = 4 := by
  decide

/-- `select city, tree_form, city ? count_distinct(tag) by * < 0 as v0 where
avg(height) by species != 1`: a total decides nothing finer than the city.
Collapsed to its city as if it did, the filter lost the species its rows meet
the forms on: A beside linden, four rows for three
(`total_splits_no_key`, 111272). Kept `(city, form)` pairs. -/
theorem total_splits_no_key :
    let kept : List (Nat × Nat) := [(0, 0), (1, 0), (1, 1)]
    let cities := (kept.map (·.1)).eraseDups
    let forms := (kept.map (·.2)).eraseDups
    kept.eraseDups.length = 3 ∧ (cities.flatMap fun c => forms.map fun f => (c, f)).length = 4 := by
  decide

/-- `select tree_form ? not (species in (species ? count(tree_id) by city = 0))
as v0 where min(height) by city < 2`: no city's minimum is under 2, so the
answer is empty. The set's count was the one host keyed by `city`; hosted
there alone the WHERE emptied the set, and `not in` an empty set kept every
form (`gate_hosted_on_set_input_alone`, 117080). Trees are `(city, form)`. -/
theorem gate_hosted_on_set_input_alone :
    let trees : List (Nat × Nat) := [(0, 0), (0, 2), (1, 0), (1, 1)]
    let cityMin (c : Nat) := if c == 0 then 3 else 2
    (trees.filter fun r => cityMin r.1 < 2).length = 0 ∧
      (trees.map (·.2)).eraseDups.length = 3 := by
  decide

/-- `select city, tag, tree_form ? species in (species ? sum(height) by city,
tree_form > min(height) by species) as v0 where avg(height) by city, species
!= 3`: on each kept row the city-form sum equals the species minimum, so the
set is empty. Paired on the form alone, B's broadleaf sum 7 met Quercus's
minimum 5 and Quercus joined it (`condition_aggregates_meet_per_row`, 114764).
Kept rows are `(city, species, form, height)`. -/
theorem condition_aggregates_meet_per_row :
    let rows : List (Nat × Nat × Nat × Nat) := [(0, 1, 0, 5), (1, 0, 0, 7), (1, 3, 1, 2)]
    let sum (c f : Nat) := ((rows.filter fun r => r.1 == c && r.2.2.1 == f).map (·.2.2.2)).foldl (· + ·) 0
    let low (s : Nat) := ((rows.filter (·.2.1 == s)).map (·.2.2.2)).foldl min 100
    (rows.filter fun r => sum r.1 r.2.2.1 > low r.2.1).length = 0 ∧
      (rows.any fun r => rows.any fun o => o.2.2.1 == r.2.2.1 && sum o.1 o.2.2.1 > low r.2.1) = true := by
  decide

/-- `select fc, tag ? city in (<set>) as v0, min(height) by * as v1`: `fc` was
read off the `(city, tree_form)` minimum the total rolls up, and met the tags
on `city` alone: A's Ulmus form `o` beside Acer's `y`
(`regraft_keeps_paired_key`, 112517, open: counting FINAL as a pairing
reader broke TPC-DS q25 and q66). Trees of A are `(fc, tag)`. -/
theorem regraft_keeps_paired_key :
    let trees : List (Nat × Nat) := [(0, 0), (0, 1), (0, 0), (1, 0)]
    let fcs := (trees.map (·.1)).eraseDups
    let tags := (trees.map (·.2)).eraseDups
    trees.eraseDups.length = 3 ∧ (fcs.flatMap fun f => tags.map fun t => (f, t)).length = 4 := by
  decide

/-- `select fc, (sum(height) by fc) ? max(height) by * > 3 as v0 where
count(tree_id) by city <= 3`: the WHERE keeps B's two trees, so the sums are
`b` 7 and `o` 2. The filter's rows are the gated sum's groups, which passed the
WHERE already; gated again as unfiltered rows, they found no host and the
planner raised (`hosted_content_passed_gate`, 113128). Trees are `(city, fc,
height)`. -/
theorem hosted_content_passed_gate :
    let trees : List (Nat × Nat × Nat) := [(0, 0, 3), (0, 0, 0), (0, 0, 5), (0, 1, 0), (1, 0, 7), (1, 1, 2)]
    let count (c : Nat) := (trees.filter (·.1 == c)).length
    let kept := trees.filter fun r => count r.1 ≤ 3
    let sum (f : Nat) := ((kept.filter (·.2.1 == f)).map (·.2.2)).foldl (· + ·) 0
    [0, 1].map (fun f => (f, sum f)) = [(0, 7), (1, 2)] ∧
      (kept.filter fun r => count r.1 ≤ 3).length = kept.length := by
  decide

/-- `select city, tree_form, species ? ... as v0, tag ? not (tree_form in
(tree_form ? sum(height) by city, species - min(height) by tree_form = 3)) as
v1, min(height) as v2, min(height) by city, tree_form as v3`: `v1` reads the
minimum by form (1 feeds 0), `v2` is grouped by `v1` (0 feeds 2), and `v3` is
grouped finer than the form (3). Electing `v3` as the form's partial (3 feeds
1) and `v2` as `v3`'s (2 feeds 3) is a cycle no single step shows
(`partial_chain_reads_itself`, 109332: the planner recursed). -/
theorem partial_chain_reads_itself :
    let lineage : List (Nat × Nat) := [(1, 0), (0, 2)]
    buildable ((3, 1) :: lineage) [0, 1, 2, 3] 4 = true ∧
      buildable ((2, 3) :: (3, 1) :: lineage) [0, 1, 2, 3] 4 = false := by decide

/-! ## What a set beside a stream supplies (0.3.538)

A set feeds no rows, so it supplies no column to a group beside it, and an
aggregate only a set reads holds the set's rows, not the select's. A set
planned alone under the WHERE reads the rows the WHERE kept, its filter's
own rows included, and so does a set a select target reads when FINAL
re-sources it. -/

/-- `select city, tag, (count(tree_id) by city) ? city in (<set>) as v0 where
count(tree_id) by city >= count(tree_id) by city, species`: A holds four
trees and its Acers two, so every tree passes. The select's count by city
was asked for the WHERE's count too, but dropped it as one the set beside it
could supply; a set supplies no column, and the filter recounted it per row
as 1, so A's Acers failed (`set_supplies_no_column`, 123427). Trees are
`(city, species)`. -/
theorem set_supplies_no_column :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let byCity (c : Nat) := (trees.filter (·.1 == c)).length
    let byPair (c s : Nat) := (trees.filter fun r => r.1 == c && r.2 == s).length
    (trees.filter fun r => byCity r.1 ≥ byPair r.1 r.2).length = 6 ∧
      (trees.filter fun r => 1 ≥ byPair r.1 r.2).length = 4 := by
  decide

/-- `select city, tag, species ? tree_form in (tree_form ? count_distinct(tag)
by species < 6 and city = 'B') as v1 where min(height) by city, species <= 3`:
the WHERE keeps A's Acers and B's Tilia, so the set is B's kept forms,
linden. Planned alone, the set's filter read every B tree beside the gated
count and took B's Acer's broadleaf too: A's Acers showed `Acer`
(`set_plan_gates_its_rows`, 128462). Trees are `(city, species, form,
height)`, 0 for no height. -/
theorem set_plan_gates_its_rows :
    let trees : List (Nat × Nat × Nat × Nat) :=
      [(0, 0, 0, 3), (0, 0, 0, 0), (0, 1, 0, 5), (0, 2, 2, 0), (1, 0, 0, 7), (1, 3, 1, 2)]
    let low (c s : Nat) :=
      ((trees.filter fun r => r.1 == c && r.2.1 == s).map (·.2.2.2)).filter (· != 0)
    let kept := trees.filter fun r => (low r.1 r.2.1).any (· ≤ 3)
    ((kept.filter (·.1 == 1)).map (·.2.2.1)).eraseDups = [1] ∧
      ((trees.filter (·.1 == 1)).map (·.2.2.1)).eraseDups = [0, 1] := by
  decide

/-- `select city ? city in (city ? tree_form = 'linden') as v0, tag ? ... as
v1, count(tree_id) by * as v3 where min(height) by city, species >= 6 having
v3 <= 1`: only B's Acer passes, a broadleaf, so the set is empty and `v0` is
NULL. FINAL re-sourced the filter and wired the set the graph built off
every tree, where B's Tilia is linden (`select_reads_kept_set`, 130249).
Trees are `(city, form, min height of its city and species)`. -/
theorem select_reads_kept_set :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 3), (0, 0, 3), (0, 0, 5), (0, 2, 0), (1, 0, 7), (1, 1, 2)]
    let kept := trees.filter (·.2.2 ≥ 6)
    (kept.filter (·.2.1 == 1)).length = 0 ∧ (trees.filter (·.2.1 == 1)).length = 1 := by
  decide

/-- `select species, (sum(height) by tree_form) ? tree_form in (tree_form ?
count_distinct(tag) by city, species > 6) as v0, sum(height) by tree_form as
v1`: one row per species, four. The filter holds no species, and `species`
was read off the set's `by city, species` count, the group furthest
downstream holding it: Acer once per city, five rows
(`set_input_supplies_no_key`, 129123). Groups are `(city, species)`. -/
theorem set_input_supplies_no_key :
    let groups : List (Nat × Nat) := [(0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    (groups.map (·.2)).eraseDups.length = 4 ∧ (groups.map (·.2)).length = 5 := by
  decide

/-! ## What a joined side's filter says of the rows (0.3.539)

A WHERE applied below a group filtered the rows the group read; joined back
to finer rows on the group's keys, it says nothing of those rows unless the
keys decide it. Nor does a stored aggregate a scan carries for a set's
condition identify the scan's rows, and the WHERE's twin of a shown aggregate
is read under its own name wherever the gate is applied. -/

/-- `select species, tree_form, tag ? count(tag) by city < 1 as v0,
count(tree_id) as v1 where max(height) by city, tree_form is null`: only the
formless Ulmus passes (one row). The count by city read the kept rows, so its
groups are the cities holding one, A; joined back to the trees on `city`,
every A tree meets a group, and FINAL took those four rows as the WHERE's
(`coarse_side_filters_no_row`, 151597). Trees are `(city, form, height)`, 0
for no height. -/
theorem coarse_side_filters_no_row :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 3), (0, 0, 0), (0, 0, 5), (0, 2, 0), (1, 0, 7), (1, 1, 2)]
    let heights (c f : Nat) :=
      ((trees.filter fun r => r.1 == c && r.2.1 == f).map (·.2.2)).filter (· != 0)
    let kept := trees.filter fun r => heights r.1 r.2.1 = []
    let cities := (kept.map (·.1)).eraseDups
    kept.length = 1 ∧ (trees.filter fun r => cities.contains r.1).length = 4 := by
  decide

/-- `select species ? species in (species ? count(tag) > 1) as v0, sum(height)
by city, species as m where sum(height) by city, species > 0`: the Ulmus sum
is NULL, so the kept rows are A's Acers and Quercus and B's two trees, and
only Acer holds two tags there. The set, gated off the summary, asked for the
WHERE's twin of `m` and was handed `m`, a column the gate never read
(`gate_reads_its_twin`, 62004: the planner raised). Trees are `(city,
species, height)`, 0 for no height. -/
theorem gate_reads_its_twin :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 3), (0, 0, 0), (0, 1, 5), (0, 2, 0), (1, 0, 7), (1, 3, 2)]
    let sum (c s : Nat) :=
      ((trees.filter fun r => r.1 == c && r.2.1 == s).map (·.2.2)).foldl (· + ·) 0
    let kept := trees.filter fun r => sum r.1 r.2.1 > 0
    let tags (s : Nat) := (kept.filter (·.2.1 == s)).length
    kept.length = 5 ∧ ((kept.map (·.2.1)).eraseDups.filter (tags · > 1)) = [0] := by
  decide

/-- `select city, species, city ? city in (<set>) as v0, (max(height) by city)
? city in (city ? count(tree_id) by city, species = 1) as v1`: one row per
city and species, five. On the summary the stored count rode the tree scan
for the set alone, and was read as pinning the scan's rows: FINAL joined the
six trees unduplicated and A's Acer showed twice
(`set_read_column_pins_no_row`, 152317). Trees are `(city, species)`. -/
theorem set_read_column_pins_no_row :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    trees.eraseDups.length = 5 ∧ trees.length = 6 := by
  decide

/-! ## A filter's rows beside its set and its lookups (0.3.540)

A WHERE hosted on a total gates the rows the total reads; a filter whose set
reads that total meets it only through the set, so its own rows still owe the
gate. A filtered value's step paired as a lookup holds one row per key: built
per row, every row of a key meets every other. And a row value keeps the key
of its rows to pair on, even where the scan it reads dropped that key after
its WHERE read it. -/

/-- `select species ? tree_form in (tree_form ? sum(height) by * != 3) as v0,
count(tree_id) by * as v1 where min(height) by species is not null`: the
Ulmus has no height, so three species are kept, each in the set. The gate was
hosted on the `by *` group the set's condition and `v1` share; the filter's
own rows read past it, and the Ulmus showed as a NULL fourth row
(`gate_reaches_a_filter_through_its_set`, 160158). Trees are `(species,
height)`, 0 for no height. -/
theorem gate_reaches_a_filter_through_its_set :
    let trees : List (Nat × Nat) := [(0, 3), (0, 0), (1, 5), (2, 0), (0, 7), (3, 2)]
    let kept := trees.filter fun r => (trees.filter (·.1 == r.1)).any (·.2 != 0)
    (kept.map (·.1)).eraseDups.length = 3 ∧ (trees.map (·.1)).eraseDups.length = 4 := by
  decide

/-- `select tree_form, species ? max(height) by species < 5 as v0,
count(tree_id) as v1, sum(height ? tag = 'x') as v2 where species in
(species ? height is not null)`: four broadleaf trees, three of them Acers.
The `v0` step, built per tree under the WHERE, was paired with the trees on
`species` as a lookup, so each Acer met all three and the count read 10
(`lookup_side_counts_once`, 167663). Broadleaf trees are their species. -/
theorem lookup_side_counts_once :
    let trees : List Nat := [0, 0, 1, 0]
    let paired := trees.map fun s => (trees.filter (· == s)).length
    trees.length = 4 ∧ paired.foldl (· + ·) 0 = 10 := by
  decide

/-- `select height > 2 as big, tree_form ? species in (species ? species !=
'Tilia') as v0 where tree_id in (tree_id ? count(tree_id) by city < 8)`: every
tree passes, and the six trees show four `(big, v0)` pairs. The scan under the
membership dropped `tree_id` once the WHERE read it, so `big` was deduped to
its three values with no key left, and FINAL crossed them with `v0`'s two
(`row_value_keeps_its_key`, 178393). Trees are `(big, v0)`: big 0 false, 1
true, 2 NULL; v0 0 broadleaf, 1 NULL. -/
theorem row_value_keeps_its_key :
    let trees : List (Nat × Nat) := [(1, 0), (2, 0), (1, 0), (2, 1), (1, 0), (0, 1)]
    trees.eraseDups.length = 4 ∧
      (trees.map (·.1)).eraseDups.length * (trees.map (·.2)).eraseDups.length = 6 := by
  decide

/-! ## A window's rows, a filtered aggregate's rows, a gate beside a set (0.3.541)

A window reads the statement's rows before FINAL could gate them, so its merge
keeps only the side a WHERE gate filtered. A filtered aggregate keyed by fewer
of the shown keys than its rows hold is a value of those rows, not a group of
its own. A predicate reading an aggregate is no grouping key of the group that
computes it. And a `by *` total in the WHERE beside a membership still gates
the statement's rows. -/

/-- `select city, tree_form, city ? city = 'B' as v0, sum(max(height) by city,
tree_form, v0) over (order by city) as v2 where avg(height) by species > 6`:
no species averages over 6, so there are no rows. The gate was hosted on the
maximum; the window's merge FULL-joined it to the filter's four `(city,
tree_form)` pairs, which never passed the gate, and showed all four
(`window_reads_gated_rows`, 196634). Trees are `(pair, species average)`. -/
theorem window_reads_gated_rows :
    let trees : List (Nat × Nat) := [(0, 5), (0, 5), (0, 5), (1, 0), (2, 5), (3, 2)]
    (trees.filter (·.2 > 6)).length = 0 ∧ (trees.map (·.1)).eraseDups.length = 4 := by
  decide

/-- `select city, tree_form, (count(tree_id) by tree_form) ? tree_form != 'x'
as v0, max(height) by city as v1`: four `(city, tree_form)` pairs. The filter
was read off the count by form alone, so nothing paired the forms with the
cities and FINAL crossed two cities with three forms
(`filtered_aggregate_reads_its_rows`, 190095). Trees are `(city, form)`. -/
theorem filtered_aggregate_reads_its_rows :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 0), (0, 2), (1, 0), (1, 1)]
    trees.eraseDups.length = 4 ∧
      (trees.map (·.1)).eraseDups.length * (trees.map (·.2)).eraseDups.length = 6 := by
  decide

/-- `select upper(tree_form) as fc, tree_form ? min(height) by tree_form != 0
as v0, city as v1`: `v0` is one value per form, so grouping by the form alone
gives its groups. Folded into the minimum's group, `v0` was listed as a key
beside the form, and its `min` sat in the GROUP BY
(`predicate_aggregate_keys_no_group`, 196668). Trees are `(form, v0)`. -/
theorem predicate_aggregate_keys_no_group :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 0), (2, 2), (0, 0), (1, 1)]
    (trees.map (·.1)).eraseDups.length = trees.eraseDups.length := by
  decide

/-- `select height ? tag in (tag ? count(tree_id) by city >= 1) as v0 where
max(height) by * <= max(height) by city and tag in (tag ? city = 'A')`: only
B's tallest reaches the total, and both of B's trees carry one of A's tags, so
the heights are 7 and 2. The total was kept off the statement's rows beside
the membership, its only hosts were the set's, and the plan raised
(`total_gate_reaches_the_rows`, 182211). Trees are `(city, height)`. -/
theorem total_gate_reaches_the_rows :
    let trees : List (Nat × Nat) := [(0, 3), (0, 0), (0, 5), (0, 0), (1, 7), (1, 2)]
    let total := (trees.map (·.2)).foldl max 0
    let top c := ((trees.filter (·.1 == c)).map (·.2)).foldl max 0
    (trees.filter fun r => total ≤ top r.1).map (·.2) = [7, 2] := by
  decide

/-! ## A gate's grain is its atom's (0.3.542)

A WHERE atom reading aggregates at two grains is decided at their union: an
aggregate reading one of its values at that value's grain meets the rows
only on that value's keys, so it filters the rows the atom kept no more than
any other reader does. -/

/-- `select city, city ? city in (city ? sum(height) by city > 0 and height >
2) as v2 where sum(height) by city - max(height) by city, species > 3`: the
WHERE keeps A's Acer (8 - 3) and B's Tilia (9 - 2), and only the Acer is over
2, so the set is A alone. The gate was taken as met by the set's `by city`
sum, which reads the gate's `by city` value at its own grain, and the set's
`height > 2` read every row: B's Acer put B in the set
(`gate_grain_is_its_atoms`, 190661). Trees are `(city, species, height)`. -/
theorem gate_grain_is_its_atoms :
    let trees : List (Nat × Nat × Nat) := [(0, 0, 3), (0, 1, 5), (1, 0, 7), (1, 2, 2)]
    let total c := ((trees.filter (·.1 == c)).map (·.2.2)).foldl (· + ·) 0
    let top c s :=
      ((trees.filter fun r => r.1 == c && r.2.1 == s).map (·.2.2)).foldl max 0
    let kept := trees.filter fun r => total r.1 > top r.1 r.2.1 + 3
    ((kept.filter (·.2.2 > 2)).map (·.1)).eraseDups = [0] ∧
      ((trees.filter (·.2.2 > 2)).map (·.1)).eraseDups = [0, 1] := by
  decide

/-- `select species, tree_form, tag ? tag in (tag ? min(height) by species is
not null) as v0 where tree_form is null`: the WHERE keeps the Ulmus alone,
which is unmeasured, so the set is empty and `v0` is NULL. The form was
peeled onto a scan of the species joined back at FINAL, which filters the
shown rows; the set is a subselect, which no join reaches, and it read the
Acers' tag `x` (`peel_skips_a_set`, 201221). Trees are `(species, tag,
measured)`. -/
theorem peel_skips_a_set :
    let trees : List (Nat × Nat × Bool) :=
      [(0, 0, true), (0, 1, false), (1, 0, true), (2, 0, false), (0, 1, true), (3, 0, true)]
    let measured (rows : List (Nat × Nat × Bool)) s := rows.any fun r => r.1 == s && r.2.2
    let set (rows : List (Nat × Nat × Bool)) :=
      ((rows.filter fun r => measured rows r.1).map (·.2.1)).eraseDups
    let kept := trees.filter (·.1 == 2)
    set kept = [] ∧ set trees = [0, 1] := by
  decide

/-- `select city, height > 2 as big, tree_form ? species in (species ? height
is null) as v0, max(height) by city as v1` on the summary: the filter's grain
is `species`, but it outputs `city` too, so its rows are one per `(city,
species)`. FINAL paired them with the trees on `species` alone and took
`city` off the filter: A's Acer met B's, and B showed a broadleaf beside A's
maximum (`filter_rows_keyed_by_outputs`, 201879). Trees are `(city,
species)`. -/
theorem filter_rows_keyed_by_outputs :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let filtered := trees.eraseDups
    filtered.length = 5 ∧
      (trees.any fun t => filtered.any fun f => t.2 == f.2 && t.1 != f.1) = true := by
  decide

/-! ## A gate that holds on NULL, restated over padding (0.3.543)

An atom that holds on an absent group (`sum(height) by tree_form is null`)
passes wherever a join pads the side it reads its value off: the padded row
looks like a group that has no rows. So a side that applied such an atom is
the population, and a filter read off the host's own groups needs no
restatement. -/

/-- `select species, (sum(height) by species) ? species = 'Acer' as v0, tag ?
(species in (species ? max(height) by city > 3) or city = 'Z') as v1 where
sum(height) by tree_form is null`: only the formless Ulmus is unmeasured, so
it alone is kept. FINAL restated the gate on the tags, reading its value off
the gated species joined RIGHT: every other species was padded there, read
NULL, and passed (`padding_passes_a_restated_gate`, 226656). Groups are
`(species, sum)`. -/
theorem padding_passes_a_restated_gate :
    let groups : List (Nat × Option Nat) := [(0, some 10), (1, some 5), (2, none), (3, some 2)]
    let kept := groups.filter (·.2.isNone)
    let padded k := (kept.lookup k).join
    kept.map (·.1) = [2] ∧
      (groups.filter fun g => (padded g.1).isNone).map (·.1) = [0, 1, 2, 3] := by
  decide

/-- `select city, tree_form, city ? not (city in (city ? height is not null))
as v0, (count(tree_id) by city, tree_form) ? city in ('B') as v1 where (tag in
(<empty set>) or min(height) by city, tree_form is null)`: both filters are
read off the gated `(city, tree_form)` groups, so their rows passed the gate.
Taking `v0`'s content `city` for a row column, FINAL restated the gate over a
FULL join with the trees and kept every padded group
(`hosted_key_passed_gate`, 217166). Groups are `(city, form, min)`. -/
theorem hosted_key_passed_gate :
    let groups : List (Nat × Nat × Option Nat) :=
      [(0, 0, some 3), (0, 1, none), (1, 0, some 7), (1, 2, some 2)]
    let kept := groups.filter (·.2.2.isNone)
    let shown := kept.map fun g => (g.1, g.2.1)
    (shown.filter fun k => (kept.find? fun g => (g.1, g.2.1) == k).isSome) = [(0, 1)] ∧
      (groups.filter fun g =>
        ((kept.find? fun h => h.1 == g.1 && h.2.1 == g.2.1).bind (·.2.2)).isNone).length = 4 := by
  decide

/-- `select coalesce(tree_form, 'none') as form0, city ? count(tree_id) by
city > 5 or city in (city ? max(height) by species < 4) as v0`: the set is
both cities, so each tree shows its own form and city. The filter was read
off the count by city alone and the form off a scan of the species, which
share no column: FINAL joined them on nothing and crossed every form with
every city. Paired on the form's input `species` they meet per tree
(`derived_key_pairs_on_its_inputs`, 222515). Trees are `(city, species)`;
species 0 and 1 are broadleaf, 2 has no form, 3 is linden. -/
theorem derived_key_pairs_on_its_inputs :
    let trees : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let form (s : Nat) : Nat := if s ≤ 1 then 0 else if s == 2 then 1 else 2
    let paired := (trees.map fun t => (form t.2, t.1)).eraseDups
    let forms := (trees.map (form ·.2)).eraseDups
    let cities := (trees.map (·.1)).eraseDups
    paired.length = 4 ∧ (forms.flatMap fun f => cities.map (f, ·)).length = 6 := by
  decide

/-- `select tag, species ? species in (species ? sum(height) by * >= 8) as v0
where min(height) by * < count(tree_id) by city`: the trees are read with the
all-rows marker and the gates deferred. The marker's grain is the select's,
`(tag, v0)`; expanded as grain keys, it demanded `v0`, a filter no scan holds,
so the one scan holding every other column covered nothing and the statement
raised (`all_rows_marker_is_no_key`, 225484 and 11 more). Columns are `city,
height, species, tag, tree_id` (0-4) and `v0` (9). -/
theorem all_rows_marker_is_no_key :
    let scan : List Nat := [0, 1, 2, 3, 4]
    let request : List Nat := [0, 1, 2, 3, 4]
    let marker : List Nat := [3, 9]
    request.all scan.contains = true ∧ (request ++ marker).all scan.contains = false := by
  decide

/-- `select ..., sum(height ? tag in (tag ? height between 3 and 7 and
sum(height) by tree_form != 3)) as v2 where count_distinct(tag) by * >
count_distinct(tag) by city, tree_form`: the WHERE keeps the groups with
fewer tags than the two in all, so A's broadleaf trees go, and the set is the
kept trees' tags between 3 and 7, B's Acer's `y`. Planned alone, the set took
the gate's per-group count onto its rows and left the total it is compared
with on the aggregate: the atom stayed there, the set read A's Acer and took
`x` too (`gate_takes_its_total`, 235307). Trees are `(city, form, height,
tag)`, 0 for no height. -/
theorem gate_takes_its_total :
    let trees : List (Nat × Nat × Nat × Nat) :=
      [(0, 0, 3, 0), (0, 0, 0, 1), (0, 0, 5, 0), (0, 2, 0, 0), (1, 0, 7, 1), (1, 1, 2, 0)]
    let tags (rows : List (Nat × Nat × Nat × Nat)) := (rows.map (·.2.2.2)).eraseDups.length
    let group (r : Nat × Nat × Nat × Nat) := trees.filter fun t => t.1 == r.1 && t.2.1 == r.2.1
    let kept := trees.filter fun r => tags trees > tags (group r)
    let formSum (f : Nat) := ((kept.filter (·.2.1 == f)).map (·.2.2.1)).foldl (· + ·) 0
    let set (rows : List (Nat × Nat × Nat × Nat)) :=
      ((rows.filter fun r => 3 ≤ r.2.2.1 && r.2.2.1 ≤ 7 && formSum r.2.1 != 3).map
        (·.2.2.2)).eraseDups
    set kept = [1] ∧ set trees = [0, 1] := by
  decide

/-- `select city, species, tree_form, species ? avg(height) by * is not null
as v0, count_distinct(tag) by species as v1, row_number() over (partition by
city order by species desc, ...) as v2 where max(height) by city, tree_form
!= 1 and ...`: the WHERE drops A's formless Ulmus, whose maximum is NULL, so
A numbers two species. The gate was hosted on the aggregates alone; the
window read the filter's rows, which met the gate only through the total its
condition reads, and numbered Ulmus too: A's Acer read 3
(`window_counts_ungated_rows`, 240133). Rows are `(city, species, max)`, 0 for
NULL. -/
theorem window_counts_ungated_rows :
    let rows : List (Nat × Nat × Nat) := [(0, 0, 3), (0, 1, 5), (0, 2, 0), (1, 0, 7), (1, 3, 2)]
    let kept := rows.filter fun r => r.2.2 != 0 && r.2.2 != 1
    (kept.filter (·.1 == 0)).length = 2 ∧ (rows.filter (·.1 == 0)).length = 3 := by
  decide

/-- `select city, tag, species ? species = 'Ulmus' as v1, min(max(height))
over (partition by city, tag) as v2, min(sum(height)) over (partition by tag,
v1) as v3`: five `(city, tag, v1)` groups. Each window was planned as a
group by its partition, and FINAL paired the two on their one shared key,
`tag`: B's `x` met A's Ulmus (`partitions_pair_on_the_select_grain`, 257753).
Groups are `(city, tag, v1)`, 0 for NULL. -/
theorem partitions_pair_on_the_select_grain :
    let groups : List (Nat × Nat × Nat) := [(0, 0, 1), (0, 0, 0), (0, 1, 0), (1, 0, 0), (1, 1, 0)]
    let left := (groups.map fun g => (g.1, g.2.1)).eraseDups
    let right := (groups.map fun g => (g.2.1, g.2.2)).eraseDups
    let paired := left.flatMap fun l =>
      (right.filter (·.1 == l.2)).map fun r => (l.1, l.2, r.2)
    paired.length = 6 ∧ (paired.filter (! groups.contains ·)) = [(1, 0, 1)] := by
  decide

/-- `select tree_id ? species in (<S1>) as v0, city ? city in (<S2>) as v1
where height > 2 and count(tree_id) by tree_form > 3`: `v0` reads S1 and `v1`
reads S2 (0 feeds 2, 1 feeds 3). Both sets claim the select's grain, `v0, v1`.
Taking out of each only its own reader keeps `v0` a key of S2 and `v1` one of
S1, and the four have no build order: planned standalone, each set asked for
the other's reader and the planner recursed (`sets_key_no_readers`, 256563
and the `OPEN` seeds 349, 667, 3730, 10955, 12695). The cycle needs the set's
own reader in the grain: a set no select column reads (one in the WHERE) keeps
the others as keys, and dropping them there joined its rows on no key (3343
and 200 more). Nodes are S1, S2, `v0`, `v1` (0-3). -/
theorem sets_key_no_readers :
    let lineage : List (Nat × Nat) := [(0, 2), (1, 3)]
    buildable lineage [0, 1, 2, 3] 4 = true ∧
      buildable ((2, 1) :: (3, 0) :: lineage) [0, 1, 2, 3] 4 = false := by decide

/-- `select city, city ? city in (city ? species = 'Tilia') as v0,
sum(min(height)) over () as v1 where sum(height) by tree_form <= 2`: only the
linden form sums to 2 or less, so the WHERE keeps B's Tilia and the select is
one row for B. The minimum by city hosted the gate and fed only the window's
total; the filter showing `city` read every row, its set the kept ones. FINAL
dropped the host as covered, its gate counted as applied because the set
under the filter had met it: A came back (`set_gate_restricts_no_row`,
269661). Trees are `(city, form, height)`, form 2 for none, height 0 for
NULL. -/
theorem set_gate_restricts_no_row :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 3), (0, 0, 0), (0, 0, 5), (0, 2, 0), (1, 0, 7), (1, 1, 2)]
    let formSum (f : Nat) := ((trees.filter (·.2.1 == f)).map (·.2.2)).foldl (· + ·) 0
    let kept := trees.filter fun r => r.2.1 != 2 && formSum r.2.1 ≤ 2
    let set := ((kept.filter (·.2.1 == 1)).map (·.1)).eraseDups
    set = [1] ∧ (kept.map (·.1)).eraseDups = [1] ∧ (trees.map (·.1)).eraseDups = [0, 1] := by
  decide

/-- `select city, city ? city in (city ? sum(height) by city, tree_form > 3)
as v0, sum(min(height)) over () as v1 where sum(height) by tree_form <= 2`:
the same WHERE, and a set no kept tree satisfies. FINAL took `city` off the
filter, whose rows never met the gate, over the gated minimum by city: the
cover ranks rows that passed a WHERE first only for an atom holding on an
absent group (`cover_reads_gated_rows`, 270862). -/
theorem cover_reads_gated_rows :
    let trees : List (Nat × Nat × Nat) :=
      [(0, 0, 3), (0, 0, 0), (0, 0, 5), (0, 2, 0), (1, 0, 7), (1, 1, 2)]
    let formSum (f : Nat) := ((trees.filter (·.2.1 == f)).map (·.2.2)).foldl (· + ·) 0
    let kept := trees.filter fun r => r.2.1 != 2 && formSum r.2.1 ≤ 2
    let pairSum (c f : Nat) :=
      ((kept.filter fun t => t.1 == c && t.2.1 == f).map (·.2.2)).foldl (· + ·) 0
    (kept.filter fun r => pairSum r.1 r.2.1 > 3) = [] ∧
      (kept.map (·.1)).eraseDups = [1] := by
  decide

/-- `select city, tree_form, city ? city in (<set>) as v0, sum(height) by city
as v1 where sum(height) by city, species <= 8`: A's Ulmus sums to NULL, so the
WHERE drops it and A shows only broadleaf. The gate's host is the sum by city,
species, whose members include `city`: a filtered `city` was taken for a
value that host computes, whose groups had passed, though its rows were every
tree's. A came back with no form (`key_content_hosts_no_rows`, 260615). Trees
are `(city, species, form, height)`, form 2 for none, height 0 for NULL. -/
theorem key_content_hosts_no_rows :
    let trees : List (Nat × Nat × Nat × Nat) :=
      [(0, 0, 0, 3), (0, 0, 0, 0), (0, 1, 0, 5), (0, 2, 2, 0), (1, 0, 0, 7), (1, 3, 1, 2)]
    let measured (c s : Nat) := trees.any fun t => t.1 == c && t.2.1 == s && t.2.2.2 != 0
    let kept := trees.filter fun r => measured r.1 r.2.1
    let shown (rows : List (Nat × Nat × Nat × Nat)) := (rows.map fun r => (r.1, r.2.2.1)).eraseDups
    shown kept = [(0, 0), (1, 0), (1, 1)] ∧ shown trees = [(0, 0), (0, 2), (1, 0), (1, 1)] := by
  decide

/-- `select species, species ? sum(height) by species >= 8 as v0 where city in
(city ? height > 2)`: the summary stores the sum by city, species; Acer's are
3 and 7. The filter read each stored partial as the sum by species and none
reached 8, so Acer showed NULL; re-summed first, its 10 does
(`filter_reads_finished_partial`, 294582). -/
theorem filter_reads_finished_partial :
    let acer : List Nat := [3, 7]
    acer.all (· < 8) = true ∧ acer.foldl (· + ·) 0 ≥ 8 := by decide

/-- `select species, (sum(height) by tree_form) ? tree_form = 'linden' as v0,
max(height) as v1, count(tree_id) by tree_form as v2`: the maximum is grouped
by `species, v0`, and the filter holds `tree_form, v0, v2`. The plan paired
them on `species, v0`, the filter's claimed grain, though its node holds no
species; rendered on `v0` alone, every species with no linden met both forms
whose `v0` is NULL: Acer showed `v2` 4 and 1. Read off what the filter holds,
the maximum looks up `tree_form` by `species` to pair
(`filter_pairs_on_built_columns`, 279431). Species are `(species, form)`,
filter rows `(form, v0, v2)` with 0 for NULL `v0`. -/
theorem filter_pairs_on_built_columns :
    let species : List (Nat × Nat) := [(0, 0), (1, 0), (2, 2), (3, 1)]
    let filter : List (Nat × Nat × Nat) := [(0, 0, 4), (1, 2, 1), (2, 0, 1)]
    let onForm := species.flatMap fun s =>
      (filter.filter (·.1 == s.2)).map fun f => (s.1, f.2.2)
    let onV0 := species.flatMap fun s =>
      (filter.filter fun f => f.2.1 == (filter.filter (·.1 == s.2)).foldl (fun _ g => g.2.1) 0).map
        fun f => (s.1, f.2.2)
    onForm.length = 4 ∧ (onV0.filter (·.1 == 0)).eraseDups = [(0, 4), (0, 1)] := by
  decide

/-- `select city, species, species ? min(height) by species = 8 as v0,
avg(sum(height)) over (partition by city, v0) as v1`: five `(city,
species)` pairs. FINAL held the sums by city, species, the window by city,
`v0` and the filter by species, `v0`. The first shares exactly its grain
with the other two, so it was folded away as a join that adds nothing; but
`city` sat on one survivor and `species` on the other, which then met on
`v0` alone and showed every city with every species
(`split_grain_join_pairs`, 261790). -/
theorem split_grain_join_pairs :
    let pairs : List (Nat × Nat) := [(0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let cities := (pairs.map (·.1)).eraseDups
    let kinds := (pairs.map (·.2)).eraseDups
    (cities.flatMap fun c => kinds.map fun s => (c, s)).length = 8 ∧ pairs.length = 5 := by
  decide

/-- `select coalesce(tree_form, 'none') as form0, city ? city = 'A' as v0
where tag in (tag ? height between 3 and 7)`: the kept trees are `(form0,
v0)` rows. The filter was grouped off them to `v0` alone, so FINAL held no
column to pair it with `form0` on and showed every form with both values;
the filter's node holds `tree_form`, from which `form0` is a row function,
so it keeps the pair (`filter_derives_its_row_keys`, 268726). Forms are
broadleaf 0, linden 1, none 2; `v0` 1 for A, 0 for NULL. -/
theorem filter_derives_its_row_keys :
    let rows : List (Nat × Nat) := [(0, 1), (0, 0), (2, 1), (1, 0), (0, 0)]
    let forms := (rows.map (·.1)).eraseDups
    let values := (rows.map (·.2)).eraseDups
    rows.eraseDups.length = 4 ∧
      (forms.flatMap fun f => values.map fun v => (f, v)).length = 6 := by
  decide

/-- `select city, species, tree_form, (max(height) by tree_form) ? tree_form
in (<set>) as v0, min(height) by tree_form as v1, sum(height) as v2 where
not (tree_form in (tree_form ? max(height) by city is null))`: five `(city,
species)` rows. FINAL looked `tree_form` up off the WHERE set's stream, whose
rows its gate kept: no city's maximum is NULL, so it held none and the join
dropped every row. A filter supplies its own value, never a row column it
passes through (`lookup_reads_ungated_rows`, 244567; 177741, 219659).
Species are Acer 0, Quercus 1, Ulmus 2, Tilia 3. -/
theorem lookup_reads_ungated_rows :
    let rows : List (Nat × Nat) := [(0, 0), (0, 1), (0, 2), (1, 0), (1, 3)]
    let forms : List (Nat × Nat) := [(0, 0), (1, 0), (2, 2), (3, 1)]
    let gated : List (Nat × Nat) := forms.filter fun _ => false
    let look (t : List (Nat × Nat)) := rows.flatMap fun r =>
      (t.filter (·.1 == r.2)).map fun f => (r.1, r.2, f.2)
    (look forms).length = 5 ∧ look gated = [] := by
  decide

/-- `select city, case when tree_form = 'broadleaf' then 'b' else 'o' end as
fc, city ? avg(height) by city < 5 or city in ('B') as v0, max(count(tree_id))
over (partition by fc, v0) as v2`: the `(city, fc)` pairs are (A, b), (A, o)
and (B, b), and `v0` is the city. FINAL met the counts by `(city, fc)` with
the window by `(fc, v0)` on `fc` alone and showed (A, b) beside B's window
too; the counts look the filter up on `city`, its every key, so they pair
on `(fc, v0)` (`pair_on_looked_up_value`, 110957). Cities and forms are A 0,
B 1, b 0, o 1. -/
theorem pair_on_looked_up_value :
    let pairs : List (Nat × Nat) := [(0, 0), (0, 1), (1, 0)]
    let window := pairs.map fun p => (p.2, p.1)
    let onFc := pairs.flatMap fun p => (window.filter (·.1 == p.2)).map fun w => (p.1, p.2, w.2)
    let onBoth := pairs.flatMap fun p =>
      (window.filter fun w => w.1 == p.2 && w.2 == p.1).map fun w => (p.1, p.2, w.2)
    onFc.length = 5 ∧ onBoth.length = 3 := by
  decide

/-- `select city, tag, city ? count(tree_id) by city > 3 as v0,
avg(count(tree_id) by city, tag, v0) over (partition by tag, v0) as v1 where
species in ('Acer', 'Ulmus')`: the kept trees hold (A, x), (A, y) and (B, y).
`city` and `tag` are properties of the tree, shown without `tree_id`, so the
pair is the key of the select's rows; the cover counted only declared keys,
took each column off its own group and FINAL showed (B, x) too
(`shown_properties_key_one_tuple`, 287310). Cities and tags are A 0, B 1,
x 0, y 1. -/
theorem shown_properties_key_one_tuple :
    let rows : List (Nat × Nat) := [(0, 0), (0, 0), (0, 1), (1, 1)]
    let cities := (rows.map (·.1)).eraseDups
    let tags := (rows.map (·.2)).eraseDups
    rows.eraseDups.length = 3 ∧
      (cities.flatMap fun c => tags.map fun t => (c, t)).length = 4 := by
  decide

end Trilogy.Filters
