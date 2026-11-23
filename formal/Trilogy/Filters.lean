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
a key regrafted beside a paired one keeps it (`regraft_keeps_paired_key`);
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
(`regraft_keeps_paired_key`, 112517). Trees of A are `(fc, tag)`. -/
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

end Trilogy.Filters
