import Trilogy.Agg
import Trilogy.Rows
import Trilogy.Windows

/-!
# An unspecified aggregate is at the raw select grain

An aggregate either names its grain (`sum(y) by g`) or names none (`sum(y)`).
One that names none is evaluated at the grain of the select it appears in, the
raw grain the select's own keys give, exactly as if it were written `by` those
keys. That holds wherever it appears: projected, in a HAVING or WHERE, nested
in a scalar expression, behind a name (`auto s <- sum(y)`), or in a window's
ORDER BY. A window does not give it another grain: neither its first argument
nor the tuple it ranks.

- `bare_is_select_grain`: an unspecified aggregate is the aggregate `by` the
  select grain. Its value is a function of the select grain
  (`bare_on_grain`), and so is any scalar expression over it
  (`bare_expression_on_grain`), which is what makes a window ordered by it a
  value per select-grain tuple.
- `bare_empty_grain_is_total`: in a select with no keys (only aggregates
  projected) it is the total over every row, a WHERE's window included, so a
  rank ordered by it ties everything; the per-argument rank is written
  `sum(y) by <argument>`.
- The select grain read here is the raw one: a key computed from a window
  ordered by such an aggregate (a `top_x_by_metric` bucket) is read as that
  window's own keys, since the aggregate cannot be grouped by a value
  computed from it (`Windows.regroup_by_value_exact` re-sums the buckets).
- `specified_ignores_select`: an aggregate `by` fixed keys is the same in any
  select, so a window ordered by one is a value of those keys, a value a
  select can group by.
- `bare_within_partition`: when the select grain fixes a window's partition,
  the bare aggregate reads only that partition's rows.
- An aggregate `by` fixed keys in a select (`select g, sum(y) by id as s`)
  is a value of its keys (`fixed_on_keys`), and the select's grain is the
  projected tuple, the value included. A bare aggregate beside it counts the
  rows of each `(g, s)` tuple, the re-sum of the key groups with that value
  (`bare_beside_fixed_value`), and each tuple is one row. Its keys refine the
  tuple (`keys_refine_value`) but do not stand in for it: grouping by them
  splits a tuple into one row per key (`by_keys_split_tuple`) and repeats it
  (`fixed_value_rows_distinct`).
- An aggregate over that value, or over any scalar of it (`sum(s + 0)`,
  `sum(t)` with `t <- s`), reads it once per distinct key in each select
  tuple (`ofValue`), since a scalar of it is still a value of its keys
  (`scalar_of_fixed_on_keys`) and a repeated row does not change it
  (`of_value_repeat_invariant`); read per row it double counts
  (`fixed_value_per_row_double_counts`). A rowset member holding the value
  is a column of the rowset's distinct rows and a key of an outer select
  (`rowset_member_dropped_totals`).
- Two pins Trilogy applied to a window's order aggregate give another value:
  its first argument alone (`first_argument_pin_misreads`), and the tuple
  the window ranks (`window_tuple_pin_misreads`), for a select at a finer
  grain than that tuple.
- A membership's member is read at its own `by` too: built at the select
  grain, `count(id) by species in (1)` gated each city on its own count
  (`membership_pin_misreads`).
- A property selected beside its key comes with the key: read off a group
  that holds it without the key (`max(x) by city, tree_form` beside `species`),
  it pairs each key with every value its other columns share
  (`property_without_its_key_misjoins`); joined on the key's own value it is
  exact (`property_with_its_key_exact`).
-/

namespace Trilogy.SelectGrain

open Trilogy

variable {α : Type}

/-- A tuple of key values: the select grain, or an aggregate's `by`. -/
abbrev Key := List Int

/-- The grain an aggregate names: none, or `by` a key. -/
inductive Scope (α : Type) where
  | bare
  | by_ (key : α → Key)

/-- The keys an aggregate is evaluated per, in a select whose grain is
`grain`. -/
def Scope.keys (grain : α → Key) : Scope α → (α → Key)
  | .bare => grain
  | .by_ key => key

/-- The sum of `v` over the rows sharing `r`'s key. -/
def sumPer (key : α → Key) (v : α → Int) (rows : List α) (r : α) : Int :=
  sumBy v (slice key (key r) rows)

/-- An aggregate on row `r` of a select at grain `grain`. -/
def eval (s : Scope α) (grain : α → Key) (v : α → Int) (rows : List α) (r : α) :
    Int :=
  sumPer (s.keys grain) v rows r

/-- **An unspecified aggregate is the aggregate `by` the select grain.** -/
theorem bare_is_select_grain (grain : α → Key) (v : α → Int) (rows : List α) (r : α) :
    eval .bare grain v rows r = eval (.by_ grain) grain v rows r := rfl

/-- Rows sharing the select-grain key read the same unspecified aggregate. -/
theorem bare_on_grain (grain : α → Key) (v : α → Int) (rows : List α) (r₁ r₂ : α)
    (h : grain r₁ = grain r₂) :
    eval .bare grain v rows r₁ = eval .bare grain v rows r₂ := by
  simp only [eval, Scope.keys, sumPer, h]

/-- A scalar over unspecified aggregates (`sum(y) / count(id)`, a CASE over
one, a name for one) is a function of the select grain: a window ordered by
it orders the select's tuples. -/
theorem bare_expression_on_grain {β : Type} (grain : α → Key) (v w : α → Int)
    (rows : List α) (F : Int → Int → β) (r₁ r₂ : α) (h : grain r₁ = grain r₂) :
    F (eval .bare grain v rows r₁) (eval .bare grain w rows r₁) =
      F (eval .bare grain v rows r₂) (eval .bare grain w rows r₂) := by
  rw [bare_on_grain grain v rows r₁ r₂ h, bare_on_grain grain w rows r₁ r₂ h]

/-- In a select with no keys the unspecified aggregate is the total. -/
theorem bare_empty_grain_is_total (v : α → Int) (rows : List α) (r : α) :
    eval .bare (fun _ => []) v rows r = sumBy v rows := by
  simp only [eval, Scope.keys, sumPer, slice, decide_true]
  rw [List.filter_eq_self.mpr (by simp)]

/-- **An aggregate `by` fixed keys is the same in every select.** -/
theorem specified_ignores_select (key grain grain' : α → Key) (v : α → Int)
    (rows : List α) (r : α) :
    eval (.by_ key) grain v rows r = eval (.by_ key) grain' v rows r := rfl

/-- When the select grain fixes the partition `p`, the unspecified aggregate
on `r` reads only the rows of `r`'s partition. -/
theorem bare_within_partition (grain : α → Key) (p : α → Int) (v : α → Int)
    (rows : List α) (r : α) (hp : ∀ s, grain s = grain r → p s = p r) :
    eval .bare grain v rows r =
      sumPer grain v (rows.filter fun s => decide (p s = p r)) r := by
  simp only [eval, Scope.keys, sumPer, slice, List.filter_filter]
  congr 2
  funext s
  by_cases h : grain s = grain r
  · simp [h, hp s h]
  · simp [h]

/-- Rows `(p, g, y)` and the select `select p, g, rank(g) over (partition by
p order by sum(y))`, grain `(p, g)`. Pinned to its first argument `g`, `sum(y)`
reads both partitions: `a` and `b` total 30 and tie inside `Q`. At the select
grain `b` leads `Q`, 25 to 20. -/
theorem first_argument_pin_misreads :
    let rows : List (Int × Int × Int) := [(0, 0, 10), (1, 0, 20), (0, 1, 5), (1, 1, 25)]
    let y : Int × Int × Int → Int := fun r => r.2.2
    let grain : Int × Int × Int → Key := fun r => [r.1, r.2.1]
    let first : Int × Int × Int → Key := fun r => [r.2.1]
    eval (.by_ first) grain y rows (1, 0, 20) = eval (.by_ first) grain y rows (1, 1, 25) ∧
      eval .bare grain y rows (1, 0, 20) < eval .bare grain y rows (1, 1, 25) := by
  decide

/-- The same rows and `select g, p, rank(g) over (order by sum(y))`, grain
`(g, p)`. Pinned to the tuple the window ranks, `g`, the rank's `sum(y)` is 30
on both of `a`'s rows; at the select grain it is 10 under `P` and 20 under
`Q`, the value projected beside it as `sum(y)`. -/
theorem window_tuple_pin_misreads :
    let rows : List (Int × Int × Int) := [(0, 0, 10), (1, 0, 20), (0, 1, 5), (1, 1, 25)]
    let y : Int × Int × Int → Int := fun r => r.2.2
    let grain : Int × Int × Int → Key := fun r => [r.2.1, r.1]
    let tuple : Int × Int × Int → Key := fun r => [r.2.1]
    eval (.by_ tuple) grain y rows (0, 0, 10) = 30 ∧
      eval .bare grain y rows (0, 0, 10) = 10 ∧ eval .bare grain y rows (1, 0, 20) = 20 := by
  decide

/-- A membership reads its member at the member's own `by`, as a comparison
does. Rows `(city, species)` and `select city, count(id) where count(id) by
species in (1)`: species 1 and 2 hold one row each, so each city keeps one
row. Read at the select grain the member is each city's count, 3 and 2, which
`(1)` does not hold, and every row is dropped: the IN form built its member
inline, losing the `by`. -/
theorem membership_pin_misreads :
    let rows : List (Int × Int) := [(0, 0), (0, 0), (0, 1), (1, 0), (1, 2)]
    let one : Int × Int → Int := fun _ => 1
    let grain : Int × Int → Key := fun r => [r.1]
    let species : Int × Int → Key := fun r => [r.2]
    (rows.filter (fun r => eval (.by_ species) grain one rows r ∈ [1])).map (·.1) =
        [0, 1] ∧
      (rows.filter (fun r => eval .bare grain one rows r ∈ [1])).isEmpty := by
  decide

/-- A property selected beside its key is read off the key's own rows: `select
city, species, tree_form, max(x) by city, tree_form`, `tree_form = form
species`, pairs each `(city, species)` with the gate rows of its own form. -/
theorem property_with_its_key_exact (keys : List (Int × Int)) (form : Int → Int)
    (gate : List (Int × Int)) :
    ∀ t ∈ keys.flatMap (fun r =>
        (gate.filter fun p => p.1 == r.1 && p.2 == form r.2).map fun p => (r.1, r.2, p.2)),
      t.2.2 = form t.2.1 := by
  intro t ht
  simp only [List.mem_flatMap, List.mem_map, List.mem_filter, Bool.and_eq_true,
    beq_iff_eq] at ht
  obtain ⟨r, -, p, ⟨-, -, h⟩, rfl⟩ := ht
  exact h

/-- City 0 holds species 1 and 2, of forms 1 and 0. The group `(city, form)`
holds the form without the species; read from it and joined to the `(city,
species)` rows on `city` alone, each species is paired with both forms: four
rows for two, two of them with the other species' form. -/
theorem property_without_its_key_misjoins :
    let keys : List (Int × Int) := [(0, 1), (0, 2)]
    let form : Int → Int := fun s => s % 2
    let gate := keys.map fun r => (r.1, form r.2)
    let joined := keys.flatMap fun r =>
      (gate.filter fun p => p.1 == r.1).map fun p => (r.1, r.2, p.2)
    joined.length = 4 ∧ (joined.filter fun t => t.2.2 != form t.2.1).length = 2 := by
  decide

/-! ## An aggregate `by` fixed keys in a select -/

/-- The tuple `select dims, s` projects, `s` an aggregate `by key`. -/
def fixedTuple (dims key : α → Key) (v : α → Int) (rows : List α) (r : α) : Key :=
  dims r ++ [eval (.by_ key) dims v rows r]

/-- An aggregate `by key` is a value of `key`. -/
theorem fixed_on_keys (key grain : α → Key) (v : α → Int) (rows : List α) (r₁ r₂ : α)
    (h : key r₁ = key r₂) :
    eval (.by_ key) grain v rows r₁ = eval (.by_ key) grain v rows r₂ := by
  simp only [eval, Scope.keys, sumPer, h]

/-- Rows sharing the dimensions and the aggregate's keys share the tuple. -/
theorem keys_refine_value (dims key : α → Key) (v : α → Int) (rows : List α) (r₁ r₂ : α)
    (hd : dims r₁ = dims r₂) (hk : key r₁ = key r₂) :
    fixedTuple dims key v rows r₁ = fixedTuple dims key v rows r₂ := by
  simp only [fixedTuple, hd, fixed_on_keys key dims v rows r₁ r₂ hk]

/-- **A bare count beside a fixed aggregate is per its tuple**: the re-sum,
over the `(dims, key)` tuples with that value, of their groups. -/
theorem bare_beside_fixed_value [DecidableEq α] (dims key : α → Key) (v : α → Int)
    (rows : List α) (ks : List (Key × Key)) (hnd : ks.Nodup)
    (hcov : ∀ r ∈ rows, (dims r, key r) ∈ ks) (t : Key) :
    Windows.countBy (fixedTuple dims key v rows) rows t =
      ((ks.filter fun u => u.1 ++ [sumBy v (slice key u.2 rows)] = t).map
        (Windows.countBy (fun r => (dims r, key r)) rows)).sum :=
  Windows.regroup_by_value_exact (fun r => (dims r, key r))
    (fun u => u.1 ++ [sumBy v (slice key u.2 rows)]) ks hnd rows hcov t

/-- Rows `(id, g, y)` and `select g, sum(y) by id as s, count(id)`. Ids 2 and
3 share `(c, 0)`, so its count is 2; grouped by `(g, id)`, the aggregate's
keys in its place, each counts 1. -/
theorem by_keys_split_tuple :
    let rows : List (Int × Int × Int) := [(1, 0, 5), (2, 0, 0), (3, 0, 0)]
    let y : Int × Int × Int → Int := fun r => r.2.2
    let id : Int × Int × Int → Key := fun r => [r.1]
    let g : Int × Int × Int → Key := fun r => [r.2.1]
    let one : Int × Int × Int → Int := fun _ => 1
    eval .bare (fixedTuple g id y rows) one rows (2, 0, 0) = 2 ∧
      eval .bare (fun r => g r ++ id r) one rows (2, 0, 0) = 1 := by
  decide

/-- The same select has two rows, `(c, 5)` and `(c, 0)`; one row per id
repeats `(c, 0)`. -/
theorem fixed_value_rows_distinct :
    let rows : List (Int × Int × Int) := [(1, 0, 5), (2, 0, 0), (3, 0, 0)]
    let y : Int × Int × Int → Int := fun r => r.2.2
    let id : Int × Int × Int → Key := fun r => [r.1]
    let g : Int × Int × Int → Key := fun r => [r.2.1]
    (dedup (rows.map (fixedTuple g id y rows))).length = 2 ∧
      (rows.map (fixedTuple g id y rows)).length = 3 := by
  decide

/-! ## An aggregate of a fixed value -/

/-- An aggregate over a value of `key` (`sum(s)`, `sum(s + 0)`, `sum(t)` with
`t <- s`) in a select at grain `grain`: the value once per distinct key among
the rows of the select-grain tuple. -/
def ofValue (grain key : α → Key) (w : Key → Int) (rows : List α) (r : α) : Int :=
  ((dedup ((slice grain (grain r) rows).map key)).map w).sum

/-- A scalar of a fixed aggregate is still a value of its keys, so an
aggregate over it is read at those keys like the aggregate itself. -/
theorem scalar_of_fixed_on_keys {β : Type} (key grain : α → Key) (v : α → Int)
    (f : Int → β) (rows : List α) (r₁ r₂ : α) (h : key r₁ = key r₂) :
    f (eval (.by_ key) grain v rows r₁) = f (eval (.by_ key) grain v rows r₂) := by
  rw [fixed_on_keys key grain v rows r₁ r₂ h]

/-- **Row multiplicity does not reach it**: a repeated row (a second id with
the same grain and key) leaves the aggregate of the value unchanged. -/
theorem of_value_repeat_invariant (grain key : α → Key) (w : Key → Int)
    (rows : List α) (r₀ r : α) (h : r₀ ∈ rows) :
    ofValue grain key w (r₀ :: rows) r = ofValue grain key w rows r := by
  unfold ofValue slice
  by_cases hg : grain r₀ = grain r
  · have hm : key r₀ ∈ dedup ((rows.filter fun x => decide (grain x = grain r)).map key) :=
      (mem_dedup _ _).2 (List.mem_map.2 ⟨r₀, List.mem_filter.2 ⟨h, by simp [hg]⟩, rfl⟩)
    simp [hg, dedup, hm]
  · simp [hg]

/-- Rows `(id, g, h, y)` and `select g, h, sum(s)`, `s <- sum(y) by g`. Ids 2
and 3 share `(g, h) = (0, 0)` and `g = 0` has `s = 5`, so the sum is 5; read
once per row, as `sum(s + 0)` and `sum(t)` with `t <- s` were, it is 10. -/
theorem fixed_value_per_row_double_counts :
    let rows : List (Int × Int × Int × Int) := [(1, 0, 1, 1), (2, 0, 0, 2), (3, 0, 0, 2)]
    let y : Int × Int × Int × Int → Int := fun r => r.2.2.2
    let g : Int × Int × Int × Int → Key := fun r => [r.2.1]
    let gh : Int × Int × Int × Int → Key := fun r => [r.2.1, r.2.2.1]
    let s : Key → Int := fun k => sumBy y (slice g k rows)
    ofValue gh g s rows (2, 0, 0, 2) = 5 ∧
      sumBy (fun r => s (g r)) (slice gh (gh (2, 0, 0, 2)) rows) = 10 := by
  decide

/-- A rowset of `select sum(y) by id as v` holds the distinct values, so the
outer `select r.v, count(r.v)` counts each once; with `r.v` dropped from the
outer grain, as a rowset member of an aggregate was, the count is the total
over the rowset, 2 on every row. -/
theorem rowset_member_dropped_totals :
    let rows : List Int := dedup [1, 2, 2]
    let v : Int → Key := fun x => [x]
    let one : Int → Int := fun _ => 1
    eval .bare v one rows 1 = 1 ∧ eval .bare (fun _ => []) one rows 1 = 2 := by
  decide

/-! ## A select's keys are the keys of the rows its WHERE keeps -/

/-- The key rows of `select k, ... where p`: the distinct keys of the rows `p`
keeps, whatever grain the aggregates beside them are read at. -/
def keptKeys (k : α → Key) (p : α → Bool) (rows : List α) : List Key :=
  dedup ((rows.filter p).map k)

/-- **Each key row holds the WHERE on one of its rows.** -/
theorem kept_key_holds (k : α → Key) (p : α → Bool) (rows : List α) (t : Key)
    (h : t ∈ keptKeys k p rows) : ∃ r ∈ rows, p r = true ∧ k r = t := by
  unfold keptKeys at h
  rw [mem_dedup] at h
  obtain ⟨r, hr, rfl⟩ := List.mem_map.1 h
  obtain ⟨hr, hp⟩ := List.mem_filter.1 hr
  exact ⟨r, hr, hp, rfl⟩

/-- **A regroup of the kept groups holds only kept keys**
(`regrouped_gate_is_applied`): grouping the WHERE's kept `(city, species)`
groups again by a function of their key (`tree_form`) gives the keys of the
kept rows, so a FINAL re-applying the WHERE there changes nothing. -/
theorem regrouped_gate_is_applied (k : α → Key) (f : Key → Key) (p : α → Bool)
    (rows : List α) (t : Key) :
    t ∈ keptKeys (fun r => f (k r)) p rows ↔ ∃ u ∈ keptKeys k p rows, f u = t := by
  unfold keptKeys
  simp only [mem_dedup, List.mem_map, List.mem_filter]
  constructor
  · rintro ⟨r, hr, rfl⟩
    exact ⟨k r, ⟨r, hr, rfl⟩, rfl⟩
  · rintro ⟨u, ⟨r, hr, rfl⟩, rfl⟩
    exact ⟨r, hr, rfl⟩

/-- Membership in a literal list: identity matching against each member. -/
def memberOf (v : Int) (cs : List Int) : Bool := cs.contains v

/-- **A literal membership is a disjunction of comparisons**: `agg(x) by g in
(7, 1)` keeps exactly the rows `agg(x) by g = 7 or agg(x) by g = 1` keeps, so it
is placed like the comparison. -/
theorem literal_membership_is_a_comparison (v c : Int) (cs : List Int) :
    memberOf v (c :: cs) = (v == c || memberOf v cs) := by
  simp only [memberOf, List.contains_cons]

/-- A count on a key with no rows reads 0, so a literal membership decides it
by whether 0 is a member: `count(id) by k in (5, 3)` never holds there. -/
theorem count_membership_on_absent [DecidableEq κ] (k : α → κ) (rows : List α)
    (t : κ) (cs : List Int) (h : ∀ r ∈ rows, k r ≠ t) :
    memberOf ((slice k t rows).length : Int) cs = memberOf 0 cs := by
  have : slice k t rows = [] := by
    unfold slice
    exact List.filter_eq_nil_iff.2 fun r hr => by simpa using h r hr
  simp [this]

/-- **An aggregate `by` a grain the select grain determines is a value of the
select grain**: `sum(x) by city, tree_form` beside `city, species`, with
`tree_form` a property of `species`, is one value per select row, so a HAVING
over it never changes the select grain. -/
theorem determined_by_on_grain (grain key : α → Key) (f : Key → Key)
    (hk : ∀ r, key r = f (grain r)) (v : α → Int) (rows : List α) (r₁ r₂ : α)
    (h : grain r₁ = grain r₂) :
    eval (.by_ key) grain v rows r₁ = eval (.by_ key) grain v rows r₂ :=
  fixed_on_keys key grain v rows r₁ r₂ (by rw [hk, hk, h])

/-- Rows `(city, species, x)` and `select species, min(x) by * where max(x) by
city, species in (7, 1)`: only species 1 in city 1 has max 7, so the key rows
are `[1]`. Read off the gate's own group, which holds every `(city, species)`,
the WHERE dropped out of the key rows and all three species came back. -/
theorem literal_membership_gate_dropped :
    let rows : List (Int × Int × Int) := [(0, 1, 3), (0, 2, 5), (1, 1, 7), (1, 3, 2)]
    let gate : Int × Int × Int → Bool := fun r =>
      memberOf (((rows.filter fun q => q.1 == r.1 && q.2.1 == r.2.1).map (·.2.2)).foldl max 0) [7, 1]
    let species : Int × Int × Int → Key := fun r => [r.2.1]
    keptKeys species gate rows = [[1]] ∧
      keptKeys species (fun _ => true) rows = [[2], [1], [3]] := by
  decide

/-- Rows `(city, species)` and `select species, min(x) by * where
count_distinct(tag) by species = 1 and city = 1`: species 2 and 3 pass the gate
but only species 3 grows in city 1. Read off the gate's group, which reads
every row, the row atom never reached the key rows
(`gate_group_skips_row_atoms`). -/
theorem gate_group_skips_row_atoms :
    let rows : List (Int × Int) := [(0, 1), (0, 2), (1, 1), (1, 3)]
    let gate : Int × Int → Bool := fun r => r.2 != 1
    let species : Int × Int → Key := fun r => [r.2]
    keptKeys species (fun r => gate r && r.1 == 1) rows = [[3]] ∧
      keptKeys species gate rows = [[2], [3]] := by
  decide

/-- City 0 holds species 1 and 2, of forms 1 and 0, and `select city, species,
max(x) by city, tree_form, count(id)` does not select the form. The group
`(city, form)` is joined to the `(city, species)` counts on `city` alone: four
rows for two. Joined on the form each species determines, it is two, each with
its own form's value (`property_with_its_key_exact`). -/
theorem by_property_without_its_key_misjoins :
    let keys : List (Int × Int) := [(0, 1), (0, 2)]
    let form : Int → Int := fun s => s % 2
    let gate := keys.map fun r => (r.1, form r.2)
    (keys.flatMap fun r => gate.filter fun p => p.1 == r.1).length = 4 ∧
      (keys.flatMap fun r =>
        gate.filter fun p => p.1 == r.1 && p.2 == form r.2).length = 2 := by
  decide

/-! ## An atom drops whole groups only when its grain is the group's

A WHERE atom over a row column and a gate (`tree_form = 'linden' or max(x) by
city, species = 3`) is decided per `(city, species)`. Beside a count by
`species`, it drops whole species only when `species` determines everything it
reads; otherwise it filters the rows the count reads, never the counts. -/

/-- **An atom decided per group key drops whole groups**: filtering the rows
before grouping by `g` is filtering the groups, when the atom is a function of
the group key. -/
theorem atom_on_group_key_drops_groups [DecidableEq κ] (g : α → κ) (q : κ → Bool)
    (rows : List α) (t : κ) :
    slice g t (rows.filter fun r => q (g r)) =
      if q t then slice g t rows else [] := by
  unfold slice
  rw [List.filter_filter]
  by_cases hq : q t
  · simp only [hq, ↓reduceIte]
    congr 1
    funext r
    by_cases h : g r = t
    · simp [h, hq]
    · simp [h]
  · simp only [hq, Bool.false_eq_true, ↓reduceIte]
    apply List.filter_eq_nil_iff.2
    intro r _
    by_cases h : g r = t
    · simp [h, hq]
    · simp [h]

/-- Rows `(city, species)` and `select species, count(id) where (form =
linden or max(x) by city, species = 3)`: species 1 passes in city 0 only, so
it counts one row. Applied after the count, as a species-keyed semijoin once
`form` was peeled off the row stream, the atom kept species 1 whole: two
(`mixed_atom_after_count_misreads`). -/
theorem mixed_atom_after_count_misreads :
    let rows : List (Int × Int) := [(0, 1), (1, 1), (1, 2)]
    let atom : Int × Int → Bool := fun r => r.1 == 0 && r.2 == 1
    let species : Int × Int → Int := fun r => r.2
    (slice species 1 (rows.filter atom)).length = 1 ∧
      (if rows.any fun r => atom r && species r == 1 then
        (slice species 1 rows).length else 0) = 2 := by
  decide

/-! ## A join pair is implied only on the side that holds it

Joining a right side keyed by `species` onto two left sides, the counts
`(city, species)` and a gate `(city, form)` paired on `city` alone, the pair
on `form` is implied by `species` only for the side that pairs `species`:
the gate's `form` is free. -/

/-- Counts `(city, species)` `[(0, 1), (0, 2)]`, a gate `(city, form)` with
both forms in city 0, and species `s` of form `s % 2`. Dropping the gate's
`form` pair, as implied by `species`, pairs each species with both forms:
four rows for two (`pair_implied_off_its_side_misjoins`). -/
theorem pair_implied_off_its_side_misjoins :
    let counts : List (Int × Int) := [(0, 1), (0, 2)]
    let gate : List (Int × Int) := [(0, 0), (0, 1)]
    let form : Int → Int := fun s => s % 2
    let joined := counts.flatMap fun c => (gate.filter fun g => g.1 == c.1).map (c, ·)
    joined.length = 4 ∧
      (joined.filter fun p => p.2.2 == form p.1.2).length = 2 := by
  decide

/-! ## Keys split over two filtered branches pair only kept tuples

`select city, tree_form, count(id) by tree_form, count(id) by city where
min(x) by city, species != 7`: each count's group keys are kept keys of its
own column, but the select's rows are the kept `(city, tree_form)` tuples.
Paired through a row stream the WHERE never filtered, every pair of kept
columns comes back, the dropped tuples among them. -/

/-- **A kept tuple's columns are kept columns**: each key row's projections are
key rows of the projections, so pairing two filtered branches never loses a
kept tuple, only adds dropped ones. -/
theorem kept_pair_in_product (a b : α → Key) (p : α → Bool) (rows : List α)
    (t : Key × Key) (h : t ∈ dedup ((rows.filter p).map fun r => (a r, b r))) :
    t.1 ∈ keptKeys a p rows ∧ t.2 ∈ keptKeys b p rows := by
  rw [mem_dedup] at h
  obtain ⟨r, hr, rfl⟩ := List.mem_map.1 h
  exact ⟨by unfold keptKeys; rw [mem_dedup]; exact List.mem_map.2 ⟨r, hr, rfl⟩,
    by unfold keptKeys; rw [mem_dedup]; exact List.mem_map.2 ⟨r, hr, rfl⟩⟩

/-- Rows `(city, form)` `[(0, 0), (1, 0), (1, 1)]` with `(1, 0)` dropped by the
WHERE: cities 0 and 1 and forms 0 and 1 are all kept, but only `(0, 0)` and
`(1, 1)` are kept tuples. Paired off the unfiltered rows, `(1, 0)` returned
(`split_keys_pair_unkept`). -/
theorem split_keys_pair_unkept :
    let rows : List (Int × Int) := [(0, 0), (1, 0), (1, 1)]
    let p : Int × Int → Bool := fun r => r != (1, 0)
    let city : Int × Int → Key := fun r => [r.1]
    let form : Int × Int → Key := fun r => [r.2]
    let kept := dedup ((rows.filter p).map fun r => (city r, form r))
    let paired := dedup ((rows.filter fun r =>
      (keptKeys city p rows).contains (city r) &&
        (keptKeys form p rows).contains (form r)).map fun r => (city r, form r))
    kept.length = 2 ∧ paired.length = 3 := by
  decide

/-! ## A `by` value of keys the select determines is no grain key

`select tree_id, sum(x) by city as v` with `tree_id` binding `city`: `v` is one
value per `tree_id`, so the select grain stays `tree_id`. Kept as a grain key,
it was joined on as a column, and a row's own `x` stood in for it. -/

/-- **A `by` value of a determined key is a value of the select grain**
(`by_value_is_no_grain_key`): rows sharing `tree_id` share `city`, hence `v`. -/
theorem by_value_is_no_grain_key (grain key : α → Key) (f : Key → Key)
    (hk : ∀ r, key r = f (grain r)) (v : α → Int) (rows : List α) (r₁ r₂ : α)
    (h : grain r₁ = grain r₂) :
    eval (.by_ key) grain v rows r₁ = eval (.by_ key) grain v rows r₂ :=
  determined_by_on_grain grain key f hk v rows r₁ r₂ h

/-- Rows `(id, city, x)`: `sum(x) by city` is 8 on city 0's rows. Read at the
`id` grain the value was taken for `x` itself, and joined on, only the row
whose `x` equals its city's total survived (`by_value_as_key_misjoins`). -/
theorem by_value_as_key_misjoins :
    let rows : List (Int × Int × Int) := [(1, 0, 3), (2, 0, 5), (3, 1, 2)]
    let total : Int → Int := fun c => sumBy (·.2.2) (rows.filter (·.2.1 == c))
    (rows.filter fun r => r.2.2 == total r.2.1).length = 1 ∧ rows.length = 3 := by
  decide

/-- **A conjunction of a condition with itself is the condition**: `city = 'B'
and city = 'B'` keeps what `city = 'B'` keeps, so the planner folds a
condition into itself to the same filter (`conjunction_idempotent`). -/
theorem conjunction_idempotent (p : α → Bool) (rows : List α) :
    rows.filter (fun r => p r && p r) = rows.filter p := by
  simp

/-! ## A WHERE filters the rows every aggregate reads

`select city, max(x ? (city = 'A' or sum(x) by * is null)) where (min(x) by *
= 6 or city = 'B')`: the planner hosted the WHERE only on the `sum(x) by *`
group the filter's condition reads, and the select's own `max` aggregated the
raw rows beside that value. A value joined in filters nothing it is not
computed from. -/

/-- **An aggregate of the kept rows reads only kept rows**: grouping after the
WHERE equals filtering each group (`where_filters_group_rows`). -/
theorem where_filters_group_rows (p : α → Bool) (g : α → Int) (k : Int)
    (rows : List α) :
    (rows.filter p).filter (fun r => g r == k) =
      (rows.filter fun r => g r == k).filter p := by
  simp only [List.filter_filter, Bool.and_comm]

/-- Rows `(city, x)` `[(0, 3), (0, 5), (1, 7), (1, 2)]`, WHERE `city = 1`: city
0 has no kept rows, so no group, but the unfiltered stream beside the gate's
host still counted 2 of its rows (`gate_host_skips_select_rows`). -/
theorem gate_host_skips_select_rows :
    let rows : List (Int × Int) := [(0, 3), (0, 5), (1, 7), (1, 2)]
    ((rows.filter (·.1 == 1)).filter (·.1 == 0)).length = 0 ∧
      (rows.filter (·.1 == 0)).length = 2 := by
  decide

/-! ## A value is paired by its grain, never by itself

Two groups both holding `max(x) by form` beside `form` were joined on the
value with plain `=` as well as on `form`. The value is determined by `form`,
so the grain's pair decides it; its own `=` is unknown on a group whose value
is NULL, and the group was dropped. -/

/-- SQL `=`: unknown (not true) when either side is NULL. -/
def sqlEq (a b : Option Int) : Bool :=
  match a, b with
  | some x, some y => x == y
  | _, _ => false

/-- **Equal grains give equal values** (`value_paired_by_grain`). -/
theorem value_paired_by_grain (f : Option Int → Option Int) (k₁ k₂ : Option Int)
    (h : k₁ = k₂) : f k₁ = f k₂ := by
  rw [h]

/-- The Ulmus group: `form` NULL, `max(x) by form` NULL. Paired null-safely on
`form` it matches itself; adding the value's `=` drops it
(`value_pair_drops_null_group`). -/
theorem value_pair_drops_null_group :
    let form : Option Int := none
    let v : Option Int := none
    (form == form) = true ∧ sqlEq v v = false := by
  decide

/-! ## A guard conjoined to a disjunction is parenthesized

A grand total is no row when no row was kept, so its HAVING gains `count(*) >
0`. Appended to `a or b` without parentheses it read `a or (b and guard)`. -/

/-- **The guard holds of the whole HAVING** (`guard_conjoins_having`). -/
theorem guard_conjoins_having (h g : Bool) : (h && g) = true ↔ h = true ∧ g = true := by
  simp

/-- `a = true`, `b = false`, no rows kept: `a or (b and guard)` emits the empty
total; `(a or b) and guard` does not (`guard_precedence_emits_empty_total`). -/
theorem guard_precedence_emits_empty_total :
    let a := true
    let b := false
    let guard := false
    (a || (b && guard)) = true ∧ ((a || b) && guard) = false := by
  decide

/-! ## A HAVING `by` the select's rows bind is coarser than the select

`select tree_id, max(x ? c) having count(id) by city > 2` with `tree_id`
binding `city` (or `city as v2`, whose rename finalize writes into the `by`):
the HAVING's value is one per `tree_id`, so promoting it keeps the grain. The
validator read only property-to-key facts and rejected it as growing it. -/

/-- **A HAVING on a determined `by` decides whole grain rows**
(`having_by_bound_key_is_coarser`). -/
theorem having_by_bound_key_is_coarser (grain key : α → Key) (f : Key → Key)
    (hk : ∀ r, key r = f (grain r)) (v : α → Int) (q : Int → Bool)
    (rows : List α) (r₁ r₂ : α) (h : grain r₁ = grain r₂) :
    q (eval (.by_ key) grain v rows r₁) = q (eval (.by_ key) grain v rows r₂) := by
  rw [by_value_is_no_grain_key grain key f hk v rows r₁ r₂ h]

/-! ## A gate's value is read off its own group, never past a host

`select city, species, min(x ? max(x) by * = 6) where avg(x) by * is null and
city = 'B'`: the WHERE is hosted on the `?` total and on the per-key group, and
the per-key group read `avg(x) by *` off the total's host. The WHERE rejected
the total, so that host held no row; joined preserving, its value was padding
NULL, and `is null` held on it. -/

/-- The value a consumer reads off a host of `atom`: the host keeps `t` only
where the atom holds, else pads NULL. -/
def pastHost (atom : Option Int → Bool) (t : Option Int) : Option Int :=
  (([t].filter atom).head?).join

/-- **Past a host, a null-rejecting atom reads as on its own group**
(`gate_past_host_exact_if_null_rejecting`). -/
theorem gate_past_host_exact_if_null_rejecting (atom : Option Int → Bool)
    (t : Option Int) (h : atom none = false) : atom (pastHost atom t) = atom t := by
  unfold pastHost
  cases ht : atom t <;> simp [List.filter, ht, h]

/-- `avg(x) by *` is 4: `is null` rejects it on its own group, and holds on the
padding read past its host (`gate_read_past_its_host_passes_absent`). -/
theorem gate_read_past_its_host_passes_absent :
    let atom : Option Int → Bool := fun v => v.isNone
    atom (some 4) = false ∧ atom (pastHost atom (some 4)) = true := by
  decide

/-! ## A value at a grain the rows determine rides the rows

`select tree_id, species, --sum(x) by city, species as s` read `s` with the rows
(off the summary) and then split it into a bucket at `(city, species)`, which
the rows joined on `species` alone: a species in two cities paired each tree
with both cities' groups. -/

/-- Rows `(id, city, species)`: species 0 in cities 0 and 1. Paired on
`species` alone, each of its two trees meets both `(city, species)` groups
(`value_bucket_pairs_short_of_its_grain`). -/
theorem value_bucket_pairs_short_of_its_grain :
    let rows : List (Int × Int × Int) := [(1, 0, 0), (2, 1, 0)]
    let groups : List (Int × Int) := [(0, 0), (1, 0)]
    (rows.flatMap fun r => groups.filter (·.2 == r.2.2)).length = 4 ∧
      (rows.flatMap fun r => groups.filter (fun g => g == (r.2.1, r.2.2))).length = 2 := by
  decide

/-! ## The select's keys come off one row stream

`select city, species, sum(x) by * where (max(x) by city, form <= 3 or min(x)
by species != 6)`: the FINAL took `city` off the gate's `(city, form)` group and
`species` off the `species` group, and paired them on `form`. Every city met
every species of its form: tuples no row holds. -/

/-- **One stream's tuples are its rows' tuples** (`keys_off_one_stream_exact`). -/
theorem keys_off_one_stream_exact [DecidableEq κ] (a b : α → κ) (rows : List α)
    (t : κ × κ) :
    t ∈ dedup (rows.map fun r => (a r, b r)) ↔ ∃ r ∈ rows, (a r, b r) = t := by
  rw [mem_dedup, List.mem_map]

/-- Rows `(city, species, form)`: city 1 holds species 0 and 2, city 0 species 0
and 1, species 0 and 1 of form 0. Paired on `form` off two groups, `(1, 1)`
appears (`keys_off_two_groups_pair_unheld`). -/
theorem keys_off_two_groups_pair_unheld :
    let rows : List (Int × Int × Int) := [(0, 0, 0), (0, 1, 0), (1, 0, 0), (1, 2, 1)]
    let cities := dedup (rows.map fun r => (r.1, r.2.2))
    let species := dedup (rows.map fun r => (r.2.1, r.2.2))
    let paired := dedup (cities.flatMap fun c =>
      (species.filter (·.2 == c.2)).map fun s => (c.1, s.1))
    (dedup (rows.map fun r => (r.1, r.2.1))).length = 4 ∧ paired.length = 5 := by
  decide

/-! ## A bridge holds the key it was elected for

`select tree_id, species, min(x) by city, form as v1, city as v2 where ...`: the
row stream computing `v2` was elected to bridge `v1` onto the rows on `(city,
form)`, but `form` sat below a dedup it was never carried through, so `v1` met
the rows on `city` alone (`value_bucket_pairs_short_of_its_grain`). A dedup
carries any column its outputs determine without changing its rows. -/

/-- **A determined column adds no row to a dedup** (`dedup_carries_determined`). -/
theorem dedup_carries_determined [DecidableEq κ] [DecidableEq τ] (o : α → κ)
    (f : κ → τ) (rows : List α) (t : κ × τ) :
    t ∈ dedup (rows.map fun r => (o r, f (o r))) ↔
      t.1 ∈ dedup (rows.map o) ∧ t.2 = f t.1 := by
  rw [mem_dedup, mem_dedup, List.mem_map, List.mem_map]
  constructor
  · rintro ⟨r, hr, rfl⟩
    exact ⟨⟨r, hr, rfl⟩, rfl⟩
  · rintro ⟨⟨r, hr, h⟩, h2⟩
    exact ⟨r, hr, Prod.ext h (by simp [h, h2])⟩

/-! ## Each argument of an atom is read off its own group

`select count(id ? max(x) by * != 10), sum(x ? ...) where (sum(x) by * is null
and max(x) by city > 3)`: the `?` row stream read `sum(x) by *` off the select's
total, a host of the WHERE, which kept no row; joined preserving, the total was
padding. `max(x) by city > 3` proves the conjunction rejects a NULL `max`, but
says nothing of the total: the reroute must ask per argument. -/

/-- **An argument the atom rejects NULL in reads as on its own group**
(`gate_arg_past_host_exact_if_rejecting`). -/
theorem gate_arg_past_host_exact_if_rejecting (atom : Option Int → Option Int → Bool)
    (a b : Option Int) (h : atom none b = false) :
    atom (pastHost (fun v => atom v b) a) b = atom a b :=
  gate_past_host_exact_if_null_rejecting (fun v => atom v b) a h

/-- The total is 17, `max(x) by city` 7: on its own group the conjunction is
false, read past its host it holds although its other conjunct rejects NULL
(`conjunct_read_past_its_host_passes_absent`). -/
theorem conjunct_read_past_its_host_passes_absent :
    let atom : Option Int → Option Int → Bool := fun s m =>
      s.isNone && (m.map (fun x => decide (x > 3))).getD false
    atom none none = false ∧ atom (some 17) (some 7) = false ∧
      atom (pastHost (fun v => atom v (some 7)) (some 17)) (some 7) = true := by
  decide

/-! ## A side the FINAL filters carries the gate's keys

`select species, form, max(x) where min(x) by city, form is null`: the FINAL
re-applies the WHERE to the side supplying `form`, but that side was sourced
from the dimension alone, with no `city`, and a second scan carried `city` to
the gate. The dimension's rows met the gate through no tree, padded, and `is
null` held on every species. Read per row, a row meets its own group. -/

/-- Trees `(city, species)` `[(0, 0), (0, 1)]`, forms 0 and 1, the gate `some
3` on `(0, 0)` and NULL on `(0, 1)`: per tree only species 1 passes; a species
read off the dimension meets the gate through no row, and both pass
(`dimension_side_pads_gate`). -/
theorem dimension_side_pads_gate :
    let trees : List (Int × Int) := [(0, 0), (0, 1)]
    let gate : Int × Int → Option Int := fun g => if g == (0, 0) then some 3 else none
    let perRow := trees.filter fun t => (gate (t.1, t.2)).isNone
    let padded := [0, 1].filter fun _ => (none : Option Int).isNone
    perRow.map (·.2) = [1] ∧ padded = [0, 1] := by
  decide

/-! ## A key comes off rows the WHERE filtered

`select city, form, count(id ? (city = 'A' or min(x) by city = 2)), ... where
not (...) and (...)`: the FINAL took `(city, form)` off the `?` filter's row
stream, the most downstream exposer, whose rows only joined beside a host's
per-city value (`gate_host_skips_select_rows`), and returned a group the WHERE
rejected. The aggregate hosting the WHERE exposes the same keys. -/

/-- Rows `(city, form)` `[(0, 0), (0, 1)]` and a WHERE rejecting form 1: the
host's keys are one, the unfiltered stream's two
(`key_off_unfiltered_stream_returns_rejected`). -/
theorem key_off_unfiltered_stream_returns_rejected :
    let rows : List (Int × Int) := [(0, 0), (0, 1)]
    (dedup ((rows.filter (·.2 == 0)).map id)).length = 1 ∧
      (dedup rows).length = 2 := by
  decide

/-! ## A FINAL gate pairs on its whole grain

`select city, species, sum(x) by species, sum(min(x) by city, species, form)
where count_distinct(tag) by city, form < 3`: the FINAL re-applies the gate,
but the one side holding `form` was folded into a sibling rendering `city,
species`, so the gate's group met the rows on `city` alone, and every city
repeated once per kept form. -/

/-- Rows `(city, species, form)`: city 0 holds species 0 of form 0 and species
1 of form 1, and the gate keeps both `(city, form)` groups of city 0. Paired on
its whole grain each row meets one gate row; on `city` alone, two
(`gate_paired_short_of_its_grain`). -/
theorem gate_paired_short_of_its_grain :
    let rows : List (Int × Int × Int) := [(0, 0, 0), (0, 1, 1)]
    let gate : List (Int × Int) := [(0, 0), (0, 1)]
    (rows.flatMap fun r => gate.filter (· == (r.1, r.2.2))).length = 2 ∧
      (rows.flatMap fun r => gate.filter (·.1 == r.1)).length = 4 := by
  decide

/-! ## A count past its host reads 0, not NULL

`select city, species, form, max(x ? ...) where min(x) by species - count(id)
by * > 3`: the count is non-null wherever the atom holds, so 0.3.496 read it
past a host of the WHERE. But a COUNT on a group no row formed is 0, not NULL:
`5 - 0 > 3` holds on the host's padding. -/

/-- The count a consumer reads off a host of `atom`: the kept count, else the
padding, which a COUNT reads as 0. -/
def countPastHost (atom : Int → Bool) (n : Int) : Int :=
  (([n].filter atom).head?).getD 0

/-- **Past a host, an atom false on 0 reads the count as on its own group**
(`count_past_host_exact_if_zero_rejecting`). -/
theorem count_past_host_exact_if_zero_rejecting (atom : Int → Bool) (n : Int)
    (h : atom 0 = false) : atom (countPastHost atom n) = atom n := by
  unfold countPastHost
  cases hn : atom n <;> simp [List.filter, hn, h]

/-- `min(x) by species` is 5 and the total count 6: on its own group `5 - 6 >
3` is false, read past its host it holds
(`count_arg_past_host_passes_absent`). -/
theorem count_arg_past_host_passes_absent :
    let atom : Int → Bool := fun c => decide (5 - c > 3)
    atom 6 = false ∧ atom (countPastHost atom 6) = true := by
  decide

/-! ## A WHERE argument's producer is a lookup

`select species, form, sum(x) where (count(id) by form < count(id) by species
or avg(x) by city, form is null)`: the FINAL re-applies the WHERE over values
its argument producers supply, and one was joined FULL. Its species with no
kept row came back padded, NULL for the `avg`, and `is null` held. -/

/-- **A lookup keeps the rows it is joined onto** (`lookup_keeps_rows`): each
row reads its key's value, padded where the producer has none. -/
theorem lookup_keeps_rows [DecidableEq κ] (k : α → κ) (vals : List (κ × Int))
    (rows : List α) :
    (rows.map fun r => (r, (vals.find? (·.1 == k r)).map (·.2))).map (·.1) = rows := by
  rw [List.map_map]
  exact List.map_id' rows

/-- Species 3 alone passes (its `avg` is NULL); species 0 and 1 have `avg` 4.
Looked up, one row passes; preserved, the producer's species 0 and 1 come back
padded and pass too (`lookup_preserved_passes_padding`). -/
theorem lookup_preserved_passes_padding :
    let kept : List Int := [3]
    let avg : Int → Option Int := fun s => if s == 3 then none else some 4
    let producer : List Int := [0, 1, 3]
    let lookup := kept.filter fun s => (avg s).isNone
    let full := lookup ++ ((producer.filter fun s => !kept.contains s).filter
      fun _ => (none : Option Int).isNone)
    lookup = [3] ∧ full = [3, 0, 1] := by
  decide

end Trilogy.SelectGrain
