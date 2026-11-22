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
- A value over aggregates at two grains is one per pair the rows hold; off
  the two groups joined on nothing it invents pairs
  (`two_grain_value_without_its_pairs_invents`). An aggregate `by` keys its
  host's grain does not determine, rendered there, is read at the host's
  grain (`host_grain_pin_misreads`). A constant holds per row kept: gated by
  such a value's groups alone, it keeps a city no row passes
  (`constant_gated_off_its_rows_invents`).
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

/-- `select city, form, 12 where ...`: the trees `(city, species)` pair with
the kept species' forms on `species`. Wrapped to `city` alone the trees lose
that key, and the two sides cross: city 0 meets form 0, which only city 1's
kept species has. Two kept pairs become four. -/
theorem wrap_without_pair_key_crosses :
    let rows : List (Int × Int) := [(0, 1), (0, 3), (1, 2)]
    let form : Int → Int := fun s => s % 2
    let kept := (rows.filter fun r => r.2 != 3)
    let pairs := dedup (kept.map fun r => (r.1, form r.2))
    let cities := dedup (rows.map (·.1))
    let forms := dedup (kept.map fun r => form r.2)
    pairs.length = 2 ∧ (cities.flatMap fun c => forms.map fun f => (c, f)).length = 4 := by
  decide

/-- `select city, species, sum(x) by species / count(id) by city`: one value
per `(city, species)` pair the rows hold. Off the two groups alone, which
share no key, it is one per city and species: city 1 holds no species 2, and
three pairs become four. -/
theorem two_grain_value_without_its_pairs_invents :
    let rows : List (Int × Int) := [(0, 1), (0, 2), (1, 1)]
    let cities := dedup (rows.map (·.1))
    let species := dedup (rows.map (·.2))
    (dedup rows).length = 3 ∧
      (cities.flatMap fun c => species.map fun s => (c, s)).length = 4 := by
  decide

/-- `where x = 3 or sum(x) by city, species = ...` hosted on a group at
`species`: rendered there, the aggregate is read at the host's grain. Species
0 grows in cities 0 and 1: its row in city 1 sums to 7 by `(city, species)`
and to 10 by `species`. -/
theorem host_grain_pin_misreads :
    let rows : List (Int × Int × Int) := [(0, 0, 3), (1, 0, 7)]
    let x : Int × Int × Int → Int := fun r => r.2.2
    let pair : Int × Int × Int → Key := fun r => [r.1, r.2.1]
    let species : Int × Int × Int → Key := fun r => [r.2.1]
    eval (.by_ pair) species x rows (1, 0, 7) = 7 ∧
      eval .bare species x rows (1, 0, 7) = 10 := by
  decide

/-- `select city, 4 where <gate over by city and by species>`: the constant
holds per row kept. Constrained by the gate's two groups alone it pairs every
city with every species: the gate `species = 2` keeps city 0, and the cross
keeps city 1 too, which holds no species 2. -/
theorem constant_gated_off_its_rows_invents :
    let rows : List (Int × Int) := [(0, 1), (0, 2), (1, 1)]
    let gate : Int × Int → Bool := fun r => r.2 == 2
    let cities := dedup (rows.map (·.1))
    let species := dedup (rows.map (·.2))
    let cross := cities.flatMap fun c => species.map fun s => (c, s)
    dedup ((rows.filter gate).map (·.1)) = [0] ∧
      dedup ((cross.filter gate).map (·.1)) = [0, 1] := by
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

/-! ## A sole contributor carries the gate's whole grain

`select city, form, max(x) by form * count(id) by city where sum(x) by city,
species < 6`: the one side computing the value holds `(city, form)` off a row
stream that also holds `species`, but it projected only `city`, so FINAL met
the gate on `city` and kept a form every one of whose `(city, species)` rows
failed (0.3.513). A side short of every gate key is widened to them (47126
paired on nothing and raised). -/

/-- Rows `(city, species, form)` and the `(city, species)` groups the gate
keeps: on the gate's grain the kept `(city, form)` pairs are two, on `city`
alone three (`sole_gate_paired_short_of_its_grain`). -/
theorem sole_gate_paired_short_of_its_grain :
    let rows : List (Int × Int × Int) := [(0, 0, 0), (1, 0, 0), (1, 1, 1)]
    let kept : List (Int × Int) := [(0, 0), (1, 1)]
    (dedup ((rows.filter fun r => kept.contains (r.1, r.2.1)).map
        fun r => (r.1, r.2.2))).length = 2 ∧
      (dedup ((rows.filter fun r => kept.any (·.1 == r.1)).map
        fun r => (r.1, r.2.2))).length = 3 := by
  decide

/-! ## A finished value is no partial of the rows beside it

`select city, species, form, count(id) by city, form + count(id) by species,
sum(x)`: FINAL paired the two counts through the per-tree rows and found
`sum(x)`, already one value per `(city, species)`, at a grain finer than its
own, so it re-summed it as a stored partial, once per tree (0.3.513). Only
what a source stores as a partial is re-summed. -/

/-- A `(city, species)` of two trees whose height sum is 3: read once it is
3, re-summed per tree 6 (`finished_value_resummed_per_row`). -/
theorem finished_value_resummed_per_row :
    let perTree : List Int := [3, 3]
    perTree.head? = some 3 ∧ perTree.foldl (· + ·) 0 = 6 := by
  decide

/-! ## A gate that holds on NULL is not re-applied over padding

`select city, form, count_distinct(tag) by city + min(x) by * where sum(x)
by city is null`: the value's side read past the gate's host and held only
the kept cities; FINAL kept the unfiltered `(city, form)` rows beside it and
re-applied the gate after the join, where every rejected city's padding read
NULL and passed (0.3.513). A side past filters the merge re-applies is kept
whole only when no re-applied atom can hold on its padding. -/

/-- Cities 0 and 1, neither of null sum, so the gate keeps none: joined to
the kept side the rows are none; re-checked on the padded rows (`none` for
every city) both pass (`gate_on_padding_passes_rejected`). -/
theorem gate_on_padding_passes_rejected :
    let rows : List Int := [0, 1]
    let sums : List (Int × Option Int) := [(0, some 8), (1, some 9)]
    let kept := sums.filter (·.2 == none)
    (rows.filter fun c => kept.any (·.1 == c)).length = 0 ∧
      (rows.filter fun c => ((kept.find? (·.1 == c)).bind (·.2)) == none).length
        = 2 := by
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
its argument producers supply, and one, keyed off another producer's columns,
was joined FULL. Its species with no kept row came back padded, NULL for the
`avg`, and `is null` held. -/

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

/-! ## A total pairs on a constant

`select city where sum(x) by form < sum(x) by *`: the total's group is one row
keyed on the `all_rows` constant, and the rows it filters are a join of the
trees to the enrichment (`form` is the species' column). The cover search binds
no constant, so the joined rows were refused for not carrying it, and the
select raised. A constant is a literal in any projection: carried inline, it
pairs every row with the one total, which is the cross product the `by *`
means. -/

/-- **Paired on a constant, a row meets the one total** (`constant_key_pairs_every_row`):
the join on the constant is the cross product with a one-row side. -/
theorem constant_key_pairs_every_row (rows : List α) (total : Int) :
    (rows.flatMap fun r => ([((), total)].filter (·.1 == ())).map fun t => (r, t.2)) =
      rows.map fun r => (r, total) := by
  have one : ∀ r : α,
      (([((), total)].filter (·.1 == ())).map fun t => (r, t.2)) = [(r, total)] := by
    intro r; rfl
  simp only [one]
  induction rows with
  | nil => rfl
  | cons r rs ih => simp [List.flatMap_cons, ih]

/-! ## A bridge is joined where the plan pairs on it

`select tree_id, avg(x) by species, min(x ? ...) where max(x) by city, form !=
6 having count_distinct(tag) by city >= 2`: the FINAL plan pairs the species
value with one scan of the trees on `species`, and elects a second scan for
`tree_id` and `city`. The second scan also exposes `species`, so the first was
skipped as joining no new island; the plan states no pairing of the species
value with the second, and the join had no keys. A side that is some
contributor's only planned partner is joined. -/

/-- Three trees of two species: looked up on `species` each tree reads one
value; with no keys it reads every value (`keyless_pairing_fans_out`). -/
theorem keyless_pairing_fans_out :
    let trees : List (Int × Int) := [(0, 0), (1, 0), (2, 1)]
    let values : List (Int × Int) := [(0, 5), (1, 2)]
    (trees.flatMap fun t => values.filter (·.1 == t.2)).length = 3 ∧
      (trees.flatMap fun _ => values).length = 6 := by
  decide

/-! ## An alias of a row value is no spelling of its pinned read

`auto s2 <- status` aliases the row value of `status` (a CASE over an order's
delivery date, stored on the orders), while a select reads `status` pinned to
its own row, where the CASE's ELSE fires for a customer with no order. The
alias made the two one name class, and a column storing the alias answered
the pinned read: on the padded row it is NULL, and `status is null` kept the
customer. -/

/-- A customer with no order: the pinned read evaluates the CASE on the padded
row (`'in-transit'`, 1 here); the stored column read through the padding is
NULL (`alias_is_not_the_pinned_read`). -/
theorem alias_is_not_the_pinned_read :
    let delivery : Option Int := none
    let pinned : Option Int := some (if delivery.isSome then 0 else 1)
    let stored : Option (Option Int) := none
    pinned = some 1 ∧ stored.join = none ∧ pinned ≠ stored.join := by
  decide

/-! ## A padded side is not the WHERE's population

`select city, species, tree_form, ..., sum(x ? min(x) by form - count(id) by
form < 0) where count_distinct(tag) by species != 10 and avg(x) by city, form
is null`: the `?` filter's row stream is the trees FULL-joined to its per-form
values, which applied the WHERE. The FINAL read that stream as filtered, since
a condition was applied somewhere beneath it, and then refused the aggregate
that did apply the WHERE as the population: the stream's keys are NULL on its
padded rows, which "passed" only if the stream had. So the aggregate was
LEFT-joined onto the stream and every species came back. A condition applied
under a side a join pads filters none of the joined rows; a partner's NULL key
lets its rows pass only when that partner applied the WHERE. -/

/-- Four species, one kept by the WHERE: the stream pads the kept values with
every species. Joined onto it preserving, all four come back; joined to the
side that applied the WHERE, one (`padded_side_is_not_the_population`). -/
theorem padded_side_is_not_the_population :
    let species : List Int := [0, 1, 2, 3]
    let kept := species.filter (· == 2)
    let stream := kept ++ species.filter fun s => !kept.contains s
    let preserved := stream
    let inner := stream.filter kept.contains
    preserved.length = 4 ∧ inner = [2] := by
  decide

/-! ## A filtered value is a value of its row

`select city, species, count(id ? sum(x) by city, species is not null) where
avg(x) by * is not null`: the filtered `id` was built at the select's grain,
`(city, species)`, and read as a value of it. Grouped with the keys before the
WHERE's join it held one row per tree, but claimed the select's grain, so the
count was read off each row as `1` or `0` and every tree came back as a row.
`id ? c` is one value per `id`: its keys say what determines it, not the grain
of the select it was built in. -/

/-- Two trees of one species: counted over their group, 2; read per tree as a
value of the group, two rows of 1 (`filtered_value_is_per_row`). -/
theorem filtered_value_is_per_row :
    let group : List (Option Int) := [some 1, some 2]
    let count := (group.filter Option.isSome).length
    let perRow := group.map fun v => if v.isSome then 1 else 0
    count = 2 ∧ perRow = [1, 1] := by
  decide

/-! ## A gate feeder is no row of the statement

`select id, species where count(id) by form <= count(id) by species and form =
'linden'`: the row atom filters the scan, and the two counts are fed beside it
and joined. A COUNT is 0 on a group a feeder lacks, so the merge was left to
join typing, which preserved the species feeder: a species whose trees the row
atom rejected came back with a NULL `id`. The scan applied the WHERE's row
atoms; a merge onto it may drop its rows, never add a feeder's. -/

/-- Two species, one kept by the row atom: preserving the feeder returns both,
the kept one with its tree and the other padded; the scan returns one
(`gate_feeder_is_no_population`). -/
theorem gate_feeder_is_no_population :
    let scan : List (Int × Int) := [(1, 7)]
    let feeder : List Int := [1, 2]
    let preserved := feeder.map fun s => (s, (scan.find? (·.1 == s)).map (·.2))
    let inner := scan.filter fun r => feeder.contains r.1
    preserved = [(1, some 7), (2, none)] ∧ inner = [(1, 7)] := by
  decide

/-! ## A peel joined only to a gate skips the row filter

`select form, avg(x) by * where max(x) by species - count_distinct(tag) by
species > 0 and x > 2`: `form` was peeled to the species table, keyed on the
gate's `species`, and joined only to the gate's groups. The gate's groups are
every species whose trees pass it, whatever `x > 2` keeps: a species whose
only tree is 2 tall still came back. A selected column peeled onto a key that
no select-phase aggregate groups by holds the WHERE's population only when
every row filter is a value of that key. -/

/-- Two species pass the gate; the row filter keeps trees of one. Read off the
gate the select returns both (`peel_skips_row_filter`). -/
theorem peel_skips_row_filter :
    let trees : List (Int × Int) := [(1, 5), (2, 2)]
    let gate : List Int := [1, 2]
    let kept := (trees.filter (·.2 > 2)).map (·.1)
    (gate.filter fun s => kept.contains s) = [1] ∧ gate = [1, 2] := by
  decide

/-! ## A rename keeps the rows it was read on

`select id, city as v0, ... where x between 3 and 7 having count(id) by city,
species = 2`: the rename `v0` was read off the WHERE's filtered scan, then
re-sourced from a fresh scan of the trees, which nothing filters; the HAVING's
count over it counted the trees the WHERE dropped. -/

/-- Two trees in a group, one kept by the WHERE: the count over the kept rows
is 1, over the rename's fresh scan 2 (`rename_keeps_filtered_rows`). -/
theorem rename_keeps_filtered_rows :
    let heights : List (Option Int) := [some 3, none]
    let kept := heights.filter fun h => h.any fun v => 3 ≤ v && v ≤ 7
    kept.length = 1 ∧ heights.length = 2 := by
  decide

/-! ## A peel pairs on every key it is identified by

`select id, species, sum(x ? count(tag) by city, species >= 1) having min(x) by
city, form != 6 or sum(x) by city, species = 5`, with `sum(x) by city, species`
stored: the stored value is a dimension peel at `(city, species)`, and the
FINAL paired the per-tree value with it on `species` alone, and the gate by
`(city, form)` on `city` alone. Each tree met its species' value in every city.
A peel's rows are identified by its keys as a group's are by its grain: a
pairing on fewer of them needs a side holding the rest, and the peel carries
the keys its own keys determine (`form`). -/

/-- One species in two cities, three trees: paired on `species` alone each tree
meets both cities' values (`peel_paired_short_of_its_keys`). -/
theorem peel_paired_short_of_its_keys :
    let trees : List (Int × Int) := [(0, 0), (1, 0), (2, 1)]
    let peel : List ((Int × Int) × Int) := [((0, 0), 5), ((1, 0), 7)]
    (trees.flatMap fun _ => peel.filter (·.1.2 == 0)).length = 6 ∧
      (trees.flatMap fun t => peel.filter (·.1 == (t.2, 0))).length = 3 := by
  decide

/-! ## A one-row argument is read off its own group

`select form, min(x ? count_distinct(tag) by * <= 6), sum(x) where count(id) by
* < 2`: the FINAL re-applies the WHERE and reads `count(id) by *` off the
most derived group carrying it, the per-tree filter. That side pairs with no
other on any key, and the plan joined it keyless. A one-row value is read off
the group computing it, which pairs with every row (`constant_key_pairs_every_row`). -/

/-! ## A parenthesized WHERE is its conjunction

`where (a and b)` was read as the bare value `(a and b) = true`: one atom, so
no rule could place `a` or `b` apart, and a select the unparenthesized WHERE
planned raised for a keyless join. In three-valued logic the two are the same
filter (`parenthesized_conjunction_is_its_atoms`). -/

/-- A three-valued AND: false wins, then unknown. -/
def and3 : Option Bool → Option Bool → Option Bool
  | some false, _ => some false
  | _, some false => some false
  | some true, some true => some true
  | _, _ => none

/-- `(a and b) = true` keeps a row exactly when both atoms do
(`parenthesized_conjunction_is_its_atoms`). -/
theorem parenthesized_conjunction_is_its_atoms (a b : Option Bool) :
    (and3 a b == some true) = (a == some true && b == some true) := by
  cases a with
  | none => cases b with
    | none => rfl
    | some y => cases y <;> rfl
  | some x => cases x <;> cases b with
    | none => rfl
    | some y => cases y <;> rfl

/-! ## A gate beside a membership filters the rows its keys pair on (0.3.523)

The `compositions` sweep past 30,000. Each law below is one the planner broke
on a shape of it. -/

/-- `where max(h) by species < 1 or species in (species ? 3 <= h <= 7)`
beside `select city, form, count(id) by city`: the kept rows are the WHERE's,
and the `(city, form)` tuples are theirs. Applied only to the count's input,
the tuples were every row's, so the form-less species leaked
(`gate_off_outputs_keeps_key_rows`, seed 17410). Rows are `(city, species,
form)`, form 9 for none. -/
theorem gate_off_outputs_keeps_key_rows :
    let rows : List (Int × Int × Int) :=
      [(0, 0, 0), (0, 0, 0), (0, 1, 0), (0, 2, 9), (1, 0, 0), (1, 3, 1)]
    let inSet : Int → Bool := fun s => s == 0 || s == 1
    (dedup ((rows.filter fun r => inSet r.2.1).map fun r => (r.1, r.2.2))).length = 2 ∧
      (dedup (rows.map fun r => (r.1, r.2.2))).length = 4 := by
  decide

/-- A value re-aggregated off a nullable stored partial is NULL where every
partial is (`min(h) - min(h) by city, form` of the form-less group), so its
`=` drops the group FINAL pairs it on (`resum_of_nullable_partial_is_nullable`,
seed 12435): the summary's `lo` is `?`, and the re-sum inherits it. -/
theorem resum_of_nullable_partial_is_nullable :
    let partials : List (Option Int) := [none]
    let lo := partials.foldl (fun a b => match a, b with
      | some x, some y => some (min x y) | some x, none => some x
      | none, y => y) none
    lo = none ∧ sqlEq lo lo = false := by
  decide

/-- **An aggregate over a value constant in its group is that value**
(`aggregate_of_group_constant`): `sum(max(h) by city)` at the city grain reads
one `max` per city, so it is the `max`, never a sum nested around a re-max
(seed 35546). -/
theorem aggregate_of_group_constant (v : Int) : [v].foldl (· + ·) 0 = v := by
  simp

/-- A value re-summing partials at `fc` grouped also by `fc`'s own grain
(`species`) is one row per species: `sum(h) + count(id)` by `fc` came out as
two `b` rows (`resum_split_by_key_grain`, seed 33686). -/
theorem resum_split_by_key_grain :
    let fc : Int → Int := fun s => if s ≤ 1 then 0 else 1
    let parts : List (Int × Int) := [(0, 10), (1, 5), (2, 0), (3, 2)]
    (dedup (parts.map fun p => fc p.1)).length = 2 ∧
      (dedup (parts.map fun p => (fc p.1, p.1))).length = 4 := by
  decide

/-! ## Keys off values at two grains (0.3.524)

The `compositions` sweep past 60,000. Rows are the six trees, `(id, city,
species)`. -/

def trees : List (Int × Int × Int) :=
  [(1, 0, 0), (2, 0, 0), (3, 0, 1), (4, 0, 2), (5, 1, 0), (6, 1, 3)]

/-- `select city, fc, max(h) by fc, sum(h) by city where max(h) by species >= 1`:
the `(city, fc)` tuples are the kept rows', which only the `(city, species)`
rows and `fc` by species hold together. Paired off the two values' groups on
nothing, every kept city meets every kept `fc`: four tuples for three, and the
form-less species' `(A, o)` leaks (`keys_off_two_values_cross`, seed 15258). -/
theorem keys_off_two_values_cross :
    let fc : Int → Int := fun s => if s ≤ 1 then 0 else 1
    let kept := trees.filter (·.2.2 != 2)
    let cities := dedup (kept.map (·.2.1))
    let fcs := dedup (kept.map fun r => fc r.2.2)
    (cities.flatMap fun c => fcs.map fun f => (c, f)).length = 4 ∧
      (dedup (kept.map fun r => (r.2.1, fc r.2.2))).length = 3 := by
  decide

/-- `select tag, max(h) by * where species in (species ? city = 'A') and
count(id) by city > 2`: the gate keeps the rows of city 0, four of six. The set
reads `city` too, and the select's rows lost it to the set's own scan; a gate
joined to rows without its key keeps every row once any city passes
(`gate_key_read_by_a_set`, seed 57225). -/
theorem gate_key_read_by_a_set :
    let pass := ([(0, 4), (1, 2)] : List (Int × Int)).filter (·.2 > 2)
    (trees.filter fun r => pass.any (·.1 == r.2.1)).length = 4 ∧
      (trees.filter fun _ => !pass.isEmpty).length = 6 := by
  decide

/-- `count_distinct(tag) by form = count(id) by species`: the broadleaf form's
distinct tags are 2, but counted inside the `(species, form)` groups Quercus
reads its own 1 and passes beside its one tree. The species group carried the
value through the set that gated it, which feeds no rows, and recomputed it at
its own grain (`distinct_count_at_a_finer_grain`, seed 55868). Rows are
`(species, tag)` of the broadleaf trees. -/
theorem distinct_count_at_a_finer_grain :
    let rows : List (Int × Int) := [(0, 0), (0, 1), (1, 0), (0, 1)]
    (dedup (rows.map (·.2))).length = 2 ∧
      (dedup ((rows.filter (·.1 == 1)).map (·.2))).length = 1 := by
  decide

/-- A set is the keys its condition keeps, not rows of the select: `tag in (tag ?
c)` with `c` NULL on Ulmus' tree and true on another `x` keeps Ulmus. Joined as
a bridge, the set's own rows dropped it (`set_rows_are_no_bridge`, seed
55114); read for a gate's value FINAL re-applies, an empty set dropped every
row (46333). Rows are `(id, tag, c)`. -/
theorem set_rows_are_no_bridge :
    let rows : List (Int × Int × Option Bool) := [(1, 0, some true), (4, 0, none)]
    let set := dedup ((rows.filter (·.2.2 == some true)).map (·.2.1))
    (rows.filter fun r => set.contains r.2.1).length = 2 ∧
      (rows.filter (·.2.2 == some true)).length = 1 := by
  decide

/-! ## Sets written in the select, bridges under a window (0.3.528)

The `windows` oracle family, seeds 0 to 4,000: windows other than a rank,
memberships inside a select target's `?` and in a HAVING, `/` by a `by`
aggregate. -/

/-- `select count(id ? species in (species ? h > 6)) where city = 'A'`: a
set written in the select reads the rows the WHERE kept, as an aggregate
there does. No tree of city A is taller than 6, so the set is empty and the
count 0; read over every row the set holds Acer (b1 is 7) and the count is 2
(`select_set_reads_kept_rows`; the base model answered 0 and the summary 2).
A set in the WHERE is the other case: it defines the kept rows, so it reads
every row. -/
theorem select_set_reads_kept_rows :
    let rows : List (String × String × Option Int) :=
      [("A", "Acer", some 3), ("A", "Acer", none), ("A", "Quercus", some 5),
        ("A", "Ulmus", none), ("B", "Acer", some 7), ("B", "Tilia", some 2)]
    let tall : String × String × Option Int → Bool := fun r => match r.2.2 with
      | some h => h > 6
      | none => false
    let kept := rows.filter (·.1 == "A")
    let count (scope : List (String × String × Option Int)) :=
      (kept.filter fun r => ((scope.filter tall).map (·.2.1)).contains r.2.1).length
    count kept = 0 ∧ count rows = 2 := by
  decide

/-- `select species, tree_form, min(h ? id in (id ? tree_form is null))`:
the set's feeder holds the one species its condition kept. `tree_form` read
off it, in place of the enrichment rows, keeps Ulmus and drops three species
from the select (`set_supplies_no_output`, seed 1465). -/
theorem set_supplies_no_output :
    let enrichment : List (String × Option String) :=
      [("Acer", some "broadleaf"), ("Quercus", some "broadleaf"),
        ("Tilia", some "linden"), ("Ulmus", none)]
    (enrichment.filter (·.2 == none)).length = 1 ∧ enrichment.length = 4 := by
  decide

/-- `select h > 2 as big, count(id ? tag in (tag ? city = 'A'))`: the set's
feeder holds the four trees of city A beside `height`. `big`, computed off
it in place of the trees, loses the two trees of B and with them the group
`big = false` held only there once NULLs are apart
(`set_is_no_row_parent`). -/
theorem set_is_no_row_parent :
    let trees : List (String × Option Int) :=
      [("A", some 3), ("A", none), ("A", some 5), ("A", none), ("B", some 7), ("B", some 2)]
    let big : String × Option Int → Option Bool := fun r => r.2.map (· > 2)
    (trees.map big).eraseDups.length = 3 ∧
      ((trees.filter (·.1 == "A")).map big).eraseDups.length = 2 := by
  decide

/-- `select species, tree_form, min(h ? species in (...))`: the filtered
column is read off the trees, three of them Acer, and the set beside it is a
subselect, one row per species. Taking the stream's grain from the set, the
`min` saw rows already at `species` and was not computed: Acer came back
three times (`set_source_is_no_grain`, seed 2257). -/
theorem set_source_is_no_grain :
    let trees := ["Acer", "Acer", "Quercus", "Ulmus", "Acer", "Tilia"]
    let set := ["Tilia"]
    set.eraseDups.length = set.length ∧ trees.eraseDups.length = 4 ∧
      trees.length = 6 := by
  decide

/-- `select id, count_distinct(tag) by city, tree_form + 1, row_number id
order by sum(h), id`: the value has a row per `(city, tree_form)`, and city A
holds two forms. Joined to the trees on `city` alone, as the window's merge
did once it dropped the bridge holding both keys, each of A's four trees
meets both rows and each of B's two both of B's: twelve rows for six trees,
and the window numbers all twelve. Through the trees' own `(city,
tree_form)` each meets one (`keying_bridge_under_window`, seed 3769). -/
theorem keying_bridge_under_window :
    let trees : List (String × Option String) :=
      [("A", some "b"), ("A", some "b"), ("A", some "b"), ("A", none),
        ("B", some "b"), ("B", some "l")]
    let groups := trees.eraseDups
    (trees.flatMap fun t => groups.filter (·.1 == t.1)).length = 12 ∧
      (trees.flatMap fun t => groups.filter (· == t)).length = 6 := by
  decide

end Trilogy.SelectGrain

/-! ## A widening ends with the padding it kept

`select city, species, form, count_distinct(tag) by species where max(x) by
city, form != 10`: the species table joined the per-`(city, form)` gate on
`form` LEFT, and the `(city, species)` pairs the trees hold joined after it
were widened to LEFT to keep that join's padded rows. The WHERE then proved the
gate present and its join narrowed to INNER, but the widening stayed: every
species sharing a form with a city came back with that city, though no tree of
the pair exists. A widening only keeps rows an earlier join padded; once none
pads, the join takes back its own type. -/

/-- Species 1 and 2 share form 1, which both cities have; species 3 has form 3,
city 2 alone. The trees hold four `(city, species)` pairs. Kept LEFT, the
form pairing returns five, `(2, 2)` among them; INNER returns the four
(`widening_outlives_its_padding`). -/
theorem widening_outlives_its_padding :
    let forms : List (Int × Int) := [(1, 1), (2, 1), (3, 3)]
    let gate : List (Int × Int) := [(1, 1), (2, 1), (2, 3)]
    let trees : List (Int × Int) := [(1, 1), (1, 2), (2, 1), (2, 3)]
    let paired := forms.flatMap fun (s, f) =>
      (gate.filter (·.2 == f)).map fun (c, _) => (c, s)
    paired.length = 5 ∧ paired.contains (2, 2) ∧
      paired.filter trees.contains = [(1, 1), (2, 1), (1, 2), (2, 3)] := by
  decide

/-! ## A WHERE's total is no group of the select

`select count(min(x) by city, species) as v0 where count_distinct(tag) by * =
2 and sum(x) by city < 2`: the select's total is the `()` group of the rows
the WHERE keeps, and no city passes, so there is no group and no row. The
`by *` gate is a total over every row, read before the WHERE, so it holds a
row either way. Two select totals joined on nothing stay FULL (each is the
group of its own rows); the gate was taken for one, padded the missing count,
and the count's zero fill returned `(0,)`. -/

/-- A one-row side joined INNER on nothing keeps the other side's rows. -/
theorem one_row_cross_keeps_rows (g : Int) (rows : List Int) :
    (rows.flatMap fun r => [(r, g)]).length = rows.length := by
  induction rows with
  | nil => rfl
  | cons _ _ ih => simp [List.flatMap_cons, ih]

/-- The gate holds `2`; the WHERE keeps no row, so the count has no group. FULL
on nothing pads the gate's row and fills the count with 0; INNER returns none
(`gate_total_is_no_group`). -/
theorem gate_total_is_no_group :
    let gate : List Int := [2]
    let counts : List Int := []
    let full := gate.flatMap fun g =>
      if counts.isEmpty then [(g, 0)] else counts.map fun c => (g, c)
    let inner := gate.flatMap fun g => counts.map fun c => (g, c)
    full = [(2, 0)] ∧ inner = [] := by
  decide

namespace Trilogy.SelectGrain

/-! ## A total is read off its own row, never off a row stream

`select city, form, sum(x) by * as v0, sum(x ? sum(x) by * >= 2) as v1 where
max(x) by city, species is null`: the cover read `v0` off the `?` filter's row
stream, the most derived group carrying it, and the FINAL took the keys off
that stream too. The WHERE filtered the total and the filtered sum, not the
stream, so every `(city, form)` the trees hold came back. A total is one row,
which pairs with every kept row; read off a stream, it brings the stream's
rows with it (`total_off_row_stream_keeps_rejected`). -/

/-- Four `(city, form)` pairs, one kept by the WHERE. Read off the stream, the
total comes back on all four; joined as its own row onto the kept pairs, on
one (`total_off_row_stream_keeps_rejected`). -/
theorem total_off_row_stream_keeps_rejected :
    let stream : List (Int × Int) := [(1, 0), (1, 1), (2, 1), (2, 2)]
    let kept : List (Int × Int) := [(1, 0)]
    let total : List Int := [0]
    (stream.flatMap fun k => total.map fun t => (k, t)).length = 4 ∧
      (kept.flatMap fun k => total.map fun t => (k, t)) = [((1, 0), 0)] := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A join key read off several sides is their coalesce

Several left sides of one key render `coalesce(r.k, a.k) = g.k`. Pruning
`r.k` because `r`'s other pairs imply it leaves `a.k`, which a padded `a`
holds NULL: the gate `g` misses every row `a` did not match, and a WHERE that
NULL lets hold (`not (x != y)`) keeps them (`pruned_coalesce_member_pads_gate`).
The key read off the side nothing pads is the coalesce wherever it is present
(`coalesce_unpadded_member`). -/

/-- `coalesce` of a present value and anything is that value. -/
theorem coalesce_unpadded_member (r : Int) (a : Option Int) :
    (some r).or a = some r := rfl

/-- Two rows of form `1`; the side `a` matched only the second. The gate holds
form `1`. Keyed on the coalesce both rows find it; keyed on `a.k` alone the
first row reads NULL, and `not (NULL != y)` keeps it. -/
theorem pruned_coalesce_member_pads_gate :
    let rows : List (Int × Option Int) := [(1, none), (1, some 1)]
    let gate : Int → Option Int := fun k => if k = 1 then some 7 else none
    let read (k : Option Int) : Option Int := k.bind gate
    rows.map (fun (r, a) => read ((some r).or a)) = [some 7, some 7] ∧
      rows.map (fun (_, a) => read a) = [none, some 7] := by
  decide

/-! ## A cross product is one relation whichever side it starts from

Two plans for one merge may join two one-row CTEs `a ⋈ b` and `b ⋈ a`, both on
nothing. Merged into one CTE, keeping both joins references a CTE twice: an
invalid statement, or with aliases a second cross product
(`mirrored_cross_join_is_one`, `second_cross_join_multiplies`). -/

theorem mirrored_cross_join_is_one (a b : List Int) (x y : Int) :
    (x, y) ∈ (a.flatMap fun x => b.map fun y => (x, y)) ↔
      (y, x) ∈ (b.flatMap fun y => a.map fun x => (y, x)) := by
  simp only [List.mem_flatMap, List.mem_map, Prod.mk.injEq]
  constructor
  · rintro ⟨x', hx, y', hy, h1, h2⟩
    subst h1 h2
    exact ⟨y', hy, x', hx, rfl, rfl⟩
  · rintro ⟨y', hy, x', hx, h1, h2⟩
    subst h1 h2
    exact ⟨x', hx, y', hy, rfl, rfl⟩

theorem second_cross_join_multiplies :
    let a : List Int := [1]
    let b : List Int := [2, 3]
    (a.flatMap fun x => b.map fun y => (x, y)).length = 2 ∧
      ((a.flatMap fun x => b.map fun y => (x, y)).flatMap fun p =>
        b.map fun z => (p, z)).length = 4 := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## FINAL re-reads no column finer than its rows

`select species, form, min(x) where (max(x) by species > 8 or city = 'B') and
max(x) by city > 20`: the aggregate by species applied the OR, and FINAL
re-applied it for the form projection. FINAL's rows are species; `city` has no
value on them, so FINAL read it off the city gate joined FULL on nothing. With
no species kept, that join padded a row per city and `city = 'B'` kept one of
NULLs (`final_reads_finer_column_off_gate`). -/

/-- No species kept; the gate holds cities `1` and `2`. FULL on nothing pads a
row per city, and `city = 2` keeps one with no species. -/
theorem final_reads_finer_column_off_gate :
    let kept : List Int := []
    let gate : List Int := [1, 2]
    let full : List (Option Int × Int) :=
      if kept.isEmpty then gate.map fun c => (none, c)
      else kept.flatMap fun s => gate.map fun c => (some s, c)
    full.filter (fun (_, c) => c == 2) = [(none, 2)] := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A peel the WHERE's atom is a function of carries the atom

`select species, sum(x) by species as v0, sum(x ? (min(x) by form < 10 or
max(x) by form != 6)) as v1 where form is null`: `v0` is peeled onto the
summary at `species`, which determines `form`, and the WHERE was left on the
row stream. The stream reached FINAL only through `v1`'s group, joined outer,
so the unfiltered peel kept every species (`unfiltered_peel_keeps_rejected`).
The atom is a function of the peel's key, so the peel applies it and keeps
exactly the species a kept row has (`Rollup.key_filter_is_semijoin`). -/

/-- Species `1..4` with `v0`; only species 3 has `form` NULL. Joined LEFT from
the peel all four return; the peel filtered by the atom keeps species 3. -/
theorem unfiltered_peel_keeps_rejected :
    let peel : List (Nat × Option Int) := [(1, some 10), (2, some 5), (3, none), (4, some 2)]
    let formNull : Nat → Bool := (· == 3)
    peel.length = 4 ∧ (peel.filter (formNull ·.1)).map (·.1) = [3] := by
  decide

/-! ## One CTE joined from two sides is one conjunction

Two plans merged into one CTE each paired `w` with their own side (`a JOIN w`,
`u JOIN w`); rendered, `w` is referenced twice and the statement is invalid.
Over INNER joins the pairs are one conjunction wherever they are written, so
the second pairing moves onto the join that brings in `u`
(`rejoined_cte_is_one_conjunction`). -/

theorem rejoined_cte_is_one_conjunction (rows : List (Int × Int × Int)) :
    (rows.filter fun r => r.1 == r.2.2).filter (fun r => r.2.1 == r.2.2) =
      rows.filter fun r => r.1 == r.2.2 && r.2.1 == r.2.2 := by
  simp [List.filter_filter, Bool.and_comm]

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A lookup of a split key is no key of its own

`select city, species, form where max(x) by species - min(x) by city, species
< 1 having ...`: each WHERE aggregate keeps its own key, and the select's rows
are the kept `(city, species)` tuples. `form` is a function of `species`, so it
adds no tuple: the select still pairs keys split over two branches, and a
FINAL that skipped the WHERE for it read a pair the WHERE dropped
(`lookup_beside_split_keys_pairs_unkept`). -/

/-- **A lookup adds no tuple** (`lookup_adds_no_tuple`): a row's `(a, b, f b)`
is kept exactly when its `(a, b)` is, with the looked-up value fixed by `b`. -/
theorem lookup_adds_no_tuple (a b : α → Key) (f : Key → Key) (rows : List α)
    (x y z : Key) :
    (x, y, z) ∈ dedup (rows.map fun r => (a r, b r, f (b r))) ↔
      z = f y ∧ (x, y) ∈ dedup (rows.map fun r => (a r, b r)) := by
  simp only [mem_dedup, List.mem_map, Prod.mk.injEq]
  constructor
  · rintro ⟨r, hr, h1, h2, h3⟩
    subst h1 h2 h3
    exact ⟨rfl, r, hr, rfl, rfl⟩
  · rintro ⟨rfl, r, hr, h1, h2⟩
    subst h1 h2
    exact ⟨r, hr, rfl, rfl, rfl⟩

/-- Rows `(city, species)` `[(0, 1), (0, 2), (1, 1)]` with `(0, 1)` dropped by
the WHERE and `form` a function of species: two kept tuples, but cities 0 and
1 and species 1 and 2 are all kept, so paired off the unfiltered rows, with or
without the lookup, `(0, 1)` comes back. -/
theorem lookup_beside_split_keys_pairs_unkept :
    let rows : List (Int × Int) := [(0, 1), (0, 2), (1, 1)]
    let p : Int × Int → Bool := fun r => r != (0, 1)
    let city : Int × Int → Key := fun r => [r.1]
    let species : Int × Int → Key := fun r => [r.2]
    let form : Key → Key := fun s => s.map (· % 2)
    let kept := dedup ((rows.filter p).map fun r => (city r, species r, form (species r)))
    let paired := dedup ((rows.filter fun r =>
      (keptKeys city p rows).contains (city r) &&
        (keptKeys species p rows).contains (species r)).map fun r =>
          (city r, species r, form (species r)))
    kept.length = 2 ∧ paired.length = 3 := by
  decide

/-! ## A column a side exposes is no key it is joined on

A merge joins each side on the keys its plan pairs it on. A filtered row
stream elected to bridge `v0`'s `tree_id` and a `by species` gate also exposes
`city`, but is paired on neither `city` nor `tree_form`; read as joining the
`by city` groups, it left the row stream that pairs them unjoined, and they
met on nothing (`exposed_column_joins_nothing`). Keyed on `city`, a lookup of
the groups keeps the stream's rows (`keyed_lookup_keeps_rows`). -/

/-- **A lookup on a key the right side is unique on keeps the left's rows.** -/
theorem keyed_lookup_keeps_rows (rows : List (Int × Int)) (g : Int → Int) :
    (rows.flatMap fun r => ([r.1].map fun c => (r, c, g c)).filter
      fun t => t.2.1 == t.1.1).length = rows.length := by
  induction rows with
  | nil => rfl
  | cons r rs ih => simp_all [List.flatMap_cons]

/-- Five trees in two cities and a `by city` group of two rows: joined on
nothing, ten rows; keyed on `city`, five. -/
theorem exposed_column_joins_nothing :
    let trees : List (Int × Int) := [(1, 0), (2, 0), (3, 0), (4, 1), (5, 1)]
    let groups : List (Int × Int) := [(0, 5), (1, 7)]
    (trees.flatMap fun t => groups.map (t, ·)).length = 10 ∧
      ((trees.flatMap fun t => groups.map (t, ·)).filter
        fun p => p.1.2 == p.2.1).length = 5 := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A blend beside finer rows is no pairing

`select tree_id, ... having sum(x) by species != 0 or max(x) by city, form > 2`:
the rows are trees, and each tree has one species group and one `(city, form)`
group. The two HAVING groups share `form`, which identifies neither, so joined
to each other on `form` alone a tree meets every species of its form
(`blend_beside_finer_rows_fans_out`). A side unique on the key a tree holds
meets it once (`unique_side_one_partner`): the row stream pairs each group on
its own identity. -/

/-- **A side unique on a key has one partner for a row holding it.** -/
theorem unique_side_one_partner (side : List (Int × Int)) (k : Int)
    (h : (side.map Prod.fst).Nodup) (hk : k ∈ side.map Prod.fst) :
    (side.filter fun s => s.1 == k).length = 1 := by
  induction side with
  | nil => simp at hk
  | cons s rest ih =>
    simp only [List.map_cons, List.nodup_cons, List.mem_map] at h
    simp only [List.map_cons, List.mem_cons, List.mem_map] at hk
    by_cases hs : s.1 = k
    · subst hs
      have : (rest.filter fun t => t.1 == s.1) = [] := by
        rw [List.filter_eq_nil_iff]
        intro t ht
        simp only [beq_iff_eq]
        intro he
        exact h.1 ⟨t, ht, he⟩
      simp [this]
    · have hk' : k ∈ rest.map Prod.fst := by
        rcases hk with hk | ⟨t, ht, he⟩
        · exact absurd hk.symm hs
        · exact List.mem_map.mpr ⟨t, ht, he⟩
      simp [hs, ih h.2 hk']

/-- Three trees `(id, species, form)`, two species of form 0: the species
groups joined to each tree on `form` give five rows, on `species` three. -/
theorem blend_beside_finer_rows_fans_out :
    let trees : List (Int × Int × Int) := [(1, 0, 0), (2, 1, 0), (3, 2, 1)]
    let bySpecies : List (Int × Int) := [(0, 0), (1, 0), (2, 1)]
    (trees.flatMap fun t => bySpecies.filter fun g => g.2 == t.2.2).length = 5 ∧
      (trees.flatMap fun t => bySpecies.filter fun g => g.1 == t.2.1).length = 3 := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A filtered copy of the rows pairs with the rows on their keys

`select city, species, tree_form, sum(x) by city, tree_form, count_distinct(t)
where max(x) by species > 6 and ...`: FINAL held the trees and the
WHERE-filtered trees, both identified by nothing, and joined them only through
the `(city, tree_form)` group. Every species of a kept form came back off the
unfiltered side (`filtered_copy_paired_through_group_keeps_rejected`); paired
on the keys the merge is identified by, the copy is a semijoin
(`filtered_copy_paired_on_keys_exact`). -/

/-- Rows `(city, species, form)`. -/
def copyRows : List (Int × Int × Int) := [(0, 0, 0), (0, 1, 0), (1, 0, 0)]

def copyKept : List (Int × Int × Int) := copyRows.filter fun r => r.2.1 == 0

theorem filtered_copy_paired_on_keys_exact :
    dedup ((copyRows.filter fun r =>
      copyKept.any fun k => k.1 == r.1 && k.2.1 == r.2.1).map fun r => (r.1, r.2.1)) =
      dedup (copyKept.map fun r => (r.1, r.2.1)) := by
  decide

theorem filtered_copy_paired_through_group_keeps_rejected :
    (0, 1) ∈ dedup ((copyRows.filter fun r =>
      copyKept.any fun k => k.1 == r.1 && k.2.2 == r.2.2).map fun r => (r.1, r.2.1)) ∧
      (0, 1) ∉ dedup (copyKept.map fun r => (r.1, r.2.1)) := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A value over a group holds what the group holds

`select city, species, sum(x) by city, tree_form, count(id) + 1`: the count is
grouped by `(city, species)` and `+ 1` is computed off those rows. The plan
said the value could be widened to `tree_form`, which its species determines,
and paired it with the `(city, tree_form)` group on both keys. No row of a
group reaches a column the group did not output, so the built sides joined on
`city` alone and each species met every form of its city
(`value_over_group_holds_no_finer_key`). Through the rows holding `(city,
species, tree_form)` each group meets its own form
(`value_over_group_bridged_exact`). The same holds inside one value: `max(x)
by city, tree_form * count(id)` at `tree_id` joins its two inputs through the
trees although they already share `city`
(`keying_bridge_joined_whatever_the_islands`). -/

/-- Rows `(city, species, form)`: species 0 and 1 are form 0, 2 is form 1, 3
is form 2. -/
def valueRows : List (Int × Int × Int) :=
  [(0, 0, 0), (0, 0, 0), (0, 1, 0), (0, 2, 1), (1, 0, 0), (1, 3, 2)]

/-- The `(city, species)` groups and the `(city, form)` groups. -/
def valueGroups : List (Int × Int) := dedup (valueRows.map fun r => (r.1, r.2.1))
def formGroups : List (Int × Int) := dedup (valueRows.map fun r => (r.1, r.2.2))

theorem value_over_group_holds_no_finer_key :
    valueGroups.length = 5 ∧
      (valueGroups.flatMap fun g => formGroups.filter fun f => f.1 == g.1).length = 10 := by
  decide

theorem value_over_group_bridged_exact :
    (dedup (valueRows.flatMap fun r =>
      ((valueGroups.filter fun g => g.1 == r.1 && g.2 == r.2.1).flatMap fun g =>
        (formGroups.filter fun f => f.1 == r.1 && f.2 == r.2.2).map fun f =>
          (g, f)))).length = 5 := by
  decide

/-- Six trees, each one row of its own group, and the `(city, form)` groups:
one island on `city`, twelve rows; through each tree's own keys, six. -/
theorem keying_bridge_joined_whatever_the_islands :
    (valueRows.flatMap fun r => formGroups.filter fun f => f.1 == r.1).length = 12 ∧
      (valueRows.flatMap fun r =>
        formGroups.filter fun f => f.1 == r.1 && f.2 == r.2.2).length = 6 := by
  decide

/-! ## A blend the reader's grain pins

`select city, species, max(x) by species + sum(x) by city, tree_form`: the
value is read at `(city, species)`, which determines both inputs' keys. They
share `tree_form`; paired on it, every species of a form meets every city of
it, and `(1, 1)`, a pair no tree has, comes back
(`blend_under_a_finer_reader_invents_pairs`). -/

theorem blend_under_a_finer_reader_invents_pairs :
    let bySpecies := dedup (valueRows.map fun r => (r.2.1, r.2.2))
    let paired := dedup (formGroups.flatMap fun f =>
      (bySpecies.filter fun s => s.2 == f.2).map fun s => (f.1, s.1))
    (1, 1) ∈ paired ∧ (1, 1) ∉ valueGroups ∧ paired.length = valueGroups.length + 1 := by
  decide

/-! ## A NULL key value is a group

`select height > 2 as big, count(id) / count(id) by *`: `big` is NULL where
the height is, and that is one of the select's groups. The total was re-summed
off the per-`big` counts after a join back on `big`; the comparison was read
as never NULL, so the join was a plain `=` and the NULL group's count left the
total (`null_key_group_is_a_group`). -/

/-- Counts per `big`: three true, one unknown. -/
def bigCounts : List (Option Bool × Int) := [(some true, 3), (none, 1)]

/-- SQL `=`: NULL equals nothing. -/
def bigEq : Option Bool → Option Bool → Bool
  | some a, some b => a == b
  | _, _ => false

theorem null_key_group_is_a_group :
    ((bigCounts.filter fun g => bigCounts.any fun h => bigEq g.1 h.1).map (·.2)).sum = 3 ∧
      ((bigCounts.filter fun g => bigCounts.any fun h => g.1 == h.1).map (·.2)).sum = 4 := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## An expression of a bare aggregate is at the select grain

`select city, species, count(id) * min(x) by city`: the count is grouped by
`(city, species)`, so the product is one value per `(city, species)`. Its
grain was the union of its aggregates' `by` keys, `city` alone, so the plan
grouped it by `(city, value)` and joined it back on `city`: each species met
every value its city has (`by_keys_alone_fan_out`, eight rows for five
groups). Keyed by the select grain each group meets its own
(`bare_beside_by_is_select_grain`). `count(id) - count_distinct(tag)` was
keyed by the aggregated columns' key, `id`, and a summary's row read beside
it was repeated per tree. -/

/-- `(city, species)` and the count of its trees. -/
def treeCounts : List ((Int × Int) × Int) :=
  [((0, 0), 2), ((0, 1), 1), ((0, 2), 1), ((1, 0), 1), ((1, 3), 1)]

theorem by_keys_alone_fan_out :
    let byCity := dedup (treeCounts.map fun g => (g.1.1, g.2))
    (treeCounts.flatMap fun g => byCity.filter fun v => v.1 == g.1.1).length = 8 ∧
      treeCounts.length = 5 := by
  decide

theorem bare_beside_by_is_select_grain :
    (treeCounts.flatMap fun g => treeCounts.filter fun h => h.1 == g.1).length = 5 := by
  decide

/-! ## A bridge's rows passed the WHERE

`select id, count(id) by city, form - count(id) where form = 0 and max(x) by
city, form > 1`: the per-tree value is paired with the `(city, form)` value
through rows holding `(id, city, form)`. The condition-phase scan holds them
too, and it came first by name; its rows never passed the WHERE, and the
aggregates joined it preserved, so every tree came back
(`unfiltered_bridge_keeps_rejected`). -/

/-- Trees `(id, form)`: form 0 for four, 1 and 2 for one each. -/
def formTrees : List (Int × Int) := [(0, 0), (1, 0), (2, 0), (3, 1), (4, 0), (5, 2)]

theorem unfiltered_bridge_keeps_rejected :
    (formTrees.filter fun t => t.2 == 0).length = 4 ∧ formTrees.length = 6 := by
  decide

/-- `select coalesce(form, 'none') where max(x) by city, species < 4`: the
key is a lookup of the species, and a fresh scan of the forms in place of
the gated rows holds every form, `none` included
(`dimension_rescan_drops_gate`). Species `(id, form, kept)`. -/
def gatedSpecies : List (Int × Int × Bool) :=
  [(0, 0, true), (1, 0, false), (2, 3, false), (3, 1, true)]

theorem dimension_rescan_drops_gate :
    (dedup (gatedSpecies.map (·.2.1))).length = 3 ∧
      (dedup ((gatedSpecies.filter (·.2.2)).map (·.2.1))).length = 2 := by
  decide

/-! ## A total of a table the WHERE's table joins

`select form, min(x) + count_distinct(tag) by * where form = 'linden'`: the
summary answers `min(x)` by form, so the trees feed only the `by *` total and
share no table with the other reads. They join the forms on `species`, so the
WHERE keeps only the linden trees; the total was read off every tree
(`peel_of_joined_table_reads_rejected`). -/

/-- Trees `(tag, form)`. -/
def tagTrees : List (Int × Int) := [(0, 0), (1, 0), (0, 0), (0, 1), (1, 0), (0, 2)]

theorem peel_of_joined_table_reads_rejected :
    (dedup (tagTrees.map (·.1))).length = 2 ∧
      (dedup ((tagTrees.filter fun t => t.2 == 2).map (·.1))).length = 1 := by
  decide

/-! ## A re-sum reads its partial's rows

`select form0, count(id) by *, count(id) where sum(x) by form > 3 and x > 3`:
the total is re-summed off the per-`form0` counts, which already kept only
the rows passing both atoms. The planner judged the re-sum to read rows no
host filtered (the columns its function names come from no input), copied
`x > 3` onto it, and `x` is not a column of a per-`form0` row
(`resum_reads_partial_rows`: the partials' sum is the filtered count). -/

/-- Rows `(form0, x)` passing the WHERE, and the per-`form0` counts. -/
def keptRows : List (Int × Int) := [(0, 5), (0, 7)]

theorem resum_reads_partial_rows :
    ((dedup (keptRows.map (·.1))).map fun f =>
        (keptRows.filter fun r => r.1 == f).length).sum = keptRows.length := by
  decide


/-! ## A value over groups is one row per tuple

`select city, species, form, count(id) / count(id), count(id) by form /
count(id) by city`: the value at `(city, form)` reads two aggregates that hold
neither pair, so the rows by `(city, species)` bridge them. Built over the
bridge, the value is one row per species, so it repeated each `(city, form)`
once per species, and FINAL, joining it back to the rows on `(city, form)`,
gave every row as many partners as its form has species in its city
(`bridge_joined_whole_repeats_its_pairs`). Deduped to its tuples, each row
meets its value once (`unique_side_one_partner`). -/

/-- The `(city, species, form)` rows of `valueRows` bridge the value at `(city,
form)`: five rows, seven partners off the whole bridge, five off its pairs. -/
theorem bridge_joined_whole_repeats_its_pairs :
    let bridge : List (Int × Int × Int) := dedup valueRows
    let whole := bridge.map fun r => (r.1, r.2.2)
    let pairs := dedup whole
    bridge.length = 5 ∧
      (bridge.flatMap fun r => whole.filter fun v => v == (r.1, r.2.2)).length = 7 ∧
      (bridge.flatMap fun r => pairs.filter fun v => v == (r.1, r.2.2)).length = 5 := by
  decide

/-! ## A membership, a set and a derived key are no grains of the rows

The `compositions` family (0.3.522) writes `x in (x ? c)` beside the WHERE's
own atoms, a `by` over a derived key, and a nested aggregate beside a value of
the select. Each law below is one the planner broke on a shape of it. -/

/-- The trees `(id, city, species)` of the sweep model, species numbered. -/
def compRows : List (Int × Int × Int) :=
  [(1, 0, 0), (2, 0, 0), (3, 0, 1), (4, 0, 2), (5, 1, 0), (6, 1, 3)]

/-- **A membership is a row predicate, so it filters the key rows**
(`membership_keys_are_kept_keys`): `select species, count(id) by * where id in
S or g` holds the species of the rows the WHERE keeps, as any atom does. -/
theorem membership_keys_are_kept_keys (k : α → Key) (S : List α → List Key)
    (g : α → Bool) (rows : List α) (t : Key)
    (h : t ∈ keptKeys k (fun r => (S rows).contains (k r) || g r) rows) :
    ∃ r ∈ rows, ((S rows).contains (k r) || g r) = true ∧ k r = t :=
  kept_key_holds k _ rows t h

/-- City 1's trees by the set, species 0 by a gate (`count by species > 2`):
two species are kept. Applied only to the total's input, the key rows were
every species (`membership_left_off_keys_keeps_rejected`, sweep seed 337). -/
theorem membership_left_off_keys_keeps_rejected :
    let p : Int × Int × Int → Bool := fun r => r.2.1 == 1 || r.2.2 == 0
    let species : Int × Int × Int → Key := fun r => [r.2.2]
    (keptKeys species p compRows).length = 2 ∧
      (keptKeys species (fun _ => true) compRows).length = 4 := by
  decide

/-- A group's count read as a per-row presence flag: one row per tree, `1`
each, deduped to the group's tuple. Right only when the rows are already one
per group; a set listed first in the scan (`tree_form ? c`, one row per form)
claimed that grain for the trees (`set_grain_is_no_row_grain`, seed 5544):
species 0's count read 1 for 3. -/
theorem set_grain_is_no_row_grain :
    let species0 := compRows.filter (·.2.2 == 0)
    species0.length = 3 ∧ (dedup (species0.map fun r => (r.2.2, (1 : Int)))).length = 1 := by
  decide

/-- **A derived key's own grain is no key of its value**
(`derived_key_grain_splits_its_tuple`, seed 2081): `fc` of species 0 and 1 is
the same `b`, so `(city, fc)` is one tuple where `(city, fc, species)` is two,
and a FINAL grouped by the latter returned `(A, b)` twice. -/
theorem derived_key_grain_splits_its_tuple :
    let fc : Int → Int := fun s => if s ≤ 1 then 0 else 1
    let rows := compRows.filter (·.2.1 == 0)
    (dedup (rows.map fun r => (r.2.1, fc r.2.2))).length = 2 ∧
      (dedup (rows.map fun r => (r.2.1, fc r.2.2, r.2.2))).length = 3 := by
  decide

/-- A total read off per-key partials is their re-aggregate. `min(h)` beside
`sum(count(id) by species)` under a WHERE was kept per species and read as the
total, one row per species (`partial_read_as_total_splits`, seed 876): three
minima where the total is one. -/
theorem partial_read_as_total_splits :
    let h : List (Int × Int) := [(0, 3), (0, 7), (1, 5), (3, 2)]
    let per := dedup (h.map (·.1)) |>.map fun s =>
      ((h.filter (·.1 == s)).map (·.2)).foldl min 100
    per.length = 3 ∧ per.foldl min 100 = 2 := by
  decide

/-! ## Sets in an OR, partials beside a membership (0.3.525)

The `compositions` sweep to 130,000. -/

/-- Does `a` reach `b` along `edges` in at most `n` steps? -/
def reachesIn (edges : List (Nat × Nat)) : Nat → Nat → Nat → Bool
  | 0, a, b => a == b
  | n + 1, a, b => a == b || edges.any fun e => e.1 == a && reachesIn edges n e.2 b

/-- `max(h) by city, form - sum(h) by form = 2 or (form in (form ? count(id) by
form != 10) and form in (form ? max(h) by city, form is null))`. Groups: the
`(city, form)` max 0, the form group holding the sum and the count 1, the two
sets 2 and 3, the form column 4. Each set is read by its own membership, the
form: ordered before 4, the plan is acyclic. Ordered before every row
argument of the OR, each set precedes the aggregate the other is computed
from, and the groups close a loop (`set_read_by_its_own_membership`, seeds
41277, 66218 raised `Graph contains a cycle`). -/
theorem set_read_by_its_own_membership :
    let lineage := [(0, 2), (1, 3)]
    let own := lineage ++ [(2, 4), (3, 4)]
    let every := own ++ [(2, 1), (3, 0)]
    ([0, 1, 2, 3, 4].all fun n => !(own.any fun e => e.1 == n && reachesIn own 4 e.2 n)) ∧
      (every.any fun e => e.1 == 0 && reachesIn every 4 e.2 0) := by
  decide

/-- `count(id) by species != 3 or city in S`, off a summary of `(city,
species)` counts: Acer's count is 3, so its trees fail the gate, but read off
the stored `(A, Acer)` and `(B, Acer)` partials, 2 and 1, each passes
(`partial_is_no_gate_value`, seed 37601). -/
theorem partial_is_no_gate_value :
    let acer : List Int := [2, 1]
    acer.foldl (· + ·) 0 = 3 ∧ acer.all (· != 3) := by
  decide

/-- `species in (species ? max(h) by * = 7)`: the total is 7, so every species
is in. Re-maxed per species off the summary, the set compared each species'
own maximum and kept Acer alone (`stored_total_not_remaxed`, seed 47456).
Maxima by species: Acer 7, Quercus 5, Tilia 2, Ulmus NULL. -/
theorem stored_total_not_remaxed :
    let maxima : List (Option Int) := [some 7, some 5, some 2, none]
    (maxima.filterMap id).foldl max 0 = 7 ∧
      (maxima.filter (· == some 7)).length = 1 := by
  decide

/-- `count(max(h) by city)` per city is 1. A scan whose one parent is a set's
feeder at city grain still holds its own `(city, species)` rows: read as at
the city, city A counts its three species' maxima, two non-null
(`feeder_is_no_row_parent`, seed 23531). -/
theorem feeder_is_no_row_parent :
    let cityA : List (Option Int) := [some 3, some 5, none]
    (dedup [cityA.filterMap id |>.foldl max 0]).length = 1 ∧
      (cityA.filterMap id).length = 2 := by
  decide

/-- `sum(h) by form - count(id) by form` over the kept broadleaf trees is
`15 - 4 = 11`. Computed off the per-species partials before the re-sum, it is
`10 - 3 = 7` for Acer and `5 - 1 = 4` for Quercus, and a join on the value
pairs no row (`value_off_partials_is_no_key`, seed 38002). -/
theorem value_off_partials_is_no_key :
    let parts : List (Int × Int) := [(10, 3), (5, 1)]
    let whole := parts.foldl (fun a p => a + p.1) 0 - parts.foldl (fun a p => a + p.2) 0
    whole = 11 ∧ (parts.map fun p => p.1 - p.2).all (· != whole) := by
  decide

/-- `city ? count(id) by city, species > 1` keeps a city with a `(city,
species)` group of two trees: A alone. Its condition re-summed to the city off
the stored counts reads A's 4 and B's 2, and keeps B too
(`set_condition_at_its_own_keys`, seed 83051). Rows are `(city, species,
count)`. -/
theorem set_condition_at_its_own_keys :
    let counts : List (Int × Int × Int) :=
      [(0, 0, 2), (0, 1, 1), (0, 2, 1), (1, 0, 1), (1, 3, 1)]
    let byCity := fun c => (counts.filter (·.1 == c)).foldl (fun a r => a + r.2.2) 0
    dedup ((counts.filter (·.2.2 > 1)).map (·.1)) = [0] ∧
      ([0, 1].filter fun c => byCity c > 1) = [0, 1] := by
  decide

/-! ## Values at their own grain, sets beside their own column (0.3.526)

The `compositions` sweep past 130,000 and the open summary raises. -/

/-- `select city, min(h) by city, species`: one row per distinct `(city,
value)`. City A's species minima are 3, 5 and NULL, three rows. Folding the
dedup into the summary scan re-minned the stored minima to the city and
returned one row, 3 (`value_at_its_own_grain_kept`). -/
theorem value_at_its_own_grain_kept :
    let cityA : List (Option Int) := [some 3, some 5, none]
    (dedup cityA).length = 3 ∧ (cityA.filterMap id).foldl min 100 = 3 := by
  decide

/-- `max(h) by form` beside `min(min(h) by species)`: the stored value has
one row per form, the nested aggregate one per species. Hosted as a key of the
species rows, the value is a column those rows never held: Acer and Quercus
share the broadleaf 7, so the four species meet three form rows and no species
key pairs them (`stored_value_is_no_host_key`, seed 108631 raised a keyless
join). -/
theorem stored_value_is_no_host_key :
    let forms : List (Option String × Option Int) :=
      [(some "broadleaf", some 7), (some "linden", some 2), (none, none)]
    let species : List (String × Option String) :=
      [("Acer", some "broadleaf"), ("Quercus", some "broadleaf"),
        ("Tilia", some "linden"), ("Ulmus", none)]
    forms.length = 3 ∧ species.length = 4 ∧
      (species.map fun s => (forms.filter (·.1 == s.2)).length).all (· == 1) := by
  decide

/-- `city in (city ? city = 'B')`: the set's feeder, grouped on `city`, holds
one row per city in the set. Joined as a row source on that city, it pairs
every kept row with the feeder's row and adds nothing; it is read as a
subselect, never as rows (`set_column_is_no_row`, seeds 5754, 32125 raised an
unplanned merge source). -/
theorem set_column_is_no_row :
    let rows : List (String × Int) := [("B", 7), ("B", 2)]
    let feeder : List String := ["B"]
    (rows.filter fun r => feeder.contains r.1) = rows := by
  decide

/-! ## Gate keys owed at FINAL, sets' condition columns, lookup bridges (0.3.527)

The `compositions` sweep from 200,000 to 260,000. -/

/-- `select city, fc, ... where max(h) by species < 6`: the pair `(A, b)`
holds Acer (max 7) and Quercus (max 5), so the gate keeps one and rejects
the other. It is no function of the pair: re-applied at FINAL, it needs a
side holding `species` (`gate_key_owed_at_final`, seed 203313 raised a
keyless join when the side asked to carry `species` was computed off grouped
rows and held none). -/
theorem gate_key_owed_at_final :
    let pairA_b : List (String × Option Int) := [("Acer", some 7), ("Quercus", some 5)]
    let gate : Option Int → Bool := fun m => match m with
      | some v => v < 6
      | none => false
    (pairA_b.map (gate ·.2)) = [false, true] := by
  decide

/-- `species in (species ? tree_form = 'broadleaf') and tree_form =
'broadleaf'`: the set's feeder carries `tree_form`, the column its own
condition reads, for the species it kept. Beside the enrichment rows that
supply `tree_form`, it adds no row and changes no value; it is read as a
subselect (`set_condition_column_is_no_row`, seed 227275 raised an
unplanned merge source). -/
theorem set_condition_column_is_no_row :
    let enrichment : List (String × Option String) :=
      [("Acer", some "broadleaf"), ("Quercus", some "broadleaf"),
        ("Tilia", some "linden"), ("Ulmus", none)]
    let feeder := enrichment.filter (·.2 == some "broadleaf")
    let kept := enrichment.filter fun r => r.2 == some "broadleaf" && feeder.contains r
    kept = feeder := by
  decide

/-- `select species, sum(h) by form, rank species order by ...`: the
species scan, widened to `tree_form`, keeps one row per species, since a
lookup adds none. It pairs the window by species with the value by form
(`lookup_bridge_adds_no_rows`, seed 234282 raised a keyless join between
them). -/
theorem lookup_bridge_adds_no_rows :
    let species := ["Acer", "Quercus", "Tilia", "Ulmus"]
    let form : String → Option String := fun s =>
      if s == "Tilia" then some "linden" else if s == "Ulmus" then none
      else some "broadleaf"
    (species.map fun s => (s, form s)).length = species.length := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A derived key read off groups holds only their keys

`select city, fc, count(id) by fc, min(h ? max(h) by city, tree_form >= 8)`
with `fc` a CASE over `tree_form`: the groups by `(city, tree_form)` hold
`tree_form`, so `fc` can be computed off them. But `count(id) by fc` pairs
`fc` with the trees on `species`, which those groups dropped: paired on
nothing, every tree meets every `fc`
(`derived_key_off_groups_pairs_on_nothing`). Off the rows that hold `species`
each tree meets its own. The same blend arises under a filter that reads two
values at different grains: `blend_under_a_finer_reader_invents_pairs` holds
for a `?` as for a BASIC. -/

/-- Trees as `(id, species)`, `fc` as a function of species. -/
theorem derived_key_off_groups_pairs_on_nothing :
    let trees : List (Nat × Nat) := [(1, 0), (2, 0), (3, 1)]
    let fc : Nat → Nat := fun s => s % 2
    let offGroups := dedup (trees.map fun t => fc t.2)
    let offRows := dedup (trees.map fun t => (t.2, fc t.2))
    (trees.flatMap fun _ => offGroups).length = 6 ∧
      (trees.flatMap fun t => offRows.filter fun r => r.1 == t.2).length = 3 := by
  decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A WHERE is applied once

A HAVING on a column the select does not project is a semijoin on the
select's keys: the keys of the kept rows the predicate holds on. Those rows
are the WHERE's, so the set is planned under it. Trilogy also wrote the WHERE
into the set's own filter. A row predicate applied twice is itself
(`row_where_idempotent`); one reading a set or a total is not, since the copy
reads them off the rows the first application kept
(`where_over_total_not_idempotent`): `where tag in (tag ? count(id) by * -
sum(h) by species > 3) having species in (...)` counted four trees for six
and dropped every group. -/

theorem row_where_idempotent {α : Type} (p : α → Bool) (rows : List α) :
    (rows.filter p).filter p = rows.filter p := by
  simp [List.filter_filter]

/-- Keep the rows below the count of the rows read. -/
def keepBelowCount (rows : List Nat) : List Nat := rows.filter fun v => v < rows.length

theorem where_over_total_not_idempotent :
    keepBelowCount [1, 2, 3] = [1, 2] ∧
      keepBelowCount (keepBelowCount [1, 2, 3]) = [1] := by decide

end Trilogy.SelectGrain

namespace Trilogy.SelectGrain

/-! ## A property shown without its key -/

/-- `select tree_form as f where tree_form in (<set>) or tag = 'x'`: a
property shown without its key is its distinct values. The plan grouped the
rows by the property's key, `species`, hidden, and showed broadleaf once per
broadleaf species (`hidden_key_repeats_property`). A filtered value's own
keys are the one hidden grouping a select keeps (`select name, bulk_name` is
a row per product). -/
theorem hidden_key_repeats_property :
    -- (species, tree_form): Acer and Quercus are broadleaf
    let species : List (Nat × Nat) := [(0, 0), (1, 0), (2, 1)]
    (species.map (·.2)).eraseDups.length = 2 ∧
      (species.eraseDups.map (·.2)).length = 3 := by
  decide

end Trilogy.SelectGrain
