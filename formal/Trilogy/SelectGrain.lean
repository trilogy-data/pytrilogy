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
- Two pins Trilogy applied to a window's order aggregate give another value:
  its first argument alone (`first_argument_pin_misreads`), and the tuple
  the window ranks (`window_tuple_pin_misreads`), for a select at a finer
  grain than that tuple.
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

end Trilogy.SelectGrain
