/-!
# Windows: what a window ranks, and where NULLs sort

A window is evaluated over the rows of its own grain. A numbering window
(`rank(a, ...) over (partition by p order by o)`) ranks the distinct tuples of
its arguments, partition and order columns; its value is then read back onto
every row that holds the tuple. An aggregate window with no order
(`count(x) over (partition by p)`) is the aggregate `by p`, and an empty
`over ()` is the aggregate over every row, `by *`.

- `rank_reads_values`: a rank over the grain's distinct values depends only on
  which values occur, so any stream holding the same values ranks the same.
  Ranking a stream that repeats a value (the rows of a finer key the select
  also asks for) counts each repeat (`row_stream_rank_counts_repeats`), and
  `rank_of_distinct` says the stream rank is right when it holds each value
  once.
- `lookup_keeps_rows`: reading a value back from a table unique on its key
  keeps one row per row. Reading it from the stream, which repeats its key,
  multiplies each row by the repeats (`stream_join_fans_out`).
- `empty_over_is_total`: `count(x) over ()` counts every row. Partitioned by
  the select's grain (`id`), as Trilogy read it, it counts one row's `x`
  (`grain_count_is_not_total`).
- `regroup_by_value_exact`: grouping rows by a window's value is the re-sum
  of the groups of the tuple it is a function of, over the tuples of that
  value. Grouping by the tuple and projecting the value splits a value into
  a group per tuple (`group_by_tuple_splits_value`), and a value window is
  no function of its operand (`lag_reads_rows_not_values`).
- An aggregate a window orders by: one with no `by` is at the select's raw
  grain, like any other (`SelectGrain.bare_is_select_grain`). When that grain
  holds the partition it reads only the partition's rows
  (`order_aggregate_per_partition`); pinned to the window's first argument
  it read every partition's (`anchor_sum_crosses_partitions`). One `by`
  fixed keys makes the window a value of those keys, so the window's keys
  must hold them or the read-back fans out (`order_keys_dropped_fans_out`).

ORDER BY names a direction and a NULL placement. A plain direction places
NULLs last, DuckDB's placement; `nulls auto` leaves it to the engine. Engines
disagree on where an unplaced ORDER BY sorts NULLs: last (DuckDB, Presto,
ClickHouse), smallest (SQLite, MySQL, SQL Server, BigQuery) or largest
(Postgres, Snowflake).

- `render_places`: the rendering places NULLs as asked on every engine, with
  NULLS FIRST/LAST where it parses and a leading NULL-flag term where it does
  not (MySQL, SQL Server).
- `bare_direction_misplaces`: rendering the direction alone, as Trilogy did,
  put SQLite's NULLs first under a plain `asc`, so a window ordered on a
  nullable column ranked differently per engine.
-/

namespace Trilogy.Windows

/-! ## Ranking the distinct values of a grain -/

/-- `rank` of `v` over the distinct values in `xs`, all below `bound`: one plus
the number of distinct values below `v`. At its own grain the order key has no
ties, so this is `rank` and `dense_rank` alike. -/
def rankD (bound : Nat) (xs : List Nat) (v : Nat) : Nat :=
  ((List.range bound).filter fun u => decide (u < v) && decide (u ∈ xs)).length + 1

/-- SQL `rank` over a row stream: one plus the rows ordered strictly before. -/
def streamRank (xs : List Nat) (v : Nat) : Nat :=
  (xs.filter fun u => decide (u < v)).length + 1

theorem rank_reads_values (bound : Nat) (xs ys : List Nat) (v : Nat)
    (h : ∀ u, u ∈ xs ↔ u ∈ ys) : rankD bound xs v = rankD bound ys v := by
  unfold rankD
  congr 2
  apply List.filter_congr
  intro u _
  simp [h u]

theorem rank_of_distinct (bound : Nat) (xs : List Nat) (v : Nat)
    (hnd : xs.Nodup) (hb : ∀ u ∈ xs, u < bound) :
    streamRank xs v = rankD bound xs v := by
  unfold streamRank rankD
  congr 1
  have hperm : xs.Perm ((List.range bound).filter fun u => decide (u ∈ xs)) := by
    apply (List.perm_ext_iff_of_nodup hnd ((List.nodup_range).filter _)).mpr
    intro u
    simp only [List.mem_filter, List.mem_range, decide_eq_true_eq]
    exact ⟨fun hu => ⟨hb u hu, hu⟩, fun h => h.2⟩
  rw [(hperm.filter fun u => decide (u < v)).length_eq, List.filter_filter]

/-- Rows `[a, a, b]` (`a = 0`, `b = 1`): `b` ranks 2 among the distinct
values, 3 among the rows. -/
theorem row_stream_rank_counts_repeats :
    rankD 2 [0, 0, 1] 1 = 2 ∧ streamRank [0, 0, 1] 1 = 3 := by decide

/-! ## Reading the value back onto the rows -/

/-- Each row's key paired with every table row on that key. -/
def lookup (rows : List Nat) (table : List (Nat × Nat)) : List (Nat × Nat) :=
  rows.flatMap fun k => (table.filter fun t => t.1 == k).map fun t => (k, t.2)

theorem lookup_keeps_rows (rows : List Nat) (table : List (Nat × Nat))
    (h : ∀ k ∈ rows, (table.filter fun t => t.1 == k).length = 1) :
    (lookup rows table).length = rows.length := by
  induction rows with
  | nil => rfl
  | cons k rest ih =>
    simp only [lookup, List.flatMap_cons, List.length_append, List.length_map,
      List.length_cons] at ih ⊢
    rw [h k (by simp), ih fun k' hk => h k' (by simp [hk])]
    omega

/-- The stream, ranked per row and joined back on the value: each of the two
`a` rows meets both `a` ranks. -/
theorem stream_join_fans_out :
    let stream := [0, 0, 1].map fun u => (u, streamRank [0, 0, 1] u)
    (lookup [0, 0, 1] stream).length = 5 := by decide

/-! ## An empty `over ()` -/

/-- `count(x) over (partition by p)` on row `r`: the non-NULL `x` among the
rows sharing `r`'s partition key. -/
def countOver (rows : List (Nat × Option Int)) (p : Nat → Nat) (r : Nat) : Nat :=
  (rows.filter fun t => p t.1 == p r && t.2.isSome).length

/-- `over ()` is one partition holding every row: the total. -/
theorem empty_over_is_total (rows : List (Nat × Option Int)) (r : Nat) :
    countOver rows (fun _ => 0) r = (rows.filter (·.2.isSome)).length := by
  simp [countOver]

/-- Read at the select's grain (one `id`), the count is that row's `x`. -/
theorem grain_count_is_not_total :
    let rows : List (Nat × Option Int) := [(0, some 1), (1, none), (2, some 3)]
    countOver rows (fun _ => 0) 1 = 2 ∧ countOver rows id 1 = 0 := by decide

/-! ## Grouping by a window's value

`select r, count(id)` groups the rows by `r`'s value. A window is a value of
its grain's tuple (`rank(x) over (partition by g ...)` is a function of
`(x, g)`), but grouping by that tuple and projecting the value is not
grouping by the value: two tuples of one rank are two groups
(`group_by_tuple_splits_value`). A value window is not even a function of its
operand: `lag(x)` differs on two rows with the same `x`
(`lag_reads_rows_not_values`). Trilogy grouped by both: a window in a select
gave the statement its keys in its place. The exact regrouping is the re-sum
of the tuple's groups over the tuples of one value (`regroup_by_value_exact`),
and grouping by the value itself is that. -/

/-- The rows whose `w` is `v`: the count of `v`'s group. -/
def countBy {α β : Type} [DecidableEq β] (w : α → β) (rows : List α) (v : β) : Nat :=
  (rows.filter fun r => w r = v).length

theorem countBy_cons {α β : Type} [DecidableEq β] (w : α → β) (r : α) (rows : List α)
    (v : β) : countBy w (r :: rows) v = (if w r = v then 1 else 0) + countBy w rows v := by
  unfold countBy
  rw [List.filter_cons]
  by_cases h : w r = v <;> simp [h] <;> omega

theorem sum_indicator_nodup {κ : Type} [DecidableEq κ] (a : κ) (us : List κ)
    (hnd : us.Nodup) :
    (us.map fun u => if a = u then 1 else 0).sum = if a ∈ us then 1 else 0 := by
  induction us with
  | nil => simp
  | cons u rest ih =>
    have ⟨hu, hrest⟩ := List.nodup_cons.mp hnd
    simp only [List.map_cons, List.sum_cons, ih hrest, List.mem_cons]
    by_cases h : a = u
    · subst h; simp [hu]
    · simp [h]

theorem sum_map_add {κ : Type} (us : List κ) (f g : κ → Nat) :
    (us.map fun u => f u + g u).sum = (us.map f).sum + (us.map g).sum := by
  induction us with
  | nil => rfl
  | cons u rest ih => simp only [List.map_cons, List.sum_cons, ih]; omega

/-- **Grouping by a function of a tuple is the re-sum of the tuple's groups**
over the tuples it maps to `v`: the count of `v` is never one group's count. -/
theorem regroup_by_value_exact {α κ β : Type} [DecidableEq κ] [DecidableEq β]
    (k : α → κ) (f : κ → β) (ks : List κ) (hnd : ks.Nodup) (rows : List α)
    (hcov : ∀ r ∈ rows, k r ∈ ks) (v : β) :
    countBy (fun r => f (k r)) rows v =
      ((ks.filter fun u => f u = v).map (countBy k rows)).sum := by
  induction rows with
  | nil =>
    have e : countBy k ([] : List α) = fun _ => 0 := funext fun _ => rfl
    rw [e]
    simp only [countBy, List.filter_nil, List.length_nil]
    induction (ks.filter fun u => f u = v) with
    | nil => rfl
    | cons _ _ ih' => simp only [List.map_cons, List.sum_cons, ← ih']
  | cons r rest ih =>
    rw [countBy_cons, ih fun r' h => hcov r' (List.mem_cons_of_mem _ h)]
    have e : countBy k (r :: rest) = fun u => (if k r = u then 1 else 0) + countBy k rest u :=
      funext fun u => countBy_cons k r rest u
    rw [e, sum_map_add, sum_indicator_nodup _ _ (hnd.filter _)]
    have hk := hcov r List.mem_cons_self
    by_cases h : f (k r) = v <;> simp [h, hk]

/-- `rank(x) over (partition by g order by x)` over rows `(g, x)`:
`(a, 1)` and `(b, 1)` both rank 1. Grouped by the tuple, rank 1 is two
groups of one row each; grouped by the value, one group of two. -/
theorem group_by_tuple_splits_value :
    let rows : List (Nat × Nat) := [(0, 1), (1, 1)]
    let rank : Nat × Nat → Nat := fun _ => 1
    ((rows.eraseDups.filter fun t => rank t = 1).map (countBy id rows)) = [1, 1] ∧
      countBy (fun t => rank (id t)) rows 1 = 2 := by decide

/-- `lag(x) over (order by id)` over `x = [10, 10, 20]`: the two rows with
`x = 10` lag `none` and `10`, so `x` does not determine the lag. -/
theorem lag_reads_rows_not_values :
    let xs : List Nat := [10, 10, 20]
    let lag : Nat → Option Nat := fun i => if i = 0 then none else xs[i - 1]?
    xs[0]? = xs[1]? ∧ lag 0 ≠ lag 1 := by decide

/-! ## An aggregate a window orders by

`rank(g) over (partition by p order by sum(y))` orders by `sum(y)` at the
select's grain, as if it were written `sum(y) by <select grain>`
(`SelectGrain.bare_is_select_grain`; the window does not give it a grain of
its own). In `select p, g, ...` that grain is `(p, g)`, so the sum reads only
the rows of partition `p` (`order_aggregate_per_partition`). Trilogy pinned
it to the window's first argument, summing over every partition, so `b` tied
`a` inside a partition where it led (`anchor_sum_crosses_partitions`); 0.3.459
pinned it to the tuple the window ranks instead, which is the select grain
only when the select is no finer (`SelectGrain.window_tuple_pin_misreads`).

A window ordered by an aggregate `by` fixed keys (`sum(y) by g, p`) is a value
of those keys. Its keys must include them: read as `{g}` alone — the parser
hydrated the window before the name it orders by — each row met every rank of
its `g` (`order_keys_dropped_fans_out`). -/

/-- The sum of `y` over the rows `keep` holds. -/
def sumWhere {α : Type} (keep : α → Bool) (y : α → Nat) (rows : List α) : Nat :=
  ((rows.filter keep).map y).sum

/-- **Grouped by a grain holding the partition, the aggregate reads only the
partition's rows**: summing per `(p, g)` reads exactly the rows of `p`. -/
theorem order_aggregate_per_partition {α π γ : Type} [DecidableEq π] [DecidableEq γ]
    (p : α → π) (g : α → γ) (y : α → Nat) (rows : List α) (p0 : π) (g0 : γ) :
    sumWhere (fun r => decide (p r = p0) && decide (g r = g0)) y rows =
      sumWhere (fun r => decide (g r = g0)) y (rows.filter fun r => decide (p r = p0)) := by
  unfold sumWhere
  rw [List.filter_filter]
  congr 3
  funext r
  exact Bool.and_comm _ _

/-- Rows `(p, g, y)`: partition `Q` holds `a = 20` and `b = 25`, partition `P`
holds `a = 10` and `b = 5`. Summed per `g` alone, `a` and `b` both total 30 and
tie inside `Q`; per `(p, g)`, `b` leads `Q`. -/
theorem anchor_sum_crosses_partitions :
    let rows : List (Nat × Nat × Nat) := [(0, 0, 10), (1, 0, 20), (0, 1, 5), (1, 1, 25)]
    let y : Nat × Nat × Nat → Nat := fun r => r.2.2
    let perG (g0 : Nat) := sumWhere (fun r => decide (r.2.1 = g0)) y rows
    let perPG (g0 : Nat) :=
      sumWhere (fun r => decide (r.1 = 1) && decide (r.2.1 = g0)) y rows
    perG 0 = perG 1 ∧ perPG 0 < perPG 1 := by decide

/-- Ranks per `(g, p)` read back on `g` alone: each of four rows meets both
ranks of its `g`. -/
theorem order_keys_dropped_fans_out :
    let ranks : List (Nat × Nat) := [(0, 2), (0, 3), (1, 1), (1, 4)]
    (lookup [0, 0, 1, 1] ranks).length = 8 := by decide

/-! ## Where NULLs sort -/

/-- An engine's placement when ORDER BY names none. -/
inductive NullSort
  | last
  | smallest
  | largest
  deriving DecidableEq, Repr

def NullSort.nullsFirst : NullSort → Bool → Bool
  | .last, _ => false
  | .smallest, desc => !desc
  | .largest, desc => desc

/-- An ORDER BY term: a direction and the placement asked for (`none` is
`nulls auto`). A plain direction is `some false`: NULLs last. -/
structure Order where
  desc : Bool
  nullsFirst : Option Bool
  deriving DecidableEq, Repr

structure Engine where
  sort : NullSort
  parses : Bool
  deriving DecidableEq, Repr

/-- What is rendered: the direction, an explicit placement, and a leading
`x IS NULL` flag term sorted so NULLs go first (`true`) or last. -/
structure Rendered where
  desc : Bool
  explicit : Option Bool
  flag : Option Bool
  deriving DecidableEq, Repr

def render (e : Engine) (o : Order) : Rendered :=
  match o.nullsFirst with
  | none => ⟨o.desc, none, none⟩
  | some first =>
    if first = e.sort.nullsFirst o.desc then ⟨o.desc, none, none⟩
    else if e.parses then ⟨o.desc, some first, none⟩
    else ⟨o.desc, none, some first⟩

/-- Where NULLs land when an engine runs a rendering. -/
def Rendered.placement (e : Engine) (r : Rendered) : Bool :=
  match r.flag, r.explicit with
  | some f, _ => f
  | none, some x => x
  | none, none => e.sort.nullsFirst r.desc

theorem render_places (e : Engine) (o : Order) (first : Bool)
    (h : o.nullsFirst = some first) : (render e o).placement e = first := by
  unfold render
  rw [h]
  by_cases hd : first = e.sort.nullsFirst o.desc
  · simp [hd, Rendered.placement]
  · by_cases hs : e.parses <;> simp [hd, hs, Rendered.placement]

/-- `nulls auto` keeps the engine's own placement. -/
theorem auto_is_engine_default (e : Engine) (desc : Bool) :
    (render e ⟨desc, none⟩).placement e = e.sort.nullsFirst desc := rfl

def sqlite : Engine := { sort := .smallest, parses := true }
def duckdb : Engine := { sort := .last, parses := true }

/-- The direction alone: SQLite places a plain `asc`'s NULLs first, DuckDB
last. -/
theorem bare_direction_misplaces :
    let bare : Rendered := ⟨false, none, none⟩
    bare.placement sqlite = true ∧ bare.placement duckdb = false := by decide

end Trilogy.Windows
