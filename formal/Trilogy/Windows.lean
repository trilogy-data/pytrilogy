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
