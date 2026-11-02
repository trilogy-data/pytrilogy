/-!
# Ordering NULLs, and numbering windows over their argument's values

**NULL placement.** Trilogy sorts NULLs last in both directions unless an
ordering says `nulls first`; a bare `asc` / `desc` and `nulls auto` mean the
same. Engines disagree on the bare form: DuckDB, Presto and ClickHouse sort
NULLs last both ways, SQLite, MySQL, SQL Server and BigQuery treat NULL as the
smallest value, Postgres and Snowflake as the largest. MySQL and SQL Server have
no `NULLS FIRST` / `LAST` syntax, so a placement is a leading null-flag term.

- `render_exact`: on every engine, the rendered terms order any two values as
  Trilogy's ordering does. A bare direction renders bare only where the
  engine's default is Trilogy's (`bare_asc_differs`: SQLite put a NULL first),
  and a flag alone does not order the values (`flag_alone_ties`).

**Numbering windows.** `rank x order by ...` ranks the distinct values of its
argument (with its partition and order fields): the value stream, not the rows
`x` lives on. So a rank is a function of those values and joins back to any
row stream on them.

- `rank_dup_invariant`: repeating a value in the stream leaves every rank
  unchanged, so the rank does not depend on which other columns a select
  projects. Ranking the row stream instead does (`row_stream_rank_differs`:
  `select id, v, rank v` gave NULL rank 5 and `select v, rank v` gave 4).
- `join_unique_keeps_count`: joining rows to a table holding one entry per
  value keeps the row count. Joining them to the row-level window output on
  the value meets each row once per row sharing it (`row_output_join_fans_out`:
  7 rows became 15).
-/

namespace Trilogy
namespace Window

/-! ## NULL placement -/

/-- An ordering: direction and whether NULLs come first. -/
structure Ordering where
  desc : Bool
  nullsFirst : Bool
  deriving DecidableEq, Repr

/-- `x` sorts strictly before `y` under a direction and NULL placement. -/
def cmp (desc nullsFirst : Bool) : Option Int → Option Int → Bool
  | none, none => false
  | none, some _ => nullsFirst
  | some _, none => !nullsFirst
  | some a, some b => if desc then decide (b < a) else decide (a < b)

/-- Trilogy's order for an ordering. -/
def want (o : Ordering) : Option Int → Option Int → Bool := cmp o.desc o.nullsFirst

structure Engine where
  /-- where a bare direction puts NULLs, indexed by descending -/
  ascNullsFirst : Bool
  descNullsFirst : Bool
  /-- accepts `NULLS FIRST` / `NULLS LAST` -/
  placementSyntax : Bool

def Engine.default (e : Engine) (desc : Bool) : Bool :=
  if desc then e.descNullsFirst else e.ascNullsFirst

def duckdb : Engine := ⟨false, false, true⟩
def sqlite : Engine := ⟨true, false, true⟩
def postgres : Engine := ⟨false, true, true⟩
def mysql : Engine := ⟨true, false, false⟩

/-- One rendered ORDER BY term. -/
inductive Term where
  /-- a bare direction: the engine places NULLs -/
  | bare (desc : Bool)
  | explicit (desc nullsFirst : Bool)
  /-- `x IS NULL`, sorted descending iff NULLs go first -/
  | nullFlag (nullsFirst : Bool)

def Term.before (e : Engine) : Term → Option Int → Option Int → Bool
  | .bare d, x, y => cmp d (e.default d) x y
  | .explicit d nf, x, y => cmp d nf x y
  | .nullFlag nf, x, y => x.isNone != y.isNone && (x.isNone == nf)

def Term.ties : Term → Option Int → Option Int → Bool
  | .nullFlag _, x, y => x.isNone == y.isNone
  | _, x, y => x == y

/-- Lexicographic order over a term list. -/
def before (e : Engine) : List Term → Option Int → Option Int → Bool
  | [], _, _ => false
  | t :: ts, x, y => t.before e x y || (t.ties x y && before e ts x y)

/-- `BaseDialect.render_ordering`. -/
def render (e : Engine) (o : Ordering) : List Term :=
  if o.nullsFirst == e.default o.desc then [.bare o.desc]
  else if e.placementSyntax then [.explicit o.desc o.nullsFirst]
  else [.nullFlag o.nullsFirst, .bare o.desc]

theorem render_exact (e : Engine) (o : Ordering) (x y : Option Int) :
    before e (render e o) x y = want o x y := by
  obtain ⟨af, df, syn⟩ := e
  obtain ⟨d, nf⟩ := o
  cases x <;> cases y <;> cases af <;> cases df <;> cases syn <;> cases d <;> cases nf <;>
    simp [render, before, want, Term.before, Term.ties, cmp, Engine.default]

/-- A bare `asc` on SQLite sorted a NULL before 0. -/
theorem bare_asc_differs :
    before sqlite [.bare false] none (some 0) = true ∧
      want ⟨false, false⟩ none (some 0) = false := by decide

/-- Postgres's bare `desc` put NULLs first. -/
theorem bare_desc_differs :
    before postgres [.bare true] none (some 0) = true ∧
      want ⟨true, false⟩ none (some 0) = false := by decide

/-- The null flag alone ties every pair of values. -/
theorem flag_alone_ties :
    before mysql [.nullFlag false] (some 1) (some 0) = false ∧
      want ⟨false, false⟩ (some 0) (some 1) = true := by decide

/-! ## Numbering windows over the value stream -/

/-- Keeps the last copy of each value. -/
def dedup : List (Option Int) → List (Option Int)
  | [] => []
  | a :: t => if a ∈ t then dedup t else a :: dedup t

/-- `rank`: one more than the number of distinct stream values before `x`. -/
def rank (o : Ordering) (stream : List (Option Int)) (x : Option Int) : Nat :=
  1 + (dedup (stream.filter (want o · x))).length

/-- SQL's `rank()` over a row stream: one more than the rows before `x`. -/
def rowRank (o : Ordering) (rows : List (Option Int)) (x : Option Int) : Nat :=
  1 + (rows.filter (want o · x)).length

theorem dedup_append_new (t : List (Option Int)) (a : Option Int) (h : a ∉ t) :
    (dedup (t ++ [a])).length = (dedup t).length + 1 := by
  induction t with
  | nil => simp [dedup]
  | cons b s ih =>
    have hab : a ≠ b := fun e => h (e ▸ List.mem_cons_self ..)
    have has : a ∉ s := fun m => h (List.mem_cons_of_mem _ m)
    have hb : (b ∈ s ++ [a]) ↔ b ∈ s := by
      simp only [List.mem_append, List.mem_singleton]
      exact ⟨fun m => m.elim id (fun e => absurd e.symm hab), Or.inl⟩
    by_cases m : b ∈ s
    · simp only [List.cons_append, dedup, hb.mpr m, m, ↓reduceIte]
      exact ih has
    · have m' : b ∉ s ++ [a] := fun x => m (hb.mp x)
      simp only [List.cons_append, dedup, m', m, ↓reduceIte, List.length_cons]
      rw [ih has]

theorem dedup_append_mem (l : List (Option Int)) (y : Option Int) (h : y ∈ l) :
    (dedup (l ++ [y])).length = (dedup l).length := by
  induction l with
  | nil => simp at h
  | cons a t ih =>
    by_cases hy : y ∈ t
    · have ha : (a ∈ t ++ [y]) ↔ a ∈ t := by
        simp only [List.mem_append, List.mem_singleton]
        exact ⟨fun m => m.elim id (fun e => e ▸ hy), Or.inl⟩
      by_cases m : a ∈ t
      · simp only [List.cons_append, dedup, ha.mpr m, m, ↓reduceIte]
        exact ih hy
      · have m' : a ∉ t ++ [y] := fun x => m (ha.mp x)
        simp only [List.cons_append, dedup, m', m, ↓reduceIte, List.length_cons]
        rw [ih hy]
    · have hya : y = a := by
        cases List.mem_cons.mp h with
        | inl e => exact e
        | inr e => exact absurd e hy
      subst hya
      have m : y ∈ t ++ [y] := List.mem_append_right _ (List.mem_singleton_self _)
      simp only [List.cons_append, dedup, m, hy, ↓reduceIte, List.length_cons]
      exact dedup_append_new t y hy

/-- Repeating a stream value leaves every rank unchanged. -/
theorem rank_dup_invariant (o : Ordering) (stream : List (Option Int))
    (y x : Option Int) (h : y ∈ stream) :
    rank o (stream ++ [y]) x = rank o stream x := by
  unfold rank
  rw [List.filter_append]
  by_cases p : want o y x = true
  · have hm : y ∈ stream.filter (want o · x) := List.mem_filter.mpr ⟨h, p⟩
    simp only [List.filter_cons, p, ↓reduceIte, List.filter_nil]
    rw [dedup_append_mem _ _ hm]
  · simp [p]

/-- `v` over the rows `id` 1..7 of the probe: `[10, NULL, 10, 5, NULL, 7, NULL]`. -/
def probeRows : List (Option Int) :=
  [some 10, none, some 10, some 5, none, some 7, none]

/-- Ranked over the rows, a NULL `v` is 5th; over the values, 4th. -/
theorem row_stream_rank_differs :
    rowRank ⟨false, false⟩ probeRows none = 5 ∧
      rank ⟨false, false⟩ probeRows none = 4 := by decide

/-- Join rows to a table of `(value, payload)` on the value, NULL matching NULL. -/
def joinOn (rows : List (Option Int)) (table : List (Option Int × Nat)) :
    List (Option Int × Nat) :=
  rows.flatMap fun v => table.filter (·.1 == v)

theorem join_unique_keeps_count (rows : List (Option Int))
    (table : List (Option Int × Nat))
    (h : ∀ v ∈ rows, (table.filter (·.1 == v)).length = 1) :
    (joinOn rows table).length = rows.length := by
  induction rows with
  | nil => rfl
  | cons v t ih =>
    simp only [joinOn, List.flatMap_cons, List.length_append, List.length_cons] at *
    rw [h v (List.mem_cons_self ..), ih (fun w m => h w (List.mem_cons_of_mem _ m))]
    omega

/-- The row-level window output: one `(v, rank)` entry per row. -/
def rowOutput (o : Ordering) (rows : List (Option Int)) : List (Option Int × Nat) :=
  rows.map fun v => (v, rowRank o rows v)

/-- Joined back on `v`, the probe's 7 rows became 15. -/
theorem row_output_join_fans_out :
    (joinOn probeRows (rowOutput ⟨false, false⟩ probeRows)).length = 15 := by decide

end Window
end Trilogy
