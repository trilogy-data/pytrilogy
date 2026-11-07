/-!
# Padding: what an outer join puts in a row it could not match

A left join keeps every left row; a left row with no match on the right gets
the right side's columns padded. The language reads that pad as NULL on every
engine, so an aggregate over a right column skips it, and `count(r)` counts
only the matches. ClickHouse pads with each type's default (`0`, `''`) unless
`join_use_nulls` is set; with it, the pad is NULL there too.

- `null_pad_count_eq_matches`: with a NULL pad, `count` of a right column is
  the number of matched rows whose value is present.
- `null_pad_sum_eq_matches`: a NULL pad adds nothing to a `sum`.
- `default_pad_counts_unmatched`: a `0` pad counts a left row with no match,
  and `default_pad_max_raises`: it is the `max` of a right column whose values
  are all negative.
- `cast_keeps_null`: a cast is strict, NULL on a NULL, which a cast to a
  NOT NULL type (ClickHouse's bare `Int64`) cannot represent.

Checked by `tests/engine/test_clickhouse_null_laws.py` on chdb, and by the
window and select-grain law suites, which run on DuckDB, SQLite and ClickHouse.
-/

namespace Trilogy
namespace Padding

/-- A left row and the right values its key matched. -/
abbrev Left := List (Option Int)

/-- The right column of a left join: each match, or `pad` for a row with none. -/
def leftJoin (pad : Option Int) (rows : List Left) : List (Option Int) :=
  rows.flatMap fun m => if m.isEmpty then [pad] else m

def count (xs : List (Option Int)) : Nat := (xs.filter Option.isSome).length

def sum (xs : List (Option Int)) : Int := (xs.filterMap id).foldl (· + ·) 0

def max? (xs : List (Option Int)) : Option Int :=
  (xs.filterMap id).foldl (fun a b => some (match a with | none => b | some a => Max.max a b)) none

theorem null_pad_count_eq_matches (rows : List Left) :
    count (leftJoin none rows) = count rows.flatten := by
  induction rows with
  | nil => rfl
  | cons m rest ih =>
    simp only [count, leftJoin, List.flatMap_cons, List.flatten_cons,
      List.filter_append, List.length_append] at ih ⊢
    rw [ih]
    cases m <;> simp

theorem null_pad_filterMap (rows : List Left) :
    (leftJoin none rows).filterMap id = rows.flatten.filterMap id := by
  induction rows with
  | nil => rfl
  | cons m rest ih =>
    simp only [leftJoin, List.flatMap_cons, List.flatten_cons,
      List.filterMap_append] at ih ⊢
    rw [ih]
    cases m <;> simp

theorem null_pad_sum_eq_matches (rows : List Left) :
    sum (leftJoin none rows) = sum rows.flatten := by
  simp only [sum, null_pad_filterMap]

/-- A customer with one order and one with none. -/
def orders : List Left := [[some 10], []]

theorem default_pad_counts_unmatched :
    count (leftJoin (some 0) orders) = 2 ∧ count (leftJoin none orders) = 1 := by
  decide

theorem default_pad_max_raises :
    max? (leftJoin (some 0) [[some (-3)], []]) = some 0 ∧
      max? (leftJoin none [[some (-3)], []]) = some (-3) := by
  decide

/-- A cast to a type that holds NULL, and what it does to a value. -/
def cast (f : Int → Int) : Option Int → Option Int := Option.map f

theorem cast_keeps_null (f : Int → Int) : cast f none = none := rfl

theorem cast_present (f : Int → Int) (v : Int) : cast f (some v) = some (f v) := rfl

end Padding
end Trilogy
