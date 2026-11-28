/-!
# Lookup: a WHERE on a dimension's column, read off a fact

A fact row reaches a dimension's column through its key, which may be
missing (`~`) or NULL (`?`) on the fact. Reading the column is a lookup: the
fact's rows, each with the dimension's value or a pad. A WHERE on that column
keeps the fact rows whose value passes, so it filters and never adds a row.

- `where_on_lookup_is_inner`: the WHERE over the lookup is the INNER join
  under it, whether or not the fact's key is NULL on some rows.
- `full_lookup_adds_dimension_rows`: a FULL join keeps the dimension's rows
  no fact row holds; one that passes the WHERE is a row with every fact
  column NULL (tpc-ds q83's spurious `(NULL, NULL)` group).
- `zero_test_ignores_repeats`: a test on a count that holds alike for every
  count of one or more (`> 0`, `>= 1`, `= 0`, `!= 0`) reads `count(x)` and
  `count(distinct x)` alike, since both are zero on the same rows;
  `exact_test_reads_repeats`: `= 1` does not.

Checked by `tests/discovery/test_dimension_filter_lookup.py`.
-/

namespace Trilogy
namespace Lookup

/-- A fact row: its dimension key (NULL or absent as `none`) and a measure. -/
abbrev Fact := Option Int × Int

/-- The dimension: the keys it holds and the column read off each. -/
structure Dim where
  keys : List Int
  col : Int → Int

def Dim.read (d : Dim) (k : Option Int) : Option Int :=
  k.bind fun k => if k ∈ d.keys then some (d.col k) else none

/-- A NULL fails a WHERE. -/
def passes (p : Int → Bool) : Option Int → Bool
  | some v => p v
  | none => false

@[simp] theorem passes_none (p : Int → Bool) : passes p none = false := rfl

@[simp] theorem passes_some (p : Int → Bool) (v : Int) : passes p (some v) = p v := rfl

/-- Every fact row, with the dimension's column or a pad. -/
def lookup (d : Dim) (facts : List Fact) : List (Option Int × Option Int) :=
  facts.map fun (k, m) => (d.read k, some m)

/-- The fact rows with a dimension partner. -/
def inner (d : Dim) (facts : List Fact) : List (Option Int × Option Int) :=
  facts.filterMap fun (k, m) => (d.read k).map fun v => (some v, some m)

/-- The lookup, and a row of padded fact columns per dimension key no fact
row holds. -/
def full (d : Dim) (facts : List Fact) : List (Option Int × Option Int) :=
  lookup d facts ++
    (d.keys.filter fun k => !facts.any fun f => f.1 == some k).map
      fun k => (some (d.col k), none)

def where_ (p : Int → Bool) (rows : List (Option Int × Option Int)) :=
  rows.filter fun r => passes p r.1

theorem where_on_lookup_is_inner (p : Int → Bool) (d : Dim) (facts : List Fact) :
    where_ p (lookup d facts) = where_ p (inner d facts) := by
  induction facts with
  | nil => rfl
  | cons f rest ih =>
    obtain ⟨k, m⟩ := f
    simp only [where_, lookup, inner, List.map_cons, List.filterMap_cons] at ih ⊢
    cases h : d.read k with
    | none => simpa [List.filter_cons, h] using ih
    | some v => by_cases hp : p v <;> simpa [List.filter_cons, h, hp] using ih

/-- Week 10 holds days 1 and 2, week 99 day 3; no return is on day 3, and one
is on no day. -/
def days : Dim := ⟨[1, 2, 3], fun k => if k = 3 then 99 else 10⟩

def returns : List Fact := [(some 1, 5), (none, 3), (some 2, 4)]

theorem full_lookup_adds_dimension_rows :
    where_ (fun w => w = 10 || w = 99) (full days returns) =
        where_ (fun w => w = 10 || w = 99) (lookup days returns) ++ [(some 99, none)] ∧
      where_ (fun w => w = 10 || w = 99) (lookup days returns) =
        [(some 10, some 5), (some 10, some 4)] := by
  decide

/-- The distinct values of a list. -/
def distinct : List Int → List Int
  | [] => []
  | x :: xs => if x ∈ distinct xs then distinct xs else x :: distinct xs

def count (xs : List (Option Int)) : Nat := (xs.filterMap id).length

def countDistinct (xs : List (Option Int)) : Nat := (distinct (xs.filterMap id)).length

theorem distinct_nil_iff (xs : List Int) : distinct xs = [] ↔ xs = [] := by
  induction xs with
  | nil => simp [distinct]
  | cons x rest ih =>
    simp only [distinct, reduceCtorEq, iff_false]
    split
    · rename_i h
      intro hd
      rw [hd] at h
      simp at h
    · simp

theorem count_zero_iff (xs : List (Option Int)) :
    countDistinct xs = 0 ↔ count xs = 0 := by
  simp only [countDistinct, count, List.length_eq_zero_iff, distinct_nil_iff]

theorem zero_test_ignores_repeats (f : Nat → Bool) (hf : ∀ n, 1 ≤ n → f n = f 1)
    (xs : List (Option Int)) : f (countDistinct xs) = f (count xs) := by
  by_cases h : count xs = 0
  · rw [h, (count_zero_iff xs).mpr h]
  · have hd : countDistinct xs ≠ 0 := fun hd => h ((count_zero_iff xs).mp hd)
    rw [hf _ (Nat.one_le_iff_ne_zero.mpr hd), hf _ (Nat.one_le_iff_ne_zero.mpr h)]

/-- Order 1 on two of an item's lines. -/
def orders : List (Option Int) := [some 1, some 1]

theorem exact_test_reads_repeats :
    (countDistinct orders == 1) = true ∧ (count orders == 1) = false := by
  decide

end Lookup
end Trilogy
