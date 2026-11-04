/-!
# Set operators: `except`, `intersect`, `union`

The table-valued `except(...)` and `intersect(...)` are SQL set operators: a
row of the result occurs once, and two rows match when every column is equal
or both are NULL. `not in` is a comparison, so a NULL on either side makes it
UNKNOWN and the row is dropped.

- `except_keeps_no_match`, `intersect_iff`: a value is in `A except B` iff it
  is in `A` and matches nothing in `B`, NULL included; in `A intersect B` iff
  it is in both.
- `except_fold`: three arms fold left, `(A except B) except C`.
- `not_in_drops_null`: `x not in B` drops a NULL `x` that `except` keeps, and
  a NULL in `B` drops every `x` (`not_in_null_member_drops_all`).

Checked by `tests/setops/test_setops_laws.py` on DuckDB and SQLite.
-/

namespace Trilogy
namespace SetOps

/-- `EXCEPT`: the distinct values of `a` that match no value of `b`. -/
def except' (a b : List (Option Int)) : List (Option Int) :=
  (a.filter (· ∉ b)).eraseDups

def intersect (a b : List (Option Int)) : List (Option Int) :=
  (a.filter (· ∈ b)).eraseDups

theorem except_keeps_no_match (a b : List (Option Int)) (x : Option Int) :
    x ∈ except' a b ↔ x ∈ a ∧ x ∉ b := by
  simp [except', List.mem_filter]

theorem intersect_iff (a b : List (Option Int)) (x : Option Int) :
    x ∈ intersect a b ↔ x ∈ a ∧ x ∈ b := by
  simp [intersect, List.mem_filter]

theorem except_fold (a b c : List (Option Int)) (x : Option Int) :
    x ∈ except' (except' a b) c ↔ x ∈ a ∧ x ∉ b ∧ x ∉ c := by
  simp only [except_keeps_no_match, and_assoc]

/-- Kleene `x not in b`: UNKNOWN (`none`) when `x` or a member is NULL and no
member equals it. -/
def notIn (x : Option Int) (b : List (Option Int)) : Option Bool :=
  match x with
  | none => if b = [] then some true else none
  | some v =>
    if b.contains (some v) then some false
    else if b.contains none then none else some true

/-- A WHERE keeps a row only when its condition is TRUE. -/
def whereNotIn (a b : List (Option Int)) : List (Option Int) :=
  (a.filter (notIn · b == some true)).eraseDups

theorem not_in_drops_null :
    none ∈ except' [none, some 1] [some 2] ∧
      none ∉ whereNotIn [none, some 1] [some 2] := by decide

theorem not_in_null_member_drops_all :
    except' [some 1] [none] = [some 1] ∧ whereNotIn [some 1] [none] = [] := by decide

end SetOps
end Trilogy
