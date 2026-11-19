/-!
# Nullability: which computed values can be NULL

A plan claims, per column of each relation, whether it can hold NULL; join
planning reads the claim to pick `=` or a null-safe `is not distinct from`.
A claim that a value is never NULL is a promise every join on it keeps its
rows.

- `max_of_null_group_is_null`: an aggregate other than a count is NULL on a
  group whose argument values are all NULL, though the group has rows; so a
  `max` over a nullable column is nullable, and so is `max(x) + 1`
  (`plus_of_null_is_null`).
- `count_of_null_group_is_zero`: a count of such a group is 0, never NULL.
- `plain_rejoin_drops_null_value`: a relation joined back to itself on a
  NULL-valued column with `=` loses that row; `null_safe_rejoin_keeps_rows`:
  with `is not distinct from` it keeps every row. Expressions 645783 re-joined
  a `(city, tree_form)` group on `max(height) by city, tree_form + 1`,
  claimed non-null, and dropped a4.

Checked by `tests/discovery/test_sweep_expr_650k_shapes.py`.
-/

namespace Trilogy
namespace Nullability

def max? (xs : List (Option Int)) : Option Int :=
  (xs.filterMap id).foldl (fun m x => some (match m with | none => x | some y => max x y)) none

def count (xs : List (Option Int)) : Nat := (xs.filter Option.isSome).length

/-- A group of two rows whose argument values are both NULL. -/
def nullGroup : List (Option Int) := [none, none]

theorem max_of_null_group_is_null : nullGroup.length > 0 ∧ max? nullGroup = none := by
  decide

theorem plus_of_null_is_null : (max? nullGroup).map (· + 1) = none := by
  decide

theorem count_of_null_group_is_zero : count nullGroup = 0 := by
  decide

/-- SQL `=`: unknown, so false, when either side is NULL. -/
def plainEq : Option Int → Option Int → Bool
  | some a, some b => a == b
  | _, _ => false

/-- A relation joined back to itself on `key`, row by row. -/
def rejoin (eq : Option Int → Option Int → Bool) (rows : List (Option Int)) :
    List (Option Int) :=
  rows.filter fun r => eq r r

theorem plain_rejoin_drops_null_value :
    rejoin plainEq [some 6, none] = [some 6] := by
  decide

theorem null_safe_rejoin_keeps_rows (rows : List (Option Int)) :
    rejoin (· == ·) rows = rows := by
  simp [rejoin]

end Nullability
end Trilogy
