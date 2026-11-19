/-!
# Identity: a planned source's identifier

A merge folds two parents with one identifier into one relation
(`QueryDatasource.__add__`), so an identifier must name one relation. It is
spelled from its members: a join lists them, `_join_` between, and every
source appends its grain (`_at_g`). A source wrapping one other appends its
grain to the other's.

- `naive_wrap_of_join_collides`: unbracketed, wrapping a join and joining a
  wrapped side are one spelling. Spellings 404253 folded the two FINAL parents
  so spelled, kept one join of the pair, and rendered a column off a CTE the
  FINAL never joined.
- `bracketed_injective_bounded`: with a join's members bracketed, no two
  sources up to depth 2 (the colliding pair's) share a spelling.

Checked by `tests/discovery/test_sweep_expr_500k_shapes.py`.
-/

namespace Trilogy
namespace Identity

inductive Tok where
  | name | join | at | lp | rp
  deriving DecidableEq, Repr

/-- A source: a table, one source regrouped or projected, or a join of two.
Names and grains are one each: the collision is in the shape. -/
inductive Src where
  | leaf
  | wrap (s : Src)
  | join (a b : Src)
  deriving DecidableEq, Repr

def naive : Src → List Tok
  | .leaf => [.name]
  | .wrap s => naive s ++ [.at]
  | .join a b => naive a ++ [.join] ++ naive b ++ [.at]

def bracketed : Src → List Tok
  | .leaf => [.name]
  | .wrap s => bracketed s ++ [.at]
  | .join a b => [.lp] ++ bracketed a ++ [.join] ++ bracketed b ++ [.rp, .at]

theorem naive_wrap_of_join_collides :
    Src.wrap (.join .leaf .leaf) ≠ Src.join .leaf (.wrap .leaf) ∧
      naive (.wrap (.join .leaf .leaf)) = naive (.join .leaf (.wrap .leaf)) := by
  decide

theorem bracketed_separates :
    bracketed (.wrap (.join .leaf .leaf)) ≠ bracketed (.join .leaf (.wrap .leaf)) := by
  decide

/-- Every source of depth at most `n`. -/
def upTo : Nat → List Src
  | 0 => [.leaf]
  | n + 1 =>
    let prev := upTo n
    .leaf :: prev.map .wrap ++ prev.flatMap (fun a => prev.map (.join a))

theorem bracketed_injective_bounded :
    ∀ a ∈ upTo 2, ∀ b ∈ upTo 2, bracketed a = bracketed b → a = b := by
  decide +kernel

end Identity
end Trilogy
