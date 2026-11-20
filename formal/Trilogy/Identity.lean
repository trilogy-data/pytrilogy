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

- `twin_scans_share_one_read`: two projections of one table under one
  condition are projections of a single scan carrying both, so a scan
  filtered at plan time and its pushdown-filtered twin fold into one
  (`MergeTwinScans`; expressions 362641, 364578).
- `group_reads_twin_rows`: an aggregate of the table under a condition is the
  aggregate of its row twin's read, so a grouped scan beside an ungrouped one
  under one WHERE groups the twin instead of reading the table again (a `by *`
  total beside the select's rows, expressions 500002).
- `grouped_twin_drops_repeats`: the twin must be ungrouped; grouping a
  deduplicated read undercounts repeated rows.
- `filter_reads_whole_twin`: a scan under a condition over columns `c` is the
  unconditioned twin's read, carrying `c`, filtered by the same condition, so a
  filtered scan beside a whole-table scan reads the table once (expressions
  650115: a WHERE's `by` aggregates read every row, the select its kept rows).

Checked by `tests/discovery/test_sweep_expr_500k_shapes.py` and
`tests/optimization/test_merge_twin_scans.py`.
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

/-- One read of `rows` under `p`, carrying what `f` and `g` project. -/
def twinRead {α β γ : Type} (rows : List α) (p : α → Bool) (f : α → β) (g : α → γ) :
    List (β × γ) :=
  (rows.filter p).map fun r => (f r, g r)

theorem twin_scans_share_one_read {α β γ : Type} (rows : List α) (p : α → Bool)
    (f : α → β) (g : α → γ) :
    (rows.filter p).map f = (twinRead rows p f g).map Prod.fst ∧
      (rows.filter p).map g = (twinRead rows p f g).map Prod.snd := by
  simp [twinRead, List.map_map, Function.comp_def]

theorem group_reads_twin_rows {α β γ δ : Type} (rows : List α) (p : α → Bool)
    (f : α → β) (g : α → γ) (agg : List β → δ) :
    agg ((rows.filter p).map f) = agg ((twinRead rows p f g).map Prod.fst) := by
  rw [(twin_scans_share_one_read rows p f g).1]

theorem filter_reads_whole_twin {α β κ : Type} (rows : List α) (c : α → κ)
    (q : κ → Bool) (f : α → β) :
    (rows.filter (fun r => q (c r))).map f =
      ((twinRead rows (fun _ => true) f c).filter (fun x => q x.2)).map Prod.fst := by
  induction rows with
  | nil => rfl
  | cons r rs ih => by_cases h : q (c r) = true <;> simp_all [twinRead]

theorem grouped_twin_drops_repeats :
    ([1, 1].filter (fun _ => true)).length ≠ ([1, 1].eraseDups.filter (fun _ => true)).length := by
  decide

end Identity
end Trilogy
