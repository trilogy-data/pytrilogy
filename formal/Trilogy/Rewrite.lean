import Trilogy.NullReject
import Trilogy.Agg
import Trilogy.Rows

/-!
# Optimizer rule soundness: each rewrite keeps the rows

`Fixpoint.lean` proves the optimizer stops; this module proves the rules it
runs keep the rows a CTE returns, each under the guard the rule checks, with a
counterexample showing the guard is needed.

- `strip_sound`: `StripRedundantNotNull` drops `x is not null` from a WHERE
  whose input has no NULL `x`. Its facts (`_nullable_before_own_filter`,
  `_inner_join_rejected`) are read before the WHERE: a preserved side keeps
  its columns non-null (`left_side_non_null`), a plain-equality INNER pair
  holds its keys non-null (`inner_key_non_null`), and a padded side does not
  (`padded_side_breaks_strip`), nor does a later FULL join
  (`later_full_pads_inner_key`). Judging the WHERE by the nullability that
  very WHERE narrowed would strip the only thing keeping `x` non-null
  (`own_filter_strip_unsound`).
- `full_to_left`, `left_to_inner`: `UpgradeJoinOnGuards` makes an outer join
  stricter when the WHERE fails on every row the stricter join drops. A
  disjunction forces a side when every disjunct proves some column of that
  side, not necessarily the same one (`or_forces_side`); one disjunct with
  no proof keeps the padded row (`or_unproved_disjunct_keeps_padded`).
- `push_inner`, `push_preserved`, `push_padded`: `PredicatePushdown` copies a
  WHERE atom into a parent. Into an INNER join's side or an outer join's
  preserved side it commutes; into a padded side only when the atom fails on
  the all-NULL row (`_predicate_safe_past_null_extension`), else a row padded
  by the push passes it (`push_null_accepting_past_padding`). Dropping the
  consumer's copy needs the parent unpadded (`remove_past_padding`). Into a
  GROUP BY only on its keys (`push_group_key`, `push_group_nonkey_changes_sum`),
  past a window only on its partition (`push_window_partition`,
  `push_window_order_changes`), never past a LIMIT (`push_past_limit`).
- `pad_strict`, `pad_null_sensitive`: a scan computing a value for its own
  rows hands a padded row NULL. That is the value the join would compute only
  for a strict expression. `ret is not null` is `false` on the padded row, so
  the planner computes a null-sensitive value after the join
  (`network_build._candidate`).
- `kept_exact`, `absent_accepting_keeps_rejected`: a WHERE over an aggregate
  hosted as the aggregate's HAVING reaches the other rows through a join to
  the groups it kept. That decides it only when the atom is false on an empty
  group; `count(...) = 0` is not, so a key the HAVING rejected reads as one
  with no rows.
- `fk_anchor_keeps_padded`: dropping a `~` for one query is `left_to_inner`,
  and a WHERE on an order column passes the order whose customer the `~`
  table lacks.
- `grouped_filter_fans_out`: a filtered value computed per row and grouped
  with its keys splits a key into a value group and a NULL group.
- `alias_free`, `alias_shadowed`: a HAVING names a SELECT alias, but a FROM
  column of the same name binds first, so the alias is safe only where every
  FROM column is known (`base._having_aliases`).
-/

namespace Trilogy.Rewrite

open Trilogy.B3 Trilogy.NullReject

/-! ## Joins with either side padded -/

/-- A joined row: either side may be the all-NULL padding. A consumer reads
left column `i` at `2i` and right column `i` at `2i+1`. -/
structure FRow where
  l : Option Row
  r : Option Row

def FRow.view (j : FRow) : Row := fun n =>
  if n % 2 = 0 then j.l.bind (· (n / 2)) else j.r.bind (· (n / 2))

def hits (on : Row → Row → Bool) (l : Row) (R : List Row) : List FRow :=
  (R.filter (on l)).map fun r => ⟨some l, some r⟩

def innerJ (on : Row → Row → Bool) (L R : List Row) : List FRow :=
  L.flatMap fun l => hits on l R

def pad (l : Row) : FRow := ⟨some l, none⟩

/-- A left row's rows in a LEFT join: its matches, or itself padded. -/
def leftRows (on : Row → Row → Bool) (l : Row) (R : List Row) : List FRow :=
  if (R.filter (on l)).isEmpty then [pad l] else hits on l R

def leftJ (on : Row → Row → Bool) (L R : List Row) : List FRow :=
  L.flatMap fun l => leftRows on l R

/-- The right rows no left row matched, padded on the left. -/
def rightOnly (on : Row → Row → Bool) (L R : List Row) : List FRow :=
  (R.filter fun r => !L.any fun l => on l r).map fun r => ⟨none, some r⟩

def fullJ (on : Row → Row → Bool) (L R : List Row) : List FRow :=
  leftJ on L R ++ rightOnly on L R

/-- Plain equality: a NULL key matches nothing. -/
def eqOn (i k : Nat) (l r : Row) : Bool :=
  match l i, r k with
  | some a, some b => a == b
  | _, _ => false

def nulls : Row := fun _ => none

theorem view_left_pad (r : Option Row) (i : Nat) : (FRow.view ⟨none, r⟩) (2 * i) = none := by
  simp [FRow.view, Nat.mul_mod_right]

theorem view_right_pad (l : Option Row) (i : Nat) :
    (FRow.view ⟨l, none⟩) (2 * i + 1) = none := by
  simp [FRow.view, Nat.add_mod]

/-- A row that is padded on the right reads NULL at every right column. -/
def RightPadded (v : Row) : Prop := ∀ n, n % 2 = 1 → v n = none

def LeftPadded (v : Row) : Prop := ∀ n, n % 2 = 0 → v n = none

theorem right_padded (l : Option Row) : RightPadded (FRow.view ⟨l, none⟩) := by
  intro n hn
  simp [FRow.view, hn]

theorem left_padded (r : Option Row) : LeftPadded (FRow.view ⟨none, r⟩) := by
  intro n hn
  simp [FRow.view, hn]

/-! ## `StripRedundantNotNull` -/

def keepB (p : Row → Bool) (js : List FRow) : List FRow := js.filter fun j => p j.view

/-- Dropping `x is not null` from a WHERE over rows with no NULL `x`. -/
theorem strip_sound (js : List FRow) (i : Nat) (p : Row → Bool)
    (h : ∀ j ∈ js, j.view i ≠ none) :
    keepB (fun v => (v i).isSome && p v) js = keepB p js := by
  apply List.filter_congr
  intro j hj
  cases hv : j.view i with
  | none => exact absurd hv (h j hj)
  | some _ => simp [hv]

theorem mem_hits {on : Row → Row → Bool} {l : Row} {R : List Row} {j : FRow}
    (h : j ∈ hits on l R) : j.l = some l ∧ ∃ r ∈ R, on l r = true ∧ j.r = some r := by
  simp only [hits, List.mem_map, List.mem_filter] at h
  obtain ⟨r, ⟨hr, ho⟩, rfl⟩ := h
  exact ⟨rfl, r, hr, ho, rfl⟩

theorem hits_nil {on : Row → Row → Bool} {l : Row} {R : List Row}
    (h : (R.filter (on l)).isEmpty = true) : hits on l R = [] := by
  simp [hits, List.isEmpty_iff.mp h]

/-- A filter failing on the padded row sees only the matches. -/
theorem filter_leftRows (on : Row → Row → Bool) (l : Row) (R : List Row)
    (F : FRow → Bool) (hF : F (pad l) = false) :
    (leftRows on l R).filter F = (hits on l R).filter F := by
  unfold leftRows
  split
  · rename_i h
    simp [hits_nil h, hF]
  · rfl

theorem leftRows_l {on : Row → Row → Bool} {l : Row} {R : List Row} :
    ∀ j ∈ leftRows on l R, j.l = some l := by
  intro j hj
  unfold leftRows at hj
  split at hj
  · simp only [List.mem_singleton] at hj
    rw [hj]
    rfl
  · exact (mem_hits hj).1

theorem mem_leftJ {on : Row → Row → Bool} {L R : List Row} {j : FRow}
    (h : j ∈ leftJ on L R) : ∃ l ∈ L, j.l = some l := by
  simp only [leftJ, List.mem_flatMap] at h
  obtain ⟨l, hl, hj⟩ := h
  exact ⟨l, hl, leftRows_l j hj⟩

/-- `_nullable_before_own_filter`: the preserved side's non-null column stays
non-null through a left join. -/
theorem left_side_non_null (on : Row → Row → Bool) (L R : List Row) (i : Nat)
    (hL : ∀ l ∈ L, l i ≠ none) : ∀ j ∈ leftJ on L R, j.view (2 * i) ≠ none := by
  intro j hj
  obtain ⟨l, hl, he⟩ := mem_leftJ hj
  simp only [FRow.view, Nat.mul_mod_right, ↓reduceIte, he, Option.bind_some,
    Nat.mul_div_cancel_left i (by decide : 0 < 2)]
  exact hL l hl

/-- `_inner_join_rejected`: a plain-equality INNER pair holds both keys
non-null on every row. -/
theorem inner_key_non_null (L R : List Row) (i k : Nat) :
    ∀ j ∈ innerJ (eqOn i k) L R, j.view (2 * i) ≠ none ∧ j.view (2 * k + 1) ≠ none := by
  intro j hj
  simp only [innerJ, List.mem_flatMap] at hj
  obtain ⟨l, _, hm⟩ := hj
  obtain ⟨hl, r, _, ho, hr⟩ := mem_hits hm
  have hv0 : j.view (2 * i) = l i := by
    simp [FRow.view, hl, Nat.mul_mod_right]
  have hk : (2 * k + 1) / 2 = k := by omega
  have hv1 : j.view (2 * k + 1) = r k := by
    simp [FRow.view, hr, Nat.add_mod, hk]
  rw [hv0, hv1]
  simp only [eqOn] at ho
  cases hli : l i <;> cases hrk : r k <;> simp_all

def one : Row := fun _ => some 1
def two : Row := fun _ => some 2

/-- A padded side's column is NULL on the padded row, however non-null its
table holds it: stripping `x is not null` there keeps that row. -/
theorem padded_side_breaks_strip :
    (keepB (fun v => (v 1).isSome) (leftJ (eqOn 0 0) [one] [two])).length = 0 ∧
      (keepB (fun _ => true) (leftJ (eqOn 0 0) [one] [two])).length = 1 := by
  decide

/-- A later FULL join pads the left key an earlier INNER pair held non-null
(`_inner_join_rejected` skips a pair any later RIGHT/FULL join follows). -/
theorem later_full_pads_inner_key :
    (fullJ (eqOn 0 0) [one] [two]).map (fun j => j.view 0) = [some 1, none] := by
  decide

/-- Judging the WHERE by nullability the WHERE itself narrowed: after
`x is not null` the CTE's `x` is non-null, so a strip trusting that drops
the very atom that made it so, and the NULL row returns. -/
theorem own_filter_strip_unsound :
    let js : List FRow := [⟨some nulls, none⟩]
    let narrowed := keepB (fun v => (v 0).isSome) js
    (∀ j ∈ narrowed, j.view 0 ≠ none) ∧ narrowed.length = 0 ∧ (keepB (fun _ => true) js).length = 1 := by
  decide

/-! ## `UpgradeJoinOnGuards` -/

/-- The FULL join's right-only rows are padded on the left. -/
theorem rightOnly_left_padded (on : Row → Row → Bool) (L R : List Row) :
    ∀ j ∈ rightOnly on L R, LeftPadded j.view := by
  intro j hj
  simp only [rightOnly, List.mem_map] at hj
  obtain ⟨_, _, rfl⟩ := hj
  exact left_padded _

/-- FULL to LEFT: a WHERE failing on every left-padded row drops exactly the
right-only rows the LEFT join never had. -/
theorem full_to_left (on : Row → Row → Bool) (L R : List Row) (p : Row → Bool)
    (hp : ∀ v, LeftPadded v → p v = false) :
    keepB p (fullJ on L R) = keepB p (leftJ on L R) := by
  have h : (rightOnly on L R).filter (fun j => p j.view) = [] :=
    List.filter_eq_nil_iff.mpr fun j hj => by
      simpa using hp _ (rightOnly_left_padded on L R j hj)
  simp only [keepB, fullJ, List.filter_append, h, List.append_nil]

/-- LEFT to INNER: a WHERE failing on every right-padded row drops exactly
the rows the INNER join never had. -/
theorem left_to_inner (on : Row → Row → Bool) (L R : List Row) (p : Row → Bool)
    (hp : ∀ v, RightPadded v → p v = false) :
    keepB p (leftJ on L R) = keepB p (innerJ on L R) := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [keepB, leftJ, innerJ, List.flatMap_cons, List.filter_append] at ih ⊢
    rw [ih, filter_leftRows on l R _ (hp _ (right_padded _))]

/-- `side_forced_by_or`: an OR forces the right side when each disjunct
proves some right column non-null, each its own. -/
theorem or_forces_side (p q : Cond) (hp : p.Sound) (hq : q.Sound) (i k : Nat)
    (hi : 2 * i + 1 ∈ p.proofs) (hk : 2 * k + 1 ∈ q.proofs) :
    ∀ v, RightPadded v → isT ((Cond.or p q).eval v) = false := by
  intro v hv
  have h1 := proofs_sound v p hp _ hi (hv _ (by omega))
  have h2 := proofs_sound v q hq _ hk (hv _ (by omega))
  simp only [Cond.eval]
  revert h1 h2
  cases p.eval v <;> cases q.eval v <;> simp [holds, isT, or3]

/-- `x > 0 or y = 1` with `x` right and `y` left: the padded row passes on `y`. -/
theorem or_unproved_disjunct_keeps_padded :
    let c := Cond.or (.cmp .lt (.lit 0) (.col 1)) (.cmp .eq (.col 0) (.lit 1))
    isT (c.eval (FRow.view ⟨some one, none⟩)) = true := by
  decide

/-! ## `PredicatePushdown` and `PredicatePushdownRemove` -/

/-- An atom reading only the left row. -/
def onLeft (q : Row → Bool) (j : FRow) : Bool := j.l.any q

/-- Into an INNER join's side. -/
theorem push_inner (on : Row → Row → Bool) (L R : List Row) (q : Row → Bool) :
    (innerJ on L R).filter (onLeft q) = innerJ on (L.filter q) R := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [innerJ, List.flatMap_cons, List.filter_append, List.filter_cons] at ih ⊢
    rw [ih]
    cases hq : q l
    · simp only [Bool.false_eq_true, ↓reduceIte]
      rw [List.filter_eq_nil_iff.mpr, List.nil_append]
      intro j hj
      simp [onLeft, (mem_hits hj).1, hq]
    · simp only [↓reduceIte, List.flatMap_cons]
      congr 1
      rw [List.filter_eq_self.mpr]
      intro j hj
      simp [onLeft, (mem_hits hj).1, hq]

/-- Into an outer join's preserved side. -/
theorem push_preserved (on : Row → Row → Bool) (L R : List Row) (q : Row → Bool) :
    (leftJ on L R).filter (onLeft q) = leftJ on (L.filter q) R := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [leftJ, List.flatMap_cons, List.filter_append, List.filter_cons] at ih ⊢
    rw [ih]
    cases hq : q l
    · simp only [Bool.false_eq_true, ↓reduceIte]
      rw [List.filter_eq_nil_iff.mpr, List.nil_append]
      intro j hj
      simp [onLeft, leftRows_l j hj, hq]
    · simp only [↓reduceIte, List.flatMap_cons]
      congr 1
      rw [List.filter_eq_self.mpr]
      intro j hj
      simp [onLeft, leftRows_l j hj, hq]

/-- An atom reading the right row; `q none` is its value on the padded row. -/
def onRight (q : Option Row → Bool) (j : FRow) : Bool := q j.r

theorem filter_hits (on : Row → Row → Bool) (l : Row) (R : List Row) (q : Row → Bool) :
    (hits on l R).filter (onRight (fun r => r.any q)) = hits on l (R.filter q) := by
  simp only [hits, List.filter_map, Function.comp_def, onRight, Option.any_some]
  congr 1
  rw [List.filter_filter, List.filter_filter]
  apply List.filter_congr
  intro r _
  exact Bool.and_comm _ _

theorem filter_idem (q : Row → Bool) (R : List Row) : (R.filter q).filter q = R.filter q :=
  List.filter_eq_self.mpr fun _ hr => (List.mem_filter.mp hr).2

/-- Into an outer join's padded side, the consumer keeping its copy: equal
when the atom fails on the padded row. -/
theorem push_padded (on : Row → Row → Bool) (L R : List Row) (q : Row → Bool) :
    (leftJ on L R).filter (onRight (fun r => r.any q)) =
      (leftJ on L (R.filter q)).filter (onRight (fun r => r.any q)) := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [leftJ, List.flatMap_cons, List.filter_append] at ih ⊢
    have hF : onRight (fun r => r.any q) (pad l) = false := by simp [onRight, pad]
    rw [ih, filter_leftRows on l R _ hF, filter_leftRows on l (R.filter q) _ hF,
      filter_hits, filter_hits, filter_idem]

/-- `cost is null` passes on the padded row: pushed into the padded side it
turns a row whose match fails into a padded row the consumer keeps. -/
theorem push_null_accepting_past_padding :
    let q : Option Row → Bool := fun r => (r.bind (· 0)).isNone || r.any (fun r => r 0 == some 1)
    let push := fun (r : Row) => q (some r)
    ((leftJ (fun _ _ => true) [one] [two]).filter (onRight q)).length = 0 ∧
      ((leftJ (fun _ _ => true) [one] ([two].filter push)).filter (onRight q)).length = 1 := by
  decide

/-- `PredicatePushdownRemove` drops the consumer's copy once the parent
filters: sound over an INNER join, unsound over the padded side, whose
padded rows the consumer's copy was rejecting. -/
theorem remove_inner (on : Row → Row → Bool) (L R : List Row) (q : Row → Bool) :
    (innerJ on L (R.filter q)).filter (onRight (fun r => r.any q)) =
      innerJ on L (R.filter q) := by
  apply List.filter_eq_self.mpr
  intro j hj
  simp only [innerJ, List.mem_flatMap] at hj
  obtain ⟨l, _, hm⟩ := hj
  obtain ⟨_, r, hr, _, he⟩ := mem_hits hm
  simp only [List.mem_filter] at hr
  simp [onRight, he, hr.2]

theorem remove_past_padding :
    let q : Row → Bool := fun r => r 0 == some 1
    ((leftJ (fun _ _ => true) [one] ([two].filter q)).filter
        (onRight (fun r => r.any q))).length = 0 ∧
      (leftJ (fun _ _ => true) [one] ([two].filter q)).length = 1 := by
  decide

/-- Into a GROUP BY: an atom on the key drops whole groups, each kept group's
aggregate unchanged. -/
theorem push_group_key {α κ : Type} [DecidableEq κ] (key : α → κ) (v : α → Int)
    (q : κ → Bool) (rows : List α) (k : κ) :
    sumBy v (slice key k (rows.filter (q ∘ key))) =
      if q k then sumBy v (slice key k rows) else 0 := by
  simp only [slice, List.filter_filter]
  split
  · rename_i hk
    congr 1
    apply List.filter_congr
    intro r _
    by_cases h : key r = k <;> simp [h, hk]
  · rename_i hk
    rw [List.filter_eq_nil_iff.mpr]
    · rfl
    · intro r _
      by_cases h : key r = k <;> simp_all

/-- An atom on a non-key column filters rows inside a group. -/
theorem push_group_nonkey_changes_sum :
    let rows : List (Nat × Int) := [(0, 1), (0, 5)]
    sumBy Prod.snd (slice Prod.fst 0 (rows.filter fun r => r.2 > 2)) ≠
      sumBy Prod.snd (slice Prod.fst 0 rows) := by
  decide

/-- Past a window: an atom on the partition keeps each kept row's partition,
so its window value. -/
theorem push_window_partition {α κ : Type} [DecidableEq κ] (key : α → κ)
    (q : κ → Bool) (rows : List α) (k : κ) (hk : q k = true) :
    slice key k (rows.filter (q ∘ key)) = slice key k rows := by
  simp only [slice, List.filter_filter]
  apply List.filter_congr
  intro r _
  by_cases h : key r = k <;> simp [h, hk]

/-- An atom on the window's ORDER BY column shrinks the partition:
`count(*) over (partition by k)` reads 2 before and 1 after. -/
theorem push_window_order_changes :
    let rows : List (Nat × Int) := [(0, 1), (0, 5)]
    (slice Prod.fst 0 (rows.filter fun r => r.2 > 2)).length ≠ (slice Prod.fst 0 rows).length := by
  decide

/-- Filtering before a LIMIT changes which rows fill it. -/
theorem push_past_limit :
    ([1, 2, 3].take 1).filter (· > 1) ≠ ([1, 2, 3].filter (· > 1)).take 1 := by
  decide

/-! ## A value a partial scan computes for its own rows -/

/-- Computed in the scan, then padded: NULL where the scan has no row. -/
def computeThenPad (f : Row → Val) : Option Row → Val
  | none => none
  | some r => f r

/-- Computed after the join, from the padded columns. -/
def padThenCompute (f : Row → Val) (r : Option Row) : Val := f (r.getD nulls)

/-- A strict expression is NULL on the all-NULL row, so either place agrees. -/
theorem pad_strict (f : Row → Val) (hf : f nulls = none) (r : Option Row) :
    computeThenPad f r = padThenCompute f r := by
  cases r <;> simp [computeThenPad, padThenCompute, hf]

/-- `returned <- rqty is not null` holds `false` on an order no return lists;
computed in the returns scan it pads to NULL, so `returned = false` lost the
order (`network_build._candidate` leaves a null-sensitive value it computes
partially unbound, and the planner computes it after the join). -/
theorem pad_null_sensitive :
    let flag : Row → Val := fun r => some (if (r 0).isSome then 1 else 0)
    computeThenPad flag none = none ∧ padThenCompute flag none = some 0 := by
  decide

/-! ## A WHERE over an aggregate, decided through a join -/

/-- `count(x) by k`: the facts under key `k`. -/
def countOf (facts : List Nat) (k : Nat) : Nat := (facts.filter (· == k)).length

/-- The aggregate branch with the atom applied as a HAVING: the keys it keeps. -/
def keptKeys (P : Nat → Bool) (groups facts : List Nat) : List Nat :=
  groups.filter fun k => P (countOf facts k)

/-- A row's key joined to the kept groups, preserving the row and reading a
missing group's count as 0 (the zero-fill). -/
def viaKept (P : Nat → Bool) (groups facts : List Nat) (k : Nat) : Bool :=
  if k ∈ keptKeys P groups facts then true else P 0

/-- The atom on the row's own count. -/
def onCount (P : Nat → Bool) (facts : List Nat) (k : Nat) : Bool := P (countOf facts k)

/-- An atom false on an empty group is decided by the join to the groups it
kept: a key missing there either had no rows or failed the atom. -/
theorem kept_exact (P : Nat → Bool) (groups facts : List Nat) (k : Nat)
    (h0 : P 0 = false) (hg : 0 < countOf facts k → k ∈ groups) :
    viaKept P groups facts k = onCount P facts k := by
  unfold viaKept onCount
  by_cases hk : k ∈ keptKeys P groups facts
  · simp only [hk, ↓reduceIte]
    simp only [keptKeys, List.mem_filter] at hk
    exact hk.2.symm
  · simp only [hk, ↓reduceIte, h0]
    by_cases hc : countOf facts k = 0
    · rw [hc, h0]
    · have hmem := hg (Nat.pos_of_ne_zero hc)
      simp only [keptKeys, List.mem_filter, not_and] at hk
      cases hP : P (countOf facts k)
      · rfl
      · exact absurd hP (by simpa using hk hmem)

/-- `count(order_id) by cust_id = 0` holds on an empty group, so a key the
HAVING rejected (count 1) reads as an empty one and the row is kept
(`condition_placement.holds_on_absent_group` restates it at FINAL, over the
unfiltered counts). -/
theorem absent_accepting_keeps_rejected :
    viaKept (· == 0) [1] [1] 1 = true ∧ onCount (· == 0) [1] 1 = false := by
  decide

/-! ## Dropping a `~` for one query (`partial_bridging.decide_heal`) -/

def order (cust amt : Int) : Row := fun i => if i = 0 then some cust else some amt

/-- The heal is `left_to_inner`: sound when the WHERE fails on every row the
`~` table pads. A WHERE on the order's own column passes the padded row of an
order whose customer has no note, so an order carrying the customer anchors
it (`_pair_siblings`), as a fact keyed by it does. -/
theorem fk_anchor_keeps_padded :
    let ws := fun (v : Row) => decide ((v 2).getD 0 > 0)
    (keepB ws (leftJ (eqOn 0 0) [order 2 7] [order 1 0])).length = 1 ∧
      (keepB ws (innerJ (eqOn 0 0) [order 2 7] [order 1 0])).length = 0 := by
  decide

/-! ## Narrowing a FULL by a proof on its key -/

/-- A `~` right side whose every row has a left partner (the left holds the
key's whole domain, `complete_key_domain`) adds no right-only row: the FULL
is the LEFT join, so the left's columns are never padded
(`grain_utility.narrow_subset_full_join`). -/
theorem subset_full_is_left (on : Row → Row → Bool) (L R : List Row)
    (h : ∀ r ∈ R, L.any (fun l => on l r) = true) : fullJ on L R = leftJ on L R := by
  have hr : rightOnly on L R = [] := by
    simp only [rightOnly, List.map_eq_nil_iff, List.filter_eq_nil_iff]
    intro r hrR
    simp [h r hrR]
  simp [fullJ, hr]

/-- A WHERE proving the key non-null forces a side only where every key value
has a match there. A `~` lookup keyed by the key (notes by customer) lacks
members the other side holds: `cust_id != 8` passes the order of a customer
with no note, and the INNER join drops it (`grain_utility._partial_lookup_keys`). -/
theorem key_proof_keeps_lookup_gap :
    let key := fun (v : Row) => decide ((v 0).or (v 1) ≠ some 8)
    (keepB key (fullJ (eqOn 0 0) [one] [two])).length = 2 ∧
      (keepB key (innerJ (eqOn 0 0) [one] [two])).length = 0 := by
  decide

/-! ## A row-level filter argument beside a gate -/

/-- Orders as `(customer, value)`: customer `c` has a row passing `p`. -/
def keyedArg (p : Int → Bool) (orders : List (Nat × Int)) (c : Nat) : Bool :=
  orders.any fun o => o.1 == c && p o.2

/-- The argument joined as a bare value set, ON 1=1. -/
def crossArg (p : Int → Bool) (orders : List (Nat × Int)) (_ : Nat) : Bool :=
  orders.any fun o => p o.2

/-- Joined on the customer each order row determines, the argument keeps
exactly the customers the pre-joined rows keep. -/
theorem keyed_arg_exact (p : Int → Bool) (orders : List (Nat × Int)) (c : Nat) :
    keyedArg p orders c = true ↔ c ∈ (orders.filter (p ·.2)).map (·.1) := by
  simp only [keyedArg, List.any_eq_true, Bool.and_eq_true, beq_iff_eq, List.mem_map,
    List.mem_filter]
  constructor
  · rintro ⟨o, ho, hc, hp⟩
    exact ⟨o, ⟨ho, hp⟩, hc⟩
  · rintro ⟨o, ⟨ho, hp⟩, hc⟩
    exact ⟨o, ho, hc, hp⟩

/-- `where rqty > 1 or sum(amount) by cust_id > 8 select name`: cross-joined,
customer 2's return passes customer 1 (`group_graph._group_inputs` exposes
the gate's grain on the argument's row stream). -/
theorem cross_arg_leaks :
    crossArg (· > 1) [(2, 2)] 1 = true ∧ keyedArg (· > 1) [(2, 2)] 1 = false := by
  decide

/-- A row atom over the orders, the customer kept by a join preserving it
against the orders the atom kept, its padded row read as all-NULL. -/
def keptPreserving (P : Val → Bool) (orders : List (Nat × Val)) (c : Nat) : Bool :=
  if (orders.filter fun o => o.1 == c && P o.2).isEmpty then P none else true

def keptByRows (P : Val → Bool) (orders : List (Nat × Val)) (c : Nat) : Bool :=
  orders.any fun o => o.1 == c && P o.2

/-- An atom false on the all-NULL row is decided by the orders it kept, given
the customer has an order (a complete binding claims it does). -/
theorem preserving_exact (P : Val → Bool) (orders : List (Nat × Val)) (c : Nat)
    (h0 : P none = false) : keptPreserving P orders c = keptByRows P orders c := by
  unfold keptPreserving keptByRows
  split
  · rename_i h
    rw [h0]
    symm
    simp only [List.isEmpty_iff, List.filter_eq_nil_iff, Bool.and_eq_true, beq_iff_eq,
      not_and] at h
    simp only [List.any_eq_false, Bool.and_eq_true, beq_iff_eq, not_and]
    exact fun o ho hc => by simpa using h o ho hc
  · rename_i h
    symm
    cases ha : orders.any (fun o => o.1 == c && P o.2)
    · exfalso
      apply h
      simp only [List.isEmpty_iff, List.filter_eq_nil_iff]
      intro o ho hp
      have := List.any_eq_false.mp ha o ho
      simp_all
    · rfl

/-- `where amount is null` true on the padded row: the customer whose one
order has an amount is kept. A FULL to the `~` notes padded the dims' key,
which kept the join preserving (`subset_full_is_left`). -/
theorem null_accepting_row_keeps_rejected :
    keptPreserving (·.isNone) [(1, some 5)] 1 = true ∧
      keptByRows (·.isNone) [(1, some 5)] 1 = false := by
  decide

/-! ## A GROUP BY that removes nothing (`DropIdentityGroup`, `MergeIrrelevantGroupBy`) -/

/-- A pure DISTINCT over rows already unique at its keys is the identity
(`identity_group.rows_unique_at_group`). -/
theorem identity_group_exact {α : Type} [DecidableEq α] (rows : List α) (h : rows.Nodup) :
    dedup rows = rows :=
  dedup_of_nodup rows h

/-- An INNER join to a side holding a key twice repeats the left row, so the
DISTINCT it was guarding against does remove a row. -/
theorem fanout_breaks_identity :
    let joined := (innerJ (eqOn 0 0) [one] [one, one]).map fun j => j.view 0
    joined.length = 2 ∧ (dedup joined).length = 1 := by
  decide

/-- An aggregate read through a parent's DISTINCT equals one read through
its rows when they are unique, so the DISTINCT folds away. -/
theorem merged_group_sum_exact (rows : List Int) (h : rows.Nodup) :
    (dedup rows).sum = rows.sum := by
  rw [dedup_of_nodup rows h]

/-- A parent DISTINCT keyed on the measure it deduplicates: dropping it
counts the repeated value twice (`merge_irrelevant_group_by._drops_dedup_measure`). -/
theorem dropped_dedup_double_counts : (dedup [5, 5]).sum ≠ ([5, 5] : List Int).sum := by
  decide

/-! ## An outer join whose preserved side always matches (`UpgradeOuterFromKeySetEquivalence`) -/

/-- Every left row has a partner, so the LEFT join pads nothing and is the
INNER join: the subset proof the rule reads off the domain graph. -/
theorem matched_left_is_inner (on : Row → Row → Bool) (L R : List Row)
    (h : ∀ l ∈ L, (R.filter (on l)).isEmpty = false) : leftJ on L R = innerJ on L R := by
  induction L with
  | nil => rfl
  | cons l ls ih =>
    simp only [leftJ, innerJ, List.flatMap_cons] at ih ⊢
    have hl := h l (by simp)
    rw [ih (fun l' hl' => h l' (by simp [hl']))]
    simp [leftRows, hl]

/-- A FILTER on the superset side breaks the proof: the key it removed pads
(the rule's veto on a FILTER anywhere in a side's chain). -/
theorem filtered_superset_pads :
    (leftJ (eqOn 0 0) [one, two] ([one, two].filter fun r => r 0 == some 1)).length = 2 ∧
      (innerJ (eqOn 0 0) [one, two] ([one, two].filter fun r => r 0 == some 1)).length = 1 := by
  decide

/-! ## An existence scan folded into an aggregate's HAVING (`FoldExistenceIntoAggregate`) -/

/-- Rows as `(key, passes W)`: `count(CASE WHEN W THEN 1 END) > 0` over the
key's group decides whether the key has a row passing `W`. -/
theorem existence_fold_exact (rows : List (Nat × Bool)) (k : Nat) :
    (rows.any fun r => r.1 == k && r.2) =
      decide (0 < (rows.filter fun r => r.1 == k && r.2).length) := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.any_cons, List.filter_cons]
    split <;> simp_all

/-- The aggregate's own WHERE (`v`) is not one `W` restates: the fold reads
the aggregate's filtered rows and misses the key whose only `W` row it
dropped (the rule's "`A` reads every row `R` would"). -/
theorem existence_fold_under_filter_misses :
    let rows : List (Nat × Bool × Bool) := [(1, true, false)]
    (rows.any fun r => r.1 == 1 && r.2.1) = true ∧
      ((rows.filter (·.2.2)).any fun r => r.1 == 1 && r.2.1) = false := by
  decide

/-! ## Restricting an aggregate's groups to a feeder's keys (`PushSemiJoinIntoAggregate`) -/

/-- Keeping only the groups whose key a feeder holds leaves each kept group's
sum unchanged: `push_group_key` with the membership test. -/
theorem semi_join_exact {α : Type} (key : α → Nat) (v : α → Int) (feeder : List Nat)
    (rows : List α) (k : Nat) :
    sumBy v (slice key k (rows.filter fun r => feeder.contains (key r))) =
      if feeder.contains k then sumBy v (slice key k rows) else 0 :=
  push_group_key key v (fun k => feeder.contains k) rows k

/-- A nullable feeder key: the null-safe join the rule mirrors pairs a NULL
group with a NULL feeder key, `k in (select k ...)` drops it (the rule's
non-nullable feeder guard). -/
theorem semi_join_nullable_feeder_drops :
    let groups : List (Option Nat) := [none]
    let feeder : List (Option Nat) := [none]
    (groups.filter fun g => feeder.any (· == g)).length = 1 ∧
      (groups.filter fun g => g.isSome && feeder.contains g).length = 0 := by
  decide

/-! ## A filtered value grouped at its keys' grain -/

/-- `store_sales <- customer_item ? in_year`, computed per sale row and grouped
with its key: a pair with a sale in the year and one outside it yields two
groups, `(k, some k)` and `(k, none)`, and counts in two categories
(TPC-DS q97). `max` collapses the fan-out to the keyed value
(`CTE.filter_collapses_to_grain`, now for a value a parent computed too). -/
theorem grouped_filter_fans_out :
    let rows : List (Nat × Option Nat) := [(7, some 7), (7, none)]
    (dedup rows).length = 2 ∧
      (rows.filterMap (·.2)).max? = some 7 := by
  decide

/-! ## A HAVING naming a SELECT alias -/

/-- Name resolution in a HAVING: a FROM column first, then a SELECT alias. -/
def resolve (fromCols aliases : String → Option Int) (n : String) : Option Int :=
  (fromCols n).or (aliases n)

theorem alias_free (fromCols aliases : String → Option Int) (n : String)
    (h : fromCols n = none) : resolve fromCols aliases n = aliases n := by
  simp [resolve, h]

/-- `select g, sum(x) -> c having c > 3` over a table with a column `c`: the
HAVING reads the table's `c` (the group key, 1), not the sum (12). -/
theorem alias_shadowed :
    resolve (fun n => if n = "c" then some 1 else none)
      (fun n => if n = "c" then some 12 else none) "c" = some 1 := by
  decide

end Trilogy.Rewrite
