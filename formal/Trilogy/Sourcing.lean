import Trilogy.Domain

/-!
# Sourcing: `complete where` arms and partition unions

A `partial datasource ... complete where c` holds exactly the rows on which
`c` is TRUE. It answers a query alone when the query's WHERE implies `c`. A
family of such arms answers an unfiltered query only through their UNION ALL,
and that union is the table exactly when every row's discriminator satisfies
ONE arm: none and the row is lost, two and it is counted twice.

The planner's proof (`conditions_partition_domain`) decides that by cells.
The arms' literals cut the discriminator's domain into points and the open
gaps between adjacent literals, plus NULL. Every comparison against a literal
is constant on a cell, so one evaluation per cell is exact. The checker this
replaced stepped across gaps by an increment. That is unsound on a dense
domain, and it never checked for overlap or NULL.

`none` is NULL, as in `Domain.lean`.
-/

namespace Trilogy

variable {α : Type}

/-- The rows a `complete where c` arm holds, reading the discriminator `d`. -/
def armRows (d : α → Val) (c : Val → Bool) (rows : List α) : List α :=
  rows.filter (fun r => c (d r))

/-- The planner's union of a partition family: UNION ALL over the arms. -/
def unionAll (d : α → Val) (arms : List (Val → Bool)) (rows : List α) : List α :=
  arms.flatMap (fun c => armRows d c rows)

/-- How many arms claim the value `v`. -/
def hits (arms : List (Val → Bool)) (v : Val) : Nat :=
  arms.countP (fun c => c v)

theorem sum_ite_eq_countP_mul (arms : List (Val → Bool)) (v : Val) (n : Nat) :
    (arms.map (fun c => if c v = true then n else 0)).sum = hits arms v * n := by
  induction arms with
  | nil => simp [hits]
  | cons c cs ih =>
    simp only [List.map_cons, List.sum_cons, ih, hits, List.countP_cons]
    by_cases h : c v = true <;> simp [h, Nat.add_mul, Nat.add_comm]

theorem count_filter_ite [DecidableEq α] (p : α → Bool) (a : α) (l : List α) :
    (l.filter p).count a = if p a = true then l.count a else 0 := by
  by_cases h : p a = true
  · simp [h, List.count_filter h]
  · simp only [h]
    apply List.count_eq_zero.mpr
    intro hm
    exact h (List.mem_filter.mp hm).2

theorem perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x _ ih => simp [ih]
  | swap x y l => simp; omega
  | trans _ _ ih₁ ih₂ => rw [ih₁, ih₂]

/-- A family that claims every row's discriminator exactly once unions back
to the table, row for row, so every aggregate over it is unchanged. -/
theorem Sourcing.union_perm_of_partition [DecidableEq α] (d : α → Val)
    (arms : List (Val → Bool)) (rows : List α)
    (h : ∀ r ∈ rows, hits arms (d r) = 1) :
    (unionAll d arms rows).Perm rows := by
  rw [List.perm_iff_count]
  intro a
  simp only [unionAll, armRows, List.count_flatMap, Function.comp_def,
    count_filter_ite]
  rw [sum_ite_eq_countP_mul]
  by_cases ha : a ∈ rows
  · rw [h a ha, Nat.one_mul]
  · rw [List.count_eq_zero.mpr ha, Nat.mul_zero]

theorem Sourcing.union_sum_eq [DecidableEq α] (d : α → Val) (v : α → Int)
    (arms : List (Val → Bool)) (rows : List α)
    (h : ∀ r ∈ rows, hits arms (d r) = 1) :
    sumBy v (unionAll d arms rows) = sumBy v rows :=
  perm_sum_int ((Sourcing.union_perm_of_partition d arms rows h).map v)

/-! ### Comparison arms are Kleene: NULL satisfies none of them -/

def le (c : Int) : Val → Bool
  | some x => decide (x ≤ c)
  | none => false

def gt (c : Int) : Val → Bool
  | some x => decide (c < x)
  | none => false

def ge (c : Int) : Val → Bool
  | some x => decide (c ≤ x)
  | none => false

def isNull : Val → Bool
  | some _ => false
  | none => true

/-- Arms `x <= 10` and `x > 10` cover every value but NULL: the union loses
the NULL row. This was the planner's behavior on a nullable discriminator. -/
theorem Sourcing.null_escapes_comparison_arms :
    (unionAll id [le 10, gt 10] [some 1, none, some 20]).length ≠ 3 := by decide

/-- An `is null` arm completes the family. -/
theorem Sourcing.null_arm_partitions (c : Int) (v : Val) :
    hits [le c, gt c, isNull] v = 1 := by
  cases v with
  | none => simp [hits, le, gt, isNull]
  | some x =>
    by_cases hx : x ≤ c
    · have : ¬ c < x := by omega
      simp [hits, le, gt, isNull, hx, this]
    · have : c < x := by omega
      simp [hits, le, gt, isNull, hx, this]

/-- A gate that rejects NULL removes the NULL member from the domain the
arms must partition: filtering commutes into each arm. -/
theorem Sourcing.gated_union_perm [DecidableEq α] (d : α → Val) (g : Val → Bool)
    (arms : List (Val → Bool)) (rows : List α)
    (h : ∀ r ∈ rows, g (d r) = true → hits arms (d r) = 1) :
    ((unionAll d arms rows).filter (fun r => g (d r))).Perm
      (rows.filter (fun r => g (d r))) := by
  have commute : (unionAll d arms rows).filter (fun r => g (d r)) =
      unionAll d arms (rows.filter (fun r => g (d r))) := by
    simp only [unionAll, armRows, List.filter_flatMap, List.filter_filter,
      Bool.and_comm]
  rw [commute]
  apply Sourcing.union_perm_of_partition
  intro r hr
  rw [List.mem_filter] at hr
  exact h r hr.1 hr.2

/-- Overlapping arms `x <= 10` and `x >= 5` double-count `7`. Coverage alone,
which the old checker tested, is not enough. -/
theorem Sourcing.overlap_double_counts :
    (unionAll id [le 10, ge 5] [some 1, some 7, some 20]).length = 4 := by decide

/-- One arm answers alone when the query's WHERE implies its claim. -/
theorem Sourcing.partial_is_full_under_implication (d : α → Val) (c w : Val → Bool)
    (rows : List α) (h : ∀ v, w v = true → c v = true) :
    (armRows d c rows).filter (fun r => w (d r)) = rows.filter (fun r => w (d r)) := by
  simp only [armRows, List.filter_filter]
  congr 1
  funext r
  by_cases hw : w (d r) = true
  · simp [hw, h _ hw]
  · simp [hw]

/-! ### Why stepping across a gap is unsound

The old checker treated arms `[.., hi]` and `[lo, ..]` as adjacent when
`lo - hi` was at most one increment. On integers and dates the increment is
the successor, so that is exact. Datetimes inherited the DATE branch's one-day
step (a `datetime` is a `date` in Python). Floats used machine epsilon, which
`100.0 ± ε` rounds away. Here timestamps are in seconds. -/

def stepMeets (step hi lo : Int) : Bool := decide (lo - hi ≤ step)

/-- `t <= 0` and `t >= 72000` "meet" under a one-day step, but `t = 36000`
lies in neither. -/
theorem Sourcing.day_step_misses_a_time :
    stepMeets 86400 0 72000 = true ∧ hits [le 0, ge 72000] (some 36000) = 0 := by
  decide

/-- With the epsilon absorbed, `x < 100` reads as `x <= 100` and `x > 100` as
`x >= 100`. The bounds "meet" at a distance of zero, but `100` lies in neither
real arm. -/
theorem Sourcing.absorbed_epsilon_misses_the_cut :
    stepMeets 0 100 100 = true ∧ hits [le 99, ge 101] (some 100) = 0 := by
  decide

/-! ### Cells decide the family exactly

An atom compares the discriminator with a literal. Two values that sit on the
same side of every literal agree on every atom (`atoms_agree_in_cell`).
`cell_rep` finds, for any non-literal integer, a representative the checker
evaluates: one past the largest literal below it, else one before the
smallest. `checker_exact` assembles the two lemmas: if exactly one arm holds
on every literal, every representative and NULL, then exactly one holds
everywhere. -/

inductive Op where
  | lt | le | gt | ge | eq | ne | isNull | notNull
  deriving DecidableEq, Repr

structure Atom where
  op : Op
  lit : Int
  deriving DecidableEq, Repr

def Atom.eval (a : Atom) : Val → Bool
  | none => a.op = Op.isNull
  | some x =>
    match a.op with
    | .lt => decide (x < a.lit)
    | .le => decide (x ≤ a.lit)
    | .gt => decide (a.lit < x)
    | .ge => decide (a.lit ≤ x)
    | .eq => decide (x = a.lit)
    | .ne => decide (x ≠ a.lit)
    | .isNull => false
    | .notNull => true

def lits (atoms : List Atom) : List Int := atoms.map Atom.lit

def SameCell (L : List Int) (v w : Int) : Prop :=
  v ∉ L ∧ w ∉ L ∧ ∀ c ∈ L, (c < v ↔ c < w)

theorem atoms_agree_in_cell (atoms : List Atom) (v w : Int)
    (h : SameCell (lits atoms) v w) : ∀ a ∈ atoms, a.eval (some v) = a.eval (some w) := by
  intro a ha
  obtain ⟨hv, hw, hlt⟩ := h
  have hc : a.lit ∈ lits atoms := List.mem_map_of_mem ha
  have hside := hlt a.lit hc
  have hvne : v ≠ a.lit := fun e => hv (e ▸ hc)
  have hwne : w ≠ a.lit := fun e => hw (e ▸ hc)
  cases a with
  | mk op lit =>
    simp only at hside hvne hwne
    cases op <;> simp only [Atom.eval] <;>
      first
      | rfl
      | (apply decide_eq_decide.mpr; omega)

/-- The largest literal strictly below `v`, if any. -/
def maxBelow : List Int → Int → Option Int
  | [], _ => none
  | c :: L, v =>
    match maxBelow L v with
    | none => if c < v then some c else none
    | some m => if c < v ∧ m < c then some c else some m

theorem maxBelow_none (L : List Int) (v : Int) (h : maxBelow L v = none) :
    ∀ c ∈ L, ¬ c < v := by
  induction L with
  | nil => simp
  | cons c L ih =>
    simp only [maxBelow] at h
    split at h
    · rename_i hn
      split at h
      · cases h
      · rename_i hc
        intro c' hc'
        rcases List.mem_cons.mp hc' with rfl | hm
        · exact hc
        · exact ih hn c' hm
    · split at h <;> cases h

theorem maxBelow_some (L : List Int) (v m : Int) (h : maxBelow L v = some m) :
    m ∈ L ∧ m < v ∧ ∀ c ∈ L, c < v → c ≤ m := by
  induction L generalizing m with
  | nil => simp [maxBelow] at h
  | cons c L ih =>
    simp only [maxBelow] at h
    split at h
    · rename_i hn
      split at h
      · cases h
        refine ⟨List.mem_cons_self, by omega, ?_⟩
        intro c' hc' hlt
        rcases List.mem_cons.mp hc' with rfl | hm
        · omega
        · have := maxBelow_none L v hn c' hm; omega
      · cases h
    · rename_i m' hm'
      obtain ⟨hmem, hmv, hmax⟩ := ih m' hm'
      split at h
      · cases h
        rename_i hc
        refine ⟨List.mem_cons_self, hc.1, ?_⟩
        intro c' hc' hlt
        rcases List.mem_cons.mp hc' with rfl | hm
        · omega
        · have := hmax c' hm hlt; omega
      · cases h
        rename_i hc
        refine ⟨List.mem_cons_of_mem _ hmem, hmv, ?_⟩
        intro c' hc' hlt
        rcases List.mem_cons.mp hc' with rfl | hm
        · omega
        · exact hmax c' hm hlt

/-- The representative the checker evaluates for the gap holding `v`. -/
def cellRep (L : List Int) (v : Int) : Int :=
  match maxBelow L v with
  | some m => m + 1
  | none => (L.foldr min (v + 1)) - 1

theorem foldr_min_le (L : List Int) (b : Int) : ∀ c ∈ L, L.foldr min b ≤ c := by
  induction L with
  | nil => simp
  | cons x L ih =>
    intro c hc
    simp only [List.foldr_cons]
    rcases List.mem_cons.mp hc with rfl | hm
    · omega
    · have := ih c hm; omega

theorem foldr_min_le_base (L : List Int) (b : Int) : L.foldr min b ≤ b := by
  induction L with
  | nil => simp
  | cons x L ih => simp only [List.foldr_cons]; omega

theorem foldr_min_mem_or_base (L : List Int) (b : Int) :
    L.foldr min b ∈ L ∨ L.foldr min b = b := by
  induction L with
  | nil => simp
  | cons x L ih =>
    simp only [List.foldr_cons]
    by_cases h : x ≤ L.foldr min b
    · left; rw [Int.min_eq_left h]; exact List.mem_cons_self
    · rw [Int.min_eq_right (by omega)]
      rcases ih with hm | he
      · left; exact List.mem_cons_of_mem _ hm
      · right; exact he

theorem cell_rep (L : List Int) (v : Int) (hv : v ∉ L) :
    cellRep L v = v ∨ SameCell L v (cellRep L v) := by
  unfold cellRep
  split
  · rename_i m hm
    obtain ⟨hmem, hmv, hmax⟩ := maxBelow_some L v m hm
    by_cases he : m + 1 = v
    · left; exact he
    · right
      refine ⟨hv, ?_, ?_⟩
      · intro hin
        have := hmax (m + 1) hin (by omega)
        omega
      · intro c hc
        constructor
        · intro h; have := hmax c hc h; omega
        · intro h
          have hcm : c ≤ m := by omega
          omega
  · rename_i hn
    have hnone := maxBelow_none L v hn
    have hle := foldr_min_le L (v + 1)
    have hbase := foldr_min_le_base L (v + 1)
    right
    refine ⟨hv, ?_, ?_⟩
    · intro hin
      have := hle _ hin
      omega
    · intro c hc
      have := hnone c hc
      have := hle c hc
      constructor <;> intro h <;> omega

/-- The checker's cells: every literal, every gap representative, and NULL. -/
def CheckerAccepts (arms : List (List Atom)) (atoms : List Atom) : Prop :=
  let armHolds (arm : List Atom) (v : Val) := arm.any (fun a => a.eval v)
  let count (v : Val) := arms.countP (fun arm => armHolds arm v)
  count none = 1 ∧ (∀ c ∈ lits atoms, count (some c) = 1) ∧
    ∀ v : Int, count (some (cellRep (lits atoms) v)) = 1

/-- Exactly one arm on every checked cell means exactly one arm on every
value. `atoms` lists every atom the arms use. -/
theorem Sourcing.checker_exact (arms : List (List Atom)) (atoms : List Atom)
    (hsub : ∀ arm ∈ arms, ∀ a ∈ arm, a ∈ atoms)
    (hacc : CheckerAccepts arms atoms) (v : Val) :
    arms.countP (fun arm => arm.any (fun a => a.eval v)) = 1 := by
  obtain ⟨hnull, hlit, hrep⟩ := hacc
  cases v with
  | none => exact hnull
  | some x =>
    by_cases hx : x ∈ lits atoms
    · exact hlit x hx
    · rcases cell_rep (lits atoms) x hx with he | hsame
      · have := hrep x; rw [he] at this; exact this
      · have agree := atoms_agree_in_cell atoms x _ hsame
        have same : ∀ arm ∈ arms, (arm.any (fun a => a.eval (some x)) = true ↔
            arm.any (fun a => a.eval (some (cellRep (lits atoms) x))) = true) := by
          intro arm harm
          simp only [List.any_eq_true]
          constructor
          · rintro ⟨a, ha, hv⟩
            exact ⟨a, ha, by rw [← agree a (hsub arm harm a ha)]; exact hv⟩
          · rintro ⟨a, ha, hv⟩
            exact ⟨a, ha, by rw [agree a (hsub arm harm a ha)]; exact hv⟩
        exact (List.countP_congr same).trans (hrep x)

end Trilogy
