import Trilogy.Logic

/-!
# Datasource validation: each check is exact for the claim it guards

A datasource declares facts the planner trusts and never re-checks: a column is
non-NULL, a value lies in a declared range, a grain identifies a row, a
complete binding holds every value of its concept, a `unique` property
identifies its key, and a `complete where` arm holds only rows of its slice.
NullReject's outer-join upgrade, the cell checker's coverage proofs and the
merge planner all assume these facts. `validate` is the only place they are
tested, so each check must report a violation exactly when the claim fails.

A check is a WHERE over the datasource, and the datasource passes when it keeps
no row. Each `*_exact` theorem states `violations = [] ↔ claim`.

What the checks got wrong:

- nullability was judged on a 100-row sample ordered NULL-first, but the order
  read only the first column's NULL flag. A NULL past the sample passed
  (`sample_misses_null`). Ordering by the total NULL count would not save it:
  rows NULL in two nullable columns crowd out a row NULL in one non-nullable
  column (`null_count_order_misses`). The check now scans the table
  (`null_check_exact`).
- completeness compared distinct counts. Two complete sources holding `{1, 2}`
  and `{2, 3}` have equal counts and each lacks a value the other holds
  (`card_check_misses_disjoint`), and a partial source's values were never
  compared at all (`card_check_ignores_partial`). The check is now set
  containment (`completeness_exact`).
- the sampled range check compared a `Decimal` against the bound as a binary
  float, so `0.30` fell outside `numeric(10,2)[0.1..0.3]`
  (`float_bound_rejects_its_decimal`), while the SQL check, which reads the
  bound as the literal `0.3`, passed it.
- a `unique` property counted its keys with `count(distinct k)`, which skips a
  NULL key, so one value mapping to a NULL key and to `1` passed
  (`count_distinct_misses_null_key`). The planner groups by the property, so
  NULL is one key value there (`distinct_with_null`).
-/

namespace Trilogy.Validate

open Trilogy.B3

abbrev Val := Option Int
/-- A row is its column values, by position. -/
abbrev Row := List Val

def Row.at (r : Row) (c : Nat) : Val := r.getD c none

/-- The rows a WHERE keeps. -/
def keep (p : Row → B3) (tb : List Row) : List Row := tb.filter (fun r => isT (p r))

private theorem keep_nil (p : Row → B3) (tb : List Row) :
    keep p tb = [] ↔ ∀ r ∈ tb, p r ≠ t := by
  unfold keep
  rw [List.filter_eq_nil_iff]
  refine forall_congr' fun r => imp_congr_right fun _ => ?_
  cases p r <;> simp [isT]

/-! ## Nullability -/

def NonNull (c : Nat) (tb : List Row) : Prop := ∀ r ∈ tb, r.at c ≠ none

instance (c : Nat) (tb : List Row) : Decidable (NonNull c tb) :=
  inferInstanceAs (Decidable (∀ r ∈ tb, r.at c ≠ none))

def isNull3 : Val → B3
  | none => t
  | some _ => f

/-- `where c is null`, over the whole table. -/
theorem null_check_exact (c : Nat) (tb : List Row) :
    keep (fun r => isNull3 (r.at c)) tb = [] ↔ NonNull c tb := by
  rw [keep_nil]
  refine forall_congr' fun r => imp_congr_right fun _ => ?_
  cases r.at c <;> simp [isNull3]

/-- Shipped: the first `n` rows, ordered by the first column's NULL flag only. A
key is never NULL, so that order is the table's own. -/
def sampleFirstFlag (n : Nat) (tb : List Row) : List Row := tb.take n

def nullCount (cols : List Nat) (r : Row) : Nat :=
  (cols.filter fun c => r.at c = none).length

/-- Insert `r` before the first row with fewer NULL columns. -/
def insertByCount (cols : List Nat) (r : Row) : List Row → List Row
  | [] => [r]
  | s :: rest =>
    if nullCount cols s < nullCount cols r then r :: s :: rest
    else s :: insertByCount cols r rest

/-- The intended order: most NULL columns first, stable. -/
def sampleByCount (n : Nat) (cols : List Nat) (tb : List Row) : List Row :=
  (tb.foldr (insertByCount cols) []).take n

/-- Column 0 is the key and column 1 is non-nullable; only the last row is NULL. -/
theorem sample_misses_null :
    let tb : List Row := [[some 0, some 1], [some 1, none]]
    NonNull 1 (sampleFirstFlag 1 tb) ∧ ¬ NonNull 1 tb := by
  decide

/-- Columns 1 and 2 are nullable, column 3 is not. The first row is NULL in two
columns, the second in one. -/
theorem null_count_order_misses :
    let tb : List Row := [[some 0, none, none, some 0], [some 1, some 1, some 1, none]]
    NonNull 3 (sampleByCount 1 [1, 2, 3] tb) ∧ ¬ NonNull 3 tb := by
  decide

/-! ## Declared ranges -/

/-- An inclusive range; a missing bound is open. -/
structure Range where
  lo : Option Int
  hi : Option Int

def Range.contains (r : Range) (v : Int) : Bool :=
  r.lo.all (· ≤ v) && r.hi.all (v ≤ ·)

def InDomain (rs : List Range) (v : Int) : Prop := ∃ r ∈ rs, r.contains v = true

def lt3 : Val → Int → B3
  | none, _ => u
  | some a, b => if a < b then t else f

def gt3 : Val → Int → B3
  | none, _ => u
  | some a, b => if b < a then t else f

def notNull3 : Val → B3
  | none => f
  | some _ => t

/-- `x < lo or x > hi`, one side when the other bound is open. -/
def Range.outside (r : Range) (v : Val) : B3 :=
  match r.lo, r.hi with
  | some l, some h => or3 (lt3 v l) (gt3 v h)
  | some l, none => lt3 v l
  | none, some h => gt3 v h
  | none, none => f

/-- `x is not null and (outside r₁) and (outside r₂) ...`. -/
def domainViol (rs : List Range) (v : Val) : B3 :=
  and3 (notNull3 v) (rs.foldr (fun r acc => and3 (r.outside v) acc) t)

private theorem and3_holds {p q : B3} : and3 p q = t ↔ p = t ∧ q = t := by
  cases p <;> cases q <;> simp [and3]

private theorem or3_holds {p q : B3} : or3 p q = t ↔ p = t ∨ q = t := by
  cases p <;> cases q <;> simp [or3]

private theorem lt3_some (a b : Int) : lt3 (some a) b = t ↔ a < b := by
  unfold lt3; split <;> simp_all

private theorem gt3_some (a b : Int) : gt3 (some a) b = t ↔ b < a := by
  unfold gt3; split <;> simp_all

private theorem outside_some (r : Range) (x : Int) :
    r.outside (some x) = t ↔ r.contains x = false := by
  rcases r with ⟨lo, hi⟩
  cases lo <;> cases hi <;>
    simp [Range.outside, Range.contains, or3_holds, lt3_some, gt3_some] <;> omega

private theorem fold_some (rs : List Range) (x : Int) :
    rs.foldr (fun r acc => and3 (r.outside (some x)) acc) t = t ↔
      ∀ r ∈ rs, r.contains x = false := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [and3_holds, outside_some, ih]

theorem domain_viol_exact (rs : List Range) (v : Val) :
    domainViol rs v = t ↔ ∃ x, v = some x ∧ ¬ InDomain rs x := by
  cases v with
  | none => simp [domainViol, notNull3, and3]
  | some x => simp [domainViol, notNull3, and3_holds, fold_some, InDomain]

theorem domain_check_exact (rs : List Range) (c : Nat) (tb : List Row) :
    keep (fun r => domainViol rs (r.at c)) tb = [] ↔
      ∀ r ∈ tb, ∀ x, r.at c = some x → InDomain rs x := by
  rw [keep_nil]
  refine forall_congr' fun r => imp_congr_right fun _ => ?_
  rw [Ne, domain_viol_exact]
  constructor
  · intro h x hx; exact Classical.byContradiction fun hn => h ⟨x, hx, hn⟩
  · rintro h ⟨x, hx, hn⟩; exact hn (h x hx)

/-- The nearest binary double to `0.3` is `5404319552844595 / 2^54`, just below
it. Python orders `Decimal('0.30')` against that double exactly, so the value
read off a `numeric(10,2)` column lies above the bound its author wrote. -/
theorem float_bound_rejects_its_decimal :
    5404319552844595 * 10 < 3 * 2 ^ 54 := by decide

/-! ## Grain -/

/-- GROUP BY compares NULL keys as equal, so the grain is over `Option` keys. -/
def Grain (key : Row → List Val) (tb : List Row) : Prop := (tb.map key).Nodup

/-- `count(*) by key > 1`. -/
def grainViol (key : Row → List Val) (tb : List Row) : List Row :=
  tb.filter fun r => decide (1 < (tb.map key).count (key r))

theorem grain_check_exact (key : Row → List Val) (tb : List Row) :
    grainViol key tb = [] ↔ Grain key tb := by
  unfold grainViol Grain
  rw [List.filter_eq_nil_iff, List.nodup_iff_count_of_mem]
  simp only [List.mem_map, decide_eq_true_eq, Nat.not_lt]
  constructor
  · rintro h _ ⟨r, hr, rfl⟩
    have := List.count_pos_iff.mpr (List.mem_map_of_mem (f := key) hr)
    have := h r hr; omega
  · intro h r hr; exact Nat.le_of_eq (h _ ⟨r, hr, rfl⟩)

/-! ## Completeness -/

/-- The non-NULL values a source holds of column `c`. -/
def vals (c : Nat) (tb : List Row) : List Int := tb.filterMap (·.at c)

/-- A complete binding holds every value any other source holds. -/
def Complete (c : Nat) (a : List Row) (others : List (List Row)) : Prop :=
  ∀ b ∈ others, ∀ v ∈ vals c b, v ∈ vals c a

instance (c : Nat) (a : List Row) (others : List (List Row)) :
    Decidable (Complete c a others) :=
  inferInstanceAs (Decidable (∀ b ∈ others, ∀ v ∈ vals c b, v ∈ vals c a))

/-- The values other sources hold that `a` lacks: an anti-join. -/
def missing (c : Nat) (a : List Row) (others : List (List Row)) : List Int :=
  (others.flatMap (vals c)).filter fun v => !(vals c a).contains v

theorem completeness_exact (c : Nat) (a : List Row) (others : List (List Row)) :
    missing c a others = [] ↔ Complete c a others := by
  unfold missing Complete
  rw [List.filter_eq_nil_iff]
  simp only [List.mem_flatMap]
  constructor
  · intro h b hb v hv; simpa using h v ⟨b, hb, hv⟩
  · rintro h v ⟨b, hb, hv⟩; simpa using h b hb v hv

/-- Shipped: complete sources are compared by distinct count only. -/
def distinctCount (c : Nat) (tb : List Row) : Nat := (vals c tb).eraseDups.length

def cardCheck (c : Nat) (completes : List (List Row)) : Bool :=
  completes.all fun a => completes.all fun b => distinctCount c b ≤ distinctCount c a

def col0 (xs : List Int) : List Row := xs.map fun x => [some x]

theorem card_check_misses_disjoint :
    cardCheck 0 [col0 [1, 2], col0 [2, 3]] = true ∧
      ¬ Complete 0 (col0 [1, 2]) [col0 [2, 3]] := by
  decide

/-- A partial source is never one of the complete ones, so its values go unread. -/
theorem card_check_ignores_partial :
    cardCheck 0 [col0 [1, 2]] = true ∧ ¬ Complete 0 (col0 [1, 2]) [col0 [3]] := by
  decide

/-! ## Unique properties

`unique property k.u` lets the planner group by `u` in place of `k`
(`_determined` in `join_plan.py`). GROUP BY puts every NULL `u` in one group,
and a NULL `k` is one value of the key there too. -/

def UniqueProp (u k : Nat) (tb : List Row) : Prop :=
  ∀ r ∈ tb, ∀ s ∈ tb, r.at u = s.at u → r.at k = s.at k

instance (u k : Nat) (tb : List Row) : Decidable (UniqueProp u k tb) :=
  inferInstanceAs (Decidable (∀ r ∈ tb, ∀ s ∈ tb, r.at u = s.at u → r.at k = s.at k))

/-- A `u` group's keys: two distinct values, NULL counting as one. -/
def TwoKeys (ks : List Val) : Prop := ∃ a ∈ ks, ∃ b ∈ ks, a ≠ b

/-- Shipped: `count(distinct k) > 1`, two distinct non-NULL keys. -/
def TwoNonNull (ks : List Val) : Prop := ∃ a b : Int, some a ∈ ks ∧ some b ∈ ks ∧ a ≠ b

/-- Fixed: also `count(k) > 0 and count(*) > count(k)`, a key beside a NULL. -/
theorem distinct_with_null (ks : List Val) :
    TwoKeys ks ↔ TwoNonNull ks ∨ ((∃ a : Int, some a ∈ ks) ∧ none ∈ ks) := by
  constructor
  · rintro ⟨a, ha, b, hb, hab⟩
    cases a with
    | none =>
      cases b with
      | none => exact absurd rfl hab
      | some b => exact Or.inr ⟨⟨b, hb⟩, ha⟩
    | some a =>
      cases b with
      | none => exact Or.inr ⟨⟨a, ha⟩, hb⟩
      | some b => exact Or.inl ⟨a, b, ha, hb, fun h => hab (h ▸ rfl)⟩
  · rintro (⟨a, b, ha, hb, hab⟩ | ⟨⟨a, ha⟩, hn⟩)
    · exact ⟨_, ha, _, hb, fun h => hab (Option.some.inj h)⟩
    · exact ⟨_, ha, _, hn, by simp⟩

theorem unique_prop_exact (u k : Nat) (tb : List Row) :
    (∀ x, ¬ TwoKeys ((tb.filter fun r => r.at u = x).map (·.at k))) ↔
      UniqueProp u k tb := by
  unfold TwoKeys UniqueProp
  constructor
  · intro h r hr s hs hrs
    exact Classical.byContradiction fun hn => h (r.at u)
      ⟨_, List.mem_map_of_mem (List.mem_filter.mpr ⟨hr, by simp⟩), _,
        List.mem_map_of_mem (List.mem_filter.mpr ⟨hs, by simp [hrs]⟩), hn⟩
  · rintro h x ⟨a, ha, b, hb, hab⟩
    simp only [List.mem_map, List.mem_filter, decide_eq_true_eq] at ha hb
    obtain ⟨r, ⟨hr, hru⟩, rfl⟩ := ha
    obtain ⟨s, ⟨hs, hsu⟩, rfl⟩ := hb
    exact hab (h r hr s hs (hru.trans hsu.symm))

/-- `u = 7` maps to a NULL key and to `1`. -/
theorem count_distinct_misses_null_key :
    let tb : List Row := [[some 7, none], [some 7, some 1]]
    ¬ TwoNonNull (tb.map (·.at 1)) ∧ ¬ UniqueProp 0 1 tb := by
  refine ⟨?_, by decide⟩
  rintro ⟨a, b, ha, hb, hab⟩
  simp [Row.at] at ha hb
  omega

/-! ## `complete where` containment -/

/-- `(claim is not distinct from true) = false`. -/
def outsideClaim (p : B3) : B3 := if p = t then f else t

theorem containment_exact (claim : Row → B3) (tb : List Row) :
    keep (fun r => outsideClaim (claim r)) tb = [] ↔ ∀ r ∈ tb, claim r = t := by
  rw [keep_nil]
  refine forall_congr' fun r => imp_congr_right fun _ => ?_
  unfold outsideClaim; split <;> simp_all

/-- `claim = false` instead: an UNKNOWN claim, as on a NULL-keyed row, is
neither inside nor reported. -/
def eqFalse (p : B3) : B3 := if p = f then t else f

theorem eq_false_misses_unknown :
    keep (fun _ => eqFalse u) [[none]] = [] ∧ u ≠ t := by
  decide

end Trilogy.Validate
