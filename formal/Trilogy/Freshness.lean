import Trilogy.Sourcing

/-!
# Freshness: watermarks and the incremental refresh

A non-root datasource with `incremental by k` is judged against its roots:
it is stale when its `MAX(k)` trails theirs. Its refresh appends the source
rows with `k > m`, where `m` is the target's own `MAX(k)`.

Two separate pieces of code decide one fact. The verdict compares watermarks
in Python, and the refresh filters rows in SQL. They agree only if Python
orders watermarks the way SQL's `>` does (`Freshness.verdict_agrees_with_refresh`).
The comparator fell back to string order whenever two watermarks had
different Python types, such as a DECIMAL root beside a DOUBLE CSV target or a
DECIMAL read back from a snapshot as text. There `"10" < "9"`, so a target
that trailed its root read as fresh and was never refreshed
(`Freshness.string_order_misses_a_refresh`).

When arrivals are strictly past the watermark, the incremental refresh equals
a full rebuild (`Freshness.incremental_exact`). The model shows that this
assumption is load-bearing: a late row with a tied or NULL key is invisible to
both the verdict and the refresh (`tie_is_invisible`, `null_key_is_invisible`).

`none` is NULL, as in `Domain.lean`.
-/

namespace Trilogy

variable {α : Type}

/-- SQL's `k > m`. It is Kleene, so a NULL key is never selected. -/
def after (m : Int) : Val → Bool
  | some x => decide (m < x)
  | none => false

/-- The incremental refresh: append the source rows past the watermark. -/
def refresh (k : α → Val) (m : Int) (target src : List α) : List α :=
  target ++ src.filter (fun r => after m (k r))

/-- `MAX(k)`, which ignores NULL. -/
def maxKey (k : α → Val) : List α → Option Int
  | [] => none
  | r :: rs =>
    match k r, maxKey k rs with
    | some x, some y => some (max x y)
    | some x, none => some x
    | none, y => y

/-- The verdict: the target's watermark `m` trails the source's maximum. -/
def behind (k : α → Val) (m : Int) (src : List α) : Bool :=
  match maxKey k src with
  | some s => decide (m < s)
  | none => false

/-- Stale exactly when the refresh has a row to write. Either half alone
would be unsound: a stale verdict with nothing to load refreshes forever, and
a fresh verdict beside a loadable row never loads it. -/
theorem Freshness.verdict_agrees_with_refresh (k : α → Val) (m : Int)
    (src : List α) : src.any (fun r => after m (k r)) = behind k m src := by
  induction src with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.any_cons, ih, behind, maxKey]
    cases hk : k r <;> cases hm : maxKey k rs <;>
      simp [after]
    rename_i x y
    rw [Int.max_def]
    split <;> simp <;> omega

/-- Arrivals strictly past the watermark make the refresh a full rebuild. -/
theorem Freshness.incremental_exact (k : α → Val) (m : Int) (old new : List α)
    (hold : ∀ r ∈ old, after m (k r) = false)
    (hnew : ∀ r ∈ new, after m (k r) = true) :
    refresh k m old (old ++ new) = old ++ new := by
  simp only [refresh, List.filter_append]
  rw [List.filter_eq_nil_iff.mpr (by simpa using hold),
    List.filter_eq_self.mpr hnew]
  rfl

/-- A row that arrives after the refresh with the watermark's own value, such
as the rest of today's rows on a date key, is never loaded, and the verdict
reads fresh. -/
theorem Freshness.tie_is_invisible :
    refresh id 5 [some 5] [some 5, some 5] = [some 5] ∧
      behind id 5 [some 5, some 5] = false := by
  decide

/-- A NULL-keyed arrival is never selected by `k > m` and never moves the
maximum. -/
theorem Freshness.null_key_is_invisible :
    refresh id 5 [some 5] [some 5, none] = [some 5] ∧
      behind id 5 [some 5, none] = false := by
  decide

/-- The old comparator's fallback for mismatched types: string order on the
values' renderings. -/
def stringBehind (m s : String) : Bool := decide (m < s)

/-- `9` trails `10`, so the refresh has a row to load, but `"10" < "9"`. -/
theorem Freshness.string_order_misses_a_refresh :
    behind id 9 [some 10] = true ∧ stringBehind "9" "10" = false := by
  decide

end Trilogy
