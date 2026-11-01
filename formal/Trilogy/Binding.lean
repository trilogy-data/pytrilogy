import Trilogy.Stack

/-!
# Datasource bindings as functional dependencies

A table binding a column says the column is a function of the table's rows. The
planner reads that claim model-wide: every table contributes a dependency from
its ROW DETERMINANTS to each column it binds, and FD closure chains them
(`build_fd_closure`, `Environment.binding_fds`). A table's row determinants are
its grain, plus a discriminator it binds when it is a `complete where` arm,
since an arm is unique on its grain only among its own rows
(`Stack.unbound_arm_repeats_grain`). A table with no grain identifies no row,
so it determines nothing.

- `grain_unique_holds`: a table unique on its grain satisfies each of its own
  bindings (`binding_holds`).
- `closure_sound`: the closure over the union of every table's bindings is
  sound on any relation on which every binding holds.
- `binding_breaks_on_another_table`: each table can pass its grain check while
  the model-wide claim fails. A species dimension binding `city` claims
  `species -> city`, and a tree table whose species sits in two cities breaks
  it, so the closure claims a dependency the data does not have. Only a check
  across the tables that bind a dependency's columns can catch it
  (`cross_check_exact`).
- `stacked_arms_hold`: arms of a family, each unique on its grain, are unique
  on grain plus discriminator once stacked, and `arm_grain_alone_fails` shows
  the grain alone is not enough.
- `grainless_determines_nothing`: a grainless table contributes no dependency.
  Reading its columns as determined by the empty set claims them constant
  (`grainless_binding_claims_a_constant`).
-/

namespace Trilogy
namespace Binding

abbrev Col := Nat
abbrev Val := Option Int
/-- A row of the model-wide relation: a value per column. -/
abbrev Row := Col → Val

/-- Two rows agree on every column of `D`. -/
def Agree (D : List Col) (r s : Row) : Prop := ∀ d ∈ D, r d = s d

instance (D : List Col) (r s : Row) : Decidable (Agree D r s) :=
  inferInstanceAs (Decidable (∀ d ∈ D, r d = s d))

/-- `D -> c` holds on `rows`. -/
def Holds (rows : List Row) (D : List Col) (c : Col) : Prop :=
  ∀ r ∈ rows, ∀ s ∈ rows, Agree D r s → r c = s c

instance (rows : List Row) (D : List Col) (c : Col) : Decidable (Holds rows D c) :=
  inferInstanceAs (Decidable (∀ r ∈ rows, ∀ s ∈ rows, Agree D r s → r c = s c))

/-- A dependency read from a binding: determinants and the bound column. -/
abbrev FD := List Col × Col

structure Table where
  rows : List Row
  grain : List Col
  bound : List Col
  /-- The columns its `complete where` reads. -/
  disc : List Col := []

/-- `BuildDatasource.row_determinants`. -/
def Table.determinants (t : Table) : List Col :=
  if t.grain = [] then [] else t.grain ++ t.disc.filter (· ∈ t.bound)

/-- One dependency per bound column outside the determinants; a grainless
table identifies no row, so it contributes none. -/
def Table.fds (t : Table) : List FD :=
  if t.grain = [] then [] else (t.bound.filter (· ∉ t.determinants)).map (t.determinants, ·)

/-! ## A binding beside the grain is a dependency -/

theorem holds_mono {rows : List Row} {D E : List Col} {c : Col}
    (hsub : ∀ d ∈ D, d ∈ E) (h : Holds rows D c) : Holds rows E c :=
  fun r hr s hs hag => h r hr s hs fun d hd => hag d (hsub d hd)

private theorem eq_of_nodup_map {α β : Type} (f : α → β) :
    ∀ (l : List α), (l.map f).Nodup → ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y
  | [], _, x, hx, _, _, _ => absurd hx List.not_mem_nil
  | a :: l, h, x, hx, y, hy, hxy => by
    rw [List.map_cons, List.nodup_cons] at h
    rcases List.mem_cons.mp hx with hxa | hxl <;> rcases List.mem_cons.mp hy with hya | hyl
    · rw [hxa, hya]
    · exact absurd (hxa ▸ hxy ▸ List.mem_map_of_mem hyl) h.1
    · exact absurd (hya ▸ hxy.symm ▸ List.mem_map_of_mem hxl) h.1
    · exact eq_of_nodup_map f l h.2 x hxl y hyl hxy

/-- The grain check (`Validate.Grain`): no two rows share a grain value. -/
def GrainUnique (t : Table) : Prop := (t.rows.map fun r => t.grain.map r).Nodup

instance (t : Table) : Decidable (GrainUnique t) :=
  inferInstanceAs (Decidable (t.rows.map fun r => t.grain.map r).Nodup)

/-- A table unique on its grain determines every column from the grain. -/
theorem grain_unique_holds (t : Table) (h : GrainUnique t) (c : Col) :
    Holds t.rows t.grain c := by
  intro r hr s hs hag
  have : t.grain.map r = t.grain.map s := List.map_congr_left hag
  rw [eq_of_nodup_map _ t.rows h r hr s hs this]

theorem mem_fds {t : Table} {fd : FD} (h : fd ∈ t.fds) :
    t.grain ≠ [] ∧ fd.1 = t.determinants := by
  by_cases hg : t.grain = []
  · simp [Table.fds, hg] at h
  · simp only [Table.fds, hg, ↓reduceIte] at h
    obtain ⟨c, -, rfl⟩ := List.mem_map.mp h
    exact ⟨hg, rfl⟩

/-- **Each binding holds on its own table** once the grain check passes. -/
theorem binding_holds (t : Table) (h : GrainUnique t) :
    ∀ fd ∈ t.fds, Holds t.rows fd.1 fd.2 := by
  intro fd hfd
  obtain ⟨hg, h1⟩ := mem_fds hfd
  rw [h1]
  refine holds_mono (fun d hd => ?_) (grain_unique_holds t h fd.2)
  simp only [Table.determinants, hg, ↓reduceIte, List.mem_append]
  exact Or.inl hd

/-! ## Closure over every table's bindings -/

def step (fds : List FD) (S : List Col) : List Col :=
  S ++ (fds.filter fun fd => fd.1.all (· ∈ S)).map Prod.snd

/-- `n` rounds of the closure; the planner iterates to the fixpoint. -/
def closure (fds : List FD) (X : List Col) : Nat → List Col
  | 0 => X
  | n + 1 => step fds (closure fds X n)

/-- Every binding of every table. -/
def allFds (tables : List Table) : List FD := tables.flatMap Table.fds

/-- **Sound**: on a relation where every binding holds, everything the closure
reaches from `X` is a function of `X`. -/
theorem closure_sound (rows : List Row) (fds : List FD)
    (h : ∀ fd ∈ fds, Holds rows fd.1 fd.2) (X : List Col) :
    ∀ n, ∀ c ∈ closure fds X n, Holds rows X c := by
  intro n
  induction n with
  | zero => intro c hc r _ s _ hag; exact hag c hc
  | succ n ih =>
    intro c hc
    rcases List.mem_append.mp hc with hc | hc
    · exact ih c hc
    · obtain ⟨fd, hfd, rfl⟩ := List.mem_map.mp hc
      obtain ⟨hfd, hall⟩ := List.mem_filter.mp hfd
      intro r hr s hs hag
      refine h fd hfd r hr s hs fun d hd => ?_
      have hdS : d ∈ closure fds X n := by
        simpa using (List.all_eq_true.mp hall) d hd
      exact ih d hdS r hr s hs hag

theorem closure_sound_tables (rows : List Row) (tables : List Table)
    (h : ∀ fd ∈ allFds tables, Holds rows fd.1 fd.2) (X : List Col) (n : Nat) :
    ∀ c ∈ closure (allFds tables) X n, Holds rows X c :=
  closure_sound rows _ h X n

/-- `row_keys` of a bare key: the determinants of every table binding it. -/
def boundOn (tables : List Table) (c : Col) : List Col :=
  (tables.filter fun t => (t.fds.map Prod.snd).contains c).flatMap Table.determinants

/-- A key is a function of its `row_keys` wherever the bindings hold. -/
theorem bound_on_determines (rows : List Row) (tables : List Table)
    (h : ∀ fd ∈ allFds tables, Holds rows fd.1 fd.2) (c : Col)
    (t : Table) (ht : t ∈ tables) (hc : c ∈ t.fds.map Prod.snd) :
    Holds rows (boundOn tables c) c := by
  obtain ⟨fd, hfd, rfl⟩ := List.mem_map.mp hc
  refine holds_mono (fun d hd => ?_) (h fd (List.mem_flatMap.mpr ⟨t, ht, hfd⟩))
  have hfd1 := (mem_fds hfd).2
  refine List.mem_flatMap.mpr ⟨t, List.mem_filter.mpr ⟨ht, ?_⟩, ?_⟩
  · simpa using List.mem_map_of_mem (f := Prod.snd) hfd
  · rw [hfd1] at hd; exact hd

/-! ## A binding the data breaks -/

/-- Columns: 0 `tree_id`, 1 `species`, 2 `city`. -/
def row3 (a b c : Int) : Row := fun i => if i = 0 then some a else if i = 1 then some b else if i = 2 then some c else none

/-- A species dimension binding `city`, and a tree table with species 5 in
cities 0 and 1. Both pass the grain check; the closure from `species` claims
`city`, which the tree rows break. -/
theorem binding_breaks_on_another_table :
    let dim : Table := ⟨[row3 0 5 0], [1], [1, 2], []⟩
    let trees : Table := ⟨[row3 1 5 0, row3 2 5 1], [0], [0, 1, 2], []⟩
    GrainUnique dim ∧ GrainUnique trees ∧
      2 ∈ closure (allFds [dim, trees]) [1] 1 ∧ ¬ Holds trees.rows [1] 2 := by
  decide

/-- The rows a cross-table check compares for `D -> c`: every table binding all
of `D` and `c`, projected to `(D, c)`. -/
def pairs (tables : List Table) (D : List Col) (c : Col) : List (List Val × Val) :=
  (tables.filter fun t => (c :: D).all (· ∈ t.bound)).flatMap
    fun t => t.rows.map fun r => (D.map r, r c)

/-- The check's violations: a determinant value paired with two values. -/
def crossViol (tables : List Table) (D : List Col) (c : Col) : List (List Val × Val) :=
  (pairs tables D c).filter fun p => (pairs tables D c).any fun q => p.1 == q.1 && p.2 != q.2

/-- **The check is exact**: no violation iff `D -> c` holds over the union of
every table binding its columns. -/
theorem cross_check_exact (tables : List Table) (D : List Col) (c : Col) :
    crossViol tables D c = [] ↔
      ∀ p ∈ pairs tables D c, ∀ q ∈ pairs tables D c, p.1 = q.1 → p.2 = q.2 := by
  unfold crossViol
  rw [List.filter_eq_nil_iff]
  refine forall_congr' fun p => imp_congr_right fun _ => ?_
  simp only [List.any_eq_true, Bool.and_eq_true, beq_iff_eq, bne_iff_ne, ne_eq, not_exists,
    not_and, Decidable.not_not]

/-- Rows of a relation built from those tables agree with some table row on
`D` and `c`, so a passing check is the binding holding there. -/
theorem holds_of_cross_check (tables : List Table) (D : List Col) (c : Col)
    (rows : List Row) (hcov : ∀ r ∈ rows, (D.map r, r c) ∈ pairs tables D c)
    (hchk : crossViol tables D c = []) : Holds rows D c := by
  intro r hr s hs hag
  exact (cross_check_exact tables D c).mp hchk _ (hcov r hr) _ (hcov s hs)
    (List.map_congr_left hag)

theorem binding_breaks_caught :
    let dim : Table := ⟨[row3 0 5 0], [1], [1, 2], []⟩
    let trees : Table := ⟨[row3 1 5 0, row3 2 5 1], [0], [0, 1, 2], []⟩
    crossViol [dim, trees] [1] 2 ≠ [] := by
  decide

/-! ## Arms of a family -/

/-- Stacked arms: rows of `a` satisfy the arm on discriminator `d`, rows of
`b` its complement. Each unique on the grain, the stack is unique on grain
plus discriminator. -/
theorem stacked_arms_hold (a b : List Row) (g : List Col) (d c : Col) (q : Val → Bool)
    (ha : ∀ r ∈ a, q (r d) = true) (hb : ∀ r ∈ b, q (r d) = false)
    (hga : Holds a g c) (hgb : Holds b g c) : Holds (a ++ b) (g ++ [d]) c := by
  intro r hr s hs hag
  have hg : Agree g r s := fun x hx => hag x (List.mem_append_left _ hx)
  have hd : r d = s d := hag d (by simp)
  rcases List.mem_append.mp hr with hr | hr <;> rcases List.mem_append.mp hs with hs | hs
  · exact hga r hr s hs hg
  · have := ha r hr; rw [hd, hb s hs] at this; exact absurd this (by simp)
  · have := hb r hr; rw [hd, ha s hs] at this; exact absurd this (by simp)
  · exact hgb r hr s hs hg

/-- Columns 1 `species`, 2 `city`: arms `city = 0` and `city = 1`, each one
row per species. The stack holds species 5 twice, so `species -> city`
fails (`Stack.unbound_arm_repeats_grain`). -/
theorem arm_grain_alone_fails :
    let a := [row3 0 5 0]
    let b := [row3 0 5 1]
    Holds a [1] 2 ∧ Holds b [1] 2 ∧ ¬ Holds (a ++ b) [1] 2 := by
  decide

/-- The arm's determinants carry its bound discriminator. -/
theorem arm_determinants :
    (Table.determinants ⟨[], [1], [1, 2, 3], [2]⟩) = [1, 2] := by decide

/-! ## A grainless table -/

theorem grainless_determines_nothing (t : Table) (h : t.grain = []) : t.fds = [] := by
  simp [Table.fds, h]

/-- Read as dependencies from the empty set, a grainless table's columns join
every closure, though its rows repeat any value. -/
theorem grainless_binding_claims_a_constant :
    let rows := [row3 1 0 0, row3 2 0 0]
    0 ∈ closure [([], 0)] [2] 1 ∧ ¬ Holds rows [2] 0 := by
  decide

end Binding
end Trilogy
