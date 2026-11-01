/-!
# Optimizer fixpoint: the WHERE-atom contract

The optimizer driver (`optimize_ctes`) reruns its phases until a pass changes
nothing, capped at `MAX_OPTIMIZATION_PASSES`. A *fact remover*
(`atom_contract.FACT_REMOVERS`, today `StripRedundantNotNull`) drops a WHERE
atom its CTE's plan facts make a tautology; `R` is its predicate. The
contract (`Keeps`) is that no other phase adds an atom `R` holds of. The
driver checks it on every phase of every plan the test suite builds
(`contract_violations`, under `enforce_fixpoint_contract`), and predicate
pushdown gates its additions on `removable`, which is `R` itself.

`strip_never_cycles`: under the contract, once the remover has run, any
sequence of phases leaves it nothing to drop, so it never changes the plan
again and cannot take part in a cycle. `ungated_cycles`: pushdown before the
gate broke the contract, and every pass re-added the atom the strip dropped --
issue #830, where every such query ran the optimizer to its pass cap.

Scope: `R` is fixed while the phases run. The remover's facts are outputs,
joins and nullability; join upgrades run before it and only make more atoms
removable. A merge (`CollapseSingleParent`) moves atoms into the CTE that
absorbs another and is exempt from the check: it removes a CTE, so it happens
finitely often, and after the last one `strip_never_cycles` covers the rest.
A remover judging an atom by *other atoms* (`PredicatePushdownRemove` reads its
parents' WHERE) is outside this model: pushdown gates on it when it pushes, and
the pass cap check (`OptimizationNonConvergence`) guards it.
-/

namespace Trilogy.Fixpoint

variable {α : Type}

/-- The atoms in a CTE's WHERE. -/
abbrev State (α : Type) := α → Bool

/-- The fact remover: drop every atom `R` holds of. -/
def strip (R : α → Bool) (s : State α) : State α := fun a => s a && !R a

/-- The contract `contract_violations` checks: a phase adds no atom `R` holds of. -/
def Keeps (R : α → Bool) (f : State α → State α) : Prop :=
  ∀ s a, f s a = true → s a = false → R a = false

/-- No atom left that the remover would drop. -/
def Clean (R : α → Bool) (s : State α) : Prop := ∀ a, s a = true → R a = false

def run (fs : List (State α → State α)) (s : State α) : State α :=
  fs.foldl (fun s f => f s) s

theorem strip_clean (R : α → Bool) (s : State α) : Clean R (strip R s) := by
  intro a h
  simp only [strip, Bool.and_eq_true, Bool.not_eq_true'] at h
  exact h.2

theorem keeps_clean (R : α → Bool) (f : State α → State α) (s : State α)
    (hf : Keeps R f) (hs : Clean R s) : Clean R (f s) := by
  intro a h
  cases hsa : s a
  · exact hf s a h hsa
  · exact hs a hsa

theorem run_clean (R : α → Bool) :
    ∀ (fs : List (State α → State α)) (s : State α),
      (∀ f ∈ fs, Keeps R f) → Clean R s → Clean R (run fs s)
  | [], _, _, hs => hs
  | f :: fs, s, hfs, hs =>
    run_clean R fs (f s) (fun g hg => hfs g (.tail _ hg))
      (keeps_clean R f s (hfs f (.head _)) hs)

theorem strip_quiet (R : α → Bool) (s : State α) (hs : Clean R s) :
    strip R s = s := by
  funext a
  simp only [strip]
  cases hsa : s a
  · rfl
  · simp [hs a hsa]

/-- Once the remover has run, no sequence of contract-keeping phases gives it
anything to drop: it reports no change on every later pass. -/
theorem strip_never_cycles (R : α → Bool) (fs : List (State α → State α))
    (s : State α) (hfs : ∀ f ∈ fs, Keeps R f) :
    strip R (run fs (strip R s)) = run fs (strip R s) :=
  strip_quiet R _ (run_clean R fs _ hfs (strip_clean R s))

/-- Pushdown: add each atom the consumer filters on (`d`), gated on `R`. -/
def push (d R : α → Bool) (s : State α) : State α := fun a => s a || (d a && !R a)

/-- Pushdown before the gate: add every atom the consumer filters on. -/
def pushUngated (d : α → Bool) (s : State α) : State α := fun a => s a || d a

theorem push_keeps (d R : α → Bool) : Keeps R (push d R) := by
  intro s a h hs
  simp only [push, hs, Bool.false_or, Bool.and_eq_true, Bool.not_eq_true'] at h
  exact h.2

/-- After any ungated pass, pushdown changes the plan again and the strip
returns it to where it was: the driver never sees a quiet pass. -/
theorem ungated_cycles (d R : α → Bool) (s : State α) (a : α)
    (hd : d a = true) (hR : R a = true) :
    pushUngated d (strip R (pushUngated d s)) ≠ strip R (pushUngated d s) ∧
      strip R (pushUngated d (strip R (pushUngated d s))) =
        strip R (pushUngated d s) := by
  constructor
  · intro h
    have := congrFun h a
    simp [pushUngated, strip, hd, hR] at this
  · funext b
    simp only [pushUngated, strip]
    cases s b <;> cases d b <;> cases R b <;> rfl

theorem ungated_breaks_contract (d R : α → Bool) (a : α)
    (hd : d a = true) (hR : R a = true) : ¬ Keeps R (pushUngated d) := by
  intro h
  have := h (fun _ => false) a (by simp [pushUngated, hd]) rfl
  simp [hR] at this

end Trilogy.Fixpoint
