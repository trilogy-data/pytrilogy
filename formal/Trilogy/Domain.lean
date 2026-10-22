import Trilogy.Agg

/-!
# Domains, padding, and the grain pin

A `~` FK pads: a customer with no order still has a row in
`select customer_id, <order value>`, with the order's columns ABSENT. An
expression `e` over an order column can be evaluated

- at its own grain (on order rows), so it is absent on the padded row, which
  is exactly what a column stored at the order grain gives through the join; or
- PINNED to the select's row (`docs/grain_pin.md`), where the missing input is
  NULL and a NULL-absorbing `e` (coalesce, CASE ... ELSE) produces a value.

`none` is NULL / absent.
-/

namespace Trilogy

abbrev Val := Option Int

/-- The value a select row of a customer with `orders` carries for `e`. -/
def ownGrain (e : Val → Val) : List Val → List Val
  | [] => [none]
  | os => os.map e

def stored (e : Val → Val) (orders : List Val) : List Val :=
  ownGrain e orders

def pinned (e : Val → Val) : List Val → List Val
  | [] => [e none]
  | os => os.map e

def strict (e : Val → Val) : Prop := e none = none

/-- A column stored at the expression's own grain reads back as the
computation at that grain: what `docs/modeling_concepts.md` calls the
materialization invariant. -/
theorem Domain.materialization_invariance (e : Val → Val) (orders : List Val) :
    stored e orders = ownGrain e orders := rfl

/-- For a NULL-propagating expression the pin changes nothing. -/
theorem Domain.pin_is_own_grain_when_strict (e : Val → Val) (h : strict e)
    (orders : List Val) : pinned e orders = ownGrain e orders := by
  have h' : e none = none := h
  cases orders <;> simp [pinned, ownGrain, h']

def coalesce0 (v : Val) : Val := some (v.getD 0)

/-- The two docs disagree: `modeling_concepts.md` gives the padded row NULL
for a CASE with an ELSE ("same rows whether computed or stored"), while
`grain_pin.md` gives it the ELSE value. No one value satisfies both, so a
stored column cannot answer a pinned select. The engine follows the pin;
`modeling_concepts.md` is the stale one. -/
theorem Domain.pin_contradicts_stored_reading :
    pinned coalesce0 [] ≠ stored coalesce0 [] := by decide

/-! ### The pin is a function of the select's keys

A select pins `e` on each select key some row can hold without `e`'s inputs.
An order-grain output adds the order key, which the inputs cover, so it adds
nothing: the customer with no order keeps the ELSE value whatever else is
projected. The fuzzer's `pin_projection_invariant` law found the planner
breaking this: a comparison sibling (`amount > 15`, `delivery_date is null`)
shared the pinned value's bucket and kept it off the padded rows. -/

inductive Anchor where
  | customer | order
  deriving DecidableEq, Repr

/-- `e` reads order columns: only the customer anchor can lack them. -/
def uncovered (anchors : List Anchor) : List Anchor :=
  anchors.filter (· != Anchor.order)

def paddedValue (e : Val → Val) (anchors : List Anchor) : Val :=
  if uncovered anchors = [] then none else e none

theorem uncovered_append_orders (anchors : List Anchor) (n : Nat) :
    uncovered (anchors ++ List.replicate n Anchor.order) = uncovered anchors := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.replicate_succ', ← List.append_assoc, uncovered, List.filter_append]
    simp [uncovered] at ih ⊢

theorem Pin.projection_invariant (e : Val → Val) (anchors : List Anchor) (n : Nat) :
    paddedValue e (anchors ++ List.replicate n Anchor.order) = paddedValue e anchors := by
  simp only [paddedValue, uncovered_append_orders]

theorem Pin.pins_customer_rows :
    paddedValue coalesce0 [Anchor.customer, Anchor.order] = some 0 := by decide

/-- Named and inline spellings are one expression, so they pin alike, and
the WHERE reads the value the projection shows. -/
theorem Pin.named_eq_inline (e : Val → Val) (anchors : List Anchor) :
    let named := e
    paddedValue named anchors = paddedValue e anchors := rfl

theorem Pin.one_address_one_value (e : Val → Val) (anchors : List Anchor)
    (p : Val → Bool) : p (paddedValue e anchors) = p (paddedValue e anchors) := rfl

/-- SQL `sum` ignores NULLs and is NULL over no values. -/
def osum (vs : List Val) : Val :=
  match vs.filterMap id with
  | [] => none
  | xs => some xs.sum

theorem sum_getD_eq_sum_filterMap (vs : List Val) :
    (vs.map (fun v => v.getD 0)).sum = (vs.filterMap id).sum := by
  induction vs with
  | nil => rfl
  | cons v vs ih => cases v <;> simp [ih]

/-- `sum(coalesce(x, 0))` per group is `coalesce(sum(x), 0)`, on a pinned
padded group (one NULL row) included. -/
theorem Pin.sum_coalesce_eq_coalesce_sum (orders : List Val) :
    ((pinned coalesce0 orders).map (fun v => v.getD 0)).sum =
      (osum orders).getD 0 := by
  have h : ∀ vs : List Val, ((vs.map coalesce0).map (fun v => v.getD 0)).sum =
      (osum vs).getD 0 := by
    intro vs
    have : (vs.map coalesce0).map (fun v => v.getD 0) = vs.map (fun v => v.getD 0) := by
      simp [coalesce0, Function.comp_def]
    rw [this, sum_getD_eq_sum_filterMap]
    unfold osum
    cases vs.filterMap id <;> rfl
  cases orders with
  | nil => rfl
  | cons o os => exact h _

end Trilogy
