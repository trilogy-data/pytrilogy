# Modeling concepts

A Trilogy model is built from three ideas:

1. **Keys** define a domain, alone or in combination.
2. **Properties** hang off a domain's keys.
3. **Domains** are bounded sets of facts: a property has a value only where its
   keys exist.

Take customers and orders. A customer's address and name are properties of
`customer.id`. An order's ship date, product and destination are properties of
`order.id`.

A derived concept belongs to the domain of the keys it reads. This one is a
property of `order.id` too, whether the model declares it there or Trilogy
infers it:

```
auto is_shipped <- case when ship_date is not null then true else false end;
```

## Domains decide what NULL means

When a query is resolved, the domains of its outputs decide what each row is,
which matters most for NULLs. Suppose not every customer has an order:

```
select
    customer.id,
    ship_date,
    is_shipped,
    case when ship_date is not null then true else false end as is_shipped_two;
```

The customer with no order gets NULL for `ship_date`, and NULL for both
`is_shipped` and `is_shipped_two`, even though each CASE has an ELSE that would
seem to rule NULL out.

That follows from one invariant: **a query returns the same rows whether a
derived property is computed or stored.** Had `is_shipped` been materialized as
a column of the orders table, that table would have no row for a customer
without an order, and the join would give NULL. The computed version must
agree.

Filling in the ELSE value instead would be the same as inventing a default ship
date for an order that does not exist.
