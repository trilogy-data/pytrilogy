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

The customer with no order gets NULL for `ship_date`, and `false` for both
`is_shipped` and `is_shipped_two`: a CASE with an ELSE (or a `coalesce`)
absorbs NULL, so it is evaluated on the select's own row, where the missing
order reads as NULL and the ELSE fires (`docs/grain_pin.md`). A NULL-propagating
expression (`upper(ship_date)`, `ship_date is null` alone) stays NULL there.

The invariant is still that **a query returns the same rows whether a derived
property is computed or stored**, but a column stored at the order grain only
answers selects whose rows its orders cover. Read through the join, it would
give NULL on the padded row while the pinned value is `false`, and no single
value satisfies both (`formal/Trilogy/Domain.lean`,
`pin_contradicts_stored_reading`). So a pinned select computes the expression
and never reads the column.

The pin depends only on the select's keys: an order-grain output beside it
(`select customer.id, is_shipped, ship_date`) adds the order key, which the
reads already cover, so the customer without an order keeps `false`
(`Pin.projection_invariant`).

## Windows run over their operand's rows

`lag`/`lead` read the rows their operand lives on. A property brings its keys
into that row stream, wherever it appears in the window, so
`lag(species) over (order by dominance_rank)` walks one row per tree (species
is a property of `tree_id`), and tied `dominance_rank` values stay separate
rows. A partition or order field the stream already carries adds nothing;
any other field brings its own keys.

To walk the distinct values of a property instead, wrap the operand in
`distinct(...)`. `distinct(species)` is sugar for `group(species) by species`
anywhere an expression goes; here it makes the stream one row per species.

```
auto prev_species <- lag(distinct(species), 1) over (order by dominance_rank asc);
```

`order by` a property of `tree_id` (say `planted`) would still bring
`tree_id` back in, because one species has many planted years.

`rank`, `dense_rank` and `row_number` rank their argument's distinct values
(with the partition and order fields), not the rows it lives on:
`rank(species) over (order by count(tree_id) by species desc)` ranks species,
and a select of `tree_id` beside it reads each tree's species' rank. The rank
is the same whatever else the select projects (`formal/Trilogy/Window.lean`).

## NULLs sort last

A bare `asc` or `desc` places NULLs last in both directions, on every
dialect; `nulls first` moves them to the front, and `nulls last` and
`nulls auto` mean the bare form. Engines disagree on the bare form (SQLite,
MySQL, SQL Server and BigQuery put NULLs first under `asc`, Postgres and
Snowflake under `desc`), so those dialects render the placement explicitly.
This matters beyond display order: a rank over a nullable column numbers the
NULL group by where it sorts.
