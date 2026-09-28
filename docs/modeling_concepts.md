

Modeling is based around the following key concepts


1. Keys. Keys define a domain(s), either alone or in combination.

2. Properties; properties hang off a domain keyset.

3. Domains; domains define a bounded set of facts.


Let's make this concrete; classic customers and orders.

Customer address, name, etc are properties of the key customer.id.

Order ship date, product, destination are properties of the key order.id.

If we define a new concept - "is_it_shipped" - that is CASE WHEN ship_date is not null then True else False End - this is a _property_ of the order.id
domain as well. This can be defined explicitly in the model, or inferred 


When we go to resolve a query, the _domains_ determine what the expected resolution is - which is particularly crucial for nulls (unknown values)

Let's take an example;

if we query 
```
select
customer.id,
ship_date,
is_it_shipped,
case when ship_date is not null then True else False END is_it_shipped_two
```

and not every customer has an order, we will get NULL rows for is_it_shipped and is_it_shipped_two.
(we'll also get null rows for ship_date, but perhaps that is less surprising?)

This is confusing, right? Is_it_shipped_two defines a else that would seem to ensure we 
do not have any nulls?

But it's required because of the principled invariant; if we materialized
that case definition in the order domain to the table, we would not have rows for customers without orders;
and so the virtual select must return the same results for that property
as if it were physically materialized. 

If we populated it with the fallback, that would be equivalent to creating a default
ship_date.