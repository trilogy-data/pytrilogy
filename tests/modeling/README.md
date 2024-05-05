# Test the following assertions

## Keys
Define a dimensional space with a cardinality.

For example, consider a typical retail dataset.

Orders, stores, and products are core concepts. 

How many stores do we operate? How many products do we sell? How many orders do we have? 
are all reasonable questions to ask.

The time of an order is a property; there is one for each order, but the question
"how many "times of orders we placed" did we have?" isn't a natural question; the
answer is just how many orders were placed.

Sometimes this distinction is more of a judgement call; for example, the color
of products could be defined as property or a key, depending on if
colors were modeled as an independent dimension that could exist outside
of a product association. 

Over-declaration of keys will never result in the wrong answers, but may result
in more complex queries than a human might naturally write.

## Grains
Grains are a vector composed of keys. In query planning, joins connect
keys and the 'group' operator is used to aggregate to the grain.


## Properties
Exist along an existing dimensional space, defined by keys.

The primary feature of a property is that if it present in a query alongside
all of its key values, it does not need to be included in the grain.

A claculation defined at a grain is a property, not a metric.

## Metrics
Metrics are shorthand definitions of a property that will be materialized in an actual query.

Metrics defined in the abstract have no grain; when used in a query they will be materialized
to that grain.

## Query

A query is a desire to project a set of metrics over a set of keys and include a set of properties.

## Example

Let's make this concrete.

Let's assume we have a data model about stores, products, and orders.

```sql
key store_id int;
key product_id int;
key order_id int;
```

We have a dimensional space defined by the keys `store_id`, `product_id`, and `order_id`.

Let's enrich some of these.

```sql
property store_id.store_name string;
property product_id.product_name string;
property order_id.order_time timestamp;
```

If we look for store information,
we should see one row per store.
```python
select store_id, store_name; # this is at the grain of a store
```

If we look for for store and product information,
we will see one for each store, order. 
```python
select store_id, order_id; # this is at the grain of a store, order
```

Pulling in more order properties won't change the grain
```python
select store_id, order_id, order_time; # this is still at the grain of a store, order
```

```python
select store_id, count(order_id)->order_count_per_store; # this is a new property at store grain
```

```python
metric order_count <- count(order_id); # this is a metric
```

```python
select store_id, order_count; # this selects order_count at the grain of store without defining a new concept
```

