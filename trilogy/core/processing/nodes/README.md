# Nodes

Nodes are the initial logical planning unit for a query path. 

A query will initially resolve recursively to nodes, which are a lightweight operatoror representation.

(Nodes will then later be instantiated as QueryDatasources/Datasources, a more complete intermediate representation,
before finally becoming CTEs; a complete simplified object that is ready to be rendered as SQL).

## Union Nodes

Union nodes attempt to logically resolve union bindings.

For a union concept:

``` sql
select
    a,
    b,
    c,
    union(a,b),
    union(b,c),
    a.prop,
    b.prop,
    c.prop
```

We 