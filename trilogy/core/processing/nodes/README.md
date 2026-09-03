# Nodes

Nodes are the initial logical planning unit for a query path. 

A query will initially resolve recursively to nodes, which are a lightweight operator representation.

(Nodes will then later be instantiated as QueryDatasources/Datasources, a more complete intermediate representation,
before finally becoming CTEs; a complete simplified object that is ready to be rendered as SQL).

## Union Nodes

A `UnionNode` stacks its parents column-positionally (UNION ALL by default;
`set_operator` selects EXCEPT or INTERSECT, where parent order is semantic).
Its grain is its stacked output columns. A `union datasource` resolves to one
through `create_union_datasource_candidate`, which drops branches whose
`non_partial_for` is mutually exclusive with the query's WHERE before stacking
the rest.
