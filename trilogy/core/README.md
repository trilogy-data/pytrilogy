


### Implicit Filtering Design

An implicit filter is any case where the where clause of a statement references more fields than are in the output of a select.

In those cases, those concepts are "promoted" up into a virtual CTE before the final output is returned.

THe `wrinkle` is aggregates - aggregates must have the implicit filter pushed "inside" the aggregate via the select_context
to ensure that they aggregate appropriately. Applying the condition after creating the aggregate would result in an incorrect 
aggregation result.

### Case 1 - Basic Scalar Filtering

SELECT
    abc,
    def
where
    gde = 1


We should source
    ABC,
    DEF,
    GDE

filter on GDE = 1;

return results with optional grouping.


### Case 2 - Aggregate Filtering

SELECT
    sum(abc)
WHERE
    gde = 1

We should source

    abc | gde =1

;

### Case 3 - Mixed filter

SELECT
    sum(abc) by def,
    def,
    gde

