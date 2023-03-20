
## Query Planning

Query planning is divided into phases.

In the first pass, every output concept is treated individually. For each one,
the engine will run through strategies to return the output concept at the 
query grain from one or more datasources.

A non-exhaustive list of strategies:
- See if a datasource directly has the field at the grain
- See if an aggregation from a datasource can produce the field
- See if a join between datasources can produce the field at grain
- See if the function requires complex derivation, such as a window function
- Etc

In the second pass, all datasources will be converted to CTEs.

In the third pass, all CTEs will be merged, so that CTEs with the same
sources and grains are consolidated into a singe reference with all 
output columns.

In the last pass, the CTEs will be rendered by a backend appropriate engine,
and any extraneous CTEs will be pruned. (A CTE may become extraneous if it
was originally created to serve a concept that is now returned as a byproduct
of other concept fetching.)