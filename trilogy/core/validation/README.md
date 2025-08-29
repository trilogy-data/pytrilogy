# Validation Behavior


## Environment

Runs all checks.

## Datasource

Runs checks by comma separated list of datasource names

### Checks

- Column type bindings
- Grain

## Concepts

Run checks by comma separated list of concept names.

### Checks

- Root concepts have at least one datasource binding
- Key concepts bound to datasources are correctly partial if they do not contan full set

## Internal vs External Valid

Validation requires us to query the DB to get results to compare against in some cases, and minimally have schema access.

For example, validating bindings to a datasource requires getting all column types, which can be done per-engine based on information schema.

Validating datasource _grain_ requires either checking an enforced PK or - more generally - querying to see duplicates.

For inline evaluation in trilogy, we can internally optimize and raise errors by default.

For external cases where the trilogy engine is not being used for DB access - such as for studio - we instead can only validate
checks that do not require DB access.

For those that require DB access, we can instead return the required queries and some logical condition formatting and spec.

The client is responsible for then running the query and evaluating the results. This requires more work to integrate on the client side.

We don't have a canonical interchange format, so this will be brittle until we define that.

TODO: explore if we can offload all checks to SQL? Can we, for example, do the datasource validation by unioning multiple tables together and ensuring that the target table has the max?

