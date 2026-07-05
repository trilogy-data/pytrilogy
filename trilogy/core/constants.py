CONSTANT_DATASET: str = "preql_internal_constant_dataset"
ALL_ROWS_CONCEPT = "all_rows"
INTERNAL_NAMESPACE = "__preql_internal"
PERSISTED_CONCEPT_PREFIX = "__pre_persist"
UNNEST_NAME = "_unnest_alias"
WORKING_PATH_CONCEPT = "_env_working_path"
# Namespace prefix for the anonymous rowset an inline `(select …)` subquery is
# desugared into. Used to mint the name and to skip its auto-promoted hidden
# output when rendering (it re-derives from the inline form on reparse).
SUBQUERY_NAMESPACE_PREFIX = "_subquery_"
