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
# `grain(a, b, ...)` desugars to a hash over its members joined by these ASCII
# unit/record separators (`grain_hash`). They are control characters, so they
# cannot collide with a cast value, and the separator is also what identifies
# the desugared form downstream (`is_grain_identity`). The NULL sentinel must
# differ from the empty string, or ('', NULL) and (NULL, '') would hash alike.
GRAIN_SEPARATOR = "\x1f"
GRAIN_NULL_SENTINEL = "\x1e"
