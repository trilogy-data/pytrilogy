import re

from sqlalchemy.exc import StatementError

from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    UnresolvableQueryException,
)
from trilogy.dialect.base import BaseDialect

#: The model cannot answer a question, as opposed to the warehouse failing to.
#:
#: Both expected-side probes (``get_concept_max_watermark_abstract``,
#: ``probe_expected_partitions``) hide every non-root datasource so the planner
#: can only answer from authoritative sources. That routinely leaves a question
#: unanswerable — a derived concept need not be derivable from roots, and in a
#: project that declares no ``root`` at all *nothing* is left to answer from —
#: so "no expectation" is a real result rather than a failure.
#:
#: Narrow on purpose. Catching every exception here would report a broken
#: warehouse, a bad credential or a typo in this module as "nothing is
#: expected", which reads downstream as *fresh* — an outage would look like a
#: clean bill of health. Only planning failures are the model talking; anything
#: else is a problem the caller needs to see.
UNRESOLVABLE_ERRORS = (UnresolvableQueryException, DisconnectedConceptsException)


def is_unresolvable_error(exc: Exception) -> bool:
    """Whether ``exc`` means "the model cannot answer this" — see
    :data:`UNRESOLVABLE_ERRORS`."""
    return isinstance(exc, UNRESOLVABLE_ERRORS)


def _error_text(exc: Exception) -> str:
    if isinstance(exc, StatementError) and exc.orig:
        return str(exc.orig)
    return str(exc)


def _matches(pattern: str | None, exc: Exception) -> bool:
    if pattern is None:
        return False
    return re.search(pattern, _error_text(exc), re.IGNORECASE) is not None


def _is_table_not_found_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception is a table-not-found error for the given dialect."""
    return _matches(dialect.TABLE_NOT_FOUND_PATTERN, exc)


def _is_http_not_found_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception is an HTTP 404 error (e.g., GCS file not found)."""
    return _matches(dialect.HTTP_NOT_FOUND_PATTERN, exc)


def _is_column_not_found_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a missing column for the given dialect."""
    return _matches(dialect.COLUMN_NOT_FOUND_PATTERN, exc)


def is_missing_source_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a missing source (table or remote file)."""
    # Not gated on exception class: which SQLAlchemy error a driver maps a
    # missing remote file to varies by driver and version.
    return _is_table_not_found_error(exc, dialect) or _is_http_not_found_error(
        exc, dialect
    )


def is_schema_mismatch_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a schema mismatch (e.g., column not found)."""
    return _is_column_not_found_error(exc, dialect)
