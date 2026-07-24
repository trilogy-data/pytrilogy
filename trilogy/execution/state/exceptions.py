import re

from sqlalchemy.exc import StatementError

from trilogy.dialect.base import BaseDialect


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
