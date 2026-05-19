from sqlalchemy.exc import ProgrammingError, StatementError

from trilogy.dialect.base import BaseDialect


def _error_text(exc: Exception) -> str:
    if isinstance(exc, StatementError) and exc.orig:
        return str(exc.orig)
    return str(exc)


def _is_table_not_found_error(exc: ProgrammingError, dialect: BaseDialect) -> bool:
    """Check if exception is a table-not-found error for the given dialect."""
    pattern = dialect.TABLE_NOT_FOUND_PATTERN
    if pattern is None:
        return False
    error_msg = str(exc.orig) if exc.orig else str(exc)
    return pattern in error_msg


def _is_http_not_found_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception is an HTTP 404 error (e.g., GCS file not found)."""
    pattern = dialect.HTTP_NOT_FOUND_PATTERN
    if pattern is None:
        return False
    return pattern in _error_text(exc)


def _is_column_not_found_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a missing column for the given dialect."""
    pattern = dialect.COLUMN_NOT_FOUND_PATTERN
    if pattern is None:
        return False
    return pattern in _error_text(exc)


def is_missing_source_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a missing source (table or remote file)."""
    if isinstance(exc, ProgrammingError):
        return _is_table_not_found_error(exc, dialect)
    return _is_http_not_found_error(exc, dialect)


def is_schema_mismatch_error(exc: Exception, dialect: BaseDialect) -> bool:
    """Check if exception indicates a schema mismatch (e.g., column not found)."""
    return _is_column_not_found_error(exc, dialect)
