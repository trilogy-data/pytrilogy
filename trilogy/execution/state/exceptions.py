from sqlalchemy.exc import ProgrammingError


def _is_table_not_found_error(exc: ProgrammingError, dialect) -> bool:
    """Check if exception is a table-not-found error for the given dialect."""
    pattern = dialect.TABLE_NOT_FOUND_PATTERN
    if pattern is None:
        return False
    error_msg = str(exc.orig) if exc.orig else str(exc)
    return pattern in error_msg


def _is_http_not_found_error(exc: Exception, dialect) -> bool:
    """Check if exception is an HTTP 404 error (e.g., GCS file not found)."""
    pattern = getattr(dialect, "HTTP_NOT_FOUND_PATTERN", None)
    if pattern is None:
        return False
    error_msg = str(exc.orig) if hasattr(exc, "orig") and exc.orig else str(exc)
    return pattern in error_msg


def is_missing_source_error(exc: Exception, dialect) -> bool:
    """Check if exception indicates a missing source (table or remote file)."""
    if isinstance(exc, ProgrammingError):
        return _is_table_not_found_error(exc, dialect)
    return _is_http_not_found_error(exc, dialect)
