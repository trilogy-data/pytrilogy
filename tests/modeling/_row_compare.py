from collections.abc import Sequence
from decimal import Decimal

# DuckDB's parallel float aggregation is not bit-reproducible: the last mantissa
# bits of a stddev/avg shift with thread scheduling, so benchmark rows compared
# against a reference query differ in the ~1e-16 relative range at random. Only
# floats get tolerance -- ints, Decimals and everything else stay exact, so a
# genuinely wrong value still fails.
FLOAT_REL_TOL = 1e-9

_NUMERIC = (int, float, Decimal)


def values_match(expected: object, actual: object) -> bool:
    if isinstance(expected, float) or isinstance(actual, float):
        if not isinstance(expected, _NUMERIC) or not isinstance(actual, _NUMERIC):
            return False
        left, right = float(expected), float(actual)
        if left == right:
            return True
        return abs(left - right) <= FLOAT_REL_TOL * max(abs(left), abs(right))
    return bool(expected == actual)


def rows_match(expected: Sequence[object], actual: Sequence[object]) -> bool:
    return len(expected) == len(actual) and all(
        values_match(left, right) for left, right in zip(expected, actual)
    )
