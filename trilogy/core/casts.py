"""What a cast means, whatever the engine (formal/Trilogy/Casts.lean).

A string reads as a bool as DuckDB reads it: one of these spellings, in any
case. A parse-time fold of a literal and every dialect's rendering share it.
"""

TRUE_TEXT = ("true", "t", "1", "yes", "y")
FALSE_TEXT = ("false", "f", "0", "no", "n")


def text_to_bool(value: str) -> bool:
    folded = value.lower()
    if folded in TRUE_TEXT:
        return True
    if folded in FALSE_TEXT:
        return False
    raise ValueError(f"Cannot cast {value!r} to bool")
