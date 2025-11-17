from trilogy.core.enums import Derivation

ROOT_DERIVATIONS: list[Derivation] = [Derivation.ROOT, Derivation.CONSTANT]
SKIPPED_DERIVATIONS: list[Derivation] = [
    Derivation.AGGREGATE,
    Derivation.FILTER,
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
    Derivation.ROWSET,
    Derivation.BASIC,
    Derivation.GROUP_TO,
    Derivation.MULTISELECT,
    Derivation.UNION,
]
