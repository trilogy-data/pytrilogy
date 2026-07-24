from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from trilogy.core.enums import Modifier
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    TraitDataType,
    ValidatedType,
    is_compatible_datatype,
)


class ConfigurationException(Exception):
    pass


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: list[str]):
        super().__init__(self, message)
        self.message = message
        self.suggestions = suggestions


class FrozenEnvironmentException(Exception):
    pass


class InvalidSyntaxException(Exception):
    pass


class FunctionArgumentException(TypeError):
    """A function was called with an argument of the wrong type (e.g. `year()` on
    an integer). Subclasses `TypeError` so existing `except TypeError` handlers
    keep catching it, while letting the harness report a clean type error rather
    than an internal crash."""



class MissingParameterException(InvalidSyntaxException):
    pass


class InvalidComparison(InvalidSyntaxException):
    """A comparison/filter that can never produce a meaningful result, e.g. a
    predicate against an enum field that is tautologically true or false."""



class UnresolvableQueryException(Exception):
    pass


class NoDatasourceException(UnresolvableQueryException):
    pass


class DisconnectedConceptsException(ValueError):
    """Discovery dead-ended because the requested concepts split into multiple
    unconnected subgraphs — no declared join/merge relates their models.

    Subclasses ValueError so existing `except ValueError` discovery handlers keep
    catching it; `subgraphs` carries the partition (each entry a sorted list of
    concept addresses) so callers can render a targeted message."""

    def __init__(self, message: str, subgraphs: Sequence[Sequence[str]]):
        super().__init__(message)
        self.message = message
        self.subgraphs = [list(s) for s in subgraphs]


class UnionOutputResolutionError(ValueError):
    """A union/multiselect output column could not be mapped to a per-arm
    source column within a given CTE (``BuildMultiSelectLineage.find_source``).

    Subclasses ValueError so the renderer's pseudonym-candidate probing (which
    treats ValueError as "this candidate can't render here, try the next")
    can recover when the CTE exposes the same value under a pseudonym twin —
    e.g. a collapsed composite subset join keeps the RHS union-derived keys as
    pseudonym-only outputs. When no candidate recovers, it propagates as an
    internal planner error."""


class ModelValidationError(Exception):
    def __init__(
        self,
        message,
        children: Sequence["ModelValidationError"] | None = None,
        **kwargs,
    ):
        super().__init__(self, message, **kwargs)
        self.message = message
        self.children = children


class DatasourceModelValidationError(ModelValidationError):
    pass


class DatasourceGrainValidationError(DatasourceModelValidationError):
    pass


@dataclass
class DatasourceColumnBindingData:
    address: str
    value: Any
    value_type: CONCRETE_TYPES
    value_modifiers: list[Modifier]
    actual_type: CONCRETE_TYPES
    actual_modifiers: list[Modifier]

    def format_failure(self):
        value_mods = (
            f"({', '.join(x.name for x in self.value_modifiers)})"
            if self.value_modifiers
            else ""
        )
        actual_mods = (
            f"({', '.join(x.name for x in self.actual_modifiers)})"
            if self.actual_modifiers
            else ""
        )
        declared = self.actual_type
        if isinstance(declared, TraitDataType):
            declared = declared.type
        if (
            isinstance(declared, ValidatedType)
            and self.value is not None
            and is_compatible_datatype(self.value_type, self.actual_type)
        ):
            return f"Value '{self.value}' for concept {self.address} violates declared domain {self.actual_type!s}{actual_mods}"
        return f"Value '{self.value}' for concept {self.address} has inferred type {self.value_type}{value_mods} vs expected type {self.actual_type!s}{actual_mods}"

    def is_modifier_issue(self) -> bool:
        return len(self.value_modifiers) > 0 and any(
            [x not in self.actual_modifiers for x in self.value_modifiers]
        )

    def is_type_issue(self) -> bool:
        return self.value_type != self.actual_type


class DatasourceColumnBindingError(DatasourceModelValidationError):
    def __init__(
        self,
        address: str,
        errors: list[DatasourceColumnBindingData],
        message: str | None = None,
    ):
        if not message:
            message = f"Datasource {address} failed validation. Data type mismatch: {[failure.format_failure() for failure in errors]}"
        super().__init__(message)
        self.errors = errors
        self.dataset_address = address


class ConceptModelValidationError(ModelValidationError):
    pass


class AmbiguousRelationshipResolutionException(UnresolvableQueryException):
    def __init__(self, message, parents: list[set[str]]):
        super().__init__(self, message)
        self.message = message
        self.parents = parents
