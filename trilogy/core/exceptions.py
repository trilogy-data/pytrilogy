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


def render_datatype(datatype: CONCRETE_TYPES) -> str:
    r"""Render a declared type in authoring syntax (``string['\S+://\S+']::url_image``)
    rather than via its dataclass __str__ (``Trait<string['\\S+://\\S+'], ['url_image']>``),
    which repr-escapes the regex and buries the trait name in a list.

    Deliberately renders without an environment: the environment-aware path
    restores the authored bare alias (``string::url_image``), but an error about
    a domain violation needs to show the domain that was actually violated.
    """
    from trilogy.parsing.render import Renderer

    try:
        return Renderer().to_string(datatype)
    except Exception:
        # rendering is cosmetic - never let it mask the error being reported
        return str(datatype)


class ConfigurationException(Exception):
    pass


class QueryTimeoutException(Exception):
    """A statement was cancelled for outliving the configured query timeout.

    Deliberately not a subclass of anything the retry layer inspects: the
    timeout is the caller's own verdict, so retrying it would just spend the
    same budget again."""


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: list[str]):
        super().__init__(message)
        self.message = message
        self.suggestions = suggestions


class FrozenEnvironmentException(Exception):
    pass


class UnsupportedDialectFeature(NotImplementedError):
    """A valid query that this dialect has no SQL for. Subclasses
    `NotImplementedError` so it still reads as "unimplemented" to anything
    catching that, while naming the dialect and the feature rather than
    surfacing as a bare internal error."""


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


class NothingExecutedException(Exception):
    """A script parsed cleanly but produced no output-producing statement, so the
    run did nothing. Raised only in agent mode, where a zero-statement success is
    indistinguishable from a real one. Deliberately not an
    ``InvalidSyntaxException``: nothing is wrong with the syntax, and labelling
    it that way sends the reader hunting for a parse error."""


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
        # only the message goes to Exception.__init__ — passing `self` too would
        # make args a 2-tuple, so str(exc) renders as
        # "(ModelValidationError(...), 'the real message')" with the message
        # re-escaped by repr, mangling any regex or newline it contains
        super().__init__(message, **kwargs)
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
        expected = render_datatype(self.actual_type)
        if (
            isinstance(declared, ValidatedType)
            and self.value is not None
            and is_compatible_datatype(self.value_type, self.actual_type)
        ):
            return f"value {self.value!r} for concept {self.address} violates declared domain {expected}{actual_mods}"
        # value_type is what was observed in the data, not authored syntax -- keep
        # its plain rendering so a nullability-only mismatch stays legible as
        # "INTEGER(NULLABLE) vs expected type int"
        return f"value {self.value!r} for concept {self.address} has inferred type {self.value_type}{value_mods} vs expected type {expected}{actual_mods}"

    def is_modifier_issue(self) -> bool:
        return len(self.value_modifiers) > 0 and any(
            x not in self.actual_modifiers for x in self.value_modifiers
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
            # join the rendered failures rather than interpolating the list --
            # a list repr would re-escape every message it contains
            detail = "\n".join(f"  {failure.format_failure()}" for failure in errors)
            message = (
                f"Datasource {address} failed validation. "
                f"Data type mismatch:\n{detail}"
            )
        super().__init__(message)
        self.errors = errors
        self.dataset_address = address


class ConceptModelValidationError(ModelValidationError):
    pass


class AmbiguousRelationshipResolutionException(UnresolvableQueryException):
    def __init__(self, message, parents: list[set[str]]):
        super().__init__(message)
        self.message = message
        self.parents = parents
