from dataclasses import dataclass
from typing import Any, List, Sequence

from trilogy.core.enums import Modifier
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)


class ConfigurationException(Exception):
    pass


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: List[str]):
        super().__init__(self, message)
        self.message = message
        self.suggestions = suggestions


class FrozenEnvironmentException(Exception):
    pass


class InvalidSyntaxException(Exception):
    pass


class MissingParameterException(InvalidSyntaxException):
    pass


class UnresolvableQueryException(Exception):
    pass


class NoDatasourceException(UnresolvableQueryException):
    pass


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
    value_type: (
        DataType | ArrayType | StructType | MapType | NumericType | TraitDataType
    )
    value_modifiers: List[Modifier]
    actual_type: (
        DataType | ArrayType | StructType | MapType | NumericType | TraitDataType
    )
    actual_modifiers: List[Modifier]

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
        return f"Value '{self.value}' for concept {self.address} has inferred type {self.value_type}{value_mods} vs expected type {str(self.actual_type)}{actual_mods}"

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
    def __init__(self, message, parents: List[set[str]]):
        super().__init__(self, message)
        self.message = message
        self.parents = parents
