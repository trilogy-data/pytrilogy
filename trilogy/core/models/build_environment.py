from __future__ import annotations

import difflib
from pathlib import Path
from typing import Annotated, Dict, ItemsView, Never, ValuesView

from pydantic import BaseModel, ConfigDict, Field
from pydantic.functional_validators import PlainValidator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.exceptions import (
    UndefinedConceptException,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildFunction
from trilogy.core.models.core import DataType


class BuildEnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def values(self) -> ValuesView[BuildConcept]:  # type: ignore
        return super().values()

    def get(self, key: str, default: BuildConcept | None = None) -> BuildConcept | None:  # type: ignore
        try:
            return self.__getitem__(key)
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self, key: str, line_no: int | None = None, file: Path | str | None = None
    ) -> Never:

        matches = self._find_similar_concepts(key)
        message = f"Undefined concept: {key}."
        if matches:
            message += f" Suggestions: {matches}"

        if line_no:
            if file:
                raise UndefinedConceptException(
                    f"{file}: {line_no}: " + message, matches
                )
            raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
        raise UndefinedConceptException(message, matches)

    def __getitem__(
        self, key: str, line_no: int | None = None, file: Path | None = None
    ) -> BuildConcept:
        try:
            return super(BuildEnvironmentConceptDict, self).__getitem__(key)
        except KeyError:
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1], line_no)
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
        self.raise_undefined(key, line_no, file)

    def _find_similar_concepts(self, concept_name: str):
        def strip_local(input: str):
            if input.startswith(f"{DEFAULT_NAMESPACE}."):
                return input[len(DEFAULT_NAMESPACE) + 1 :]
            return input

        matches = difflib.get_close_matches(
            strip_local(concept_name), [strip_local(x) for x in self.keys()]
        )
        return matches

    def items(self) -> ItemsView[str, BuildConcept]:  # type: ignore
        return super().items()


class BuildEnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def __getitem__(self, key: str) -> BuildDatasource:
        try:
            return super(BuildEnvironmentDatasourceDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[BuildDatasource]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, BuildDatasource]:  # type: ignore
        return super().items()


def validate_concepts(v) -> BuildEnvironmentConceptDict:
    if isinstance(v, BuildEnvironmentConceptDict):
        return v
    elif isinstance(v, dict):
        return BuildEnvironmentConceptDict(
            **{x: BuildConcept.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


def validate_datasources(v) -> BuildEnvironmentDatasourceDict:
    if isinstance(v, BuildEnvironmentDatasourceDict):
        return v
    elif isinstance(v, dict):
        return BuildEnvironmentDatasourceDict(
            **{x: BuildDatasource.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


class BuildEnvironment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, strict=False)

    concepts: Annotated[
        BuildEnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=BuildEnvironmentConceptDict)
    datasources: Annotated[
        BuildEnvironmentDatasourceDict, PlainValidator(validate_datasources)
    ] = Field(default_factory=BuildEnvironmentDatasourceDict)
    functions: Dict[str, BuildFunction] = Field(default_factory=dict)
    data_types: Dict[str, DataType] = Field(default_factory=dict)
    namespace: str = DEFAULT_NAMESPACE
    cte_name_map: Dict[str, str] = Field(default_factory=dict)
    materialized_concepts: set[str] = Field(default_factory=set)
    alias_origin_lookup: Dict[str, BuildConcept] = Field(default_factory=dict)

    def gen_concept_list_caches(self) -> None:
        concrete_addresses = set()
        for datasource in self.datasources.values():
            for concept in datasource.output_concepts:
                concrete_addresses.add(concept.address)
        self.materialized_concepts = set(
            [
                c.address
                for c in self.concepts.values()
                if c.address in concrete_addresses
            ]
            + [
                c.address
                for c in self.alias_origin_lookup.values()
                if c.address in concrete_addresses
            ],
        )


BuildEnvironment.model_rebuild()
