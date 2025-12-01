from dataclasses import dataclass
from typing import Any, List, Optional

from trilogy.core.enums import DatasourceState
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ImportStatement,
    MergeStatementV2,
)
from trilogy.core.statements.execute import (
    ProcessedPublishStatement,
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
    ProcessedValidateStatement,
)
from trilogy.core.validation.common import ValidationTest
from trilogy.dialect.base import BaseDialect
from trilogy.engine import ResultProtocol


@dataclass
class MockResult(ResultProtocol):
    values: list["MockResultRow"]
    columns: list[str]

    def __init__(self, values: list[Any], columns: list[str]):
        processed: list[MockResultRow] = []
        for x in values:
            if isinstance(x, dict):
                processed.append(MockResultRow(x))
            elif isinstance(x, MockResultRow):
                processed.append(x)
            else:
                raise ValueError(
                    f"Cannot process value of type {type(x)} in MockResult"
                )
        self.columns = columns
        self.values = processed

    def __iter__(self):
        while self.values:
            yield self.values.pop(0)

    def fetchall(self):
        return self.values

    def fetchone(self):
        if self.values:
            return self.values.pop(0)
        return None

    def fetchmany(self, size: int):
        rval = self.values[:size]
        self.values = self.values[size:]
        return rval

    def keys(self):
        return self.columns

    def as_dict(self):
        return [x.as_dict() if isinstance(x, MockResultRow) else x for x in self.values]


@dataclass
class MockResultRow:
    _values: dict[str, Any]

    def as_dict(self):
        return self._values

    def __str__(self) -> str:
        return str(self._values)

    def __repr__(self) -> str:
        return repr(self._values)

    def __getattr__(self, name: str) -> Any:
        if name in self._values:
            return self._values[name]
        return super().__getattribute__(name)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self):
        return iter(self._values.values())

    def values(self):
        return self._values.values()

    def keys(self):
        return self._values.keys()


def generate_result_set(
    columns: List[ConceptRef], output_data: list[Any]
) -> MockResult:
    """Generate a mock result set from columns and output data."""
    names = [x.address.replace(".", "_") for x in columns]
    return MockResult(
        values=[dict(zip(names, [row])) for row in output_data], columns=names
    )
