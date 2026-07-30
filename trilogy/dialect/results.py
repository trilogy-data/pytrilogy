from collections import namedtuple
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from trilogy.core.models.author import ConceptRef
from trilogy.engine import ResultProtocol


def namedtuple_row_class(columns: Sequence[str], name: str = "Row") -> type:
    """Row class for engine adapters that do not go through SQLAlchemy.

    Consumers expect a SQLAlchemy Row: index access, attribute access, and
    equality with a plain tuple. rename=True replaces invalid identifiers with
    _0, _1, ... so column names like "local.x" don't blow up; index access
    still works regardless.
    """
    return namedtuple(name, tuple(columns), rename=True)  # type: ignore[misc]


def buffered_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    name: str = "Row",
) -> "BufferedResult":
    """Build a fully-read result whose rows behave like SQLAlchemy Rows.

    The shared tail of every non-SQLAlchemy engine adapter: take driver column
    names plus positional row values, hand back something the rest of trilogy
    cannot tell apart from a CursorResult.
    """
    if not columns:
        return BufferedResult([], list(rows))
    row_class = namedtuple_row_class(columns, name)
    return BufferedResult(list(columns), [row_class(*row) for row in rows])


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
                raise TypeError(f"Cannot process value of type {type(x)} in MockResult")
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
class BufferedResult(ResultProtocol):
    """A result read fully into memory, so it outlives the cursor that produced
    it. Rows are kept as the driver returned them."""

    columns: list[str]
    rows: list[Any]

    def __iter__(self):
        while self.rows:
            yield self.rows.pop(0)

    def fetchall(self):
        rval = self.rows
        self.rows = []
        return rval

    def fetchone(self):
        if self.rows:
            return self.rows.pop(0)
        return None

    def fetchmany(self, size: int):
        rval = self.rows[:size]
        self.rows = self.rows[size:]
        return rval

    def keys(self):
        return self.columns


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


@dataclass
class ChartResult(ResultProtocol):
    """Result type for chart statements that preserves data for re-rendering."""

    chart: Any
    data: list[list[dict]]
    statement: Any  # ProcessedChartStatement

    def __iter__(self):
        yield MockResultRow({"chart": self.chart})

    def fetchall(self):
        return [MockResultRow({"chart": self.chart})]

    def fetchone(self):
        return MockResultRow({"chart": self.chart})

    def fetchmany(self, size: int):
        return [MockResultRow({"chart": self.chart})]

    def keys(self):
        return ["chart"]


def generate_result_set(
    columns: list[ConceptRef], output_data: list[Any]
) -> MockResult:
    """Generate a mock result set from columns and output data."""
    names = [x.address.replace(".", "_") for x in columns]
    return MockResult(
        values=[dict(zip(names, [row])) for row in output_data], columns=names
    )
