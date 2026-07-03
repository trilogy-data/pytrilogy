from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

Scalar: TypeAlias = bool | float | int | str | None
Row: TypeAlias = tuple[Scalar, ...]


@dataclass(frozen=True)
class TableData:
    name: str
    columns: tuple[str, ...]
    rows: tuple[Row, ...]

    def select_sql(self) -> str:
        selects = []
        for row in self.rows:
            values = ", ".join(
                f"{sql_literal(value)} as {column}"
                for column, value in zip(self.columns, row)
            )
            selects.append(f"select {values}")
        return "\nunion all\n".join(selects)


@dataclass(frozen=True)
class SeedData:
    name: str
    description: str
    groups: TableData
    events: TableData
    left_facts: TableData
    subset_facts: TableData
    union_facts: TableData
    sales: TableData
    returns: TableData

    @property
    def tables(self) -> tuple[TableData, ...]:
        return (
            self.groups,
            self.events,
            self.left_facts,
            self.subset_facts,
            self.union_facts,
            self.sales,
            self.returns,
        )

    def oracle_ctes(self) -> str:
        return ",\n".join(
            f"{table.name} as (\n{indent(table.select_sql(), 2)}\n)"
            for table in self.tables
        )

    def trilogy_model(self) -> str:
        sources = {table.name: table.select_sql() for table in self.tables}
        return f"""key group_id int;
property group_id.group_name string;
property group_id.nullable_name string?;
property group_id.required_name string;
datasource groups (
    gid: group_id,
    name: group_name,
    nullable_name: nullable_name,
    required_name: required_name
)
grain (group_id)
query '''{sources["groups"]}''';

key event_id int;
property event_id.event_amount int;
property event_id.nullable_amount int?;
property event_id.active bool;
datasource events (
    eid: event_id,
    gid: group_id,
    amount: event_amount,
    nullable_amount: nullable_amount,
    active: active
)
grain (event_id)
query '''{sources["events"]}''';

key left_id int;
property left_id.left_key int?;
property left_id.left_value int;
datasource left_facts (id: left_id, k: left_key, value: left_value)
grain (left_id)
query '''{sources["left_facts"]}''';

key subset_id int;
property subset_id.subset_key int?;
property subset_id.subset_value int;
datasource subset_facts (
    id: subset_id,
    k: subset_key,
    value: subset_value
)
grain (subset_id)
query '''{sources["subset_facts"]}''';

key union_id int;
property union_id.union_key int?;
property union_id.union_value int;
datasource union_facts (id: union_id, k: union_key, value: union_value)
grain (union_id)
query '''{sources["union_facts"]}''';

key sale_id int;
property sale_id.sale_amount int;
datasource sales (id: sale_id, gid: group_id, amount: sale_amount)
grain (sale_id)
query '''{sources["sales"]}''';

key return_id int;
property return_id.return_amount int;
datasource returns (id: return_id, gid: group_id, amount: return_amount)
grain (return_id)
query '''{sources["returns"]}''';

"""


@dataclass(frozen=True)
class FuzzCase:
    case_id: str
    seed: str
    family: str
    description: str
    tags: tuple[str, ...]
    trilogy: str
    oracle_sql: str


def sql_literal(value: Scalar) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def indent(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in value.splitlines())


SEEDS: tuple[SeedData, ...] = (
    SeedData(
        name="edge",
        description=(
            "Small readable rows with nullable attributes, nullable join keys, "
            "side-exclusive domains, and a many-to-many chasm."
        ),
        groups=TableData(
            "groups",
            ("gid", "name", "nullable_name", "required_name"),
            (
                (1, "alpha", None, "A"),
                (2, "beta", "bee", "B"),
                (3, "gamma", None, "C"),
                (4, "delta", "dee", "D"),
            ),
        ),
        events=TableData(
            "events",
            ("eid", "gid", "amount", "nullable_amount", "active"),
            (
                (1, 1, 10, None, True),
                (2, 1, 5, 2, False),
                (3, 2, 7, 7, True),
                (4, 2, 3, None, False),
                (5, 3, 11, 1, True),
                (6, 4, 4, None, True),
            ),
        ),
        left_facts=TableData(
            "left_facts",
            ("id", "k", "value"),
            ((1, 1, 1), (2, 1, 2), (3, 2, 4), (4, 3, 8), (5, None, 16)),
        ),
        subset_facts=TableData(
            "subset_facts",
            ("id", "k", "value"),
            ((1, 1, 100), (2, 2, 200), (3, 2, 400), (4, None, 800)),
        ),
        union_facts=TableData(
            "union_facts",
            ("id", "k", "value"),
            ((1, 1, 10), (2, 2, 20), (3, 2, 40), (4, 4, 80), (5, None, 160)),
        ),
        sales=TableData(
            "sales",
            ("id", "gid", "amount"),
            ((1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 3, 40), (5, 3, 5)),
        ),
        returns=TableData(
            "returns",
            ("id", "gid", "amount"),
            ((1, 1, 1), (2, 1, 2), (3, 1, 4), (4, 2, 8), (5, 4, 16)),
        ),
    ),
    SeedData(
        name="dense",
        description=(
            "A second fixed seed with zeros, negatives, repeated values, and "
            "different fanout ratios."
        ),
        groups=TableData(
            "groups",
            ("gid", "name", "nullable_name", "required_name"),
            (
                (1, "amber", "aye", "AA"),
                (2, "blue", None, "BB"),
                (3, "cyan", "see", "CC"),
                (4, "dun", None, "DD"),
            ),
        ),
        events=TableData(
            "events",
            ("eid", "gid", "amount", "nullable_amount", "active"),
            (
                (1, 1, 0, 0, False),
                (2, 1, -2, None, True),
                (3, 2, 6, 3, True),
                (4, 2, 6, 3, True),
                (5, 3, 9, None, False),
                (6, 3, 1, 1, True),
                (7, 4, 12, None, False),
            ),
        ),
        left_facts=TableData(
            "left_facts",
            ("id", "k", "value"),
            ((1, 1, 3), (2, 2, 5), (3, 2, 7), (4, 3, 11), (5, None, 13)),
        ),
        subset_facts=TableData(
            "subset_facts",
            ("id", "k", "value"),
            ((1, 1, 17), (2, 1, 19), (3, 3, 23), (4, None, 29)),
        ),
        union_facts=TableData(
            "union_facts",
            ("id", "k", "value"),
            ((1, 2, 31), (2, 3, 37), (3, 4, 41), (4, 4, 43), (5, None, 47)),
        ),
        sales=TableData(
            "sales",
            ("id", "gid", "amount"),
            (
                (1, 1, 2),
                (2, 1, 3),
                (3, 1, 5),
                (4, 2, 7),
                (5, 2, 11),
                (6, 4, 13),
            ),
        ),
        returns=TableData(
            "returns",
            ("id", "gid", "amount"),
            ((1, 1, 17), (2, 2, 19), (3, 2, 23), (4, 3, 29)),
        ),
    ),
)
