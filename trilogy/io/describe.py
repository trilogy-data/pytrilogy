"""``--describe``: what this source produces, without producing it.

Lets ``trilogy`` generate the datasource block for a script instead of the
author hand-writing it against a schema they have to guess, and gives the
planner a way to ask what a source can push down without running it.

Type names are spelled out rather than imported from ``trilogy.core``: this
module is loaded inside every script invocation, and the whole point of
``trilogy.io`` staying leaf-level is that it costs a pyarrow import and nothing
else.
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa

from trilogy.io.contract import CONTRACT_VERSION

_UNSAFE = re.compile(r"[^A-Za-z0-9_]")


def trilogy_type(field_type: pa.DataType) -> str:
    if pa.types.is_boolean(field_type):
        return "bool"
    if pa.types.is_integer(field_type):
        return "bigint" if field_type.bit_width > 32 else "int"
    if pa.types.is_floating(field_type):
        return "float"
    if pa.types.is_decimal(field_type):
        return "numeric"
    if pa.types.is_date(field_type):
        return "date"
    if pa.types.is_timestamp(field_type):
        return "timestamp" if field_type.tz else "datetime"
    if pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
        return "bytes"
    if pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
        return "array"
    if pa.types.is_struct(field_type):
        return "struct"
    if pa.types.is_map(field_type):
        return "map"
    if pa.types.is_null(field_type):
        return "null"
    return "string"


def datasource_stub(schema: pa.Schema, script: str, name: str | None = None) -> str:
    """A ready-to-paste trilogy datasource block for this script."""
    identifier = name or _UNSAFE.sub("_", Path(script).stem)
    columns = ",\n".join(f"    {f.name}: {f.name}" for f in schema)
    return (
        f"datasource {identifier}(\n{columns}\n)\n"
        f"grain ({schema.names[0] if schema.names else ''})\n"
        f"file `{script}`;"
    )


def payload(
    schema: pa.Schema, pushdown: tuple[str, ...], script: str
) -> dict[str, object]:
    return {
        "contract": CONTRACT_VERSION,
        "schema": [
            {"name": f.name, "type": trilogy_type(f.type), "nullable": f.nullable}
            for f in schema
        ],
        "pushdown": list(pushdown),
        "datasource": datasource_stub(schema, script),
    }
