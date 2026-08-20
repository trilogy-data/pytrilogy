import math
import random
from binascii import crc32
from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from trilogy.constants import logger
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    Modifier,
    Purpose,
)
from trilogy.core.models.author import (
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    SubselectComparison,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    ArrayType,
    DataType,
    EnumType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
    ValidatedType,
)
from trilogy.core.models.datasource import Address, ColumnAssignment, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import ProcessedMockStatement
from trilogy.dialect.results import MockResult

if TYPE_CHECKING:
    import pyarrow
    from pyarrow import Table

DEFAULT_SCALE_FACTOR = 100
ARRAY_MOCK_SIZE = 5
MAP_MOCK_SIZE = 3
# Share of a key's domain a `~` binding covers; the tail is what makes
# extension rows (never-ordered customers, never-sold products) exist.
PARTIAL_COVERAGE = 0.8
MOCK_SEED = 8675309
# Rows multiply by this per level of entity nesting: a table referencing
# another entity's key is a fact about it and must carry several rows per
# member, or nothing in the mock ever multiplies rows on a join.
FANOUT_FACTOR = 3
MAX_FANOUT_DEPTH = 3
# Share of a NULLABLE column's rows left empty. A column that declares it can
# be empty and never is makes three-valued logic unobservable.
NULL_FRACTION = 0.1
# element types assumed for parameterless `array`/`map` declarations
BARE_ARRAY_DEFAULT = ArrayType(type=DataType.INTEGER)
BARE_MAP_DEFAULT = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)


def safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def mock_email(scale_factor: int, is_key: bool = False) -> list[str]:
    providers = ["example.com", "test.com", "mock.com", "sample.org"]
    if is_key:
        return [
            f"user{i}@{providers[i % len(providers)]}"
            for i in range(1, scale_factor + 1)
        ]
    return [
        f"user{random.randint(1, 999999)}@{random.choice(providers)}"
        for _ in range(scale_factor)
    ]


def mock_hex_code(scale_factor: int, is_key: bool = False) -> list[str]:
    if is_key:
        return [f"#{i:06x}" for i in range(1, scale_factor + 1)]
    return [f"#{random.randint(0, 0xFFFFFF):06x}" for _ in range(scale_factor)]


def mock_strings(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [f"key_{i}" for i in range(1, scale_factor + 1)]
    return [f"mock_string_{random.randint(0, 999_999)}" for _ in range(scale_factor)]


def mock_integers(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return list(range(1, scale_factor + 1))
    return [random.randint(0, 999_999) for _ in range(scale_factor)]


def mock_floats(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [float(i) for i in range(1, scale_factor + 1)]
    return [random.uniform(0, 999_999) for _ in range(scale_factor)]


def mock_bytes(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [f"key_{i}".encode() for i in range(1, scale_factor + 1)]
    return [random.randbytes(12) for _ in range(scale_factor)]


def mock_bools(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        # the whole 2-value domain, once each: a solely bool-grained mock
        # table caps at 2 rows; in a composite grain the other components
        # carry the row count (see MockManager.column_values)
        return [False, True][:scale_factor]
    return [random.choice([True, False]) for _ in range(scale_factor)]


def mock_dates(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        base_date = date(2023, 1, 1)
        return [
            date.fromordinal(base_date.toordinal() + i) for i in range(scale_factor)
        ]
    # a full year, not a single month: a rollup grained on a date part is one
    # row wide if every mocked instant lands in the same bucket
    base_date = date(2023, 1, 1)
    return [
        base_date + timedelta(days=random.randint(0, 364)) for _ in range(scale_factor)
    ]


def mock_datetimes(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        base_dt = datetime(2023, 1, 1, 0, 0, 0)
        return [
            datetime.fromtimestamp(base_dt.timestamp() + i) for i in range(scale_factor)
        ]
    return [
        datetime(2023, 1, 1)
        + timedelta(days=random.randint(0, 364), seconds=random.randint(0, 86_399))
        for _ in range(scale_factor)
    ]


def mock_decimals(
    precision: int, scale: int, scale_factor: int, is_key: bool
) -> list[Any]:
    # Decimal, not float: exact-numeric columns must not pick up binary-float
    # artifacts. Values are quantized to the declared scale.
    unit = Decimal(1).scaleb(-scale)
    if is_key:
        return [Decimal(i) * unit for i in range(1, scale_factor + 1)]
    hi = min(10**precision - 1, 999_999 * 10**scale)
    return [Decimal(random.randint(0, hi)) * unit for _ in range(scale_factor)]


def mock_arrays(datatype: ArrayType, scale_factor: int, is_key: bool) -> list[Any]:
    element = datatype.value_data_type
    if is_key:
        # rows are distinguished by a unique, same-typed leading element
        leads = mock_datatype(element, element, scale_factor, True)
        tail = mock_datatype(element, element, ARRAY_MOCK_SIZE - 1, False)
        return [[lead, *tail] for lead in leads]
    pool = mock_datatype(element, element, scale_factor * ARRAY_MOCK_SIZE, False)
    return [
        pool[i * ARRAY_MOCK_SIZE : (i + 1) * ARRAY_MOCK_SIZE]
        for i in range(scale_factor)
    ]


def mock_maps(datatype: MapType, scale_factor: int, is_key: bool) -> list[Any]:
    key_type = datatype.key_data_type
    value_type = datatype.value_data_type
    keys = mock_datatype(key_type, key_type, MAP_MOCK_SIZE, True)
    if is_key:
        # rows are distinguished by a unique value under the first key
        firsts = mock_datatype(value_type, value_type, scale_factor, True)
        rest = mock_datatype(value_type, value_type, MAP_MOCK_SIZE - 1, False)
        return [dict(zip(keys, [first, *rest])) for first in firsts]
    pool = mock_datatype(value_type, value_type, scale_factor * MAP_MOCK_SIZE, False)
    return [
        dict(zip(keys, pool[i * MAP_MOCK_SIZE : (i + 1) * MAP_MOCK_SIZE]))
        for i in range(scale_factor)
    ]


def mock_structs(datatype: StructType, scale_factor: int, is_key: bool) -> list[Any]:
    columns = {
        name: mock_datatype(ftype, ftype, scale_factor, is_key and idx == 0)
        for idx, (name, ftype) in enumerate(datatype.field_types.items())
    }
    return [
        {name: values[i] for name, values in columns.items()}
        for i in range(scale_factor)
    ]


BASE_GENERATORS: dict[DataType, Callable[[int, bool], list[Any]]] = {
    DataType.STRING: mock_strings,
    DataType.INTEGER: mock_integers,
    DataType.BIGINT: mock_integers,
    DataType.FLOAT: mock_floats,
    DataType.DOUBLE: mock_floats,
    DataType.NUMBER: mock_floats,
    DataType.BYTES: mock_bytes,
    DataType.BOOL: mock_bools,
    DataType.DATE: mock_dates,
    DataType.DATETIME: mock_datetimes,
    DataType.TIMESTAMP: mock_datetimes,
}


def with_bounds(pool: list[Any], bounds: list[Any]) -> list[Any]:
    """Seat every declared endpoint in the pool.

    A range's edges are where inclusive/exclusive and off-by-one bugs live, and
    a pool that samples the interior never lands on one. Endpoints replace
    leading entries rather than extending the pool, so the caller's size (and
    with it the table's row count) is unchanged.
    """
    missing = [b for b in dict.fromkeys(bounds) if b not in pool]
    if not missing or len(pool) < len(missing):
        return pool
    return missing + pool[len(missing) :]


def mock_validated(
    full_type: ValidatedType, scale_factor: int, is_key: bool
) -> list[Any]:
    if full_type.pattern is not None:
        raise NotImplementedError(
            f"Mocking is not implemented for regex-validated type {full_type}"
        )
    base = full_type.data_type
    ranges = full_type.ranges
    if base in (DataType.INTEGER, DataType.BIGINT):
        pool: list[int] = []
        edges: list[Any] = []
        for r in ranges:
            lo = int(r.min) if r.min is not None else int(r.max) - 999_999  # type: ignore[arg-type]
            hi = int(r.max) if r.max is not None else int(r.min) + 999_999  # type: ignore[arg-type]
            pool.extend(range(lo, min(hi, lo + scale_factor - 1) + 1))
            edges += [lo, hi]
        if is_key:
            # A key must not repeat: a domain smaller than scale_factor caps
            # the row count (the mock table sizes to its grain).
            return with_bounds(pool[:scale_factor], edges)
        return with_bounds([random.choice(pool) for _ in range(scale_factor)], edges)
    if base in (DataType.FLOAT, DataType.DOUBLE, DataType.NUMBER, DataType.NUMERIC):
        bounds: list[tuple[float, float]] = []
        for r in ranges:
            flo = float(r.min) if r.min is not None else float(r.max) - 999_999.0  # type: ignore[arg-type]
            fhi = float(r.max) if r.max is not None else float(r.min) + 999_999.0  # type: ignore[arg-type]
            bounds.append((flo, fhi))
        edges = [edge for pair in bounds for edge in pair]
        if is_key:
            flo, fhi = bounds[0]
            step = (fhi - flo) / max(scale_factor, 1)
            return with_bounds([flo + step * i for i in range(scale_factor)], edges)
        return with_bounds(
            [random.uniform(*random.choice(bounds)) for _ in range(scale_factor)],
            edges,
        )
    if base in (DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP):
        default_span = (
            timedelta(days=999) if base == DataType.DATE else timedelta(seconds=999_999)
        )
        spans: list[tuple[Any, int]] = []
        for r in ranges:
            rmin: Any = r.min
            rmax: Any = r.max
            tlo = rmin if rmin is not None else rmax - default_span
            thi = rmax if rmax is not None else rmin + default_span
            delta = thi - tlo
            units = delta.days if base == DataType.DATE else int(delta.total_seconds())
            spans.append((tlo, units))
        unit = "days" if base == DataType.DATE else "seconds"
        edges = [
            edge
            for tlo, units in spans
            for edge in (tlo, tlo + timedelta(**{unit: units}))
        ]
        if is_key:
            tlo, units = spans[0]
            return with_bounds(
                [
                    tlo + timedelta(**{unit: i})
                    for i in range(min(scale_factor, units + 1))
                ],
                edges,
            )
        out: list[Any] = []
        for _ in range(scale_factor):
            tlo, units = random.choice(spans)
            out.append(tlo + timedelta(**{unit: random.randint(0, units)}))
        return with_bounds(out, edges)
    raise NotImplementedError(f"Mocking not implemented for validated type {full_type}")


def mock_datatype(
    full_type: Any, datatype: CONCRETE_TYPES, scale_factor: int, is_key: bool = False
) -> list[Any]:
    if isinstance(full_type, ValidatedType):
        return mock_validated(full_type, scale_factor, is_key)
    if isinstance(full_type, EnumType):
        values = list(full_type.values)
        if not values:
            raise ValueError(f"Enum {full_type} has no values to mock")
        if is_key:
            # A key must not repeat: each enum value at most once. A domain
            # smaller than scale_factor caps the row count instead — the mock
            # table sizes to its grain (see create_mock_table).
            return values[:scale_factor]
        return [random.choice(values) for _ in range(scale_factor)]
    if isinstance(full_type, TraitDataType):
        # An enum under a trait keeps its finite domain — the trait generator
        # would step outside it (e.g. enum<string>[...]::email_address). A
        # regex-validated string under a trait is the opposite case: the
        # generator is the only way to satisfy the pattern.
        if (
            not isinstance(full_type.type, EnumType)
            and full_type.type == DataType.STRING
        ):
            # TODO: get stdlib inventory some other way?
            if full_type.traits == ["email_address"]:
                # email mock function
                return mock_email(scale_factor, is_key)
            elif full_type.traits == ["hex"]:
                return mock_hex_code(scale_factor, is_key)
        return mock_datatype(full_type.type, full_type.type, scale_factor, is_key)

    concrete: Any = (
        full_type
        if isinstance(full_type, (NumericType, ArrayType, MapType, StructType))
        else datatype
    )
    # parameterless spellings of parameterized types get canonical defaults
    if concrete == DataType.NUMERIC:
        concrete = NumericType()
    elif concrete == DataType.ARRAY:
        concrete = BARE_ARRAY_DEFAULT
    elif concrete == DataType.MAP:
        concrete = BARE_MAP_DEFAULT

    if isinstance(concrete, NumericType):
        return mock_decimals(concrete.precision, concrete.scale, scale_factor, is_key)
    if isinstance(concrete, ArrayType):
        return mock_arrays(concrete, scale_factor, is_key)
    if isinstance(concrete, MapType):
        return mock_maps(concrete, scale_factor, is_key)
    if isinstance(concrete, StructType):
        return mock_structs(concrete, scale_factor, is_key)

    base = concrete if isinstance(concrete, DataType) else concrete.data_type
    generator = BASE_GENERATORS.get(base)
    if generator is None:
        raise NotImplementedError(f"Mocking is not implemented for datatype {datatype}")
    return generator(scale_factor, is_key)


def arrow_column_type(datatype: CONCRETE_TYPES) -> "pyarrow.DataType | None":
    """Explicit arrow type where inference would pick the wrong container:
    python dicts infer as struct, so declared maps need pa.map_. Everything
    else infers correctly from the generated values; None means infer."""
    import pyarrow as pa

    while isinstance(datatype, TraitDataType):
        datatype = datatype.type
    if datatype == DataType.MAP:
        datatype = BARE_MAP_DEFAULT
    if not isinstance(datatype, MapType):
        return None
    key = arrow_scalar_type(datatype.key_data_type)
    value = arrow_scalar_type(datatype.value_data_type)
    if key is None or value is None:
        return None
    return pa.map_(key, value)


def arrow_scalar_type(datatype: CONCRETE_TYPES) -> "pyarrow.DataType | None":
    import pyarrow as pa

    while isinstance(datatype, TraitDataType):
        datatype = datatype.type
    if isinstance(datatype, NumericType):
        return pa.decimal128(datatype.precision, datatype.scale)
    if not isinstance(datatype, DataType):
        return None
    scalar_map: dict[DataType, pyarrow.DataType] = {
        DataType.STRING: pa.string(),
        DataType.INTEGER: pa.int64(),
        DataType.BIGINT: pa.int64(),
        DataType.FLOAT: pa.float64(),
        DataType.DOUBLE: pa.float64(),
        DataType.NUMBER: pa.float64(),
        DataType.NUMERIC: pa.decimal128(NumericType().precision, NumericType().scale),
        DataType.BOOL: pa.bool_(),
        DataType.DATE: pa.date32(),
        DataType.DATETIME: pa.timestamp("us"),
        DataType.TIMESTAMP: pa.timestamp("us"),
        DataType.BYTES: pa.binary(),
    }
    return scalar_map.get(datatype)


def cast_target_map(environment: Environment) -> dict[str, CONCRETE_TYPES]:
    """Concepts whose physical column feeds a datasource-declared cast
    (`concept::type: other` column bindings) must mock as values that survive
    that cast; a random string in a column cast to date aborts validation."""
    out: dict[str, CONCRETE_TYPES] = {}
    for ds in environment.datasources.values():
        for col in ds.columns:
            if (
                isinstance(col.alias, Function)
                and col.alias.operator == FunctionType.CAST
                and len(col.alias.arguments) == 2
                and isinstance(col.alias.arguments[0], ConceptRef)
                and isinstance(
                    col.alias.arguments[1],
                    (
                        DataType,
                        MapType,
                        ArrayType,
                        NumericType,
                        StructType,
                        TraitDataType,
                        EnumType,
                        ValidatedType,
                    ),
                )
            ):
                out[col.alias.arguments[0].address] = col.alias.arguments[1]
    return out


def partial_pool(pool: list[Any]) -> list[Any]:
    """The prefix of a key's domain a `~` binding covers.

    A partial binding whose column cycles the whole pool is indistinguishable
    from a complete one: LEFT and INNER return the same rows, so no unit-tier
    test can see a join-type regression on the bridge. Every partial binding of
    a concept takes the *same* prefix, so partial sources stay mutually
    consistent and the uncovered tail is stable.
    """
    return pool[: max(1, min(len(pool) - 1, int(len(pool) * PARTIAL_COVERAGE)))]


def _find(parent: dict[str, str], address: str) -> str:
    parent.setdefault(address, address)
    while parent[address] != address:
        parent[address] = parent[parent[address]]
        address = parent[address]
    return address


def _union(parent: dict[str, str], left: str, right: str) -> None:
    roots = sorted({_find(parent, left), _find(parent, right)})
    if len(roots) == 2:
        parent[roots[1]] = roots[0]


def canonical_column_map(environment: Environment) -> dict[str, str]:
    """Map every concept address to a representative of the physical column it
    is bound to.

    One model can reach the same table through several namespaces — thelook's
    ``users`` is bound once as ``user.id`` and again, through the orders import,
    as ``order.user.id``. Both write the same physical table, so mocking them as
    independent pools makes the second write contradict the first and turns any
    foreign key through them into fiction. Addresses sharing a
    (physical address, column alias) are the same column and share one pool.
    """
    groups: dict[tuple[str, str], list[str]] = {}
    for ds in environment.datasources.values():
        for alias, column in ds.concrete_columns.items():
            groups.setdefault((ds.safe_address, alias), []).append(
                column.concept.address
            )
    parent: dict[str, str] = {}
    for members in groups.values():
        for member in members[1:]:
            _union(parent, members[0], member)
    return {address: _find(parent, address) for address in list(parent)}


def covers_pool(values: Iterable[Any], pool: list[Any]) -> bool:
    try:
        return set(values) >= set(pool)
    except TypeError:
        return False


def datasource_depths(
    datasources: Iterable[Datasource], canon: Callable[[str], str]
) -> dict[str, int]:
    """How many levels of entity each datasource sits above the leaves.

    A datasource that binds another's single-component grain key outside its own
    grain is a fact about that entity — an order references a customer, a sale
    line references an order. The depth is what sizes the table: without it
    every table is the same height, every foreign key appears exactly once, and
    a join that multiplies rows is indistinguishable from one that doesn't.
    """
    datasources = list(datasources)
    columns = {
        ds.identifier: {canon(c.concept.address) for c in ds.concrete_columns.values()}
        for ds in datasources
    }
    grains = {
        ds.identifier: {canon(a) for a in ds.grain.components} for ds in datasources
    }
    owners: dict[str, str] = {}
    for ds in datasources:
        grain = grains[ds.identifier]
        if len(grain) == 1:
            owners.setdefault(next(iter(grain)), ds.identifier)
    references = {
        ds.identifier: {
            owner
            for key, owner in owners.items()
            if owner != ds.identifier
            and key in columns[ds.identifier]
            and key not in grains[ds.identifier]
        }
        for ds in datasources
    }
    depths = {identifier: 0 for identifier in references}
    # bounded rather than recursive: a reference cycle is a contradictory model,
    # not a reason to hang
    for _ in range(len(references)):
        settled = True
        for identifier, refs in references.items():
            candidate = max((depths[r] + 1 for r in refs), default=0)
            if candidate > depths[identifier]:
                depths[identifier] = candidate
                settled = False
        if settled:
            break
    return depths


LITERAL_TYPES = (str, int, float, bool, Decimal, date, datetime)


def _conjuncts(node: Any) -> Iterable[Any]:
    if isinstance(node, Conditional) and node.operator == BooleanOperator.AND:
        yield from _conjuncts(node.left)
        yield from _conjuncts(node.right)
    else:
        yield node


def _declared_domain(conjunct: Any) -> tuple[str, list[Any]] | None:
    left = getattr(conjunct, "left", None)
    right = getattr(conjunct, "right", None)
    if not isinstance(left, ConceptRef):
        return None
    if isinstance(conjunct, SubselectComparison):
        if conjunct.operator != ComparisonOperator.IN or not isinstance(
            right, (list, tuple)
        ):
            return None
        values = [v for v in right if isinstance(v, LITERAL_TYPES)]
        return (left.address, values) if len(values) == len(right) else None
    if (
        isinstance(conjunct, Comparison)
        and conjunct.operator == ComparisonOperator.EQ
        and isinstance(right, LITERAL_TYPES)
    ):
        return left.address, [right]
    return None


def declared_value_domains(
    environment: Environment,
) -> dict[str, dict[str, list[Any]]]:
    """The values a datasource's own ``where`` clause admits, per column.

    A datasource declared ``where status = 'Complete'`` describes a table that
    holds only complete rows. Mocking it with every status validates the model
    against data its own declaration says cannot exist — and any planner that
    prunes, rewrites, or claims completeness on the strength of that filter is
    then checked against a contradiction. Only conjunctions of ``=`` and ``in``
    against literals are honoured; anything else is reported rather than
    silently ignored.
    """
    out: dict[str, dict[str, list[Any]]] = {}
    for ds in environment.datasources.values():
        if ds.where is None:
            continue
        domains: dict[str, list[Any]] = {}
        for conjunct in _conjuncts(ds.where.conditional):
            found = _declared_domain(conjunct)
            if found is None:
                logger.warning(
                    "Mock: cannot honour part of the `where` on datasource %s "
                    "(%s); its rows may contradict the declared filter.",
                    ds.identifier,
                    conjunct,
                )
                continue
            address, values = found
            existing = domains.get(address)
            domains[address] = (
                [v for v in values if v in existing] if existing else values
            )
        if domains:
            out.setdefault(ds.safe_address, {}).update(domains)
    return out


def punch_nulls(values: list[Any], seed: int) -> list[Any]:
    """Empty a deterministic share of a NULLABLE column's rows, without losing
    any distinct value.

    Nothing in mock data is NULL today, so a unit run cannot see three-valued
    logic in a filter, `COUNT(col)` diverging from `COUNT(*)`, or — the case
    that matters most here — a value NULL behaving differently from an
    outer-join padding NULL. Every value keeps at least one populated row so
    the distinct-value counts `validate_multi_datasource_concept` compares are
    unchanged; a column that loses a value to nulling would read as a
    datasource missing data it declares.
    """
    try:
        remaining: dict[Any, int] = {}
        for value in values:
            remaining[value] = remaining.get(value, 0) + 1
    except TypeError:  # unhashable values can't be coverage-checked
        return values
    rng = random.Random(seed)
    order = list(range(len(values)))
    rng.shuffle(order)
    out = list(values)
    budget = int(len(values) * NULL_FRACTION)
    for index in order:
        if budget <= 0:
            break
        value = out[index]
        if remaining[value] <= 1:
            continue
        remaining[value] -= 1
        out[index] = None
        budget -= 1
    return out


def address_column_map(
    environment: Environment,
) -> dict[str, dict[str, ColumnAssignment]]:
    """Every column each physical address carries, across all the datasources
    that bind it.

    Each datasource writes its whole table, so where several bind one address
    the last write wins — and if their column sets differ, the winner drops
    columns the others need. The table is one table: it gets the union. A
    column stays partial only if every binding of it is partial, since one
    complete binding means the physical column covers its domain.
    """
    out: dict[str, dict[str, ColumnAssignment]] = {}
    for ds in environment.datasources.values():
        columns = out.setdefault(ds.safe_address, {})
        for alias, column in ds.concrete_columns.items():
            existing = columns.get(alias)
            if existing is None:
                columns[alias] = column
            elif existing.is_complete or column.is_complete:
                columns[alias] = ColumnAssignment(
                    alias=existing.alias,
                    concept=existing.concept,
                    modifiers=[m for m in existing.modifiers if m != Modifier.PARTIAL],
                )
    return out


def cycle_offset(address: str, size: int) -> int:
    """Where a key's cycle starts, keyed on the concept so the sequence is
    identical in every table that binds it.

    Two independent keys cycling dense pools from zero pair up one-to-one —
    `user_id` equals `product_id` on every row — and a join through the wrong
    key is then indistinguishable from the right one. Rotating each key by its
    own offset separates them while leaving the cycle (and so every
    distinct-value count validation compares) untouched.
    """
    return crc32(address.encode("utf-8")) % size if size else 0


class MockManager:

    def __init__(
        self, environment: Environment, scale_factor: int = DEFAULT_SCALE_FACTOR
    ):
        self.environment = environment
        self.concept_mocks: dict[str, Any] = {}
        self.scale_factor = scale_factor
        self.canonical = canonical_column_map(environment)
        self.address_columns = address_column_map(environment)
        self.value_domains = declared_value_domains(environment)
        # Concepts that must be unique-per-row to satisfy any datasource grain.
        # Without this, an aggregate datasource grained on a non-KEY concept
        # (e.g. a date) gets duplicate rows and fails grain validation.
        self.key_addresses: set[str] = {
            self.canon(addr)
            for addr, c in environment.concepts.items()
            if c.purpose == Purpose.KEY
        }
        for ds in environment.datasources.values():
            self.key_addresses.update(self.canon(a) for a in ds.grain.components)
        self.cast_targets = {
            self.canon(addr): target
            for addr, target in cast_target_map(environment).items()
        }
        # determinant address -> dependent address -> {key value: value}, from
        # every single-key datasource already mocked.
        self.dependencies: dict[str, dict[str, dict[Any, Any]]] = {}
        depths = datasource_depths(environment.datasources.values(), self.canon)
        self.row_targets = {
            identifier: scale_factor * FANOUT_FACTOR ** min(depth, MAX_FANOUT_DEPTH)
            for identifier, depth in depths.items()
        }
        # A concept's pool sizes to the SHALLOWEST table binding it — its owning
        # entity. Sizing it to the deepest would give a denormalized property
        # more distinct values in the fact than in the dimension, which
        # validate_multi_datasource_concept reads as a missing-values bug.
        self.concept_scale: dict[str, int] = {}
        for ds in environment.datasources.values():
            target = self.row_targets[ds.identifier]
            for column in ds.concrete_columns.values():
                address = self.canon(column.concept.address)
                self.concept_scale[address] = min(
                    self.concept_scale.get(address, target), target
                )

    def canon(self, address: str) -> str:
        return self.canonical.get(address, address)

    def mock_concept(self, concept: Concept | ConceptRef):
        address = self.canon(concept.address)
        if address in self.concept_mocks:
            return False
        is_key = address in self.key_addresses
        cast_target = self.cast_targets.get(address)
        size = self.concept_scale.get(address, self.scale_factor)
        try:
            if (
                cast_target is not None
                and concept.datatype.data_type == DataType.STRING
            ):
                self.concept_mocks[address] = [
                    str(v)
                    for v in mock_datatype(cast_target, cast_target, size, is_key)
                ]
            else:
                self.concept_mocks[address] = mock_datatype(
                    concept.datatype,
                    concept.output_datatype,
                    size,
                    is_key,
                )
        except NotImplementedError as e:
            raise NotImplementedError(
                f"Cannot mock column bound to {concept.address}: {e}"
            ) from e
        return True

    def _determinant(
        self,
        header: str,
        headers: list[str],
        canon_by_header: dict[str, str],
    ) -> tuple[str, dict[Any, Any]] | None:
        """The column in this table whose value fixes ``header``'s, if one of
        the already-mocked datasources established that dependency."""
        address = canon_by_header[header]
        for other in headers:
            if other == header:
                continue
            mapping = self.dependencies.get(canon_by_header[other], {}).get(address)
            if mapping is not None:
                return other, mapping
        return None

    def cycled(self, address: str, pool: list[Any], rows: int) -> list[Any]:
        offset = (
            cycle_offset(address, len(pool)) if address in self.key_addresses else 0
        )
        return [pool[(i + offset) % len(pool)] for i in range(rows)]

    def multiplied(
        self, address: str, pool: list[Any], rows: int, salt: str
    ) -> list[Any]:
        """A foreign key's values across a fact: every member at least once,
        the surplus spread at random.

        Every member has to appear or a complete binding looks like it is
        missing values, but giving them all the *same* number of rows is its own
        fiction — no customer has the mean order count, and a uniform ratio
        hides both skew handling and the difference between an aggregate that
        double-counts and one that doesn't. Drawing the surplus independently
        per column also decorrelates two foreign keys of equal cardinality,
        which cycling in lockstep pins to a diagonal.
        """
        if rows <= len(pool):
            return self.cycled(address, pool, rows)
        rng = random.Random(crc32(f"{salt}|{address}|{rows}".encode()))
        counts = [1] * len(pool)
        for _ in range(rows - len(pool)):
            counts[rng.randrange(len(pool))] += 1
        values = [value for value, count in zip(pool, counts) for _ in range(count)]
        rng.shuffle(values)
        return values

    def column_values(
        self,
        concepts: list[Concept | ConceptRef],
        headers: list[str],
        grain: set[str],
        partial_headers: set[str],
        rows: int,
        salt: str,
        header_domains: dict[str, list[Any]] | None = None,
    ) -> dict[str, list[Any]]:
        header_domains = header_domains or {}
        pools = [
            (partial_pool(pool) if h in partial_headers else pool)
            for h, pool in (
                (h, header_domains.get(h) or self.concept_mocks[self.canon(c.address)])
                for h, c in zip(headers, concepts)
            )
        ]
        canon_by_header = {h: self.canon(c.address) for h, c in zip(headers, concepts)}
        grain_lens = [
            len(pool) for h, pool in zip(headers, pools) if canon_by_header[h] in grain
        ]
        # A grain column cycles its own pool, so row i is pool[(i + offset) %
        # len]: the grain tuple stays unique through the lcm of its components'
        # domain sizes (equal rows require i ≡ j mod every length, whatever the
        # offsets). A composite grain over small domains therefore fills the
        # combination space rather than capping at its smallest column, while
        # each concept's distinct-value set stays the same in every table —
        # which validate_multi_datasource_concept compares.
        n = min(rows, math.lcm(*grain_lens) if grain_lens else rows)
        pool_by_header = dict(zip(headers, pools))
        determined: dict[str, tuple[str, dict[Any, Any]]] = {}
        for header in headers:
            if canon_by_header[header] in grain:
                continue
            found = self._determinant(header, headers, canon_by_header)
            if found:
                determined[header] = found
        # a key bound outside this table's own grain is a foreign key: it is
        # allowed — required — to repeat
        foreign = {
            h
            for h in headers
            if canon_by_header[h] not in grain
            and canon_by_header[h] in self.key_addresses
        }
        fills = {
            h: (
                self.multiplied(canon_by_header[h], pool_by_header[h], n, salt)
                if h in foreign
                else self.cycled(canon_by_header[h], pool_by_header[h], n)
            )
            for h in headers
        }
        data: dict[str, list[Any]] = {
            h: fills[h] for h in headers if h not in determined
        }
        pending = list(determined)
        while pending:
            resolved = [h for h in pending if determined[h][0] in data]
            if not resolved:
                break
            for header in resolved:
                source, mapping = determined[header]
                keys = data[source]
                projected = (
                    [mapping[k] for k in keys]
                    if all(k in mapping for k in keys)
                    else None
                )
                # Honour the `~` over the dependency when the two disagree: if
                # inheriting would reach every member of the domain, the model
                # is over-constrained (a complete determinant fixing a partial
                # dependent), and a partial binding that covers everything is
                # the failure this generator exists to prevent. Judged on what
                # the determinant actually holds here, not on the source's
                # whole mapping — a partial determinant projects to a subset.
                if projected is None or (
                    header in partial_headers
                    and covers_pool(
                        projected, self.concept_mocks[canon_by_header[header]]
                    )
                ):
                    data[header] = fills[header]
                else:
                    data[header] = projected
                pending.remove(header)
        for header in pending:
            data[header] = fills[header]
        return {header: data[header] for header in headers}

    def register_dependencies(
        self,
        grain: set[str],
        headers: list[str],
        canon_by_header: dict[str, str],
        data: dict[str, list[Any]],
    ) -> None:
        """Record the functional dependencies a single-key table establishes, so
        a later table binding the same key reads the same dependent values
        instead of cycling its own pool into a contradiction."""
        key_headers = [h for h in headers if canon_by_header[h] in grain]
        if len(key_headers) != 1:
            return
        key_header = key_headers[0]
        for header in headers:
            if header == key_header:
                continue
            try:
                mapping = dict(zip(data[key_header], data[header]))
            except TypeError:
                continue
            self.dependencies.setdefault(canon_by_header[key_header], {})[
                canon_by_header[header]
            ] = mapping

    def create_mock_table(self, datasource: Datasource) -> "Table":
        from pyarrow import array, table

        concepts: list[Concept | ConceptRef] = []
        headers: list[str] = []
        partial_headers: set[str] = set()
        nullable_headers: set[str] = set()
        domains = self.value_domains.get(datasource.safe_address, {})
        header_domains: dict[str, list[Any]] = {}
        bindings = self.address_columns.get(
            datasource.safe_address, datasource.concrete_columns
        )
        for alias, column in bindings.items():
            self.mock_concept(column.concept)
            concepts.append(column.concept)
            headers.append(alias)
            if Modifier.PARTIAL in column.modifiers:
                partial_headers.add(alias)
            if Modifier.NULLABLE in column.modifiers:
                nullable_headers.add(alias)
            if column.concept.address in domains:
                header_domains[alias] = domains[column.concept.address]
        grain = {self.canon(a) for a in datasource.grain.components}
        canon_by_header = {h: self.canon(c.address) for h, c in zip(headers, concepts)}
        data = self.column_values(
            concepts,
            headers,
            grain,
            partial_headers,
            self.row_targets.get(datasource.identifier, self.scale_factor),
            datasource.safe_address,
            header_domains,
        )
        self.register_dependencies(grain, headers, canon_by_header, data)
        for header in nullable_headers:
            # after the dependency pass: a NULL determinant has nothing to look
            # its dependents up by. Grain components stay populated — a NULL
            # there is a grain violation, not a nullable value.
            if canon_by_header[header] in grain:
                continue
            data[header] = punch_nulls(
                data[header],
                crc32(f"{datasource.safe_address}|{header}".encode()),
            )
        columns: dict[str, Any] = {}
        for header, concept in zip(headers, concepts):
            explicit = arrow_column_type(concept.datatype)
            columns[header] = (
                array(data[header], type=explicit)
                if explicit is not None
                else data[header]
            )
        return table(columns)


def concept_leaves(
    environment: Environment, address: str, seen: set[str] | None = None
) -> set[str]:
    """The bound-able concepts a (possibly derived) concept ultimately reads."""
    concept = environment.concepts.get(address)
    if concept is None or concept.lineage is None:
        return {address}
    seen = seen or set()
    if address in seen:
        return set()
    seen = seen | {address}
    out: set[str] = set()
    for argument in concept.concept_arguments:
        out |= concept_leaves(environment, argument.address, seen)
    return out or {address}


def rollup_datasources(
    datasources: list[Datasource], environment: Environment
) -> list[Datasource]:
    """Targets that add no base facts: a non-root table binding at least one
    derived concept, and nothing that isn't either derived or bound elsewhere.

    These must be *computed* from the tables they roll up, not synthesized —
    a random float for `revenue` contradicts the fact it summarizes, and a pool
    cross-product for a pair rollup claims pairs the fact says never happened.
    """
    bound: dict[str, int] = {}
    for ds in datasources:
        for column in ds.concrete_columns.values():
            address = column.concept.address
            bound[address] = bound.get(address, 0) + 1
    out: list[Datasource] = []
    for ds in datasources:
        if ds.is_root:
            continue
        addresses = [c.concept.address for c in ds.concrete_columns.values()]
        derived = {
            a
            for a in addresses
            if (concept := environment.concepts.get(a)) is not None
            and concept.lineage is not None
        }
        if not derived:
            continue
        if all(a in derived or bound[a] > 1 for a in addresses):
            out.append(ds)
    return out


def synthesis_order(
    datasources: list[Datasource], canon: Callable[[str], str]
) -> list[Datasource]:
    """Order base tables so a table that can inherit a dependent column is built
    after the single-key table that establishes it."""
    columns = {
        ds.identifier: {canon(c.concept.address) for c in ds.concrete_columns.values()}
        for ds in datasources
    }
    grains = {
        ds.identifier: {canon(a) for a in ds.grain.components} for ds in datasources
    }
    predecessors: dict[str, set[str]] = {ds.identifier: set() for ds in datasources}
    for source in datasources:
        grain = grains[source.identifier]
        if len(grain) != 1:
            continue
        key = next(iter(grain))
        supplies = columns[source.identifier] - grain
        for target in datasources:
            if target is source or key not in columns[target.identifier]:
                continue
            if supplies & (columns[target.identifier] - grains[target.identifier]):
                predecessors[target.identifier].add(source.identifier)
    ordered: list[Datasource] = []
    remaining = list(datasources)
    emitted: set[str] = set()
    while remaining:
        ready = [ds for ds in remaining if predecessors[ds.identifier] <= emitted]
        if not ready:
            # a dependency cycle can only come from contradictory grains; the
            # declaration order is as good as any other tie-break
            ordered.extend(remaining)
            break
        for ds in ready:
            ordered.append(ds)
            emitted.add(ds.identifier)
            remaining.remove(ds)
    return ordered


def handle_processed_mock_statement(
    query: ProcessedMockStatement, environment: Environment, executor
) -> MockResult:
    """Handle processed mock statements."""
    mock_environment(environment, executor, targets=list(query.targets))
    return MockResult(
        [{"target": target, "status": "mocked"} for target in query.targets],
        ["target", "status"],
    )


def mock_environment(
    environment: Environment,
    executor,
    targets: list[str] | None = None,
    scale_factor: int = DEFAULT_SCALE_FACTOR,
    address_for: Callable[[Datasource], str] | None = None,
) -> None:
    """Write a mock table for every named datasource (all of them by default).

    ``scale_factor`` sizes the shallowest entity; facts above it grow by
    ``FANOUT_FACTOR`` per level. ``address_for`` names the stand-in table, for
    callers that need something other than the datasource's own address.
    """
    # A fixture that changes shape between runs is not a fixture: a filter that
    # matched two rows yesterday can match none today. Restored afterwards so
    # mocking a datasource mid-script doesn't reseed the caller's RNG.
    rng_state = random.getstate()
    random.seed(MOCK_SEED)
    try:
        _mock_targets(environment, executor, targets, scale_factor, address_for)
    finally:
        random.setstate(rng_state)


def _mock_targets(
    environment: Environment,
    executor,
    target_names: list[str] | None,
    scale_factor: int,
    address_for: Callable[[Datasource], str] | None = None,
) -> None:
    address_for = address_for or (lambda ds: safe_name(ds.safe_address))
    mock_manager = MockManager(environment, scale_factor=scale_factor)
    targets: list[Datasource] = []
    for target in target_names or list(environment.datasources.keys()):
        datasource = environment.datasources.get(target)
        if not datasource:
            raise ValueError(f"Datasource {target} not found in environment")
        targets.append(datasource)
    rollups = rollup_datasources(targets, environment)
    available: set[str] = set()
    for datasource in synthesis_order(
        [ds for ds in targets if ds not in rollups], mock_manager.canon
    ):
        mock_datasource(datasource, mock_manager, executor, address_for(datasource))
        available.add(datasource.identifier)
    pending = list(rollups)
    while pending:
        ready = [
            ds
            for ds in pending
            if rollup_is_derivable(ds, environment, targets, available)
        ]
        if not ready:
            for datasource in pending:
                logger.warning(
                    "Mock: cannot derive rollup %s from its sources; "
                    "synthesizing its columns independently.",
                    datasource.identifier,
                )
                mock_datasource(
                    datasource, mock_manager, executor, address_for(datasource)
                )
                available.add(datasource.identifier)
            break
        for datasource in ready:
            derive_datasource(
                datasource, environment, executor, available, address_for(datasource)
            )
            available.add(datasource.identifier)
            pending.remove(datasource)


def rollup_is_derivable(
    datasource: Datasource,
    environment: Environment,
    targets: list[Datasource],
    available: set[str],
) -> bool:
    supplied: set[str] = set()
    for ds in targets:
        if ds.identifier in available:
            supplied.update(c.concept.address for c in ds.concrete_columns.values())
    for column in datasource.concrete_columns.values():
        if not concept_leaves(environment, column.concept.address) <= supplied:
            return False
    return True


def derive_datasource(
    datasource: Datasource,
    environment: Environment,
    executor,
    available: set[str],
    address: str,
) -> None:
    """Compute a rollup's rows from the tables already mocked, the same move a
    hand-written fixture makes with a CTAS: the aggregate is then consistent
    with the fact by construction rather than by coincidence."""
    headers: list[str] = []
    outputs: list[str] = []
    pins: list[str] = []
    for alias, column in datasource.concrete_columns.items():
        headers.append(alias)
        outputs.append(column.concept.address)
        if Modifier.PARTIAL in column.modifiers:
            # A `~` key holds only recorded combinations; without the pin the
            # rollup would inherit the bridge's extension rows.
            pins.append(f"{column.concept.address} is not null")
    text = "where " + " and ".join(pins) + "\n" if pins else ""
    text += "select " + ",\n".join(outputs) + ";"
    sources = environment.duplicate()
    for identifier in list(sources.datasources.keys()):
        if identifier not in available:
            sources.delete_datasource(identifier)
    original = executor.environment
    executor.environment = sources
    try:
        sql = executor.generate_sql(text)[-1]
    finally:
        executor.environment = original
    columns = ", ".join(f'"{header}"' for header in headers)
    executor.execute_write_sql(
        f"CREATE OR REPLACE TABLE {address} ({columns}) AS "
        f'SELECT * FROM ({sql.strip().rstrip(";")})'
    )
    datasource.address = Address(location=address)


def mock_datasource(
    datasource: Datasource, manager: MockManager, executor, address: str
):
    table = manager.create_mock_table(datasource)

    # duckdb load the pyarrow table
    executor.execute_raw_sql(
        "register(:name, :tbl)", {"name": "mock_tbl", "tbl": table}
    )
    executor.execute_write_sql(
        f"""CREATE OR REPLACE TABLE {address} AS SELECT * FROM mock_tbl"""
    )
    # overwrite the address since we've mangled the name
    datasource.address = Address(location=address)
