import glob as glob_module
from collections.abc import ItemsView, ValuesView
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from trilogy.constants import DEFAULT_NAMESPACE, REMOTE_PREFIXES, MagicConstants
from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
    DatasourceState,
    DatePart,
    Modifier,
)
from trilogy.core.models.author import (
    Between,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    FunctionCallWrapper,
    Grain,
    HasUUID,
    HavingClause,
    LooseConceptList,
    Namespaced,
    OrderBy,
    Parenthetical,
    SubselectComparison,
    WhereClause,
)

LOGGER_PREFIX = "[MODELS_DATASOURCE]"

if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment
    from trilogy.core.statements.author import SelectStatement


class UpdateKeyType(Enum):
    INCREMENTAL_KEY = "incremental_key"
    UPDATE_TIME = "update_time"
    KEY_HASH = "key_hash"


# Units with a fixed duration. month/quarter/year are deliberately excluded —
# they aren't fixed-length, so a lag expressed in them would be a lie.
LAG_UNIT_SECONDS: dict[DatePart, int] = {
    DatePart.SECOND: 1,
    DatePart.MINUTE: 60,
    DatePart.HOUR: 3600,
    DatePart.DAY: 86400,
    DatePart.WEEK: 604800,
}


class FreshnessLag(BaseModel):
    """How far behind its upstream a datasource may be and still count as fresh.

    ``unit`` set means a temporal watermark and a timedelta tolerance; ``unit``
    unset means the tolerance is in the watermark's own units (e.g. 500 ids
    behind on a monotonic incremental key).
    """

    value: int
    unit: DatePart | None = None

    @property
    def as_timedelta(self) -> timedelta | None:
        if self.unit is None:
            return None
        return timedelta(seconds=self.value * LAG_UNIT_SECONDS[self.unit])

    def render(self) -> str:
        if self.unit is None:
            return str(self.value)
        plural = "s" if self.value != 1 else ""
        return f"{self.value} {self.unit.value}{plural}"


@dataclass
class UpdateKey:
    """Represents a key used to track data freshness for incremental updates."""

    #: The watermark key: the concept's full address as known to the emitting
    #: environment, or the literal ``update_time`` for a table-mtime watermark.
    concept_name: str
    type: UpdateKeyType
    value: str | int | float | Decimal | datetime | date | None

    def to_comparison(self, environment: "Environment") -> "Comparison":
        """Convert this update key to a Comparison for use in WHERE clauses."""

        concept = environment.concepts[self.concept_name]
        right_value = self.value if self.value is not None else MagicConstants.NULL
        return Comparison(
            left=concept.reference,
            # A DECIMAL watermark stays exact; the build layer and every
            # dialect render it as a literal.
            right=right_value,  # type: ignore[arg-type]
            operator=ComparisonOperator.GT,
        )


@dataclass
class UpdateKeys:
    """Collection of update keys for a datasource."""

    keys: dict[str, UpdateKey] = field(default_factory=dict)

    def to_where_clause(self, environment: "Environment") -> WhereClause | None:
        """Convert update keys to a WhereClause for filtering."""

        comparisons = [
            key.to_comparison(environment)
            for key in self.keys.values()
            if key.value is not None
        ]
        if not comparisons:
            return None
        if len(comparisons) == 1:
            return WhereClause(conditional=comparisons[0])
        conditional = Conditional(
            left=comparisons[0],
            right=comparisons[1],
            operator=BooleanOperator.AND,
        )
        for comp in comparisons[2:]:
            conditional = Conditional(
                left=conditional, right=comp, operator=BooleanOperator.AND
            )
        return WhereClause(conditional=conditional)


@dataclass
class RawColumnExpr:
    text: str


class ColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | Function
    concept: ConceptRef
    modifiers: list[Modifier] = Field(default_factory=list)

    @field_validator("concept", mode="before")
    def force_reference(cls, v: ConceptRef, info: ValidationInfo):
        if isinstance(v, Concept):
            return v.reference
        return v

    def __eq__(self, other):
        if not isinstance(other, ColumnAssignment):
            return False
        return (
            self.alias == other.alias
            and self.concept == other.concept
            and self.modifiers == other.modifiers
        )

    @property
    def is_concrete(self) -> bool:
        return isinstance(self.alias, str)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignment":
        return ColumnAssignment.model_construct(
            alias=(
                self.alias.with_namespace(namespace)
                if isinstance(self.alias, Function)
                else self.alias
            ),
            concept=self.concept.with_namespace(namespace),
            modifiers=self.modifiers,
        )


@dataclass(frozen=True)
class AddressArgument:
    """One argument of a table-function address. A bound argument names a
    datasource column; the query must pin it with an equality filter, whose
    literal is passed to the function and projected back as that column."""

    value: str | int | float | bool
    bound: bool = False
    name: str | None = None


@dataclass
class Address:
    location: str
    write_location: str | None = None
    quoted: bool = False
    exists: bool = True
    type: AddressType = field(default=AddressType.TABLE)
    # Names of hive-style partition columns for file sources (e.g. parquet glob
    # trees laid out as ``col=value/``). Non-empty iff this address should be
    # read with hive_partitioning enabled.
    partition_columns: list[str] = field(default_factory=list)
    # Additional file paths when the source is an explicit array (e.g.
    # ``file [`a.parquet`, `b.parquet`]``). When non-empty, ``location`` holds
    # the first entry and readers should emit an array call.
    additional_locations: list[str] = field(default_factory=list)
    # Deployment environment this address has been namespaced into (see
    # trilogy.execution.envs). Doubles as the guard against double-prefixing.
    env_label: str | None = None
    # The source a multi-source program serves (``file `x.py#name```), passed
    # to it as ``--source name``.
    source_name: str | None = None
    # Set for a table-function address (``address fn(col, k := 'v')``).
    arguments: list[AddressArgument] | None = None

    @property
    def is_table_function(self) -> bool:
        return self.arguments is not None

    @property
    def bound_columns(self) -> list[str]:
        return [str(a.value) for a in self.arguments or [] if a.bound]

    @property
    def source_args(self) -> str:
        """The command line that selects this address's source of its program."""
        return f"--source {self.source_name}" if self.source_name else ""

    @property
    def is_query(self):
        return self.type == AddressType.QUERY

    @property
    def is_file(self):
        return self.type in {
            AddressType.PYTHON_SCRIPT,
            AddressType.EXECUTABLE,
            AddressType.CSV,
            AddressType.TSV,
            AddressType.PARQUET,
            AddressType.SQL,
        }

    @property
    def is_glob(self) -> bool:
        return any(c in self.location for c in "*?[")

    @property
    def all_locations(self) -> list[str]:
        if self.additional_locations:
            return [self.location, *self.additional_locations]
        return [self.location]

    @property
    def is_missing_locally(self) -> bool:
        """Whether a local file address currently matches nothing on disk.
        A remote location is assumed present: there is nothing cheap to stat."""
        if not self.is_file or self.location.startswith(REMOTE_PREFIXES):
            return False
        return len(glob_module.glob(self.location)) == 0


@dataclass
class Query:
    text: str


@dataclass
class File:
    path: str
    write_path: str | None
    type: AddressType
    exists: bool = True


@dataclass
class DatasourceMetadata:
    freshness_concept: Concept | None
    partition_fields: list[Concept] = field(default_factory=list)
    line_no: int | None = None


StorageValue = bool | int | float | str

#: The `storage (...)` options each written format accepts, with their types.
STORAGE_OPTIONS: dict[AddressType, dict[str, type]] = {
    AddressType.PARQUET: {"row_group_size": int},
}


def validate_storage(address: "Address", storage: dict[str, StorageValue]) -> None:
    """Refuse a `storage` option the target's format does not accept, rather
    than writing the file without it."""
    if not storage:
        return
    accepted = STORAGE_OPTIONS.get(address.type, {})
    for key, value in storage.items():
        if key not in accepted:
            allowed = ", ".join(accepted) or "none"
            raise ValueError(
                f"`storage ({key} = ...)` is not an option for a"
                f" {address.type.value} target (accepted: {allowed})."
            )
        expected = accepted[key]
        if type(value) is not expected:
            raise ValueError(
                f"`storage ({key} = {value!r})` must be {expected.__name__}."
            )
        if isinstance(value, int) and value <= 0:
            raise ValueError(f"`storage ({key} = {value})` must be positive.")


AssertionCondition = (
    SubselectComparison
    | Comparison
    | Conditional
    | Parenthetical
    | Between
    | FunctionCallWrapper
)


class DatasourceAssertion(BaseModel):
    """A boolean over aggregates of a build of the datasource, checked before
    the build replaces the asset. ``prior(expr)`` inside the condition is the
    same expression over the asset being replaced. A ``@function(...)`` call
    whose body is such a boolean is kept as the call, so it renders as
    written; planning unwraps it."""

    condition: AssertionCondition
    line_no: int | None = None

    def __str__(self) -> str:
        return str(self.condition)

    def with_namespace(self, namespace: str) -> "DatasourceAssertion":
        return DatasourceAssertion(
            condition=self.condition.with_namespace(namespace), line_no=self.line_no
        )


class DatasourceTemplate(BaseModel):
    """The declarative parts of a datasource, without a physical location.

    Declared by a ``datasource`` statement that names no address, query or
    file, and reused through ``datasource child extends template``: every
    part is added to the child's own. A template never reaches the planner;
    it lives in ``Environment.datasource_templates``.
    """

    name: str
    namespace: str = DEFAULT_NAMESPACE
    columns: list[ColumnAssignment] = Field(default_factory=list)
    grain: Grain | None = None
    where: WhereClause | None = None
    non_partial_for: WhereClause | None = None
    incremental_by: list[ConceptRef] = Field(default_factory=list)
    partition_by: list[ConceptRef] = Field(default_factory=list)
    freshness_by: list[ConceptRef] = Field(default_factory=list)
    freshness_probe: str | None = None
    refresh_script: str | None = None
    allowed_lag: FreshnessLag | None = None
    order_by: OrderBy | None = None
    storage: dict[str, StorageValue] = Field(default_factory=dict)
    assertions: list[DatasourceAssertion] = Field(default_factory=list)
    is_partial: bool = False
    column_level_partial_addresses: set[str] = Field(default_factory=set)
    # What this declaration itself extended, for rendering.
    extends: list[str] = Field(default_factory=list)
    # A concrete base's location, inherited when the child names none.
    address: "Address | None" = None

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    def with_namespace(self, namespace: str) -> "DatasourceTemplate":
        from trilogy.core.models.author import address_with_namespace

        new_namespace = (
            namespace + "." + self.namespace
            if self.namespace and self.namespace != DEFAULT_NAMESPACE
            else namespace
        )
        return DatasourceTemplate(
            name=self.name,
            namespace=new_namespace,
            columns=[c.with_namespace(namespace) for c in self.columns],
            grain=self.grain.with_namespace(namespace) if self.grain else None,
            where=self.where.with_namespace(namespace) if self.where else None,
            non_partial_for=(
                self.non_partial_for.with_namespace(namespace)
                if self.non_partial_for
                else None
            ),
            incremental_by=[c.with_namespace(namespace) for c in self.incremental_by],
            partition_by=[c.with_namespace(namespace) for c in self.partition_by],
            freshness_by=[c.with_namespace(namespace) for c in self.freshness_by],
            freshness_probe=self.freshness_probe,
            refresh_script=self.refresh_script,
            allowed_lag=self.allowed_lag,
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            storage=dict(self.storage),
            assertions=[a.with_namespace(namespace) for a in self.assertions],
            is_partial=self.is_partial,
            column_level_partial_addresses={
                address_with_namespace(addr, namespace)
                for addr in self.column_level_partial_addresses
            },
            extends=[address_with_namespace(e, namespace) for e in self.extends],
            address=self.address,
        )


def _and_where(parts: list[WhereClause | None]) -> WhereClause | None:
    present = [p for p in parts if p is not None]
    if not present:
        return None
    combined = present[0].conditional
    for clause in present[1:]:
        combined = Conditional(
            left=combined, right=clause.conditional, operator=BooleanOperator.AND
        )
    return WhereClause(conditional=combined)


def _single_setting(
    attribute: str, child: "DatasourceTemplate", bases: list["DatasourceTemplate"]
) -> Any:
    """The child's value when it declares one, else the one value the bases
    agree on. Two bases disagreeing is a conflict the child must settle."""
    own = getattr(child, attribute)
    if own:
        return own
    declared: dict[str, tuple[str, Any]] = {}
    for base in bases:
        value = getattr(base, attribute)
        if value:
            declared.setdefault(str(value), (base.identifier, value))
    if len(declared) > 1:
        spelled = ", ".join(f"{name}: {value}" for name, value in declared.values())
        raise ValueError(
            f"`{attribute.replace('_', ' ')}` differs between extended"
            f" datasources ({spelled}); declare it on the extending datasource."
        )
    return next(iter(declared.values()))[1] if declared else own


def compose_datasource(
    child: "DatasourceTemplate", bases: list["DatasourceTemplate"]
) -> "DatasourceTemplate":
    """``child`` with every base's parts added in. Columns and assertions are
    unioned (a column the child re-binds takes the child's binding), ``where``
    and ``complete where`` are AND-combined, and a single-valued setting
    (grain, refresh keys, lag, address) is the child's, else the bases'."""
    if not bases:
        return child
    columns: dict[str, ColumnAssignment] = {}
    for source in [*bases, child]:
        for column in source.columns:
            columns[column.concept.address] = column
    seen: set[str] = set()
    assertions: list[DatasourceAssertion] = []
    for source in [*bases, child]:
        for assertion in source.assertions:
            if str(assertion) not in seen:
                seen.add(str(assertion))
                assertions.append(assertion)
    return DatasourceTemplate(
        name=child.name,
        namespace=child.namespace,
        columns=list(columns.values()),
        grain=_single_setting("grain", child, bases),
        where=_and_where([*(b.where for b in bases), child.where]),
        non_partial_for=_and_where(
            [*(b.non_partial_for for b in bases), child.non_partial_for]
        ),
        incremental_by=_single_setting("incremental_by", child, bases),
        partition_by=_single_setting("partition_by", child, bases),
        freshness_by=_single_setting("freshness_by", child, bases),
        freshness_probe=_single_setting("freshness_probe", child, bases),
        refresh_script=_single_setting("refresh_script", child, bases),
        allowed_lag=_single_setting("allowed_lag", child, bases),
        order_by=_single_setting("order_by", child, bases),
        storage=_single_setting("storage", child, bases),
        assertions=assertions,
        is_partial=child.is_partial or any(b.is_partial for b in bases),
        column_level_partial_addresses=set().union(
            child.column_level_partial_addresses,
            *(b.column_level_partial_addresses for b in bases),
        ),
        extends=child.extends,
        address=_single_setting("address", child, bases),
    )


def safe_grain(v) -> Grain:
    if isinstance(v, dict):
        components = v.get("components", set())
        if isinstance(components, list):
            components = set(components)
        return Grain(components=components, where_clause=v.get("where_clause"))
    elif isinstance(v, Grain):
        return v
    elif not v:
        return Grain(components=set())
    else:
        raise ValueError(f"Invalid input type to safe_grain {type(v)}")


class Datasource(HasUUID, Namespaced, BaseModel):
    name: str
    columns: list[ColumnAssignment]
    address: Address | str
    grain: Grain = Field(
        default_factory=lambda: Grain(components=set()), validate_default=True
    )
    namespace: str | None = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: WhereClause | None = None
    non_partial_for: WhereClause | None = None
    # True when non_partial_for was *derived* from the defining SELECT's
    # WHERE/HAVING (persist-with-WHERE) rather than authored as a
    # `complete where`. The condition is already embedded in that SELECT, so a
    # persist must not inject it again — and its HAVING atoms would be illegal
    # as a pre-aggregation WHERE. See process_persist.
    non_partial_for_embedded: bool = False
    status: DatasourceState = Field(default=DatasourceState.PUBLISHED)
    incremental_by: list[ConceptRef] = Field(default_factory=list)
    partition_by: list[ConceptRef] = Field(default_factory=list)
    freshness_by: list[ConceptRef] = Field(default_factory=list)
    freshness_probe: str | None = None
    refresh_script: str | None = None
    # Lag this datasource is allowed to run behind its upstream before it
    # counts as stale. Non-roots only — a root's own freshness is never judged.
    allowed_lag: FreshnessLag | None = None
    # The order rows are written in, and options for the written format.
    # Write-side only: neither changes what the datasource answers.
    order_by: OrderBy | None = None
    storage: dict[str, StorageValue] = Field(default_factory=dict)
    assertions: list[DatasourceAssertion] = Field(default_factory=list)
    # Datasources this one was composed from (`extends a, b`), and the parts
    # it declared itself; the fields above hold the composed result. Kept so
    # the declaration renders as written rather than flattened.
    extends: list[str] = Field(default_factory=list)
    authored: DatasourceTemplate | None = None
    is_root: bool = False
    is_partial: bool = False
    # Addresses of columns that carried Modifier.PARTIAL *before* the
    # `partial datasource` keyword stamped PARTIAL onto every column. These
    # represent intrinsic column-level partials (e.g. ``~order_id``) — the
    # column is missing values even within the table's complete-for slice —
    # and survive a covering UNION. Table-level stamps do not.
    column_level_partial_addresses: set[str] = Field(default_factory=set)
    # The file this datasource was declared in; None when it was declared in
    # the entrypoint text itself. With ``name`` it identifies the declaration
    # across every namespace an import chain spells it under.
    declared_in: str | None = None

    @property
    def safe_address(self) -> str:
        if isinstance(self.address, Address):
            # Sources of one program share its location but are distinct assets.
            if self.address.source_name:
                return f"{self.address.location}#{self.address.source_name}"
            return self.address.location
        return self.address

    def repoint(self, address: "str | Address") -> "Datasource":
        """Point this datasource at a different physical location, leaving its
        columns/grain/semantics untouched. A bare string is treated as a table
        name; the renderer backticks it only if it isn't a bare identifier.
        Used to redirect any datasource — table, file, remote, query — at a
        stand-in table (e.g. a unit-test mock). Mutates in place and returns
        self for chaining."""
        if isinstance(address, str):
            address = Address(location=address, type=AddressType.TABLE)
        self.address = address
        return self

    def __eq__(self, other):
        if not isinstance(other, Datasource):
            return False
        return (
            self.name == other.name
            and self.namespace == other.namespace
            and self.grain == other.grain
            and self.address == other.address
            and self.where == other.where
            and self.columns == other.columns
            and self.non_partial_for == other.non_partial_for
        )

    def duplicate(self) -> "Datasource":
        return self.model_copy(deep=True)

    @property
    def concrete_columns(self) -> dict[str, ColumnAssignment]:
        return {c.alias: c for c in self.columns if c.is_concrete}  # type: ignore[misc]

    @property
    def hidden_concepts(self) -> list[Concept]:
        return []

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    @property
    def is_refreshable_root(self) -> bool:
        """A root trilogy drives via subprocess rather than SQL persist.

        Needs both halves: the probe decides staleness, the script fixes it.
        A root with only one of them stays untouchable.
        """
        return bool(self.is_root and self.refresh_script and self.freshness_probe)

    @property
    def is_managed(self) -> bool:
        """Whether trilogy owns refresh for this datasource."""
        return not self.is_root or self.is_refreshable_root

    @property
    def safe_identifier(self) -> str:
        return self.identifier.replace(".", "_")

    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_concepts)

    @field_validator("namespace", mode="plain")
    @classmethod
    def namespace_validation(cls, v):
        return v or DEFAULT_NAMESPACE

    @field_validator("address")
    @classmethod
    def address_enforcement(cls, v):
        if isinstance(v, str):
            v = Address(location=v)
        return v

    @field_validator("grain", mode="before")
    @classmethod
    def grain_enforcement(cls, v: Grain, info: ValidationInfo):
        grain: Grain = safe_grain(v)
        return grain

    def add_column(
        self,
        concept: Concept,
        alias: str | RawColumnExpr | Function,
        modifiers: list[Modifier] | None = None,
    ):
        self.columns.append(
            ColumnAssignment(
                alias=alias, concept=concept.reference, modifiers=modifiers or []
            )
        )

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    def __repr__(self):
        return f"Datasource<{self.identifier}@<{self.grain}>"

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return self.identifier.__hash__()

    def with_namespace(self, namespace: str):
        from trilogy.core.models.author import address_with_namespace

        new_namespace = (
            namespace + "." + self.namespace
            if self.namespace and self.namespace != DEFAULT_NAMESPACE
            else namespace
        )
        new = Datasource.model_construct(
            name=self.name,
            namespace=new_namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
            where=self.where.with_namespace(namespace) if self.where else None,
            non_partial_for=(
                self.non_partial_for.with_namespace(namespace)
                if self.non_partial_for
                else None
            ),
            non_partial_for_embedded=self.non_partial_for_embedded,
            status=self.status,
            incremental_by=[c.with_namespace(namespace) for c in self.incremental_by],
            partition_by=[c.with_namespace(namespace) for c in self.partition_by],
            freshness_by=[c.with_namespace(namespace) for c in self.freshness_by],
            # Paths to scripts, not concepts: a namespace has nothing to say
            # about them, and dropping them makes an imported refreshable root
            # unrefreshable under its alias.
            freshness_probe=self.freshness_probe,
            refresh_script=self.refresh_script,
            allowed_lag=self.allowed_lag,
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            storage=dict(self.storage),
            assertions=[a.with_namespace(namespace) for a in self.assertions],
            extends=[address_with_namespace(e, namespace) for e in self.extends],
            authored=self.authored.with_namespace(namespace) if self.authored else None,
            is_root=self.is_root,
            is_partial=self.is_partial,
            column_level_partial_addresses={
                address_with_namespace(addr, namespace)
                for addr in self.column_level_partial_addresses
            },
            declared_in=self.declared_in,
        )
        return new

    def as_template(self) -> DatasourceTemplate:
        """This datasource's declarative parts, as a base for `extends`."""
        return DatasourceTemplate(
            name=self.name,
            namespace=self.namespace or DEFAULT_NAMESPACE,
            columns=list(self.columns),
            grain=self.grain if self.grain.components else None,
            where=self.where,
            non_partial_for=self.non_partial_for,
            incremental_by=list(self.incremental_by),
            partition_by=list(self.partition_by),
            freshness_by=list(self.freshness_by),
            freshness_probe=self.freshness_probe,
            refresh_script=self.refresh_script,
            allowed_lag=self.allowed_lag,
            order_by=self.order_by,
            storage=dict(self.storage),
            assertions=list(self.assertions),
            is_partial=self.is_partial,
            column_level_partial_addresses=set(self.column_level_partial_addresses),
            extends=list(self.extends),
            address=self.address if isinstance(self.address, Address) else None,
        )

    def create_update_statement(
        self,
        environment: "Environment",
        where: WhereClause | None = None,
        line_no: int | None = None,
    ) -> "SelectStatement":
        from trilogy.core.statements.author import Metadata, SelectItem, SelectStatement

        # TODO(safeguard): inject non_partial_for into WHERE so out-of-partition rows
        # can never be written even when source-selection is wrong. Blocked on first
        # fixing source-selection to prefer the exact-matching partial source automatically.

        # The datasource's own `where` is applied to every read of it, so the
        # build writes only the rows a read keeps: a filter on the finished
        # rows (HAVING), never a WHERE that would recompute the columns'
        # aggregates over the surviving rows. Incremental keys scope the build.
        # The `where` reads only stored columns and row-level derivations of
        # them (validated at parse), so a derived gate rides along hidden
        # without changing the build's grain.
        having = (
            HavingClause(conditional=self.where.conditional) if self.where else None
        )
        stored = [col.concept.address for col in self.columns]
        derived = list(
            dict.fromkeys(
                ref.address
                for ref in (having.concept_arguments if having else [])
                if ref.address not in stored
            )
        )
        return SelectStatement.from_inputs(
            environment=environment,
            selection=[
                SelectItem(content=ConceptRef(address=address), modifiers=[])
                for address in stored
            ]
            + [
                SelectItem(
                    content=ConceptRef(address=address), modifiers=[Modifier.HIDDEN]
                )
                for address in derived
            ],
            where_clause=where,
            having_clause=having,
            meta=Metadata(line_number=line_no) if line_no else None,
        )

    @property
    def concepts(self) -> list[ConceptRef]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def nullable_concepts(self) -> list[ConceptRef]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def output_concepts(self) -> list[ConceptRef]:
        return self.concepts

    @property
    def partial_concepts(self) -> list[ConceptRef]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    @property
    def column_level_partial_concepts(self) -> list[ConceptRef]:
        """Columns with intrinsic (pre-stamp) PARTIAL — survive a covering UNION."""
        if not self.column_level_partial_addresses:
            return []
        return [
            c.concept
            for c in self.columns
            if c.concept.address in self.column_level_partial_addresses
        ]


class EnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        # See EnvironmentConceptDict.mutations — same contract. A plain-dict
        # subclass must also hook update(): dict.update bypasses __setitem__.
        self.mutations: int = 0
        # See EnvironmentConceptDict.content_version — effective writes only.
        self.content_version: int = 0
        super().__init__(self, *args, **kwargs)

    def __setitem__(self, key: str, value: Datasource) -> None:
        self.mutations += 1
        if dict.get(self, key) is not value:
            self.content_version += 1
        super().__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        self.mutations += 1
        self.content_version += 1
        super().__delitem__(key)

    def update(self, *args, **kwargs) -> None:  # type: ignore[override]
        self.mutations += 1
        self.content_version += 1
        super().update(*args, **kwargs)

    def pop(self, *args):
        self.mutations += 1
        self.content_version += 1
        return super().pop(*args)

    def __getitem__(self, key: str) -> Datasource:
        try:
            return super().__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[Datasource]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, Datasource]:  # type: ignore
        return super().items()

    def duplicate(self) -> "EnvironmentDatasourceDict":
        new = EnvironmentDatasourceDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        return new
