from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from dataclasses import replace as dc_replace
from datetime import date, datetime
from decimal import Decimal
from functools import cached_property, reduce, singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Union,
)

from trilogy.constants import (
    DEFAULT_NAMESPACE,
    PRESENCE_PROBE_PREFIX,
    VIRTUAL_CONCEPT_PREFIX,
    MagicConstants,
)
from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.domain_graph import DomainGraph, EdgeScope, assemble_full_graph
from trilogy.core.enums import (
    NAVIGATION_WINDOW_TYPES,
    NUMBERING_WINDOW_TYPES,
    AggregateGroupingMode,
    BooleanOperator,
    ComparisonOperator,
    DatasourceState,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    JoinType,
    Modifier,
    Ordering,
    Purpose,
    SetOperator,
    WindowType,
)
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    UnionOutputResolutionError,
)
from trilogy.core.models.author import (
    AggregateGrouping,
    AggregateWrapper,
    AlignClause,
    AlignItem,
    ArgBinding,
    Between,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    DeriveClause,
    DeriveItem,
    FilterItem,
    FuncArgs,
    Function,
    FunctionCallWrapper,
    Grain,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    NavigationWindowItem,
    NumberingWindowItem,
    OrderBy,
    OrderItem,
    Parenthetical,
    RowsetItem,
    RowsetLineage,
    SelectLineage,
    SubqueryItem,
    SubselectComparison,
    SubselectItem,
    UndefinedConcept,
    UnionSelectLineage,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    Addressable,
    ArrayType,
    DataType,
    DataTyped,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
    arg_to_datatype,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    DatasourceMetadata,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment
from trilogy.utility import string_to_hash

# TODO: refactor to avoid these
if TYPE_CHECKING:
    from trilogy.core.models.build_environment import BuildEnvironment
    from trilogy.core.models.execute import CTE, UnionCTE


LOGGER_PREFIX = "[MODELS_BUILD]"


def _gen_agg_name(parent: "BuildAggregateWrapper") -> str:
    target = parent.with_abstract_by() if parent.is_abstract else parent
    return f"{VIRTUAL_CONCEPT_PREFIX}_agg_{parent.function.operator.value}_{string_to_hash(_canonical_str_for_hash(target))}"


def _gen_window_name(parent: "BuildWindowItem") -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(_canonical_str_for_hash(parent))}"


def _gen_filter_name(parent: "BuildFilterItem") -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_filter_{string_to_hash(_canonical_str_for_hash(parent))}"


def _gen_function_name(parent: "BuildFunction") -> str:
    if parent.operator == FunctionType.GROUP:
        return f"{VIRTUAL_CONCEPT_PREFIX}_group_to_{string_to_hash(_canonical_str_for_hash(parent))}"
    return f"{VIRTUAL_CONCEPT_PREFIX}_func_{parent.operator.value}_{string_to_hash(_canonical_str_for_hash(parent))}"


def _gen_paren_name(parent: "BuildParenthetical") -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_paren_{string_to_hash(_canonical_str_for_hash(parent))}"


def _gen_comp_name(parent: "BuildComparison") -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_comp_{string_to_hash(_canonical_str_for_hash(parent))}"


def _gen_msl_name(parent: "BuildMultiSelectLineage") -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_msl_{string_to_hash(str(parent))}"


def _gen_default_name(parent: Any) -> str:
    return f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(_canonical_str_for_hash(parent))}"


# Initialized after classes are defined
_CONCEPT_NAME_GENERATORS: dict[type, Callable[[Any], str]] = {}


def generate_concept_name(
    parent: Any, merge_concepts: Iterable[str] | None = None
) -> str:
    generator = _CONCEPT_NAME_GENERATORS.get(type(parent))
    if generator:
        canonical_name = generator(parent)
    else:
        canonical_name = _gen_default_name(parent)
    if not merge_concepts:
        return canonical_name
    parts = sorted({canonical_name, *merge_concepts})
    return f"{VIRTUAL_CONCEPT_PREFIX}_merge_{string_to_hash('|'.join(parts))}"


def _constant_bool_comparison(value: bool) -> "BuildComparison":
    # WHERE/HAVING that constant-folds to a bool needs to round-trip through
    # the BoolExpr pipeline. `1=1` / `1=0` renders portably and has no
    # row_arguments, so downstream optimizers see a no-op condition.
    return BuildComparison(
        left=1, right=1 if value else 0, operator=ComparisonOperator.EQ
    )


class BuildConceptArgs:
    @property
    def concept_arguments(self) -> Sequence["BuildConcept"]:
        raise NotImplementedError

    @property
    def rendered_concept_arguments(self) -> Sequence["BuildConcept"]:
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        return []

    @property
    def row_arguments(self) -> Sequence["BuildConcept"]:
        return self.concept_arguments


GROUPING_IDENTITY_FUNCTIONS = frozenset(
    {FunctionType.GROUPING, FunctionType.GROUPING_ID}
)


def is_grouping_identity(concept: BuildConcept) -> bool:
    """A ``grouping()``/``grouping_id()`` indicator under a ROLLUP/CUBE/GROUPING
    SETS. It renders as an aggregate (computed inside the grouped CTE) but is
    semantically a *key*: the grouping-set bitmask is part of a rollup row's
    identity, distinguishing a subtotal/grand-total (rolled-up dim -> NULL) from a
    leaf carrying a data NULL. It can only be produced inside the grouped CTE, so
    it must be threaded through as a pass-through rather than recovered by a
    join-back on the (nullable) dims — which would collide those rows."""
    lineage = concept.lineage
    return (
        isinstance(lineage, BuildAggregateWrapper)
        and lineage.function.operator in GROUPING_IDENTITY_FUNCTIONS
    )


GroupingSpec = Tuple[
    AggregateGroupingMode, Tuple[str, ...], Tuple[Tuple[str, ...], ...]
]


def nonstandard_grouping_spec(lineage: Any) -> GroupingSpec | None:
    """Identity of the ROLLUP/CUBE/GROUPING SETS pass an aggregate computes in
    (None for standard grouping or non-aggregate lineage). Aggregates sharing a
    spec are outputs of one grouping pass; their visible dims are NOT a row
    identity across grouping sets (a rolled-up NULL and a data NULL collide), so
    they must co-source in that pass rather than be joined back on the dims."""
    if not isinstance(lineage, BuildAggregateWrapper):
        return None
    if lineage.grouping == AggregateGroupingMode.STANDARD:
        return None
    return (
        lineage.grouping,
        tuple(c.address for c in lineage.by),
        tuple(tuple(c.address for c in gs) for gs in lineage.grouping_sets),
    )


def _trace_grouping_passes(
    concept: BuildConcept,
) -> tuple[set[GroupingSpec], bool, bool, set[str]]:
    """Walk ``concept``'s lineage down to grouping-pass boundaries.

    Returns (specs, pointwise, windowed, root_addresses):
    - specs: grouping passes whose outputs the lineage reads (descent stops at a
      pass aggregate — what's below it are the pass's inputs, not outputs)
    - pointwise: False when any path crosses a non-scalar wrapper (window,
      filter, standard aggregate, rowset, ...) that computes OVER a pass output
    - windowed: True when any path crosses a window item
    - root_addresses: lineage-less leaves reached without crossing a pass"""
    specs: set[GroupingSpec] = set()
    pointwise = True
    windowed = False
    roots: set[str] = set()
    seen: set[str] = set()
    stack: list[BuildConcept] = [concept]
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        lineage = current.lineage
        if lineage is None:
            roots.add(current.address)
            continue
        spec = nonstandard_grouping_spec(lineage)
        if spec is not None:
            specs.add(spec)
            continue
        if isinstance(lineage, (BuildNumberingWindowItem, BuildNavigationWindowItem)):
            windowed = True
            pointwise = False
        elif not isinstance(lineage, (BuildFunction, BuildParenthetical)):
            pointwise = False
        if isinstance(lineage, BuildConceptArgs):
            stack.extend(lineage.concept_arguments)
    return specs, pointwise, windowed, roots


def colocatable_in_grouping_pass(concept: BuildConcept, spec: GroupingSpec) -> bool:
    """True when ``concept`` is an output of grouping pass ``spec``: the pass's
    aggregate itself (incl. grouping()/grouping_id() identity) or a pointwise
    scalar over such outputs, grouping keys, and constants. Such a concept can
    be emitted by the pass's grouped CTE directly instead of being recovered
    with a join back on its nullable dims."""
    specs, pointwise, _, roots = _trace_grouping_passes(concept)
    if not pointwise or specs != {spec}:
        return False
    grouping_keys = set(spec[1]).union(*spec[2]) if spec[2] else set(spec[1])
    return roots.issubset(grouping_keys)


def windowed_over_grouping_pass(concept: BuildConcept, spec: GroupingSpec) -> bool:
    """True when ``concept`` computes a window (anywhere in its lineage) over an
    output of grouping pass ``spec``. Recovering such a sibling by joining the
    pass output back on its visible dims collides grouping-set rows; the caller
    must decline so the window-first path co-sources the pass instead."""
    specs, _, windowed, _ = _trace_grouping_passes(concept)
    return windowed and spec in specs


def concept_is_relevant(
    concept: BuildConcept,
    others: list[BuildConcept],
) -> bool:
    # ``other_addresses`` is invariant across the whole recursion; compute it
    # once here rather than re-deriving it at every recursive node.
    return _concept_is_relevant(
        concept, concept_collection_equivalent_addresses(others)
    )


def _concept_is_relevant(
    concept: BuildConcept,
    other_addresses: set[str],
) -> bool:
    if concept.is_aggregate and not (
        isinstance(concept.lineage, BuildAggregateWrapper) and concept.lineage.by
    ):

        return False
    if (
        concept.derivation == Derivation.MULTISELECT
        and concept.keys
        and any(c in other_addresses for c in concept.keys)
    ):
        return False
    if concept.purpose in (Purpose.PROPERTY, Purpose.METRIC) and concept.keys:
        if all([c in other_addresses for c in concept.keys]):
            return False
    if (
        concept.purpose == Purpose.KEY
        and concept.keys
        and all([c in other_addresses and c != concept.address for c in concept.keys])
    ):
        return False
    if concept.purpose in (Purpose.METRIC,):
        if all([c in other_addresses for c in concept.grain.components]):
            return False
        if (
            isinstance(concept.lineage, BuildAggregateWrapper)
            and concept.lineage.by
            and all(
                c.address in other_addresses
                or not _concept_is_relevant(c, other_addresses)
                for c in concept.lineage.by
            )
        ):
            return False
    if concept.derivation in (Derivation.UNNEST,):
        return True
    if concept.derivation in (Derivation.BASIC,):
        return any(
            _concept_is_relevant(c, other_addresses) for c in concept.concept_arguments
        )
    if concept.derivation == Derivation.WINDOW:
        if all([c in other_addresses for c in concept.grain.components]):
            return False
        return any(
            _concept_is_relevant(c, other_addresses) for c in concept.concept_arguments
        )
    if concept.granularity == Granularity.SINGLE_ROW:
        return False
    return True


def concept_collection_equivalent_addresses(
    concepts: Iterable[BuildConcept],
) -> set[str]:
    addresses: set[str] = set()
    for concept in concepts:
        addresses.update(concept.equivalent_addresses)
    return addresses


def _concept_by_equivalent_address(
    address: str,
    concepts: Iterable[BuildConcept],
) -> BuildConcept | None:
    return next(
        (concept for concept in concepts if address in concept.equivalent_addresses),
        None,
    )


def resolve_concepts_with_equivalents(
    addresses: Iterable[str],
    environment: "BuildEnvironment",
    equivalents: Iterable[BuildConcept] | None = None,
) -> list[BuildConcept]:
    candidates = list(equivalents or [])
    return [
        _concept_by_equivalent_address(address, candidates)
        or environment.concepts[address]
        for address in addresses
    ]


def _addr_keys(
    addr: str,
    environment: "BuildEnvironment",
    pmap: dict[str, "BuildConcept"],
) -> frozenset[str]:
    concept = pmap.get(addr) or environment.concepts.get(addr)
    return frozenset(concept.keys) if concept and concept.keys else frozenset()


def _key_reduces_to(
    components: frozenset[str],
    target: set[str],
    environment: "BuildEnvironment",
    pmap: dict[str, "BuildConcept"],
    _seen: frozenset[str] = frozenset(),
) -> bool:
    """Do `components` transitively reduce — via FK key bindings — to a subset
    of `target`? A component is covered if it is already in `target` or every
    key it binds to reduces to `target` (e.g. nation.id -> customer.id)."""
    for comp in components:
        if comp in target:
            continue
        if comp in _seen:
            return False
        keys = _addr_keys(comp, environment, pmap)
        if not keys:
            return False
        if not _key_reduces_to(keys, target, environment, pmap, _seen | {comp}):
            return False
    return True


def concepts_to_build_grain_concepts(
    concepts: Iterable[BuildConcept | str], environment: "BuildEnvironment" | None
) -> set[str]:
    pconcepts: list[BuildConcept] = []
    for c in concepts:
        if isinstance(c, BuildConcept):
            pconcepts.append(c)
        elif environment:
            pconcepts.append(environment.concepts[c])

        else:
            raise ValueError(
                f"Unable to resolve input {c} without environment provided to concepts_to_grain call"
            )

    # The sort key is load-bearing, not cosmetic. Pseudonym links are
    # one-way: an alias's equivalent_addresses contains the canonical, but
    # the canonical's does not contain its aliases. The dedup below only
    # rejects a concept if `final` already shares an equivalent address
    # with it, so if an alias is processed before its canonical, the
    # canonical's equivalent_addresses (just itself) won't intersect
    # `final` and both end up retained. Callers feed components from a
    # `set`, so without a deterministic order this silently produces
    # different grains run-to-run and breaks downstream
    # `grain_satisfied_by_pregrain` checks (a stray alias makes pregrain
    # look like a superset of the target and force_group=True flips on).
    # Sorting by `len(equivalent_addresses)` puts canonicals first.
    final: set[str] = set()
    for sub in sorted(
        pconcepts, key=lambda c: (len(c.equivalent_addresses), c.address)
    ):
        if not concept_is_relevant(sub, pconcepts):
            continue
        if final & sub.equivalent_addresses:
            continue
        final.add(sub.address)

    # Key-hierarchy reduction: drop a component whose FK key chain transitively
    # reduces to the other retained components. `concept_is_relevant` only drops
    # a key one level away (its immediate `keys` are present); this folds the
    # full chain (e.g. nation.id -> customer.id -> order.id), so a grain never
    # carries a key that another retained key already determines.
    if environment is not None:
        pmap = {c.address: c for c in pconcepts}
        reduced = set(final)
        for addr in sorted(final):
            keys = _addr_keys(addr, environment, pmap)
            if keys and _key_reduces_to(keys, reduced - {addr}, environment, pmap):
                reduced.discard(addr)
        final = reduced

    return final


@dataclass(slots=True)
class LooseBuildConceptList:
    concepts: Sequence[BuildConcept]
    addresses: set[str] = field(init=False)
    sorted_addresses: List[str] = field(init=False)

    def __post_init__(self):
        self.addresses = {s.address for s in self.concepts}
        self.sorted_addresses = sorted(self.addresses)

    def __str__(self) -> str:
        return f"lcl{str(self.sorted_addresses)}"

    def __iter__(self):
        return iter(self.concepts)

    def __eq__(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses == other.addresses

    def issubset(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.issubset(other.addresses)

    def __contains__(self, other):
        if isinstance(other, str):
            return other in self.addresses
        if not isinstance(other, BuildConcept):
            return False
        return other.address in self.addresses

    def difference(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.difference(other.addresses)

    def isdisjoint(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.isdisjoint(other.addresses)


@dataclass(slots=True)
class CanonicalBuildConceptList:
    concepts: Sequence[BuildConcept]
    addresses: set[str] = field(init=False)
    sorted_addresses: List[str] = field(init=False)

    def __post_init__(self):
        self.addresses = {s.canonical_address for s in self.concepts}
        self.sorted_addresses = sorted(self.addresses)

    def __str__(self) -> str:
        return f"lcl{str(self.sorted_addresses)}"

    def __iter__(self):
        return iter(self.concepts)

    def __eq__(self, other):
        if not isinstance(other, CanonicalBuildConceptList):
            return False
        return self.addresses == other.addresses

    def issubset(self, other):
        if not isinstance(other, CanonicalBuildConceptList):
            return False
        return self.addresses.issubset(other.addresses)

    def __contains__(self, other):
        if isinstance(other, str):
            return other in self.addresses
        if not isinstance(other, BuildConcept):
            return False
        return other.canonical_address in self.addresses

    def difference(self, other):
        if not isinstance(other, CanonicalBuildConceptList):
            return False
        return self.addresses.difference(other.addresses)

    def isdisjoint(self, other):
        if not isinstance(other, CanonicalBuildConceptList):
            return False
        return self.addresses.isdisjoint(other.addresses)


class ConstantInlineable:

    def inline_constant(self, concept: BuildConcept):
        raise NotImplementedError


def get_concept_row_arguments(expr) -> List["BuildConcept"]:
    output = []
    if isinstance(expr, BuildConcept):
        output += [expr]

    elif isinstance(expr, BuildConceptArgs):
        output += expr.row_arguments
    return output


def get_concept_arguments(expr) -> List["BuildConcept"]:
    output = []
    if isinstance(expr, BuildConcept):
        output += [expr]

    elif isinstance(
        expr,
        BuildConceptArgs,
    ):
        output += expr.concept_arguments
    return output


def get_rendered_concept_arguments(expr) -> List["BuildConcept"]:
    output = []
    if isinstance(expr, BuildConcept):
        output += [expr]

    elif isinstance(
        expr,
        BuildConceptArgs,
    ):
        output += expr.rendered_concept_arguments
    return output


@dataclass(slots=True)
class BuildParamaterizedConceptReference(DataTyped):
    concept: BuildConcept

    def __str__(self):
        return f":{self.concept.address.replace('.', '_')}"

    @property
    def safe_address(self) -> str:
        return self.concept.safe_address

    @property
    def output_datatype(self):
        return self.concept.output_datatype

    @property
    def value(self):
        return self.concept.lineage.arguments[0]


@dataclass(slots=True)
class BuildGrain:
    components: set[str] = field(default_factory=set)
    where_clause: Optional[BuildWhereClause] = None
    _str: str | None = None
    _str_no_condition: str = field(init=False)
    abstract: bool = field(init=False)

    def __post_init__(self):
        self.abstract = not self.components or all(
            c.endswith(ALL_ROWS_CONCEPT) for c in self.components
        )
        self._str_no_condition = self._calculate_string_no_condition()

    def without_condition(self):
        if not self.where_clause:
            return self
        return BuildGrain(components=self.components)

    @classmethod
    def from_concepts(
        cls,
        concepts: Iterable[BuildConcept | str],
        environment: BuildEnvironment | None = None,
        where_clause: BuildWhereClause | None = None,
    ) -> "BuildGrain":

        return BuildGrain(
            components=concepts_to_build_grain_concepts(
                concepts, environment=environment
            ),
            where_clause=where_clause,
        )

    def __add__(self, other: "BuildGrain") -> "BuildGrain":
        if not other:
            return self
        where = self.where_clause
        if other.where_clause:
            if not self.where_clause:
                where = other.where_clause
            elif not other.where_clause == self.where_clause:
                where = BuildWhereClause(
                    conditional=BuildConditional(
                        left=self.where_clause.conditional,
                        right=other.where_clause.conditional,
                        operator=BooleanOperator.AND,
                    )
                )
                # raise NotImplementedError(
                #     f"Cannot merge grains with where clauses, self {self.where_clause} other {other.where_clause}"
                # )
        return BuildGrain(
            components=self.components.union(other.components), where_clause=where
        )

    def __sub__(self, other: "BuildGrain") -> "BuildGrain":
        return BuildGrain(
            components=self.components.difference(other.components),
            where_clause=self.where_clause,
        )

    def __eq__(self, other):
        if other is None:
            return False
        if self.components == other.components:
            return True
        if self.abstract is True and other.abstract is True:
            return True
        return False

    def issubset(self, other: "BuildGrain"):
        return self.components.issubset(other.components)

    def union(self, other: "BuildGrain"):
        addresses = self.components.union(other.components)
        return BuildGrain(components=addresses, where_clause=self.where_clause)

    def isdisjoint(self, other: "BuildGrain"):
        return self.components.isdisjoint(other.components)

    def intersection(self, other: "BuildGrain") -> "BuildGrain":
        intersection = self.components.intersection(other.components)
        return BuildGrain(components=intersection)

    def _calculate_string(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join(sorted(self.components)) + ">"
        if self.where_clause:
            base += f"|{str(self.where_clause)}"
        return base

    def _calculate_string_no_condition(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join(sorted(self.components)) + ">"
        return base

    @property
    def str_no_condition(self):
        return self._str_no_condition

    def __str__(self):
        if self._str:
            return self._str
        self._str = self._calculate_string()
        return self._str

    def __radd__(self, other) -> "BuildGrain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


DEFAULT_GRAIN = BuildGrain(components=set())


@dataclass(slots=True)
class BuildParenthetical(DataTyped, ConstantInlineable, BuildConceptArgs):
    content: "BuildExpr"

    def __add__(self, other) -> Union["BuildParenthetical", "BuildConditional"]:
        if other is None:
            return self
        elif isinstance(
            other,
            BoolExpr,
        ):
            return BuildConditional(
                left=self, right=other, operator=BooleanOperator.AND
            )
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"({str(self.content)})"

    def inline_constant(self, BuildConcept: BuildConcept):
        return BuildParenthetical(
            content=(
                self.content.inline_constant(BuildConcept)
                if isinstance(self.content, ConstantInlineable)
                else self.content
            )
        )

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        return get_concept_arguments(self.content)

    @property
    def rendered_concept_arguments(self) -> Sequence[BuildConcept]:
        return get_rendered_concept_arguments(self.content)

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        # a BuildConcept content is itself a row argument, not its expanded
        # lineage (mirrors concept_arguments)
        return get_concept_row_arguments(self.content)

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        if isinstance(self.content, BuildConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


@dataclass(slots=True)
class BuildConditional(DataTyped, BuildConceptArgs, ConstantInlineable):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BuildConcept,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
        BuildSubselectComparison,
        "BuildBetween",
        BuildFunction,
        BuildFilterItem,
    ]
    right: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BuildConcept,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
        BuildSubselectComparison,
        "BuildBetween",
        BuildFunction,
        BuildFilterItem,
    ]
    operator: BooleanOperator

    def __add__(self, other) -> "BuildConditional":
        if other is None:
            return self
        elif str(other) == str(self):
            return self
        elif isinstance(
            other,
            BoolExpr,
        ):
            return BuildConditional(
                left=self, right=other, operator=BooleanOperator.AND
            )
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{str(self.left)} {self.operator.value} {str(self.right)}"

    def __eq__(self, other):
        if not isinstance(other, BuildConditional):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: BuildConcept) -> "BuildConditional":
        assert isinstance(constant.lineage, BuildFunction)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif (
            isinstance(self.left, BuildConcept)
            and self.left.address == constant.address
        ):
            new_left = new_val
        else:
            new_left = self.left

        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif (
            isinstance(self.right, BuildConcept)
            and self.right.address == constant.address
        ):
            new_right = new_val
        else:
            new_right = self.right

        if self.right == constant:
            new_right = new_val

        return BuildConditional(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        """Return BuildConcepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BuildConcept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> list[tuple[BuildConcept, ...]]:
        output: list[tuple[BuildConcept, ...]] = []
        if isinstance(self.left, BuildConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, BuildConceptArgs):
            output += self.right.existence_arguments
        return output

    def decompose(self):
        chunks = []
        if self.operator == BooleanOperator.AND:
            for val in [self.left, self.right]:
                if isinstance(val, BuildConditional):
                    chunks.extend(val.decompose())
                else:
                    chunks.append(val)
        else:
            chunks.append(self)
        return chunks

    @property
    def output_datatype(self):
        return DataType.BOOL


@dataclass(slots=True)
class BuildWhereClause(BuildConceptArgs):
    conditional: Union[
        BuildSubselectComparison,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
        "BuildBetween",
    ]

    def __eq__(self, other):
        if not isinstance(other, (BuildWhereClause, WhereClause)):
            return False
        return self.conditional == other.conditional

    def __repr__(self):
        return str(self.conditional)

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        return self.conditional.existence_arguments


class BuildHavingClause(BuildWhereClause):
    pass


@dataclass(slots=True)
class BuildComparison(DataTyped, BuildConceptArgs, ConstantInlineable):

    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        BuildFunction,
        BuildConcept,
        BuildConditional,
        DataType,
        BuildComparison,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        ListWrapper,
        TupleWrapper,
    ]
    right: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        BuildConcept,
        BuildFunction,
        BuildConditional,
        DataType,
        BuildComparison,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        TupleWrapper,
        ListWrapper,
    ]
    operator: ComparisonOperator

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(
            other,
            BoolExpr,
        ):
            raise ValueError(f"Cannot add {type(other)} to {__class__}")
        if other == self:
            return self
        return BuildConditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        if isinstance(self.left, BuildConcept):
            left = self.left.address
        else:
            left = str(self.left)
        if isinstance(self.right, BuildConcept):
            right = self.right.address
        else:
            right = str(self.right)
        return f"{left} {self.operator.value} {right}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, BuildComparison):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: BuildConcept):
        assert isinstance(constant.lineage, BuildFunction)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif (
            isinstance(self.left, BuildConcept)
            and self.left.address == constant.address
        ):
            new_left = new_val
        else:
            new_left = self.left
        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif (
            isinstance(self.right, BuildConcept)
            and self.right.address == constant.address
        ):
            new_right = new_val
        else:
            new_right = self.right

        return self.__class__(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        """Return BuildConcepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BuildConcept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> List[Tuple[BuildConcept, ...]]:
        """Return BuildConcepts directly referenced in where clause"""
        output: List[Tuple[BuildConcept, ...]] = []
        if isinstance(self.left, BuildConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, BuildConceptArgs):
            output += self.right.existence_arguments
        return output

    @property
    def output_datatype(self):
        return DataType.BOOL


@dataclass(slots=True)
class BuildSubselectComparison(BuildComparison):
    def __eq__(self, other):
        if not isinstance(other, BuildSubselectComparison):
            return False

        comp = (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )
        return comp

    @property
    def row_arguments(self) -> List[BuildConcept]:
        return get_concept_row_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["BuildConcept", ...]]:
        return [tuple(get_concept_arguments(self.right))]


@dataclass(slots=True)
class BuildBetween(DataTyped, BuildConceptArgs, ConstantInlineable):
    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        BuildFunction,
        BuildConcept,
        DataType,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
    ]
    low: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        BuildConcept,
        BuildFunction,
        DataType,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
    ]
    high: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        BuildConcept,
        BuildFunction,
        DataType,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
    ]

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(
            other,
            BoolExpr,
        ):
            raise ValueError(f"Cannot add {type(other)} to {__class__}")
        if other == self:
            return self
        return BuildConditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        return f"{self.left} between {self.low} and {self.high}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, BuildBetween):
            return False
        return (
            self.left == other.left
            and self.low == other.low
            and self.high == other.high
        )

    def __hash__(self):
        return hash(repr(self))

    def inline_constant(self, constant: BuildConcept) -> "BuildBetween":
        assert isinstance(constant.lineage, BuildFunction)
        new_val = constant.lineage.arguments[0]

        def _replace(child):
            if isinstance(child, ConstantInlineable):
                return child.inline_constant(constant)
            if isinstance(child, BuildConcept) and child.address == constant.address:
                return new_val
            return child

        return BuildBetween(
            left=_replace(self.left),
            low=_replace(self.low),
            high=_replace(self.high),
        )

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        return (
            get_concept_arguments(self.left)
            + get_concept_arguments(self.low)
            + get_concept_arguments(self.high)
        )

    @property
    def row_arguments(self) -> List[BuildConcept]:
        return (
            get_concept_row_arguments(self.left)
            + get_concept_row_arguments(self.low)
            + get_concept_row_arguments(self.high)
        )

    @property
    def existence_arguments(self) -> List[Tuple[BuildConcept, ...]]:
        output: List[Tuple[BuildConcept, ...]] = []
        for child in (self.left, self.low, self.high):
            if isinstance(child, BuildConceptArgs):
                output += child.existence_arguments
        return output

    @property
    def output_datatype(self):
        return DataType.BOOL


@dataclass(slots=True)
class BuildConcept(Addressable, BuildConceptArgs, DataTyped):
    name: str
    canonical_name: str
    datatype: CONCRETE_TYPES
    purpose: Purpose
    build_is_aggregate: bool
    derivation: Derivation = Derivation.ROOT
    granularity: Granularity = Granularity.MULTI_ROW
    namespace: Optional[str] = field(default=DEFAULT_NAMESPACE)
    metadata: Metadata = field(
        default_factory=lambda: Metadata(description=None, line_number=None),
    )
    lineage: Optional[
        Union[
            BuildFunction,
            BuildWindowItem,
            BuildFilterItem,
            BuildAggregateWrapper,
            BuildRowsetItem,
            BuildSubselectItem,
            BuildMultiSelectLineage,
        ]
    ] = None
    keys: Optional[set[str]] = None
    grain: BuildGrain = field(default=None)  # type: ignore
    modifiers: List[Modifier] = field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = field(default_factory=set)
    address: str = field(init=False)
    canonical_address: str = field(init=False)

    def __post_init__(self):
        self.address = f"{self.namespace}.{self.name}"
        self.canonical_address = f"{self.namespace}.{self.canonical_name}"
        if (
            isinstance(self.lineage, BuildFunction)
            and self.lineage.operator == FunctionType.ALIAS
        ):
            self.pseudonyms.update(
                arg.address for arg in self.lineage.concept_arguments
            )

    @property
    def is_aggregate(self) -> bool:
        return self.build_is_aggregate

    @property
    def is_nullable(self) -> bool:
        """Intrinsic nullability — a column ``?`` or a nullable derivation."""
        return Modifier.NULLABLE in self.modifiers

    @cached_property
    def hash(self) -> int:
        return hash(
            f"{self.name}+{self.datatype}+ {self.purpose} + {str(self.lineage)} + {self.namespace} + {str(self.grain)} + {str(self.keys)}"
        )

    def __hash__(self):
        return self.hash

    def __repr__(self):
        base = f"{self.address}@{self.grain}"
        return base

    @property
    def output_datatype(self):
        return self.datatype

    def __eq__(self, other: object):
        otype = type(other)
        if otype is str and self.address == other:
            return True
        if otype is not BuildConcept:
            return False

        return (
            self.name == other.name  # type: ignore
            and self.datatype == other.datatype  # type: ignore
            and self.purpose == other.purpose  # type: ignore
            and self.namespace == other.namespace  # type: ignore
            and self.grain == other.grain  # type: ignore
            # and self.keys == other.keys
        )

    @cached_property
    def canonical_address_grain(self) -> str:
        return f"{self.namespace}.{self.canonical_name}@{str(self.grain)}"

    def __str__(self):
        grain = str(self.grain) if self.grain else "Grain<>"
        return f"{self.namespace}.{self.name}@{grain}"

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        elif self.namespace:
            return f"{self.namespace.replace('.','_')}_{self.name.replace('.','_')}"
        return self.name.replace(".", "_")

    @property
    def equivalent_addresses(self) -> set[str]:
        return {self.address, *self.pseudonyms}

    def with_materialized_source(self) -> Self:

        return self.__class__(
            name=self.name,
            canonical_name=self.canonical_name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=None,
            grain=self.grain,
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            ## bound
            derivation=Derivation.ROOT,
            granularity=self.granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    def with_grain(self, grain: Optional["BuildGrain"] = None) -> Self:
        grain = grain if grain else DEFAULT_GRAIN
        if grain == self.grain:
            return self
        return self.__class__(
            name=self.name,
            canonical_name=self.canonical_name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain if grain else DEFAULT_GRAIN,
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            ## bound
            derivation=self.derivation,
            granularity=self.granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    @property
    def _with_default_grain(self) -> Self:
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = BuildGrain(components={self.address})
        elif self.purpose == Purpose.PROPERTY:
            components = []
            if self.keys:
                components = [*self.keys]
            if self.lineage:
                for item in self.lineage.concept_arguments:
                    components += [x.address for x in item.sources]
            # TODO: set synonyms
            grain = BuildGrain(
                components=set([x for x in components]),
            )  # synonym_set=generate_BuildConcept_synonyms(components))
        elif self.purpose == Purpose.METRIC:
            grain = DEFAULT_GRAIN
        elif self.purpose == Purpose.CONSTANT:
            if self.derivation != Derivation.CONSTANT:
                grain = BuildGrain(components={self.address})
            else:
                grain = self.grain
        else:
            grain = self.grain
        if grain == self.grain:
            return self
        return self.__class__(
            name=self.name,
            canonical_name=self.canonical_name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain,
            keys=self.keys,
            namespace=self.namespace,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            ## bound
            derivation=self.derivation,
            granularity=self.granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    def with_default_grain(self) -> "BuildConcept":
        return self._with_default_grain

    @property
    def sources(self) -> List["BuildConcept"]:
        if self.lineage:
            output: List[BuildConcept] = []

            def get_sources(
                expr: Union[
                    BuildFunction,
                    BuildWindowItem,
                    BuildFilterItem,
                    BuildAggregateWrapper,
                    BuildRowsetItem,
                    BuildSubselectItem,
                    BuildMultiSelectLineage,
                ],
                output: List[BuildConcept],
            ):
                if isinstance(expr, BuildMultiSelectLineage):
                    return
                for item in expr.concept_arguments:
                    if isinstance(item, BuildConcept):
                        if item.address == self.address:
                            raise SyntaxError(
                                f"BuildConcept {self.address} references itself"
                            )
                        output.append(item)
                        output += item.sources

            get_sources(self.lineage, output)
            return output
        return []

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.lineage.concept_arguments if self.lineage else []


@dataclass(slots=True)
class BuildOrderItem(DataTyped, BuildConceptArgs):
    expr: BuildExpr
    order: Ordering

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        base: List[BuildConcept] = []
        x = self.expr
        if isinstance(x, BuildConcept):
            base += [x]
        elif isinstance(x, BuildConceptArgs):
            base += x.concept_arguments
        return base

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        if isinstance(self.expr, BuildConceptArgs):
            return self.expr.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        if isinstance(self.expr, BuildConceptArgs):
            return self.expr.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass(slots=True)
class BuildNumberingWindowItem(DataTyped, BuildConceptArgs):
    type: WindowType
    arguments: List[BuildConcept]
    order_by: List[BuildOrderItem]
    over: List["BuildConcept"] = field(default_factory=list)

    def __post_init__(self):
        assert (
            self.type in NUMBERING_WINDOW_TYPES
        ), f"BuildNumberingWindowItem requires a numbering window type, got {self.type}"

    def __repr__(self) -> str:
        return (
            f"{self.type}({self.arguments}, over={self.over}, order_by={self.order_by})"
        )

    def __str__(self):
        return self.__repr__()

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        output: List[BuildConcept] = []
        for arg in self.arguments:
            output += get_concept_arguments(arg)
        for order in self.order_by:
            output += order.concept_arguments
        for item in self.over:
            output += [item]
        return output

    @property
    def output_datatype(self):
        return DataType.INTEGER

    @property
    def output_purpose(self):
        return Purpose.PROPERTY


@dataclass(slots=True)
class BuildNavigationWindowItem(DataTyped, BuildConceptArgs):
    type: WindowType
    content: BuildConcept
    order_by: List[BuildOrderItem]
    over: List["BuildConcept"] = field(default_factory=list)
    offset: Optional[int] = None

    def __post_init__(self):
        assert (
            self.type in NAVIGATION_WINDOW_TYPES
        ), f"BuildNavigationWindowItem requires a navigation window type, got {self.type}"

    def __repr__(self) -> str:
        return f"{self.type}({self.content}, offset={self.offset}, over={self.over}, order_by={self.order_by})"

    def __str__(self):
        return self.__repr__()

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        output = get_concept_arguments(self.content)
        for order in self.order_by:
            output += order.concept_arguments
        for item in self.over:
            output += [item]
        return output

    @property
    def output_datatype(self):
        if self.type in (WindowType.COUNT, WindowType.COUNT_DISTINCT):
            return DataType.INTEGER
        return self.content.output_datatype

    @property
    def output_purpose(self):
        return Purpose.PROPERTY


# Build-side window expression union. See author.WindowItem for category split.
BuildWindowItem = BuildNumberingWindowItem | BuildNavigationWindowItem


@dataclass(slots=True)
class BuildCaseSimpleWhen(DataTyped, BuildConceptArgs):
    value_expr: "BuildExpr"
    expr: "BuildExpr"

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.value_expr)} THEN {str(self.expr)}"

    @cached_property
    def concept_arguments(self):
        return get_concept_arguments(self.value_expr) + get_concept_arguments(self.expr)

    @property
    def concept_row_arguments(self):
        return get_concept_row_arguments(self.value_expr) + get_concept_row_arguments(
            self.expr
        )

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass(slots=True)
class BuildCaseWhen(DataTyped, BuildConceptArgs):
    comparison: (
        BuildConditional | BuildSubselectComparison | BuildComparison | BuildBetween
    )
    expr: "BuildExpr"

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.comparison)} THEN {str(self.expr)}"

    @cached_property
    def concept_arguments(self):
        return get_concept_arguments(self.comparison) + get_concept_arguments(self.expr)

    @property
    def concept_row_arguments(self):
        return get_concept_row_arguments(self.comparison) + get_concept_row_arguments(
            self.expr
        )

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass(slots=True)
class BuildCaseElse(DataTyped, BuildConceptArgs):
    expr: "BuildExpr"

    @cached_property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)

    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass(slots=True)
class BuildFunction(DataTyped, BuildConceptArgs):
    operator: FunctionType
    arguments: Sequence[
        Union[
            int,
            float,
            str,
            date,
            datetime,
            MapWrapper[Any, Any],
            TraitDataType,
            DataType,
            ArrayType,
            MapType,
            NumericType,
            DatePart,
            BuildConcept,
            BuildAggregateWrapper,
            BuildFunction,
            BuildWindowItem,
            BuildParenthetical,
            BuildCaseWhen,
            BuildCaseElse,
            list,
            ListWrapper[Any],
        ]
    ]
    output_data_type: CONCRETE_TYPES
    output_purpose: Purpose = field(default=Purpose.KEY)
    arg_count: int = field(default=1)
    valid_inputs: Optional[
        Union[
            Set[DataType | ArrayType | MapType],
            List[Set[DataType | ArrayType | MapType]],
        ]
    ] = None

    def __repr__(self):
        return f'{self.operator.value}({",".join([str(a) for a in self.arguments])})'

    def __str__(self):
        return self.__repr__()

    @property
    def datatype(self):
        return self.output_data_type

    @property
    def output_datatype(self):
        return self.output_data_type

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        base = []
        for arg in self.arguments:
            base += get_concept_arguments(arg)
        return base

    @property
    def rendered_concept_arguments(self) -> List[BuildConcept]:
        if self.operator == FunctionType.GROUP:
            base = self.arguments[0]
            return get_rendered_concept_arguments(base)
        base = []
        for arg in self.arguments:
            base += get_rendered_concept_arguments(arg)
        return base

    @property
    def output_grain(self):
        # aggregates have an abstract grain
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return BuildGrain(components=[])
        # scalars have implicit grain of all arguments
        args = set()
        for input in self.concept_arguments:
            args += input.grain.components
        return BuildGrain(components=args)


@dataclass(slots=True)
class BuildAggregateWrapper(BuildConceptArgs, DataTyped):
    function: BuildFunction
    by: List[BuildConcept] = field(default_factory=list)
    grouping: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    grouping_sets: List[List[BuildConcept]] = field(default_factory=list)

    def __str__(self):
        grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def is_abstract(self):
        if not self.by:
            return True
        if all(x.name == ALL_ROWS_CONCEPT for x in self.by):
            return True
        return False

    def with_abstract_by(self) -> "BuildAggregateWrapper":
        return BuildAggregateWrapper(function=self.function, by=[])

    @property
    def datatype(self):
        return self.function.datatype

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.function.concept_arguments + self.by

    @property
    def output_datatype(self):
        return self.function.output_datatype

    @property
    def output_purpose(self):
        return self.function.output_purpose


@dataclass(slots=True)
class BuildFilterItem(BuildConceptArgs):
    content: "BuildExpr"
    where: BuildWhereClause

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def content_concept_arguments(self):
        if isinstance(self.content, BuildConcept):
            return [self.content]
        elif isinstance(self.content, BuildConceptArgs):
            return self.content.concept_arguments
        return []

    @cached_property
    def concept_arguments(self):
        return self.where.concept_arguments + get_concept_arguments(self.content)

    @property
    def rendered_concept_arguments(self):
        return (
            get_rendered_concept_arguments(self.content)
            + self.where.rendered_concept_arguments
        )


@dataclass(slots=True)
class BuildRowsetLineage(BuildConceptArgs):
    name: str
    derived_concepts: List[str]
    select: SelectLineage | MultiSelectLineage


@dataclass(slots=True)
class BuildRowsetItem(DataTyped, BuildConceptArgs):
    content: BuildConcept
    rowset: BuildRowsetLineage

    def __repr__(self):
        return f"<Rowset<{self.rowset.name}>: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def concept_arguments(self):
        return [self.content]


@dataclass(slots=True)
class BuildSubselectItem(DataTyped, BuildConceptArgs):
    content: BuildConcept
    where: Optional[BuildWhereClause] = None
    order_by: List[BuildOrderItem] = field(default_factory=list)
    limit: Optional[int] = None
    outer_arguments: List[BuildConcept] = field(default_factory=list)

    def __repr__(self):
        return f"<Subselect: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    @property
    def output_datatype(self):
        return ArrayType(type=self.content.datatype)

    @property
    def output_purpose(self):
        return Purpose.PROPERTY

    @cached_property
    def concept_arguments(self) -> List[BuildConcept]:
        # When outer_arguments exist, only expose those to the main query.
        # Inner concepts are resolved separately in gen_subselect_node.
        if self.outer_arguments:
            return list(self.outer_arguments)
        args: List[BuildConcept] = [self.content]
        if self.where:
            args += self.where.concept_arguments
        for item in self.order_by:
            args += item.concept_arguments
        return args

    @cached_property
    def inner_concept_arguments(self) -> List[BuildConcept]:
        """Inner concepts for separate resolution in subselect node."""
        args: List[BuildConcept] = [self.content]
        if self.where:
            args += self.where.concept_arguments
        for item in self.order_by:
            args += item.concept_arguments
        if self.outer_arguments:
            outer_addrs = {a.address for a in self.outer_arguments}
            args = [a for a in args if a.address not in outer_addrs]
        return args


@dataclass(slots=True)
class BuildOrderBy:
    items: List[BuildOrderItem]

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]


@dataclass(slots=True)
class BuildAlignClause:
    items: List[BuildAlignItem]


@dataclass(slots=True)
class BuildDeriveClause:
    items: List[BuildDeriveItem]


@dataclass(slots=True)
class BuildDeriveItem:
    expr: BuildExpr
    name: str
    namespace: str = field(default=DEFAULT_NAMESPACE)

    @property
    def address(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(slots=True)
class BuildSelectLineage:
    selection: List[BuildConcept]
    hidden_components: set[str]
    local_concepts: dict[str, BuildConcept]
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = field(default_factory=lambda: Metadata())
    grain: BuildGrain = field(default_factory=BuildGrain)
    where_clause: BuildWhereClause | None = field(default=None)
    having_clause: BuildHavingClause | None = field(default=None)

    @property
    def output_components(self) -> List[BuildConcept]:
        return self.selection


@dataclass(slots=True)
class BuildMultiSelectLineage(BuildConceptArgs):
    selects: List[SelectLineage]
    grain: BuildGrain
    align: BuildAlignClause
    namespace: str
    local_concepts: dict[str, BuildConcept]
    build_concept_arguments: list[BuildConcept]
    build_output_components: list[BuildConcept]
    hidden_components: set[str]
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["BuildWhereClause", None] = field(default=None)
    having_clause: Union["BuildHavingClause", None] = field(default=None)
    derive: BuildDeriveClause | None = None

    @property
    def derived_concepts(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        if self.derive:
            for ditem in self.derive.items:
                output.add(ditem.address)
        return output

    @property
    def output_components(self) -> list[BuildConcept]:
        return self.build_output_components

    @property
    def calculated_derivations(self) -> set[str]:
        output: set[str] = set()
        if not self.derive:
            return output
        for item in self.derive.items:
            output.add(item.address)
        return output

    @property
    def concept_arguments(self) -> list[BuildConcept]:
        return self.build_concept_arguments

    # these are needed to help disambiguate between parents
    def get_merge_concept(self, check: BuildConcept) -> str | None:
        for item in self.align.items:
            if check in item.concepts_lcl:
                return f"{item.namespace}.{item.alias}"
        return None

    def find_source(self, concept: BuildConcept, cte: CTE | UnionCTE) -> BuildConcept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c

        # Reaching here means this CTE can't source the column directly
        # (typically an outer aggregate grouped it away before a projection
        # re-referenced it, or a collapsed join kept it only as a pseudonym).
        # The dedicated ValueError subclass lets the renderer's candidate
        # probing recover via a pseudonym twin; unrecovered, it surfaces as an
        # internal planner error, not a "Syntax error" blaming the query.
        raise UnionOutputResolutionError(
            f"Internal planner error: could not resolve union/multiselect output "
            f"'{concept.address}' against CTE '{cte.name}' (it is not in that CTE's "
            f"outputs {sorted(cte.output_lcl.addresses)}). If this came from an "
            f"ORDER BY on a union column, order by the projected output column "
            f"instead."
        )


@dataclass(slots=True)
class BuildUnionSelectLineage(BuildMultiSelectLineage):
    """Build form of a relational `union(...)`/`except(...)`/`intersect(...)`
    TVF: a positional column-stack (SQL set operation) of arm selects. Same
    arm/align machinery as the multiselect build lineage; the distinct type
    routes the planner to the union combiner (`UnionNode`) instead of the
    FULL-JOIN merge. `build_output_components` holds only the bound columns.
    For EXCEPT the arm order is semantic (left-fold)."""

    operator: SetOperator = SetOperator.UNION_ALL


@dataclass(slots=True)
class BuildAlignItem:
    alias: str
    concepts: List[BuildConcept]
    namespace: str = field(default=DEFAULT_NAMESPACE)
    hidden: bool = False
    concepts_lcl: LooseBuildConceptList = field(init=False)

    def __post_init__(self):
        self.concepts_lcl = LooseBuildConceptList(concepts=self.concepts)

    @property
    def aligned_concept(self) -> str:
        return f"{self.namespace}.{self.alias}"


@dataclass(slots=True)
class BuildColumnAssignment:
    alias: str | RawColumnExpr | BuildFunction | BuildAggregateWrapper
    concept: BuildConcept
    modifiers: set[Modifier] = field(default_factory=set)
    # The author-declared concept address this column bound BEFORE a scoped
    # join/merge substituted it onto the relation's canonical address — the
    # column's ORIGIN DOMAIN NODE (docs/domain_graph_design.md), threaded
    # FORWARD so plan-level rulings on a same-address join pair can resolve
    # which relation endpoint each side physically carries. None = no
    # substitution; the concept's own address is its origin.
    origin_address: str | None = None

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    @property
    def origin_concept_address(self) -> str:
        """The authored concept address this column physically carries —
        `origin_address` when a scoped relation substituted the binding onto
        its canonical, else the concept's own address."""
        return (
            self.origin_address
            if self.origin_address is not None
            else self.concept.address
        )


@dataclass(slots=True)
class BuildDatasource:
    name: str
    columns: List[BuildColumnAssignment]
    address: Union[Address, str]
    grain: BuildGrain = field(
        default_factory=lambda: DEFAULT_GRAIN,
    )
    namespace: Optional[str] = field(default=DEFAULT_NAMESPACE)
    metadata: DatasourceMetadata = field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: Optional[BuildWhereClause] = None
    non_partial_for: Optional[BuildWhereClause] = None
    # See ``Datasource.column_level_partial_addresses``.
    column_level_partial_addresses: set[str] = field(default_factory=set)

    def __hash__(self):
        return self.identifier.__hash__()

    def __eq__(self, other):
        # Align with __hash__: identity is the (namespace, name) identifier.
        # The dataclass-generated default recurses through columns /
        # column_level_partial_addresses, which is needlessly expensive in
        # set/dict lookups (datasources land in QueryDatasource.source_map
        # sets alongside QueryDatasource).
        if type(other) is not BuildDatasource:
            return NotImplemented
        return self.identifier == other.identifier

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    @property
    def condition(self):
        return None

    @property
    def can_be_inlined(self) -> bool:
        if isinstance(self.address, Address) and (
            self.address.is_query or self.address.is_file
        ):
            return False
        return True

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    @property
    def safe_identifier(self) -> str:
        return self.identifier.replace(".", "_")

    @property
    def concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def output_concepts(self) -> List[BuildConcept]:
        return self.concepts

    @property
    def effective_grain(self) -> BuildGrain:
        key_outputs = {
            concept.address
            for concept in self.output_concepts
            if concept.purpose == Purpose.KEY
        }
        return self.grain + BuildGrain(components=key_outputs)

    @property
    def full_concepts(self) -> set[str]:
        return {
            c.concept.address
            for c in self.columns
            if Modifier.PARTIAL not in c.modifiers
        }

    @property
    def nullable_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def hidden_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.HIDDEN in c.modifiers]

    @property
    def partial_concepts(self) -> List[BuildConcept]:
        # Partiality is a fact about a BINDING, not the datasource: an address
        # also bound complete here (a second endpoint of a relation folded onto
        # it) is still fully providable — only addresses with NO complete
        # binding are partial for the source.
        full = self.full_concepts
        return [
            c.concept
            for c in self.columns
            if Modifier.PARTIAL in c.modifiers and c.concept.address not in full
        ]

    @property
    def column_level_partial_concepts(self) -> List[BuildConcept]:
        """Columns with intrinsic (pre-stamp) PARTIAL — survive a covering UNION."""
        if not self.column_level_partial_addresses:
            return []
        return [
            c.concept
            for c in self.columns
            if c.concept.address in self.column_level_partial_addresses
        ]

    def get_alias(
        self,
        concept: BuildConcept,
        use_raw_name: bool = True,
        force_alias: bool = False,
    ) -> Optional[str | RawColumnExpr] | BuildFunction | BuildAggregateWrapper:
        # 2022-01-22
        # this logic needs to be refined.
        # if concept.lineage:
        # #     return None
        # Several columns can bind one address when a relation folded a second
        # endpoint onto it; the concept AS ITSELF renders from its native
        # (unsubstituted, non-partial) binding, never a folded endpoint's
        # column. Tiers: first native exact match > first exact/pseudonym
        # match > canonical-address match.
        native_match = None
        exact_match = None
        canonical_match = None
        for x in self.columns:
            is_exact = (
                x.concept == concept or x.concept.with_grain(concept.grain) == concept
            )
            if is_exact or (
                concept.address in x.concept.pseudonyms
                or x.concept.address in concept.pseudonyms
            ):
                if exact_match is None:
                    exact_match = x
                if (
                    is_exact
                    and x.origin_address is None
                    and Modifier.PARTIAL not in x.modifiers
                ):
                    native_match = x
                    break
                continue
            if x.concept.canonical_address == concept.canonical_address:
                canonical_match = x
        match = native_match or exact_match or canonical_match
        if match is not None:
            if use_raw_name:
                return match.alias
            return concept.safe_address
        existing = [str(c.concept) for c in self.columns]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {concept} not found on {self.identifier}; have"
            f" {existing}."
        )

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address

    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_concepts)

    @property
    def is_union(self) -> bool:
        return False


@dataclass(slots=True)
class BuildUnionDatasource:
    children: List[BuildDatasource]
    non_partial_for: Optional[BuildWhereClause] = None

    def is_union(self) -> bool:
        return True

    @property
    def columns(self) -> List[BuildColumnAssignment]:
        # Only columns whose concept appears in every child can be safely
        # projected through the union — anything else would produce a NULL
        # branch and break the "common output" contract enum unions rely on.
        if not self.children:
            return []
        common = set(c.concept.address for c in self.children[0].columns)
        for child in self.children[1:]:
            common &= {c.concept.address for c in child.columns}
        return [c for c in self.children[0].columns if c.concept.address in common]

    @property
    def grain(self) -> BuildGrain:
        return reduce(lambda x, y: x.union(y.grain), self.children, BuildGrain())

    @property
    def partial_concepts(self) -> List[BuildConcept]:
        return []


# Anything that can sit in a WHERE / HAVING / join condition. Subclasses of
# BuildComparison (e.g. BuildSubselectComparison) are covered structurally.
BoolExpr = BuildComparison | BuildConditional | BuildParenthetical | BuildBetween


BuildExpr = (
    BuildWindowItem
    | BuildFilterItem
    | BuildConcept
    | BuildComparison
    | BuildConditional
    | BuildParenthetical
    | BuildFunction
    | BuildAggregateWrapper
    | bool
    | MagicConstants
    | int
    | str
    | float
    | list
)


def get_canonical_pseudonyms(environment: Environment) -> dict[str, set[str]]:
    roots: dict[str, set[str]] = defaultdict(set)
    for k, v in environment.concepts.items():
        roots[v.address].add(k)
        for x in v.pseudonyms:
            roots[v.address].add(x)
    for k, v in environment.alias_origin_lookup.items():
        lookup = environment.concepts[k].address
        roots[lookup].add(v.address)
        for x2 in v.pseudonyms:
            roots[lookup].add(x2)
    return roots


def requires_concept_nesting(
    expr,
) -> AggregateWrapper | WindowItem | FilterItem | Function | None:
    if isinstance(expr, (AggregateWrapper, WindowItem, FilterItem)):
        return expr
    if isinstance(expr, Function) and expr.operator in (
        FunctionType.GROUP,
        FunctionType.PARENTHETICAL,
    ):
        # group by requires nesting
        return expr
    return None


def is_constant(x):
    return isinstance(
        x, (str, int, float, bool, MagicConstants, BuildParamaterizedConceptReference)
    )


def materialize_constant(x):
    if isinstance(x, BuildParamaterizedConceptReference):
        return x.value
    elif x == MagicConstants.NULL:
        return None
    return x


def scope_tagged_joins(
    joins: list[tuple[str, str, JoinType]],
    environment: Environment,
) -> list[tuple[tuple[str, str, JoinType], EdgeScope]]:
    """Tag each build-scoped join tuple with its declaration scope: a tuple
    persisted on the environment is a global `merge`, anything else arrived
    from a statement. The scope decides what a FULL tuple DECLARES — a global
    merge asserts EQUAL domains, a query `union join` asserts INCOMPARABLE."""
    global_merges = set(environment.merges)
    return [
        (
            join,
            EdgeScope.GLOBAL if join in global_merges else EdgeScope.STATEMENT,
        )
        for join in joins
    ]


class Factory:

    def __init__(
        self,
        environment: Environment,
        local_concepts: dict[str, BuildConcept] | None = None,
        grain: Grain | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
        build_cache: dict[str, BuildConcept] | None = None,
        grain_build_cache: dict[tuple, "BuildGrain"] | None = None,
        canonical_build_cache: dict[str, BuildConcept] | None = None,
        datasource_build_cache: dict[str, "BuildDatasource"] | None = None,
        scoped_joins: list[tuple[str, str, JoinType]] | None = None,
        aggregate_grain: Grain | None = None,
        select_grouping: AggregateGrouping | None = None,
    ):
        self.grain = grain or Grain()
        # Grain at which a bare (no explicit `by`) aggregate resolves, when it
        # differs from `self.grain`. Used by the WHERE-clause factory: WHERE runs
        # at row grain (`self.grain = Grain()`) for plain predicates, but an
        # aggregate condition must co-grain to the SELECT grain (like HAVING)
        # rather than collapsing to a global scalar. None => use `self.grain`.
        self.aggregate_grain = aggregate_grain
        # SELECT-level `by rollup/cube/grouping sets` spec of the select whose
        # projection scope this factory builds. Applied to every un-pinned
        # aggregate at materialization — the select-scoped moment — so shared
        # authoring definitions never carry (or leak) the spec.
        self.select_grouping = select_grouping
        self.environment = environment
        # Build-scoped joins (query JOIN clauses plus environment MERGE
        # statements — the same relation declared at different scopes, resolved
        # identically) relate two key concepts. TWO mechanisms do this, split by
        # JOIN SEMANTICS (not by key kind — a root-keyed FULL still needs
        # coalesce):
        #
        #   * SUBSTITUTION (scoped_merge_map -> `_build_concept` swap): the
        #     source address is replaced by its canonical target everywhere
        #     (refs, grain components, datasource column bindings). Correct ONLY
        #     when the key equality holds on EVERY output row, so one logical key
        #     can render from one physical column: the FULL canonical-key
        #     registry and dependent-grain collapse.
        #
        #   * IDENTITY + pseudonym + coalesce (scoped_merge_sources +
        #     scoped_outer_identity_sources, coalesced at the merge node): the
        #     collapsed-away key keeps its OWN address and a pseudonym back to the
        #     canonical, and the merge node coalesces the distinct physical
        #     columns. Required when the correspondence MAY be ABSENT on some
        #     rows (LEFT / FULL across distinct columns), where the output key is
        #     a row-by-row `coalesce` of both columns and substitution — having
        #     destroyed one column — could not represent it.
        #
        # ROOT OUTER keys stay on the identity path end-to-end: a
        # binding-keyed OUTER source (scoped_outer_identity_sources, below)
        # keeps its own binding instead of substituting to the canonical, so
        # the FULL coalesce spans both members regardless of which side is
        # projected (pinned in tests/test_scoped_join_permutations.py).
        # scoped_partial_sources marks the LEFT-join side whose datasource
        # binding must be partial.
        self.scoped_joins: list[tuple[str, str, JoinType]] = scoped_joins or []
        # The declared-edge domain graph for this build: every scoped join is
        # a domain DECLARATION (subset / equal / incomparable) and the legacy
        # registries below are graph queries (docs/domain_graph_design.md).
        self.domain_graph = DomainGraph.from_scoped_joins(
            scope_tagged_joins(self.scoped_joins, environment)
        )
        self.scoped_merge_map = dict(self.domain_graph.canonical_map())
        self.scoped_partial_sources = self.domain_graph.subset_sources()
        self._source_identity_addresses: set[str] = set()
        # A global `merge` IS a scoped join persisted on the environment (FULL
        # non-partial / LEFT partial). It resolves identically to the equivalent
        # query-scoped join; the only difference is scope (merges are re-injected
        # at sub-build boundaries, query joins are not).
        full_join_sources = {
            source
            for source, target, join_type in self.scoped_joins
            if join_type is JoinType.FULL
        }
        self.scoped_merge_sources_by_target: dict[str, set[str]] = defaultdict(set)
        for source, target in self.scoped_merge_map.items():
            self.scoped_merge_sources_by_target[target].add(source)
        # FULL-join canonical keys (graph EQUAL/∦ endpoints) and LEFT anchor keys
        # (declared-subset anchors) are no longer materialized here — join
        # resolution queries them from BuildEnvironment.domain_graph directly
        # (outer_relation_keys / left_anchor_keys).
        self._validate_scoped_join_endpoint_identity(environment)

        def _is_binding_keyed(addr: str) -> bool:
            c = environment.concepts.get(addr)
            return c is None or c.derivation in (Derivation.ROOT, Derivation.ROWSET)

        def _is_rowset_keyed(addr: str) -> bool:
            c = environment.concepts.get(addr)
            return c is not None and c.derivation == Derivation.ROWSET

        def _is_derived_keyed(addr: str) -> bool:
            c = environment.concepts.get(addr)
            return c is not None and c.derivation == Derivation.BASIC

        # A FULL relation is symmetric, so the rowset-outer identity handling
        # must engage whichever side the rowset key was authored on — keying on
        # the source alone let `full join <derived expr> = <rowset key>` fall
        # through to the substitution path, which destroys the derived key's
        # column (the rowset key becomes the group canonical and the derived
        # side has no binding for it). LEFT stays source-keyed: its operand
        # order is directional by definition. A COALESCING (∦ — query
        # `full`/`union` join) pair whose endpoints are BOTH derived
        # expressions (`cast(a.k) = cast(b.k)`, q97) has the same problem with
        # no rowset key to catch it: substituting one virt key onto the other
        # destroys its side's compute expression, and with only one distinct
        # group member left the exposure machinery disengages — sourcing the
        # surviving key then re-enters its own rowset (infinite recursion).
        # Both endpoints keep identity; each side materializes its own key
        # column and the merge coalesces them. Gated to ∦ declarations: an
        # EQUAL (global `merge`) pair of derived concepts declares identical
        # domains, where substitution is the correct single-column plan.
        coalescing_endpoints = self.domain_graph.coalescing_relation_members()

        def _rowset_outer_pair(s: str, t: str, jt: JoinType) -> bool:
            if jt is JoinType.LEFT_OUTER:
                return _is_rowset_keyed(s)
            if jt is JoinType.FULL:
                return (
                    _is_rowset_keyed(s)
                    or _is_rowset_keyed(t)
                    or (
                        s in coalescing_endpoints
                        and t in coalescing_endpoints
                        and _is_derived_keyed(s)
                        and _is_derived_keyed(t)
                    )
                )
            return False

        self.scoped_rowset_outer_sources: set[str] = {
            s for s, t, jt in self.scoped_joins if _rowset_outer_pair(s, t, jt)
        }
        self.scoped_rowset_outer_targets: set[str] = {
            t for s, t, jt in self.scoped_joins if _rowset_outer_pair(s, t, jt)
        }
        self.scoped_key_merge_map = {
            source: target
            for source, target in self.scoped_merge_map.items()
            if source not in full_join_sources
            and source not in self.scoped_rowset_outer_targets
        }
        self.scoped_merge_sources: set[str] = set()
        # OUTER-join keys with a datasource/rowset binding (ROOT/ROWSET) keep
        # their own identity + a pseudonym back to the canonical key so both
        # joined subgraphs stay sourceable; the merge node coalesces the physical
        # key columns. Their partiality is carried by that binding / the rowset
        # machinery, NOT scoped_partial_derived, so they are tracked here and
        # subtracted from it below. (A *derived* key has no binding to carry
        # partiality, so it stays in scoped_partial_derived and out of this set.)
        self.scoped_outer_identity_sources: set[str] = set()
        scoped_pseudonym_sources: set[str] = set()
        # EVERY collapsed-away endpoint of a FULL/LEFT relation (any endpoint the
        # union-find mapped to a canonical other than itself — a LEFT's target, a
        # FULL's source, or either endpoint displaced by a chained relation) keeps
        # its own identity plus a mutual pseudonym to its canonical, so the
        # relation stays sourceable from both sides even when one side has no
        # independent source (e.g. `merge derived_metric into unbound_property`,
        # where the canonical is only reachable through the source's derivation).
        for s, t, jt in self.scoped_joins:
            if jt not in (JoinType.LEFT_OUTER, JoinType.FULL):
                continue
            for addr in (s, t):
                if addr not in self.scoped_merge_map:
                    continue
                self.scoped_merge_sources.add(addr)
                scoped_pseudonym_sources.add(addr)
                if _is_binding_keyed(addr):
                    self.scoped_outer_identity_sources.add(addr)
        self.local_concepts: dict[str, BuildConcept] = (
            {} if local_concepts is None else local_concepts
        )
        self.local_non_build_concepts: dict[str, Concept] = {}
        self.pseudonym_map = pseudonym_map or get_canonical_pseudonyms(environment)
        if self.scoped_merge_map:
            # A scoped join collapses source->target like a global `merge`, but
            # unlike `merge_concept` it never linked the source back onto the
            # target as a pseudonym. Without that link discovery only knows the
            # target's own lineage, so a *derived* join key (rowset output, basic
            # transform, union, ...) becomes unsourceable from the source side and
            # the query dead-ends. Mirror `merge_concept`: make source and target
            # mutual pseudonyms so the merged concept can be sourced via either
            # side's derivation, exactly as the global-merge path resolves it.
            # Sub-factories inherit the already-augmented map, so skip the copy
            # when the links are present.
            pending: list[tuple[str, str]] = []
            for source in scoped_pseudonym_sources:
                canonical_addr = self.scoped_merge_map[source]
                if source not in self.pseudonym_map.get(canonical_addr, ()):
                    pending.append((source, canonical_addr))
            if pending:
                augmented = {k: set(v) for k, v in self.pseudonym_map.items()}
                for source_addr, target_addr in pending:
                    augmented.setdefault(target_addr, set()).add(source_addr)
                    augmented.setdefault(source_addr, set()).add(target_addr)
                self.pseudonym_map = augmented
        self.build_cache = build_cache or {}
        # Cross-factory cache for BuildGrain keyed on (frozenset(components),
        # id(where_clause)). Same lifecycle as build_cache — propagated to
        # sub-factories that share build_cache so all factories in a parse
        # reuse the same normalized grains.
        self.grain_build_cache: dict[tuple, BuildGrain] = (
            {} if grain_build_cache is None else grain_build_cache
        )
        # Cache of BuildConcepts for "grain-stable" base concepts (no lineage
        # + explicit grain). Their BuildConcept is determined entirely by the
        # source Concept + env state and so is identical across every factory
        # in the call tree, regardless of the factory's grain context. Keyed
        # by base.address. Lifetime is one get_query_node call — created by
        # the caller and threaded through.
        self.canonical_build_cache: dict[str, BuildConcept] = (
            {} if canonical_build_cache is None else canonical_build_cache
        )
        # Cache of fully-built BuildDatasources. A datasource builds with its
        # own fixed grain and an empty local-concept scope, so the result is a
        # pure function of (Datasource, environment) — safe to reuse across
        # every sub-select in a resolution.
        self.datasource_build_cache: dict[str, BuildDatasource] = (
            {} if datasource_build_cache is None else datasource_build_cache
        )
        self.build_grain = self.build(self.grain) if self.grain else None
        # Addresses currently mid-build, in dependency order. Used to expand a
        # self-referential metric out of an abstract aggregate's resolution
        # grain (see `_abstract_resolution_grain`).
        self._building: list[str] = []

    def _scoped_join_key_groups(self) -> dict[str, set[str]]:
        """Authored join-key equivalence groups, canonical -> all members."""
        return self.domain_graph.join_key_groups()

    def _validate_scoped_join_endpoint_identity(self, environment: Environment) -> None:
        """Reject a FULL/UNION relation between two ROOT keys bound only in the
        same table(s): the unified key is a coalesce ACROSS the two endpoints'
        populations, which a single scan cannot represent — the plan silently
        renders one endpoint's own column as the unified axis instead (wrong
        whenever either domain has values the other lacks). No two-instance
        plan exists yet (docs/domain_graph_design.md, endpoint-identity seam);
        fail clean and point at the working idiom. LEFT/SUBSET relations
        resolve fine one-table: the anchor column IS the unified axis. Endpoints
        with a binding OUTSIDE the shared tables can still resolve and pass."""
        pairs = [(s, t) for s, t, jt in self.scoped_joins if jt is JoinType.FULL]
        if not pairs:
            return
        binding_map: dict[str, set[str]] | None = None
        for source, target in pairs:
            source_concept = environment.concepts.get(source)
            target_concept = environment.concepts.get(target)
            if source_concept is None or target_concept is None:
                continue
            if not (
                source_concept.derivation is Derivation.ROOT
                and target_concept.derivation is Derivation.ROOT
            ):
                continue
            if binding_map is None:
                binding_map = defaultdict(set)
                for datasource in environment.datasources.values():
                    if datasource.status != DatasourceState.PUBLISHED:
                        continue
                    for column in datasource.columns:
                        binding_map[column.concept.address].add(datasource.identifier)
            source_bindings = binding_map.get(source, set())
            target_bindings = binding_map.get(target, set())
            if source_bindings and source_bindings == target_bindings:
                shared = ", ".join(sorted(source_bindings))
                raise InvalidSyntaxException(
                    f"Cannot union '{source}' with '{target}': both keys are bound "
                    f"only in the same datasource(s) ({shared}), and the unified "
                    "key must coalesce across two separate reads of it. Import the "
                    "model twice under different namespaces and relate the two "
                    "imports' keys instead (e.g. `import emp as e1; import emp as "
                    "e2; ... join e1.key = e2.other_key`)."
                )

    def _build_keys(self, keys: set[str] | None) -> set[str] | None:
        if keys is None:
            return None
        if not self.scoped_key_merge_map:
            return keys
        return {self.scoped_key_merge_map.get(key, key) for key in keys}

    def instantiate_concept(
        self,
        arg: (
            AggregateWrapper
            | FunctionCallWrapper
            | WindowItem
            | FilterItem
            | Function
            | ListWrapper
            | MapWrapper
            | int
            | float
            | str
            | date
        ),
    ) -> tuple[Concept, BuildConcept]:
        from trilogy.parsing.common import arbitrary_to_concept, generate_concept_name

        name = generate_concept_name(arg)
        if name in self.local_concepts and name in self.local_non_build_concepts:
            # if we already have this concept, return it
            return self.local_non_build_concepts[name], self.local_concepts[name]
        new = arbitrary_to_concept(
            arg,
            environment=self.environment,
        )
        built = self._build_concept(new)
        self.local_concepts[name] = built
        self.local_non_build_concepts[name] = new
        return new, built

    # Resolved build implementations keyed by input type. singledispatchmethod
    # is too heavy for a method called hundreds of thousands of times per parse:
    # its __get__ rebuilds a wrapper (via update_wrapper) on every access, and
    # dispatch() does a weakref-cache lookup. We keep singledispatch only for
    # registration/MRO resolution and cache the result on first sight.
    _build_impl_cache: dict[type, Any] = {}

    def build(self, base):
        impl = Factory._build_impl_cache.get(base.__class__)
        if impl is None:
            impl = Factory.__dict__["_build_dispatch"].dispatcher.dispatch(
                base.__class__
            )
            Factory._build_impl_cache[base.__class__] = impl
        return impl(self, base)

    @singledispatchmethod
    def _build_dispatch(self, base):
        raise NotImplementedError("Cannot build {}".format(type(base)))

    @_build_dispatch.register
    def _(
        self,
        base: (
            int
            | str
            | float
            | Decimal
            | list
            | date
            | TupleWrapper
            | ListWrapper
            | MagicConstants
            | MapWrapper
            | DataType
            | DatePart
            | NumericType
        ),
    ) -> (
        int
        | str
        | float
        | Decimal
        | list
        | date
        | TupleWrapper
        | ListWrapper
        | MagicConstants
        | MapWrapper
        | DataType
        | DatePart
        | NumericType
    ):
        return self._build_primitive(base)

    def _build_primitive(self, base):
        if isinstance(base, MapWrapper):
            return MapWrapper(
                {k: self.build(v) for k, v in base.items()},
                key_type=base.key_type,
                value_type=base.value_type,
            )
        if isinstance(base, ListWrapper):
            return ListWrapper([self.build(v) for v in base], type=base.type)
        return base

    @_build_dispatch.register
    def _(self, base: None) -> None:
        return self._build_none(base)

    def _build_none(self, base):
        return base

    @_build_dispatch.register
    def _(self, base: Function) -> Any:
        return self._build_function(base)

    def _build_function(self, base: Function) -> Any:
        raw_args: list[Concept | FuncArgs] = []
        for arg in base.arguments:
            # to do proper discovery, we need to inject virtual intermediate concepts
            # we don't use requires_concept_nesting here by design
            if isinstance(arg, (AggregateWrapper, FilterItem, WindowItem)):
                narg, _ = self.instantiate_concept(arg)
                raw_args.append(narg)
            else:
                raw_args.append(arg)
        if base.operator == FunctionType.GROUP:
            group_base = raw_args[0]
            final_args: List[Concept | ConceptRef] = []
            if isinstance(group_base, ConceptRef):
                if group_base.address in self.environment.concepts and not isinstance(
                    self.environment.concepts[group_base.address], UndefinedConcept
                ):
                    group_base = self.environment.concepts[group_base.address]
            if (
                isinstance(group_base, Concept)
                and isinstance(group_base.lineage, AggregateWrapper)
                and not group_base.lineage.by
            ):
                arguments = raw_args[1:]
                for x in arguments:
                    if isinstance(x, (ConceptRef, Concept)):
                        final_args.append(x)
                    else:
                        # constants, etc, can be ignored for group
                        continue
                _, rval = self.instantiate_concept(
                    AggregateWrapper(
                        function=group_base.lineage.function,
                        by=final_args,
                    )
                )

                return BuildFunction(
                    operator=base.operator,
                    arguments=[
                        rval,
                        *[self.handle_constant(self.build(c)) for c in raw_args[1:]],
                    ],
                    output_data_type=base.output_datatype,
                    output_purpose=base.output_purpose,
                    valid_inputs=base.valid_inputs,
                    arg_count=base.arg_count,
                )

        farguments: list[Any] = [self.handle_constant(self.build(c)) for c in raw_args]
        if base.operator == FunctionType.CASE:
            case_args: list[Any] = []
            for arg in farguments:
                if isinstance(arg, BuildCaseWhen):
                    if arg.comparison is True:
                        return arg.expr
                    if arg.comparison is False:
                        continue
                case_args.append(arg)
            if len(case_args) == 1 and isinstance(case_args[0], BuildCaseElse):
                return case_args[0].expr
            farguments = case_args

        new = BuildFunction(
            operator=base.operator,
            arguments=farguments,
            output_data_type=base.output_datatype,
            output_purpose=base.output_purpose,
            valid_inputs=base.valid_inputs,
            arg_count=base.arg_count,
        )
        return new

    @_build_dispatch.register
    def _(self, base: ConceptRef) -> BuildConcept:
        return self._build_concept_ref(base)

    def _build_concept_ref(self, base: ConceptRef) -> BuildConcept:
        if base.address in self.local_concepts:
            full = self.local_concepts[base.address]
            if isinstance(full, BuildConcept):
                return full
        if base.address in self.environment.concepts:
            raw = self.environment.concepts[base.address]
            return self._build_concept(raw)
        if base.address in self.environment.concepts.data:
            return self._build_concept(self.environment.concepts[base.address])
        # this will error by design - TODO - more helpful message?
        return self._build_concept(self.environment.concepts[base.address])

    @_build_dispatch.register
    def _(self, base: CaseWhen) -> BuildCaseWhen:
        return self._build_case_when(base)

    def _build_case_when(self, base: CaseWhen) -> BuildCaseWhen:
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildCaseWhen(
            comparison=self.build(base.comparison),
            expr=self.build(expr),
        )

    @_build_dispatch.register
    def _(self, base: CaseSimpleWhen) -> BuildCaseSimpleWhen:
        return self._build_simple_case_when(base)

    def _build_simple_case_when(self, base: CaseSimpleWhen) -> BuildCaseSimpleWhen:
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildCaseSimpleWhen(
            value_expr=self.build(base.value_expr),
            expr=self.build(expr),
        )

    @_build_dispatch.register
    def _(self, base: CaseElse) -> BuildCaseElse:
        return self._build_case_else(base)

    def _build_case_else(self, base: CaseElse) -> BuildCaseElse:
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildCaseElse(expr=self.build(expr))

    @_build_dispatch.register
    def _(self, base: Concept) -> BuildConcept:
        return self._build_concept(base)

    def _abstract_resolution_grain(self) -> Grain:
        """Factory grain for resolving an abstract aggregate, with any metric
        that is currently mid-build (an ancestor on the build stack) replaced by
        its own grain keys.

        A selected aggregate is a grain component, so the factory grain can
        contain a metric. Resolving an abstract sub-aggregate of that metric
        against the raw grain would group it by — and rebuild — the metric,
        recursing. Expanding only on-stack metrics scopes the fix to true
        self-references: a metric used purely as a grouping dimension (e.g. the
        q13 ``count(x) by y`` distribution) is never an ancestor here, so its
        resolution is unchanged.
        """
        grain = self.aggregate_grain if self.aggregate_grain is not None else self.grain
        building = set(self._building)
        if not (building & grain.components):
            return grain
        out: list[str] = []
        seen: set[str] = set()

        def expand(addr: str) -> None:
            if addr in seen:
                return
            seen.add(addr)
            concept = self.environment.concepts.get(addr)
            if (
                addr in building
                and concept is not None
                and concept.purpose == Purpose.METRIC
                and concept.grain.components
            ):
                for component in concept.grain.component_order:
                    expand(component)
            elif addr not in out:
                out.append(addr)

        for component in grain.component_order:
            expand(component)
        return Grain(components=set(out), component_order=out)

    def _build_concept(self, base: Concept) -> BuildConcept:
        # Build-scoped merge collapse: build the canonical target instead of a
        # merged-away source. Every concept lookup (refs, grain components,
        # lineage args) funnels through here, so this collapses the source
        # everywhere.
        if (
            self.scoped_merge_map
            and base.address not in self._source_identity_addresses
            and base.address not in self.scoped_rowset_outer_sources
            and base.address not in self.scoped_rowset_outer_targets
        ):
            canonical = self.scoped_merge_map.get(base.address)
            if canonical is not None:
                base = self.environment.concepts[canonical]
        self._building.append(base.address)
        try:
            return self.__build_concept(base)
        except RecursionError as e:
            raise RecursionError(
                f"Recursion error building concept {base.address} with grain {base.grain} and lineage {base.lineage}. This is likely due to a circular reference."
            ) from e
        finally:
            self._building.pop()

    def __build_concept(self, base: Concept) -> BuildConcept:
        # TODO: if we are using parameters, wrap it in a new model and use that in rendering
        # this doesn't work for persisted addresses.
        # we need to early exit if we have it in local concepts, because in that case,
        # it is built with appropriate grain only in that dictionary

        if base.address in self.local_concepts:
            return self.local_concepts[base.address]
        # Fast path for grain-stable concepts: a concept with no lineage and
        # an explicit grain produces a BuildConcept fully determined by the
        # source Concept + env state (the factory's own grain is irrelevant —
        # get_select_grain_and_keys would just return base.grain). Cache by
        # address so every factory in this get_query_node call reuses one
        # entry — there are typically O(thousands) of these across O(hundreds)
        # of datasource factories.
        if base.lineage is None and base.grain.components:
            cached = self.canonical_build_cache.get(base.address)
            if cached is not None:
                self.local_concepts[base.address] = cached
                return cached
            new_grain = self._build_grain(base.grain)
            derivation = Concept.calculate_derivation(None, base.purpose)
            granularity = Concept.calculate_granularity(derivation, base.grain, None)
            if (
                base.granularity == Granularity.SINGLE_ROW
                and base.purpose == Purpose.PROPERTY
                and self._build_keys(base.keys)
                == {
                    f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
                }
            ):
                granularity = Granularity.SINGLE_ROW
            if base.address in self.environment.alias_origin_lookup:
                lookup_address = self.environment.concepts[base.address].address
                base_pseudonyms = {lookup_address}
            else:
                base_pseudonyms = {
                    x
                    for x in self.pseudonym_map.get(base.address, set())
                    if x != base.address
                }
            rval = BuildConcept(
                name=base.name,
                canonical_name=base.name,
                datatype=base.datatype,
                purpose=base.purpose,
                metadata=base.metadata,
                lineage=None,
                grain=new_grain,
                namespace=base.namespace,
                keys=self._build_keys(base.keys),
                modifiers=base.modifiers,
                pseudonyms=base_pseudonyms,
                derivation=derivation,
                granularity=granularity,
                build_is_aggregate=False,
            )
            self.local_concepts[base.address] = rval
            self.canonical_build_cache[base.address] = rval
            return rval
        resolution_grain = (
            self._abstract_resolution_grain() if base.is_aggregate else self.grain
        )
        new_lineage, final_grain, _ = base.get_select_grain_and_keys(
            resolution_grain, self.environment
        )
        stamped_lineage = self._apply_select_grouping_to_resolved(base, new_lineage)
        if stamped_lineage is not new_lineage:
            new_lineage = stamped_lineage
            # the concept's grain is the pass's key set, not the co-grained
            # resolution grain — e.g. a grouping() nested in a select dim would
            # otherwise keep that dim as its grain, a circular dependency
            final_grain = Grain(components={x.address for x in stamped_lineage.by})

        if new_lineage:
            build_lineage = self.build(new_lineage)
            if isinstance(build_lineage, BuildConcept):
                merge_concepts = self.scoped_merge_sources_by_target.get(base.address)
                if not merge_concepts:
                    return dc_replace(
                        build_lineage, name=base.name, namespace=base.namespace
                    )
                canonical_name = generate_concept_name(build_lineage, merge_concepts)
                return dc_replace(
                    build_lineage,
                    name=base.name,
                    namespace=base.namespace,
                    canonical_name=canonical_name,
                )
            elif isinstance(build_lineage, bool):
                build_lineage = BuildFunction(
                    operator=FunctionType.CONSTANT,
                    arguments=[build_lineage],
                    output_data_type=DataType.BOOL,
                    output_purpose=Purpose.CONSTANT,
                )

        else:
            build_lineage = None
        canonical_name = (
            generate_concept_name(
                build_lineage,
                self.scoped_merge_sources_by_target.get(base.address),
            )
            if build_lineage
            else base.name
        )
        cache_address = (
            f"{base.namespace}.{base.address}.{canonical_name}.{str(final_grain)}"
        )
        if self.select_grouping is not None:
            # build_cache persists across statements; a spec-stamped build of a
            # concept must not be served to (or from) a spec-less statement
            cache_address += f".{self._select_grouping_token()}"
        if cache_address in self.build_cache:
            return self.build_cache[cache_address]

        new_grain = self._build_grain(final_grain)

        derivation = Concept.calculate_derivation(build_lineage, base.purpose)

        granularity = Concept.calculate_granularity(
            derivation, final_grain, build_lineage
        )

        if (
            base.granularity == Granularity.SINGLE_ROW
            and base.purpose == Purpose.PROPERTY
            and base.keys
            == {
                f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
            }
        ):
            granularity = Granularity.SINGLE_ROW

        def calculate_is_aggregate(lineage):
            ltype = type(lineage)
            if ltype is BuildFunction:
                if lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                    return True
            if ltype is BuildAggregateWrapper:
                if lineage.function.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                    return True
            return False

        is_aggregate = calculate_is_aggregate(build_lineage) if build_lineage else False
        # if this is a pseudonym, we need to look up the base address
        if base.address in self.environment.alias_origin_lookup:
            lookup_address = self.environment.concepts[base.address].address
            # map only to the canonical concept, not to other merged concepts
            base_pseudonyms = {lookup_address}
        else:
            base_pseudonyms = {
                x
                for x in self.pseudonym_map.get(base.address, set())
                if x != base.address
            }

        rval = BuildConcept(
            name=base.name,
            canonical_name=canonical_name,
            datatype=base.datatype,
            purpose=base.purpose,
            metadata=base.metadata,
            lineage=build_lineage,
            grain=new_grain,
            namespace=base.namespace,
            keys=self._build_keys(base.keys),
            modifiers=base.modifiers,
            pseudonyms=base_pseudonyms,
            ## instantiated values
            derivation=derivation,
            granularity=granularity,
            build_is_aggregate=is_aggregate,
        )

        if base.address in self.local_concepts:
            return self.local_concepts[base.address]
        self.local_concepts[base.address] = rval
        # this is a global cache that can be reused across Factory instances
        self.build_cache[cache_address] = rval
        return rval

    @_build_dispatch.register
    def _(self, base: AggregateWrapper) -> BuildAggregateWrapper:
        return self._build_aggregate_wrapper(base)

    def _select_grouping_token(self) -> str:
        spec = self.select_grouping
        assert spec is not None
        by = ",".join(str(x.address) for x in spec.by)
        sets = "|".join(
            ",".join(str(x.address) for x in grouping_set)
            for grouping_set in spec.grouping_sets
        )
        return f"{spec.mode.value}:{by}:{sets}"

    def _apply_select_grouping_to_resolved(
        self, base: Concept, new_lineage: Any
    ) -> Any:
        """Stamp the select-level grouping spec onto a just-resolved bare
        aggregate. ``get_select_grain_and_keys`` fills an un-pinned aggregate's
        ``by`` from the resolution grain, so bare-ness is judged on the ORIGINAL
        lineage: a user-pinned ``sum(x) by a, b`` stays its own standard pass.
        Inferred-key specs (``by rollup ()``) keep the resolved grain as keys."""
        spec = self.select_grouping
        if spec is None or not isinstance(new_lineage, AggregateWrapper):
            return new_lineage
        if new_lineage.grouping != AggregateGroupingMode.STANDARD:
            return new_lineage
        original = (
            base.lineage.content
            if isinstance(base.lineage, FunctionCallWrapper)
            else base.lineage
        )
        if isinstance(original, AggregateWrapper) and original.by:
            return new_lineage
        return AggregateWrapper(
            function=new_lineage.function,
            by=list(spec.by) if spec.by else list(new_lineage.by),
            grouping=spec.mode,
            grouping_sets=[list(g) for g in spec.grouping_sets],
        )

    def _build_aggregate_wrapper(self, base: AggregateWrapper) -> BuildAggregateWrapper:
        spec = self.select_grouping
        grouping = base.grouping
        grouping_sets_source = base.grouping_sets
        if not base.by:
            if spec is not None and base.grouping == AggregateGroupingMode.STANDARD:
                # A bare aggregate materialized in this select's projection
                # scope computes in the select's single ROLLUP/CUBE/GROUPING
                # SETS pass; inferred-key specs (`by rollup ()`) group by the
                # resolution grain.
                grouping = spec.mode
                grouping_sets_source = spec.grouping_sets
                if spec.by:
                    by = self._build_over_items(list(spec.by))
                else:
                    agg_grain = (
                        self.aggregate_grain
                        if self.aggregate_grain is not None
                        else self.grain
                    )
                    by = [
                        self._build_concept(self.environment.concepts[c])
                        for c in agg_grain.component_order
                    ]
            else:
                agg_grain = (
                    self.aggregate_grain
                    if self.aggregate_grain is not None
                    else self.grain
                )
                by = [
                    self._build_concept(self.environment.concepts[c])
                    for c in agg_grain.component_order
                ]
        else:
            by = self._build_over_items(list(base.by))
        grouping_sets = [
            self._build_over_items(list(grouping_set))
            for grouping_set in grouping_sets_source
        ]
        if grouping == AggregateGroupingMode.STANDARD:
            by = sorted(by, key=lambda x: x.address)

        parent: BuildFunction = self._build_function(base.function)  # type: ignore
        return BuildAggregateWrapper(
            function=parent,
            by=by,
            grouping=grouping,
            grouping_sets=grouping_sets,
        )

    @_build_dispatch.register
    def _(self, base: ColumnAssignment) -> BuildColumnAssignment | None:
        return self._build_column_assignment(base)

    def _build_column_assignment(
        self, base: ColumnAssignment
    ) -> BuildColumnAssignment | None:
        address = base.concept.address
        concept: Concept | None
        if address in self.environment.alias_origin_lookup:
            concept = self.environment.alias_origin_lookup[address]
        else:
            concept = self.environment.concepts.get(address)
        # Skip columns referencing undefined concepts (e.g., at max import depth)
        if concept is None:
            return None
        fetched = self._build_concept(concept).with_grain(self.build_grain)

        # A concept declared nullable (`property x type?`) implicitly makes every
        # binding to it nullable too — column-level NULLABLE is only needed to
        # mark a non-nullable concept as having nulls in a specific source.
        modifiers = set(base.modifiers)
        if Modifier.NULLABLE in concept.modifiers:
            modifiers.add(Modifier.NULLABLE)
        # A LEFT in-query join merges this source key into its target only
        # partially — mark the binding partial so it drives a LEFT-OUTER join.
        if address in self.scoped_partial_sources:
            modifiers.add(Modifier.PARTIAL)

        return BuildColumnAssignment(
            alias=(
                self._build_function(base.alias)
                if isinstance(base.alias, Function)
                else base.alias
            ),
            concept=fetched,
            modifiers=modifiers,
            # a scoped join/merge substituted this binding onto the relation's
            # canonical address — thread the authored origin forward so
            # same-address join-pair rulings can tell the sides apart.
            origin_address=(
                address
                if address in self.scoped_merge_map and fetched.address != address
                else None
            ),
        )

    @_build_dispatch.register
    def _(self, base: OrderBy) -> BuildOrderBy:
        return self._build_order_by(base)

    def _build_order_by(self, base: OrderBy) -> BuildOrderBy:
        return BuildOrderBy(items=[self._build_order_item(x) for x in base.items])

    @_build_dispatch.register
    def _(self, base: FunctionCallWrapper) -> BuildExpr:
        return self._build_function_call_wrapper(base)

    def _build_function_call_wrapper(self, base: FunctionCallWrapper) -> BuildExpr:
        # function calls are kept around purely for the parse tree
        # so discard at the build point
        validation = requires_concept_nesting(base.content)
        if validation:
            _, built = self.instantiate_concept(validation)
            return built
        return self.build(base.content)

    @_build_dispatch.register
    def _(self, base: OrderItem) -> BuildOrderItem:
        return self._build_order_item(base)

    def _build_order_item(self, base: OrderItem) -> BuildOrderItem:
        bexpr: Any
        validation = requires_concept_nesting(base.expr)
        if validation:
            bexpr, _ = self.instantiate_concept(validation)
        else:
            bexpr = base.expr
        return BuildOrderItem(
            expr=(self.build(bexpr)),
            order=base.order,
        )

    @_build_dispatch.register
    def _(self, base: WhereClause) -> BuildWhereClause:
        return self._build_where_clause(base)

    def _build_where_clause(self, base: WhereClause) -> BuildWhereClause:
        from trilogy.core.processing.condition_utility import flatten_conditions

        conditional = self.build(base.conditional)
        if isinstance(conditional, bool):
            return BuildWhereClause(conditional=_constant_bool_comparison(conditional))
        if isinstance(
            conditional,
            BoolExpr,
        ):
            conditional = flatten_conditions(conditional)
        return BuildWhereClause(conditional=conditional)

    @_build_dispatch.register
    def _(self, base: HavingClause) -> BuildHavingClause:
        return self._build_having_clause(base)

    def _build_having_clause(self, base: HavingClause) -> BuildHavingClause:
        conditional = self.build(base.conditional)
        if isinstance(conditional, bool):
            return BuildHavingClause(conditional=_constant_bool_comparison(conditional))
        return BuildHavingClause(conditional=conditional)

    @_build_dispatch.register
    def _(self, base: NumberingWindowItem) -> BuildNumberingWindowItem:
        return self._build_numbering_window_item(base)

    @_build_dispatch.register
    def _(self, base: NavigationWindowItem) -> BuildNavigationWindowItem:
        return self._build_navigation_window_item(base)

    def _build_over_items(self, over: list) -> list[BuildConcept]:
        # `over` may contain arbitrary expressions from `partition by expr, …`;
        # materialize anything that isn't already a reference into a
        # factory-local concept so we can treat partitioning columns uniformly.
        out: list[BuildConcept] = []
        for x in over:
            if isinstance(x, ConceptRef):
                out.append(self._build_concept_ref(x))
            elif isinstance(x, Concept):
                out.append(self._build_concept(x))
            else:
                _, built = self.instantiate_concept(x)
                out.append(built)
        return out

    def _window_order_by_items(self, order_by: list, anchor: Any) -> list:
        """Resolve bare (no ``by``) aggregates in a window's ORDER BY.

        Under the select's grouping spec they compute in that single
        ROLLUP/CUBE/GROUPING SETS pass (inferred-key specs stay bare and pick up
        the resolved select grain plus the mode in ``__build_concept``);
        otherwise the window's anchor concept defines the implicit grain.
        Always copy — the OrderItem may belong to a shared authored definition,
        and an in-place ``by`` leaks across statements."""
        spec = self.select_grouping
        out = []
        for x in order_by:
            expr = x.expr
            if (
                isinstance(expr, AggregateWrapper)
                and not expr.by
                and expr.grouping == AggregateGroupingMode.STANDARD
            ):
                if spec is not None:
                    if spec.by:
                        expr = AggregateWrapper(
                            function=expr.function,
                            by=list(spec.by),
                            grouping=spec.mode,
                            grouping_sets=[list(g) for g in spec.grouping_sets],
                        )
                elif anchor is not None:
                    expr = AggregateWrapper(function=expr.function, by=[anchor])
            if expr is not x.expr:
                x = OrderItem(expr=expr, order=x.order)
            out.append(x)
        return out

    def _build_numbering_window_item(
        self, base: NumberingWindowItem
    ) -> BuildNumberingWindowItem:
        # An AggregateWrapper with empty `by` inside the order_by needs an
        # implicit grain — the rank's argument concepts define the row.
        anchor = base.arguments[0] if base.arguments else None
        final_by = self._window_order_by_items(base.order_by, anchor)
        return BuildNumberingWindowItem(
            type=base.type,
            arguments=[self._build_concept_ref(x) for x in base.arguments],
            order_by=[self.build(x) for x in final_by],
            over=self._build_over_items(list(base.over)),
        )

    def _build_navigation_window_item(
        self, base: NavigationWindowItem
    ) -> BuildNavigationWindowItem:
        content: Concept | FuncArgs = base.content
        validation = requires_concept_nesting(base.content)
        if validation:
            content, _ = self.instantiate_concept(validation)
        anchor = content if isinstance(content, (ConceptRef, Concept)) else None
        final_by = self._window_order_by_items(base.order_by, anchor)
        return BuildNavigationWindowItem(
            type=base.type,
            content=self.build(content),
            order_by=[self.build(x) for x in final_by],
            over=self._build_over_items(list(base.over)),
            offset=base.offset,
        )

    @_build_dispatch.register
    def _(self, base: Conditional) -> BuildConditional:
        return self._build_conditional(base)

    def _build_conditional(self, base: Conditional) -> BuildConditional:
        return BuildConditional(
            left=self.handle_constant(self.build(base.left)),
            right=self.handle_constant(self.build(base.right)),
            operator=base.operator,
        )

    @_build_dispatch.register
    def _(self, base: SubselectComparison) -> BuildSubselectComparison:
        return self._build_subselect_comparison(base)

    def _build_subselect_comparison(
        self, base: SubselectComparison
    ) -> BuildSubselectComparison:
        right: Any = base.right
        # this has specialized logic - include all Functions, EXCEPT a ROW_TUPLE
        # (composite-membership row constructor): it must stay a function so its
        # components are sourced individually, not collapsed into one array concept
        if isinstance(
            base.right, (AggregateWrapper, WindowItem, FilterItem, Function)
        ) and not (
            isinstance(base.right, Function)
            and base.right.operator == FunctionType.ROW_TUPLE
        ):
            right_c, _ = self.instantiate_concept(base.right)
            right = right_c
        left_built = self.handle_constant(self.build(base.left))
        right_built = self.handle_constant(self.build(right))
        # When both sides of `X in Y` resolve to the same concept the comparison
        # has collapsed to a self-comparison — usually because a `merge` made the
        # two keys one concept (or a literal `X in X`). It can't filter one model
        # by another, and compiling it emits a self-referential existence
        # subselect against a CTE not in scope (an opaque DB error downstream).
        # Raise a clear error instead of silently optimizing it away.
        if (
            isinstance(left_built, BuildConcept)
            and isinstance(right_built, BuildConcept)
            and left_built.address == right_built.address
        ):
            op = base.operator.value
            merged = sorted(left_built.pseudonyms)
            if merged:
                cause = (
                    f" — {left_built.address} and {', '.join(merged)} are merged"
                    " into one concept"
                )
            else:
                cause = ""
            raise InvalidSyntaxException(
                f"`{left_built.address} {op} {right_built.address}` compares a"
                f" concept to itself{cause}, so it cannot filter one model by"
                " another. To filter by values present in a related model, compare"
                " two distinct (unmerged) concepts (reference the other model's"
                " key directly, or drop the `merge`)."
            )
        return BuildSubselectComparison(
            left=left_built,
            right=right_built,
            operator=base.operator,
        )

    @_build_dispatch.register
    def _(self, base: Between) -> "BuildBetween | bool":
        return self._build_between(base)

    def _build_between(self, base: Between) -> "BuildBetween | bool":
        def _prep(value):
            validation = requires_concept_nesting(value)
            if validation:
                vc, _ = self.instantiate_concept(validation)
                value = vc
            return self.handle_constant(self.build(value))

        left = _prep(base.left)
        low = _prep(base.low)
        high = _prep(base.high)
        if is_constant(left) and is_constant(low) and is_constant(high):
            lval = materialize_constant(left)
            loval = materialize_constant(low)
            hival = materialize_constant(high)
            return loval <= lval <= hival
        return BuildBetween(left=left, low=low, high=high)

    @_build_dispatch.register
    def _(self, base: Comparison) -> BuildComparison | bool:
        return self._build_comparison(base)

    def _coalescing_presence_probe(self, ref: ConceptRef) -> Concept | None:
        """Per-side probe for a null test on a coalescing join key member.

        A `full`/`union` key group renders as the mandatory coalesce of every
        member; a `subset` join renders row-preserving with its subset SOURCE
        NULL-padded on anchor-only rows but the projected key coalesced. Either
        way `member is [not] null` would read the fused/coalesced column and
        never observe the member's own side being absent — full/union presence
        counts collapse to (0, 0, N) (TPC-DS q97), and a subset-side null test
        silently no-ops instead of expressing intersection (TPC-DS q59). The
        null test asks a per-ROW question ("did this side match?"), not a
        domain question, so rewrite its operand to a virt passthrough of the
        member, materialized on the member's own side BEFORE the merge — its
        rowset body for ROWSET members (gen_rowset_node), a scan pinned to its
        own datasource for ROOT members (gen_presence_probe_node); it rides
        through the join un-fused and is NULL exactly where the member's side
        is absent. Projecting the member is untouched: that remains the
        coalesced group axis. The superset/anchor side of a subset join is
        trusted (the declaration says every subset value matches it), so it is
        not eligible — a null test on it is a genuine no-op; a lying subset
        declaration is an author error (docs/subset_union_join_design.md)."""
        address = ref.address
        eligible = (
            self.domain_graph.coalescing_relation_members()
            | self.domain_graph.subset_sources()
        )
        if address not in eligible:
            return None
        member = self.environment.concepts.get(address)
        if member is None or member.derivation not in (
            Derivation.ROWSET,
            Derivation.ROOT,
        ):
            return None
        from trilogy.parsing.common import arbitrary_to_concept

        name = f"{PRESENCE_PROBE_PREFIX}{string_to_hash(address)}"
        if name in self.local_non_build_concepts:
            return self.local_non_build_concepts[name]
        # single-arg COALESCE: value-identity but structurally opaque — an
        # ALIAS would mint a structural ≡ edge and collapse the probe back
        # onto the member (defeating it)
        probe_fn = Function(
            operator=FunctionType.COALESCE,
            output_datatype=member.datatype,
            output_purpose=Purpose.PROPERTY,
            arguments=[ref],
        )
        new = arbitrary_to_concept(probe_fn, environment=self.environment, name=name)
        built = self._build_concept(new)
        # key by ADDRESS: local_concepts propagates through the statement's
        # build products (materialize_for_select, sub-select rebuilds), and
        # every consumer resolves by address — a bare-name key strands the
        # probe when a fresh factory rebuilds a lineage embedding its ref
        # (gcat multiselect env-cleanup shape)
        self.local_concepts[new.address] = built
        self.local_non_build_concepts[name] = new
        return new

    def _build_comparison(self, base: Comparison) -> BuildComparison | bool:
        if base.operator in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
            probe: Concept | None = None
            if base.right == MagicConstants.NULL and isinstance(base.left, ConceptRef):
                probe = self._coalescing_presence_probe(base.left)
                if probe is not None:
                    base = dc_replace(base, left=probe.reference)
            elif base.left == MagicConstants.NULL and isinstance(
                base.right, ConceptRef
            ):
                probe = self._coalescing_presence_probe(base.right)
                if probe is not None:
                    base = dc_replace(base, right=probe.reference)
        left = base.left
        validation = requires_concept_nesting(base.left)
        if validation:
            left_c, _ = self.instantiate_concept(validation)
            left = left_c  # type: ignore
        right = base.right
        validation = requires_concept_nesting(base.right)
        if validation:
            right_c, _ = self.instantiate_concept(validation)
            right = right_c  # type: ignore
        left = self.handle_constant(self.build(left))
        right = self.handle_constant(self.build(right))
        if is_constant(left) and is_constant(right):
            lval = materialize_constant(left)
            rval = materialize_constant(right)
            if base.operator == ComparisonOperator.EQ:
                return lval == rval
            elif base.operator == ComparisonOperator.NE:
                return lval != rval
            elif base.operator == ComparisonOperator.LT:
                return lval < rval
            elif base.operator == ComparisonOperator.LTE:
                return lval <= rval
            elif base.operator == ComparisonOperator.GT:
                return lval > rval
            elif base.operator == ComparisonOperator.GTE:
                return lval >= rval
            elif base.operator == ComparisonOperator.IS:
                return lval is rval
            elif base.operator == ComparisonOperator.IS_NOT:
                return lval is not rval

        return BuildComparison(
            left=left,
            right=right,
            operator=base.operator,
        )

    @_build_dispatch.register
    def _(self, base: AlignItem) -> BuildAlignItem:
        return self._build_align_item(base)

    def _build_align_item(self, base: AlignItem) -> BuildAlignItem:
        return BuildAlignItem(
            alias=base.alias,
            concepts=[self._build_concept_ref(x) for x in base.concepts],
            namespace=base.namespace,
            hidden=base.hidden,
        )

    @_build_dispatch.register
    def _(self, base: AlignClause) -> BuildAlignClause:
        return self._build_align_clause(base)

    def _build_align_clause(self, base: AlignClause) -> BuildAlignClause:
        return BuildAlignClause(items=[self._build_align_item(x) for x in base.items])

    @_build_dispatch.register
    def _(self, base: DeriveItem) -> BuildDeriveItem:
        return self._build_derive_item(base)

    def _build_derive_item(self, base: DeriveItem) -> BuildDeriveItem:
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildDeriveItem(
            expr=self.build(expr),
            name=base.name,
            namespace=base.namespace,
        )

    @_build_dispatch.register
    def _(self, base: DeriveClause) -> BuildDeriveClause:
        return self._build_derive_clause(base)

    def _build_derive_clause(self, base: DeriveClause) -> BuildDeriveClause:
        return BuildDeriveClause(items=[self.build(x) for x in base.items])

    @_build_dispatch.register
    def _(self, base: RowsetItem) -> BuildRowsetItem:
        return self._build_rowset_item(base)

    def _build_rowset_item(self, base: RowsetItem) -> BuildRowsetItem:
        factory = Factory(
            environment=self.environment,
            local_concepts={},
            grain=base.rowset.select.grain,
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
        )
        return BuildRowsetItem(
            content=factory._build_concept_ref(base.content),
            rowset=factory._build_rowset_lineage(base.rowset),
        )

    @_build_dispatch.register
    def _(self, base: SubqueryItem) -> BuildConcept:
        # A scalar subquery IS its single rowset output; drop the authoring
        # `select` payload and build the bare concept (grain-less → cross-join).
        return self.build(base.content)

    @_build_dispatch.register
    def _(self, base: SubselectItem) -> BuildSubselectItem:
        return self._build_subselect_item(base)

    def _build_subselect_item(self, base: SubselectItem) -> BuildSubselectItem:
        return BuildSubselectItem(
            content=self.build(base.content),
            where=self.build(base.where) if base.where else None,
            order_by=[self.build(x) for x in base.order_by],
            limit=base.limit,
            outer_arguments=[self.build(x) for x in base.outer_arguments],
        )

    @_build_dispatch.register
    def _(self, base: RowsetLineage) -> BuildRowsetLineage:
        return self._build_rowset_lineage(base)

    def _build_rowset_lineage(self, base: RowsetLineage) -> BuildRowsetLineage:
        out = BuildRowsetLineage(
            name=base.name,
            derived_concepts=[x.address for x in base.derived_concepts],
            select=base.select,
        )
        return out

    @_build_dispatch.register
    def _(self, base: Grain) -> BuildGrain:
        return self._build_grain(base)

    def _build_grain(self, base: Grain) -> BuildGrain:
        # 99.9% of _build_grain calls have no where_clause; the normalized
        # output is a pure function of (base.components, environment). Cache
        # by frozenset(components) across factories sharing build_cache.
        if not base.where_clause:
            cache_key: tuple = (frozenset(base.components), None)
            cached = self.grain_build_cache.get(cache_key)
            if cached is not None:
                return cached
            rval = BuildGrain(
                components=self._normalize_grain_components(base.components),
                where_clause=None,
            )
            self.grain_build_cache[cache_key] = rval
            return rval
        factory = Factory(
            environment=self.environment,
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
        )
        where = factory._build_where_clause(base.where_clause)
        return BuildGrain(
            components=self._normalize_grain_components(base.components),
            where_clause=where,
        )

    def _normalize_grain_components(self, components) -> set[str]:
        # Collapse any merged-away source grain key to its canonical target
        # (the build-time equivalent of a global merge rewriting the grain), so
        # dependents of a scoped-merge source group on the single canonical key.
        normalized: set[str] = set()
        env_concepts = self.environment.concepts
        for c in components:
            if c in self.scoped_merge_map:
                normalized.add(self.scoped_merge_map[c])
            elif c in env_concepts:
                normalized.add(env_concepts[c].address)
            else:
                normalized.add(c)
        return normalized

    @_build_dispatch.register
    def _(self, base: TupleWrapper) -> TupleWrapper:
        return self._build_tuple_wrapper(base)

    def _build_tuple_wrapper(self, base: TupleWrapper) -> TupleWrapper:
        return TupleWrapper(val=[self.build(x) for x in base.val], type=base.type)

    @_build_dispatch.register
    def _(self, base: ListWrapper) -> ListWrapper:
        return self._build_list_wrapper(base)

    def _build_list_wrapper(self, base: ListWrapper) -> ListWrapper:
        return ListWrapper([self.build(x) for x in base], type=base.type)

    @_build_dispatch.register
    def _(self, base: FilterItem) -> BuildFilterItem:
        return self._build_filter_item(base)

    def _build_filter_item(self, base: FilterItem) -> BuildFilterItem:
        if isinstance(
            base.content, (Function, AggregateWrapper, WindowItem, FilterItem)
        ):
            _, built = self.instantiate_concept(base.content)
        else:
            built = self.build(base.content)
        return BuildFilterItem(
            content=built, where=self._build_filter_where(base.where, built)
        )

    def _build_filter_where(
        self, where: WhereClause, content: "BuildExpr"
    ) -> BuildWhereClause:
        """Build a filter's `? <condition>` so a bare (no `by`) aggregate in it
        co-grains to the FILTERED CONTENT's grain — the rows being filtered —
        rather than inheriting the outer consuming grain. Mirrors how a SELECT's
        WHERE co-grains its aggregates to the select grain (HAVING semantics).

        Without this, `auto f <- p ? (count(x) > 4)` consumed in another model
        groups the count by the consumer's key (e.g. a foreign `catalog.item.id`
        for a `store`-derived count), producing a disconnected/unresolvable query;
        consumed at its own grain it groups by `f` itself, recursing."""
        # A non-concept content (e.g. a literal) has no grain to co-grain to.
        if not isinstance(content, BuildConcept):
            return self.build(where)
        content_grain = Grain(components=set(content.grain.components))
        if self.aggregate_grain == content_grain:
            return self.build(where)
        saved = self.aggregate_grain
        self.aggregate_grain = content_grain
        try:
            return self.build(where)
        finally:
            self.aggregate_grain = saved

    @_build_dispatch.register
    def _(self, base: Parenthetical) -> BuildParenthetical:
        return self._build_parenthetical(base)

    def _build_parenthetical(self, base: Parenthetical) -> BuildParenthetical:
        validate = requires_concept_nesting(base.content)
        if validate:
            content, _ = self.instantiate_concept(validate)
            return BuildParenthetical(content=self.build(content))
        else:
            return BuildParenthetical(content=self.build(base.content))

    @_build_dispatch.register
    def _(self, base: SelectLineage) -> BuildSelectLineage:
        return self._build_select_lineage(base)

    def _build_select_lineage(self, base: SelectLineage) -> BuildSelectLineage:
        from trilogy.core.models.build import (
            BuildSelectLineage,
            Factory,
        )

        materialized: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=materialized,
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
            scoped_joins=self.scoped_joins,
            select_grouping=base.grouping,
        )
        for k, v in base.local_concepts.items():
            materialized[k] = factory.build(v)
        where_factory = Factory(
            grain=Grain(),
            # Bare aggregates in WHERE co-grain to the SELECT grain (like HAVING)
            # instead of resolving to a global scalar; plain row predicates still
            # build at row grain (`grain=Grain()`).
            aggregate_grain=base.grain,
            environment=self.environment,
            local_concepts={},
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
            scoped_joins=self.scoped_joins,
        )
        where_clause = (
            where_factory.build(base.where_clause) if base.where_clause else None
        )
        # if the where clause derives new concepts
        # we need to ensure these are accessible from the general factory
        # post resolution
        for bk, bv in where_factory.local_concepts.items():
            # but do not override any local cahced grains
            if bk in materialized:
                continue
            materialized[bk] = bv
        final: List[BuildConcept] = []
        for original in base.selection:
            new = original
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if new.address in materialized:
                built = materialized[new.address]
            else:
                # Sometimes cached values here don't have the latest info
                # but we can't just use environment, as it might not have the right grain.
                built = factory.build(new)
                materialized[new.address] = built
            final.append(built)
        return BuildSelectLineage(
            selection=final,
            hidden_components=base.hidden_components,
            order_by=(factory.build(base.order_by) if base.order_by else None),
            limit=base.limit,
            meta=base.meta,
            local_concepts=materialized,
            grain=self.build(base.grain),
            having_clause=(
                factory.build(base.having_clause) if base.having_clause else None
            ),
            # this uses a different grain factory
            where_clause=where_clause,
        )

    @_build_dispatch.register
    def _(self, base: MultiSelectLineage) -> BuildMultiSelectLineage:
        return self._build_multi_select_lineage(base)

    @_build_dispatch.register
    def _(self, base: UnionSelectLineage) -> BuildMultiSelectLineage:
        return self._build_multi_select_lineage(
            base,
            build_cls=BuildUnionSelectLineage,
            derivation=Derivation.TVF_UNION,
            outputs_only=True,
            extra_lineage_kwargs={"operator": base.operator},
        )

    def _build_multi_select_lineage(
        self,
        base: MultiSelectLineage,
        build_cls: type[BuildMultiSelectLineage] = BuildMultiSelectLineage,
        derivation: Derivation = Derivation.MULTISELECT,
        outputs_only: bool = False,
        extra_lineage_kwargs: dict[str, Any] | None = None,
    ) -> BuildMultiSelectLineage:
        local_build_cache: dict[str, BuildConcept] = {}

        parents: list[BuildSelectLineage] = [self.build(x) for x in base.selects]
        base_local = parents[0].local_concepts

        for select in parents[1:]:
            for k, v in select.local_concepts.items():
                base_local[k] = v

        # this requires custom handling to avoid circular dependencies
        final_grain = self.build(base.grain)
        derived_base = []
        for k in base.derived_concepts:
            if k in local_build_cache:
                x = local_build_cache[k]
            else:
                base_concept = self.environment.concepts[k]
                x = BuildConcept(
                    name=base_concept.name,
                    canonical_name=base_concept.name,
                    datatype=base_concept.datatype,
                    purpose=base_concept.purpose,
                    build_is_aggregate=False,
                    derivation=derivation,
                    lineage=None,
                    grain=final_grain,
                    keys=self._build_keys(base_concept.keys),
                    namespace=base_concept.namespace,
                )
                local_build_cache[k] = x
            derived_base.append(x)
        all_input: list[BuildConcept] = []
        for parent in parents:
            all_input += parent.output_components
        all_output: list[BuildConcept] = derived_base + all_input
        # Inner-select hidden_components prune the output here. The align
        # identity stays in for the join resolver and is dropped at render
        # time via `hidden_components`.
        align_hidden: set[str] = {
            item.aligned_concept for item in base.align.items if item.hidden
        }
        select_hidden = base.hidden_components - align_hidden
        if outputs_only:
            # A union TVF exposes only its bound output columns; the arms'
            # internal (per-arm mangled) columns must not surface.
            final: list[BuildConcept] = list(derived_base)
        else:
            final = [x for x in all_output if x.address not in select_hidden]
        # Only global environment MERGEs (not the enclosing scope's query-level
        # JOIN clauses) apply when building the align/derive/order/having and
        # where clauses: a parent scope's JOIN is local to that select, but a
        # MERGE canonicalizes a key everywhere — including the arm selects, so
        # the align identities must collapse the same way for find_source.
        env_scoped_joins = [
            j for j in self.scoped_joins if j in self.environment.merges
        ]
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=local_build_cache,
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
            scoped_joins=env_scoped_joins,
        )
        where_factory = Factory(
            environment=self.environment,
            pseudonym_map=self.pseudonym_map,
            build_cache=self.build_cache,
            grain_build_cache=self.grain_build_cache,
            canonical_build_cache=self.canonical_build_cache,
            scoped_joins=env_scoped_joins,
        )
        lineage = build_cls(
            # we don't build selects here; they'll be built automatically in query discovery
            selects=base.selects,
            grain=final_grain,
            align=factory.build(base.align),
            derive=factory.build(base.derive) if base.derive else None,
            # self.align.with_select_context(
            #     local_build_cache, self.grain, environment
            # ),
            namespace=base.namespace,
            hidden_components=base.hidden_components,
            order_by=factory.build(base.order_by) if base.order_by else None,
            limit=base.limit,
            where_clause=(
                where_factory.build(base.where_clause) if base.where_clause else None
            ),
            having_clause=(
                factory.build(base.having_clause) if base.having_clause else None
            ),
            local_concepts=base_local,
            build_output_components=final,
            build_concept_arguments=all_input,
            **(extra_lineage_kwargs or {}),
        )
        for k in base.derived_concepts:
            local_build_cache[k].lineage = lineage
        return lineage

    @_build_dispatch.register
    def _(self, base: Environment):
        return self._build_environment(base)

    def _build_environment(self, base: Environment):
        from trilogy.core.models.build_environment import BuildEnvironment

        new = BuildEnvironment(
            namespace=base.namespace,
            cte_name_map=base.cte_name_map,
            scoped_partial_derived=(
                (self.scoped_merge_sources & self.scoped_partial_sources)
                - self.scoped_outer_identity_sources
            ),
            scoped_join_key_groups=self._scoped_join_key_groups(),
            domain_graph=assemble_full_graph(base, self.domain_graph),
        )

        for k, v in base.concepts.all_items():
            v_build = self._build_concept(v)
            new.concepts[k] = v_build
            new.canonical_concepts[v_build.canonical_address] = v_build
        for (
            k,
            d,
        ) in base.datasources.items():
            if d.status != DatasourceState.PUBLISHED:
                continue
            new.datasources[k] = self._build_datasource(d)
        for k, a in base.alias_origin_lookup.items():
            a_build = self._build_concept(a)
            new.alias_origin_lookup[k] = a_build
            new.canonical_concepts[a_build.canonical_address] = a_build
        # Build-scoped joins collapse each source to its canonical target in
        # new.concepts. Populate alias_origin_lookup for joined sources so
        # references / output aliases to a merged-away source still resolve. The
        # entry is the source built as ITSELF (its own identity), pointed at the
        # canonical target as a pseudonym.
        for source_addr, canonical_addr in self.scoped_merge_map.items():
            # Build the source as ITSELF. Legacy direct merge_concept calls may
            # have rewritten concepts[source] = target, so prefer the original
            # source from alias_origin_lookup when present.
            src = base.alias_origin_lookup.get(source_addr) or base.concepts.data.get(
                source_addr
            )
            if src is None:
                continue
            # Build the collapsed-away source as its OWN identity (its lineage +
            # address), not the collapsed target, so the joined key stays
            # sourceable from the collapsed side. The main build loop already
            # cached source_addr -> the collapsed target in local_concepts and
            # __build_concept would early-exit on it; evict it across this build
            # and restore the collapse mapping after.
            if source_addr in self.scoped_merge_sources:
                collapsed = self.local_concepts.pop(source_addr, None)
                previous_identity_addresses = self._source_identity_addresses
                self._source_identity_addresses = (
                    previous_identity_addresses | self.scoped_merge_sources
                )
                try:
                    alias_build = self.__build_concept(src)
                finally:
                    self._source_identity_addresses = previous_identity_addresses
                if collapsed is not None:
                    self.local_concepts[source_addr] = collapsed
            else:
                alias_build = self.__build_concept(src)
            if canonical_addr not in alias_build.pseudonyms:
                alias_build = dc_replace(
                    alias_build,
                    pseudonyms={canonical_addr, *alias_build.pseudonyms},
                )
            new.alias_origin_lookup[source_addr] = alias_build
            new.canonical_concepts[alias_build.canonical_address] = alias_build
        # add in anything that was built as a side-effect
        for bk, bv in self.local_concepts.items():
            if bk not in new.concepts:
                new.concepts[bk] = bv
                new.canonical_concepts[bv.canonical_address] = bv
        new.gen_concept_list_caches()
        return new

    @_build_dispatch.register
    def _(self, base: TraitDataType):
        return self._build_trait_data_type(base)

    def _build_trait_data_type(self, base: TraitDataType):
        return base

    @_build_dispatch.register
    def _(self, base: EnumType):
        return self._build_enum_data_type(base)

    def _build_enum_data_type(self, base: EnumType):
        return base

    @_build_dispatch.register
    def _(self, base: ArrayType):
        return self._build_array_type(base)

    def _build_array_type(self, base: ArrayType):
        return base

    @_build_dispatch.register
    def _(self, base: StructType):
        return self._build_struct_type(base)

    def _build_struct_type(self, base: StructType):
        return base

    @_build_dispatch.register
    def _(self, base: MapType):
        return self._build_map_type(base)

    def _build_map_type(self, base: MapType):
        return base

    @_build_dispatch.register
    def _(self, base: ArgBinding):
        return self._build_arg_binding(base)

    def _build_arg_binding(self, base: ArgBinding):
        return base

    @_build_dispatch.register
    def _(self, base: Ordering):
        return self._build_ordering(base)

    def _build_ordering(self, base: Ordering):
        return base

    @_build_dispatch.register
    def _(self, base: Datasource):
        return self._build_datasource(base)

    def _build_datasource(self, base: Datasource):
        from trilogy.constants import CONFIG

        use_cache = CONFIG.generation.datasource_build_cache
        ds_key = f"{base.namespace}.{base.name}"
        if use_cache:
            cached_ds = self.datasource_build_cache.get(ds_key)
            if cached_ds is not None:
                return cached_ds
        local_cache: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=local_cache,
            pseudonym_map=self.pseudonym_map,
            # build_cache = self.build_cache,
            build_cache=(
                self.build_cache if CONFIG.generation.datasource_build_cache else None
            ),
            grain_build_cache=(
                self.grain_build_cache
                if CONFIG.generation.datasource_build_cache
                else None
            ),
            canonical_build_cache=self.canonical_build_cache,
            # collapse merged-away source keys (and mark partial bindings) when
            # building this datasource's columns/grain.
            scoped_joins=self.scoped_joins,
        )
        # Filter out columns with undefined concepts (e.g., at max import depth)
        columns = [
            col
            for c in base.columns
            if (col := factory._build_column_assignment(c)) is not None
        ]
        rval = BuildDatasource(
            name=base.name,
            columns=columns,
            address=base.address,
            grain=factory._build_grain(base.grain),
            namespace=base.namespace,
            metadata=base.metadata,
            where=(factory.build(base.where) if base.where else None),
            non_partial_for=(
                factory.build(base.non_partial_for) if base.non_partial_for else None
            ),
            column_level_partial_addresses=set(base.column_level_partial_addresses),
        )
        if use_cache:
            self.datasource_build_cache[ds_key] = rval
        return rval

    def handle_constant(self, base):
        if (
            isinstance(base, BuildConcept)
            and isinstance(base.lineage, BuildFunction)
            and base.lineage.operator == FunctionType.CONSTANT
        ):
            if base.lineage.arguments[0] is bool:
                return base.lineage.arguments[0]
            return BuildParamaterizedConceptReference(concept=base)
        elif isinstance(base, ConceptRef):
            return self.handle_constant(self.build(base))
        return base


def _canonical_str_for_hash(arg: Any) -> str:
    """Canonical string repr used for canonical_name hashing.

    Substitutes BuildConcept references with `canonical_address_grain` so that
    two concepts sharing the same canonical lineage (e.g. `year(x_date)` and
    `x_date.year`) hash to the same name, and any virtual concept derived from
    them (e.g. `count(id) by x_date.year` vs `count(id) by year_via_func`)
    inherits that equivalence.
    """
    if isinstance(arg, BuildConcept):
        return arg.canonical_address_grain
    if isinstance(arg, BuildAggregateWrapper):
        if arg.by:
            grain_str = (
                "["
                + ", ".join(sorted(_canonical_str_for_hash(c) for c in arg.by))
                + "]"
            )
        else:
            grain_str = "abstract"
        return f"{_canonical_str_for_hash(arg.function)}<{grain_str}>"
    if isinstance(arg, BuildFunction):
        rendered = ",".join(_canonical_str_for_hash(a) for a in arg.arguments)
        return f"{arg.operator.value}({rendered})"
    if isinstance(arg, BuildNumberingWindowItem):
        order_str = ",".join(_canonical_str_for_hash(o) for o in arg.order_by)
        over_str = ",".join(_canonical_str_for_hash(o) for o in arg.over)
        args_str = ",".join(_canonical_str_for_hash(a) for a in arg.arguments)
        return f"{arg.type}([{args_str}], [{over_str}], [{order_str}])"
    if isinstance(arg, BuildNavigationWindowItem):
        order_str = ",".join(_canonical_str_for_hash(o) for o in arg.order_by)
        over_str = ",".join(_canonical_str_for_hash(o) for o in arg.over)
        return (
            f"{arg.type}({_canonical_str_for_hash(arg.content)} {arg.offset},"
            f" [{over_str}], [{order_str}])"
        )
    if isinstance(arg, BuildOrderItem):
        return f"{_canonical_str_for_hash(arg.expr)} {arg.order}"
    if isinstance(arg, BuildFilterItem):
        return (
            f"<Filter: {_canonical_str_for_hash(arg.content)} where "
            f"{_canonical_str_for_hash(arg.where)}>"
        )
    if isinstance(arg, BuildWhereClause):
        return _canonical_str_for_hash(arg.conditional)
    if isinstance(arg, BuildParenthetical):
        return f"({_canonical_str_for_hash(arg.content)})"
    if isinstance(arg, BuildSubselectComparison):
        return (
            f"{_canonical_str_for_hash(arg.left)} {arg.operator.value}"
            f" {_canonical_str_for_hash(arg.right)}"
        )
    if isinstance(arg, BuildComparison):
        return (
            f"{_canonical_str_for_hash(arg.left)} {arg.operator.value}"
            f" {_canonical_str_for_hash(arg.right)}"
        )
    if isinstance(arg, BuildConditional):
        return (
            f"{_canonical_str_for_hash(arg.left)} {arg.operator.value}"
            f" {_canonical_str_for_hash(arg.right)}"
        )
    if isinstance(arg, BuildCaseWhen):
        return (
            f"WHEN {_canonical_str_for_hash(arg.comparison)} THEN"
            f" {_canonical_str_for_hash(arg.expr)}"
        )
    if isinstance(arg, BuildCaseSimpleWhen):
        return (
            f"WHEN {_canonical_str_for_hash(arg.value_expr)} THEN"
            f" {_canonical_str_for_hash(arg.expr)}"
        )
    if isinstance(arg, BuildCaseElse):
        return f"ELSE {_canonical_str_for_hash(arg.expr)}"
    return str(arg)


# Initialize the concept name generators after classes are defined
_CONCEPT_NAME_GENERATORS.update(
    {
        BuildAggregateWrapper: _gen_agg_name,
        BuildNumberingWindowItem: _gen_window_name,
        BuildNavigationWindowItem: _gen_window_name,
        BuildFilterItem: _gen_filter_name,
        BuildFunction: _gen_function_name,
        BuildParenthetical: _gen_paren_name,
        BuildComparison: _gen_comp_name,
        BuildMultiSelectLineage: _gen_msl_name,
        BuildUnionSelectLineage: _gen_msl_name,
    }
)
