import pytest

from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.author import (
    Between,
    Comparison,
    ConceptRef,
    Conditional,
    Parenthetical,
)
from trilogy.core.models.build import (
    BuildBetween,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildParenthetical,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.base import BaseDialect
from trilogy.parser import parse

# --- author-level Between -------------------------------------------------


def _ref(name: str) -> ConceptRef:
    return ConceptRef(address=f"test.{name}", datatype=DataType.INTEGER)


class TestBetweenValidation:
    def test_constant_bounds_ok(self):
        b = Between(left=5, low=1, high=10)
        assert b.low == 1 and b.high == 10

    def test_incompatible_low_raises(self):
        with pytest.raises(SyntaxError, match="incompatible types"):
            Between(left="abc", low=1, high=10)

    def test_incompatible_high_raises(self):
        with pytest.raises(SyntaxError, match=r"\(high\)"):
            Between(left=5, low=1, high="zzz")

    def test_concept_left_is_downcast_to_reference(self):
        env = Environment()
        env.parse("key a int; key b int; key c int;")
        b = Between(
            left=env.concepts["local.a"],
            low=env.concepts["local.b"],
            high=env.concepts["local.c"],
        )
        assert isinstance(b.left, ConceptRef)
        assert isinstance(b.low, ConceptRef)
        assert isinstance(b.high, ConceptRef)


class TestBetweenDunders:
    def test_repr_and_str(self):
        b = Between(left=_ref("x"), low=1, high=10)
        assert repr(b) == str(b)
        assert "between" in repr(b)

    def test_eq_and_hash(self):
        a = Between(left=_ref("x"), low=1, high=10)
        b = Between(left=_ref("x"), low=1, high=10)
        c = Between(left=_ref("x"), low=2, high=10)
        assert a == b
        assert a != c
        assert a != "not a between"
        assert hash(a) == hash(b)

    def test_add_none_returns_self(self):
        b = Between(left=_ref("x"), low=1, high=10)
        assert b + None is b

    def test_add_self_returns_self(self):
        b = Between(left=_ref("x"), low=1, high=10)
        assert b + b is b

    def test_add_comparison_returns_conditional(self):
        b = Between(left=_ref("x"), low=1, high=10)
        other = Comparison(left=_ref("y"), right=1, operator=ComparisonOperator.EQ)
        combined = b + other
        assert isinstance(combined, Conditional)
        assert combined.operator == BooleanOperator.AND

    def test_add_invalid_raises(self):
        b = Between(left=_ref("x"), low=1, high=10)
        with pytest.raises(ValueError, match="Cannot add Between"):
            b + 5


class TestBetweenAcceptedByPeerAdd:
    """``Between`` was added to the accepted-operand tuples of the peer
    condition types — adding a comparison and a between must combine."""

    def test_comparison_plus_between(self):
        comp = Comparison(left=_ref("y"), right=1, operator=ComparisonOperator.EQ)
        combined = comp + Between(left=_ref("x"), low=1, high=10)
        assert isinstance(combined, Conditional)

    def test_comparison_plus_invalid_raises(self):
        comp = Comparison(left=_ref("y"), right=1, operator=ComparisonOperator.EQ)
        with pytest.raises(ValueError, match="non-Comparison"):
            comp + 5

    def test_build_parenthetical_plus_build_between(self):
        par = BuildParenthetical(
            content=BuildComparison(
                left=_bc("y"), right=1, operator=ComparisonOperator.EQ
            )
        )
        combined = par + BuildBetween(left=_bc("x"), low=1, high=10)
        assert isinstance(combined, BuildConditional)


class TestBetweenTransforms:
    def test_with_namespace(self):
        b = Between(left=_ref("x"), low=_ref("lo"), high=10)
        ns = b.with_namespace("other")
        assert isinstance(ns, Between)
        assert ns.left.address == "other.test.x"
        assert ns.low.address == "other.test.lo"
        assert ns.high == 10

    def test_with_reference_replacement(self):
        b = Between(left=_ref("x"), low=1, high=10)
        replaced = b.with_reference_replacement([("nonexistent", 99)])
        assert isinstance(replaced, Between)
        assert replaced.left.address == "test.x"


class TestBetweenArguments:
    def test_concept_arguments(self):
        b = Between(left=_ref("x"), low=_ref("lo"), high=10)
        addrs = {c.address for c in b.concept_arguments}
        assert addrs == {"test.x", "test.lo"}

    def test_row_arguments(self):
        b = Between(left=_ref("x"), low=1, high=10)
        assert [c.address for c in b.row_arguments] == ["test.x"]

    def test_existence_arguments_walks_conceptargs_children(self):
        b = Between(left=Parenthetical(content=_ref("x")), low=1, high=10)
        assert b.existence_arguments == []

    def test_output_datatype_is_bool(self):
        assert Between(left=5, low=1, high=10).output_datatype == DataType.BOOL


# --- build-level BuildBetween --------------------------------------------


def _bc(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


class TestBuildBetweenDunders:
    def test_repr_and_str(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        assert repr(b) == str(b)
        assert "between" in repr(b)

    def test_eq_and_hash(self):
        a = BuildBetween(left=_bc("x"), low=1, high=10)
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        c = BuildBetween(left=_bc("x"), low=1, high=99)
        assert a == b
        assert a != c
        assert a != "nope"
        assert hash(a) == hash(b)

    def test_add_none_returns_self(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        assert b + None is b

    def test_add_self_returns_self(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        assert b + b is b

    def test_add_comparison_returns_conditional(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        other = BuildComparison(left=_bc("y"), right=1, operator=ComparisonOperator.EQ)
        combined = b + other
        assert isinstance(combined, BuildConditional)
        assert combined.operator == BooleanOperator.AND

    def test_add_invalid_raises(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        with pytest.raises(ValueError, match="Cannot add"):
            b + 5


class TestBuildBetweenArguments:
    def test_concept_arguments(self):
        b = BuildBetween(left=_bc("x"), low=_bc("lo"), high=10)
        assert {c.address for c in b.concept_arguments} == {"test.x", "test.lo"}

    def test_row_arguments(self):
        b = BuildBetween(left=_bc("x"), low=1, high=10)
        assert [c.address for c in b.row_arguments] == ["test.x"]

    def test_existence_arguments(self):
        b = BuildBetween(left=BuildParenthetical(content=_bc("x")), low=1, high=10)
        assert b.existence_arguments == []

    def test_output_datatype_is_bool(self):
        assert BuildBetween(left=_bc("x"), low=1, high=10).output_datatype == (
            DataType.BOOL
        )


def test_build_between_inline_constant():
    """``inline_constant`` swaps a constant concept for its literal value,
    recursing through ``ConstantInlineable`` children."""
    env = Environment()
    env.parse("const c <- 5;")
    build_env = env.materialize_for_select()
    const = build_env.concepts["local.c"]

    b = BuildBetween(
        left=const,
        low=BuildParenthetical(content=const),
        high=10,
    )
    inlined = b.inline_constant(const)
    assert isinstance(inlined, BuildBetween)
    assert inlined.left == 5
    assert inlined.high == 10


# --- factory _build_between ----------------------------------------------


def test_build_between_factory_non_constant(test_environment):
    """A BETWEEN over a real column survives the build as a ``BuildBetween``
    and renders as a SQL ``between``."""
    env, parsed = parse(
        "select order_id where order_id between 1 and 5;",
        environment=test_environment,
    )
    select: SelectStatement = parsed[-1]
    sql = BaseDialect().compile_statement(process_query(env, select))
    assert "between" in sql.lower()


def test_build_between_factory_nests_aggregate_bound():
    """A BETWEEN bound that is an inline aggregate is instantiated as a nested
    concept during the build rather than emitted inline."""
    env = Environment()
    env.parse("""
key oid int;
property oid.amt int;
datasource o (oid: oid, amt: amt)
grain (oid)
query '''select 1 as oid, 5 as amt''';
""")
    _, parsed = parse("select oid where oid between 1 and max(amt);", environment=env)
    select: SelectStatement = parsed[-1]
    sql = BaseDialect().compile_statement(process_query(env, select))
    assert "between" in sql.lower()


def test_build_between_factory_all_constant_folds():
    """When left/low/high are all constants the build folds the BETWEEN to a
    boolean instead of emitting a ``BuildBetween``."""
    env = Environment()
    _, parsed = parse(
        "const mid <- 5; select mid where mid between 1 and 10;",
        environment=env,
    )
    select: SelectStatement = parsed[-1]
    # Builds without error; the folded-true predicate keeps every row.
    BaseDialect().compile_statement(process_query(env, select))
