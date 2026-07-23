from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterator

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import ComparisonOperator
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.core import (
    DataType,
    NumericType,
    TraitDataType,
    ValidatedType,
    ValueRange,
    constant_domain_violation,
    is_compatible_datatype,
)
from trilogy.core.validation.datasource import inferred_type_check, type_check
from trilogy.core.validation.environment import validate_environment
from trilogy.parsing.render import Renderer

BACKENDS = [ParserBackend.LARK, ParserBackend.PEST]


@contextmanager
def _using_backend(backend: ParserBackend) -> Iterator[None]:
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        yield
    finally:
        CONFIG.parser_backend = prev


def test_value_range():
    assert ValueRange(min=0, max=100).contains(50)
    assert ValueRange(min=0, max=100).contains(0)
    assert ValueRange(min=0, max=100).contains(100)
    assert not ValueRange(min=0, max=100).contains(101)
    assert ValueRange(min=0).contains(10**12)
    assert not ValueRange(min=0).contains(-1)
    assert ValueRange(max=100).contains(-(10**12))
    assert not ValueRange(max=100).contains(101)
    assert str(ValueRange(min=0, max=100)) == "0..100"
    assert str(ValueRange(min=0)) == "0.."
    assert str(ValueRange(max=100)) == "..100"
    assert str(ValueRange(min=42, max=42)) == "42"
    assert str(ValueRange(min=date(2020, 1, 1))) == "'2020-01-01'.."
    with pytest.raises(ValueError):
        ValueRange()
    with pytest.raises(ValueError):
        ValueRange(min=10, max=0)


def test_validated_type_model():
    vint = ValidatedType(type=DataType.INTEGER, ranges=(ValueRange(min=0, max=100),))
    assert vint.check_value(50)
    assert not vint.check_value(101)
    assert vint == DataType.INTEGER
    assert vint.data_type == DataType.INTEGER
    assert hash(vint) == hash(DataType.INTEGER)
    assert is_compatible_datatype(vint, DataType.INTEGER)
    assert is_compatible_datatype(DataType.FLOAT, vint)
    assert not is_compatible_datatype(vint, DataType.STRING)
    assert str(vint) == "int[0..100]"

    multi = ValidatedType(
        type=DataType.INTEGER,
        ranges=(ValueRange(min=0, max=10), ValueRange(min=20, max=30)),
    )
    assert multi.check_value(5)
    assert not multi.check_value(15)
    assert multi.check_value(25)

    vstr = ValidatedType(type=DataType.STRING, pattern="[A-Z]+")
    assert vstr.check_value("ABC")
    assert not vstr.check_value("abc")
    assert not vstr.check_value("ABCd")  # full match, not search
    assert vstr == DataType.STRING

    vnum = ValidatedType(
        type=NumericType(precision=20, scale=5), ranges=(ValueRange(min=0),)
    )
    assert vnum.data_type == DataType.NUMERIC
    assert str(vnum) == "numeric(20,5)[0..]"

    trait = TraitDataType(type=DataType.INTEGER, traits=["thing"])
    assert vint == trait
    assert trait == vint


def test_constant_domain_violation():
    vint = ValidatedType(
        type=DataType.INTEGER,
        ranges=(ValueRange(min=0, max=10), ValueRange(min=20, max=30)),
    )
    assert constant_domain_violation(vint, ComparisonOperator.EQ, 5) is None
    assert constant_domain_violation(vint, ComparisonOperator.EQ, 15) is not None
    # union envelope: > 15 is satisfiable via the 20..30 arm
    assert constant_domain_violation(vint, ComparisonOperator.GT, 15) is None
    assert constant_domain_violation(vint, ComparisonOperator.GT, 30) is not None
    assert constant_domain_violation(vint, ComparisonOperator.GTE, 30) is None
    assert constant_domain_violation(vint, ComparisonOperator.GTE, 31) is not None
    assert constant_domain_violation(vint, ComparisonOperator.LT, 0) is not None
    assert constant_domain_violation(vint, ComparisonOperator.LTE, -1) is not None
    # always-true predicates are not flagged
    assert constant_domain_violation(vint, ComparisonOperator.NE, 500) is None
    # open bounds disable the affected direction
    open_max = ValidatedType(type=DataType.INTEGER, ranges=(ValueRange(min=0),))
    assert constant_domain_violation(open_max, ComparisonOperator.GT, 10**9) is None
    assert constant_domain_violation(open_max, ComparisonOperator.LT, 0) is not None
    # traits unwrap
    wrapped = TraitDataType(type=vint, traits=["thing"])
    assert constant_domain_violation(wrapped, ComparisonOperator.EQ, 15) is not None
    # non-literal / mismatched values are undecidable, not errors
    assert constant_domain_violation(vint, ComparisonOperator.EQ, "abc") is None
    assert constant_domain_violation(DataType.INTEGER, ComparisonOperator.EQ, 5) is None

    vdate = ValidatedType(
        type=DataType.DATE,
        ranges=(ValueRange(min=date(2020, 1, 1), max=date(2024, 12, 31)),),
    )
    assert (
        constant_domain_violation(vdate, ComparisonOperator.EQ, date(2025, 6, 1))
        is not None
    )
    assert (
        constant_domain_violation(vdate, ComparisonOperator.EQ, date(2022, 6, 1))
        is None
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_validated_type_parsing(backend: ParserBackend):
    with _using_backend(backend):
        env = Environment()
        env.parse("""
key score int[0..100];
key temp float[-40.0..120.5];
key code string['[A-Z]+'];
key bucket int[0..10, 20..30];
key positive int[0..];
key capped int[..100];
key exact int[42];
key event_date date['2020-01-01'..'2024-12-31'];
key event_ts datetime['2020-01-01 00:00:00'..'2024-12-31 23:59:59'];
key money numeric(20,5)[0..];
""")
        score = env.concepts["score"].datatype
        assert isinstance(score, ValidatedType)
        assert score.type == DataType.INTEGER
        assert score.ranges == (ValueRange(min=0, max=100),)

        temp = env.concepts["temp"].datatype
        assert isinstance(temp, ValidatedType)
        assert temp.ranges == (ValueRange(min=-40.0, max=120.5),)

        code = env.concepts["code"].datatype
        assert isinstance(code, ValidatedType)
        assert code.pattern == "[A-Z]+"

        bucket = env.concepts["bucket"].datatype
        assert isinstance(bucket, ValidatedType)
        assert bucket.ranges == (
            ValueRange(min=0, max=10),
            ValueRange(min=20, max=30),
        )

        positive = env.concepts["positive"].datatype
        assert isinstance(positive, ValidatedType)
        assert positive.ranges == (ValueRange(min=0),)

        capped = env.concepts["capped"].datatype
        assert isinstance(capped, ValidatedType)
        assert capped.ranges == (ValueRange(max=100),)

        exact = env.concepts["exact"].datatype
        assert isinstance(exact, ValidatedType)
        assert exact.ranges == (ValueRange(min=42, max=42),)

        event_date = env.concepts["event_date"].datatype
        assert isinstance(event_date, ValidatedType)
        assert event_date.ranges == (
            ValueRange(min=date(2020, 1, 1), max=date(2024, 12, 31)),
        )

        event_ts = env.concepts["event_ts"].datatype
        assert isinstance(event_ts, ValidatedType)
        assert event_ts.ranges == (
            ValueRange(
                min=datetime(2020, 1, 1, 0, 0, 0),
                max=datetime(2024, 12, 31, 23, 59, 59),
            ),
        )

        money = env.concepts["money"].datatype
        assert isinstance(money, ValidatedType)
        assert isinstance(money.type, NumericType)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validated_type_parse_errors(backend: ParserBackend):
    with _using_backend(backend):
        # bad regex
        with pytest.raises(Exception, match="invalid regex"):
            Environment().parse("key code string['[unclosed'];")
        # string ranges are not supported
        with pytest.raises(Exception, match="regex pattern"):
            Environment().parse("key code string['a'..'z'];")
        # inverted range
        with pytest.raises(Exception, match="exceeds max"):
            Environment().parse("key score int[100..0];")
        # int base with float bounds
        with pytest.raises(Exception, match="must be integers"):
            Environment().parse("key score int[0..1.5];")
        # temporal base with unquoted bounds
        with pytest.raises(Exception, match="quoted literals"):
            Environment().parse("key event_date date[0..100];")
        # bad date literal
        with pytest.raises(Exception, match="invalid date literal"):
            Environment().parse("key event_date date['not-a-date'..'2024-01-01'];")


@pytest.mark.parametrize("backend", BACKENDS)
def test_impossible_comparisons(backend: ParserBackend):
    prelude = "key score int[0..100];\nkey code string['[A-Z]+'];\n"
    cases = [
        ("select score where score = 250;", "outside declared domain"),
        ("select score where score > 150;", "no value > 150"),
        ("select score where 150 < score;", "no value > 150"),
        ("select score where score < -5;", "no value < -5"),
        ("select score where score in (50, 250);", "outside declared domain"),
        ("select code where code = 'abc';", "can never match declared pattern"),
        ("select score where score between 150 and 200;", "no value >= 150"),
        ("select score where score between -50 and -10;", "no value <= -10"),
    ]
    with _using_backend(backend):
        for text, frag in cases:
            with pytest.raises(Exception, match=frag):
                Environment().parse(prelude + text)
        # satisfiable predicates parse cleanly
        for text in (
            "select score where score > 50;",
            "select score where score in (5, 50);",
            "select score where score between 50 and 200;",
            "select score where score != 500;",
            "select score where score not in (500,);",
            "select code where code = 'ABC';",
        ):
            Environment().parse(prelude + text)


@pytest.mark.parametrize("backend", BACKENDS)
def test_enum_constant_membership_still_allowed(backend: ParserBackend):
    """Enum domains may be sampled/inferred, so non-member comparisons stay
    legal possibly-empty filters (see tests/engine/test_enum_unions.py) —
    only user-declared ValidatedType domains are enforced at authoring time."""
    prelude = "key status enum<string>['open', 'closed'];\n"
    with _using_backend(backend):
        Environment().parse(prelude + "select status where status = 'pending';")
        Environment().parse(prelude + "select status where status = 'open';")


def test_type_check_validated():
    vint = ValidatedType(type=DataType.INTEGER, ranges=(ValueRange(min=0, max=100),))
    assert type_check(50, vint)
    assert not type_check(250, vint)
    assert not type_check(50.0, vint)  # base type still enforced
    assert type_check(None, vint)  # nullable by default
    assert not type_check(None, vint, nullable=False)

    vstr = ValidatedType(type=DataType.STRING, pattern="[A-Z]+")
    assert type_check("ABC", vstr)
    assert not type_check("abc", vstr)
    assert not type_check(123, vstr)

    vdate = ValidatedType(
        type=DataType.DATE,
        ranges=(ValueRange(min=date(2020, 1, 1), max=date(2024, 12, 31)),),
    )
    assert type_check(date(2022, 6, 1), vdate)
    assert not type_check(date(2025, 6, 1), vdate)

    assert inferred_type_check(vint, vint)
    assert not inferred_type_check(DataType.INTEGER, vint)
    assert not inferred_type_check(vint, DataType.INTEGER)


def test_datasource_validation_range_failure():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.score int[0..100];

        datasource scores (
            id: id,
            score: score,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 50 AS score UNION ALL
        SELECT 2, 250
        ''';
        """)
    with pytest.raises(ModelValidationError, match="violates declared domain"):
        validate_environment(executor.environment, exec=executor)


def test_datasource_validation_range_pass():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.score int[0..100];

        datasource scores (
            id: id,
            score: score,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 0 AS score UNION ALL
        SELECT 2, 100
        ''';
        """)
    validate_environment(executor.environment, exec=executor)


def test_datasource_validation_regex():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.code string['[A-Z]+'];

        datasource codes (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'ABC' AS code UNION ALL
        SELECT 2, 'abc'
        ''';
        """)
    with pytest.raises(ModelValidationError, match="violates declared domain"):
        validate_environment(executor.environment, exec=executor)


def test_datasource_validation_regex_null_ok():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.code string['[A-Z]+']?;

        datasource codes (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'ABC' AS code UNION ALL
        SELECT 2, NULL
        ''';
        """)
    validate_environment(executor.environment, exec=executor)


@pytest.mark.parametrize("backend", BACKENDS)
def test_render_round_trip(backend: ParserBackend):
    text = """key score int[0..100, 250..300];
key code string['[A-Z]+\\d*'];
key event_date date['2020-01-01'..'2024-12-31'];
key positive float[0.5..];
"""
    with _using_backend(backend):
        env = Environment()
        _, stmts = env.parse(text)
        rendered = "\n".join(Renderer().to_string(s) for s in stmts)
        env2 = Environment()
        env2.parse(rendered)
        for name in ("score", "code", "event_date", "positive"):
            assert env.concepts[name].datatype == env2.concepts[name].datatype, name


def test_mock_validated():
    from trilogy.dialect.mock import mock_datatype

    vint = ValidatedType(type=DataType.INTEGER, ranges=(ValueRange(min=5, max=10),))
    vals = mock_datatype(vint, DataType.INTEGER, 50, is_key=True)
    assert len(vals) == 50
    assert all(vint.check_value(v) for v in vals)
    vals = mock_datatype(vint, DataType.INTEGER, 50, is_key=False)
    assert all(vint.check_value(v) for v in vals)

    vfloat = ValidatedType(type=DataType.FLOAT, ranges=(ValueRange(min=0.0, max=1.0),))
    vals = mock_datatype(vfloat, DataType.FLOAT, 20, is_key=True)
    assert len(set(vals)) == 20
    assert all(vfloat.check_value(v) for v in vals)

    vdate = ValidatedType(
        type=DataType.DATE,
        ranges=(ValueRange(min=date(2024, 1, 1), max=date(2024, 1, 31)),),
    )
    vals = mock_datatype(vdate, DataType.DATE, 10, is_key=False)
    assert all(vdate.check_value(v) for v in vals)

    vstr = ValidatedType(type=DataType.STRING, pattern="[A-Z]+")
    with pytest.raises(NotImplementedError):
        mock_datatype(vstr, DataType.STRING, 10)


def test_query_execution_with_validators():
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.execute_text("""
        key id int;
        property id.score int[0..100];

        datasource scores (
            id: id,
            score: score,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 50 AS score UNION ALL
        SELECT 2, 90
        ''';

        select id, score where score > 60;
        """)
    rows = results[-1].fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 90
