import re

import pytest

from trilogy.dialect.mock_pattern import (
    PRINTABLE,
    STAR_CAP,
    PatternUnsupported,
    mock_pattern,
    parse_pattern,
    pattern_size,
    pattern_values,
)

SUPPORTED = [
    "abc",
    "[A-Z]{2}-[0-9]{4}",
    "a|b|c",
    "(cat|dog)s",
    "(?:ab)+",
    "colou?r",
    "ab*c",
    "x+",
    "a{3}",
    "a{2,}",
    "a{1,4}",
    r"\d{1,3}(\.\d{1,3}){3}",
    r"\w+@\w+\.[a-z]{2,3}",
    r"#[0-9a-f]{6}",
    r"[^0-9]{4}",
    r"[a-cx-z]+",
    r"[\w-]{4}",
    r"[\d.]+",
    r"\s\S\d\D\w\W",
    r"a\.b",
    "^anchored$",
    "..",
    "a*?b",
]


@pytest.mark.parametrize("pattern", SUPPORTED)
@pytest.mark.parametrize("is_key", [True, False])
def test_every_generated_value_satisfies_its_pattern(pattern: str, is_key: bool):
    """The generator's contract: validation runs the same regex back over the
    column, so a value that does not fullmatch is indistinguishable from bad
    real data."""
    values = mock_pattern(pattern, 12, is_key)
    assert values
    assert all(re.fullmatch(pattern, v) for v in values)
    if is_key:
        assert len(set(values)) == len(values)


def test_key_caps_at_a_small_language_rather_than_repeating():
    values = mock_pattern("[ab]{2}", 50, is_key=True)
    assert sorted(values) == ["aa", "ab", "ba", "bb"]


def test_key_samples_a_large_language_to_the_requested_width():
    values = mock_pattern("[a-z]{6}", 30, is_key=True)
    assert len(values) == 30
    assert len(set(values)) == 30


UNSUPPORTED = [
    "(?=secret)\\w+",
    "(?!no)x",
    "(?P<name>a)",
    r"(a)\1",
    "a" + "\\",
    "(ab",
    "[abc",
    "a{2",
    "a{x}",
    "a{1,x}",
    "a)",
    "[]",
]


@pytest.mark.parametrize("pattern", UNSUPPORTED)
def test_unsupported_constructs_stay_loud(pattern: str):
    """Emitting a value that ignores the pattern would be worse than a gap, so
    anything the grammar does not cover raises."""
    with pytest.raises(PatternUnsupported):
        mock_pattern(pattern, 5, is_key=False)
    assert issubclass(PatternUnsupported, NotImplementedError)


def test_unbounded_repetition_is_capped():
    assert {len(v) for v in mock_pattern("a+", 40, is_key=False)} <= set(
        range(1, STAR_CAP + 1)
    )
    assert {len(v) for v in mock_pattern("a*", 40, is_key=False)} <= set(
        range(STAR_CAP + 1)
    )
    assert pattern_size(parse_pattern("a{2,}"), 99) == STAR_CAP + 1


def test_anchors_contribute_nothing():
    assert sorted(pattern_values(parse_pattern("^ab$"))) == ["ab"]


def test_negated_class_excludes_its_members():
    options = set(mock_pattern("[^0-9]", 60, is_key=False))
    assert options
    assert not options & set("0123456789")
    assert options <= set(PRINTABLE)


def test_pattern_size_saturates_at_the_cap():
    node = parse_pattern("[a-z]{4}")
    assert pattern_size(node, 10) == 10
    assert pattern_size(parse_pattern("a|b|c"), 10) == 3
    assert pattern_size(parse_pattern("[ab][cd]"), 10) == 4


def test_pattern_values_enumerates_alternation_and_repetition():
    assert sorted(pattern_values(parse_pattern("(a|b)c"))) == ["ac", "bc"]
    assert sorted(pattern_values(parse_pattern("a{1,2}"))) == ["a", "aa"]
