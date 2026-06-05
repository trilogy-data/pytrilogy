"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [104]
when a top-level definition/statement (`auto NAME`, `import ...`, ...) is placed
*inside* a query — after the `where`/`select`.

Real failure mode (seen ~16x in the tpc-ds agent eval): the agent writes its
`auto` aggregate definitions right above the `select`, but below the `where`:

    where returns.store.state = 'TN'
    auto store_total <- sum(returns.amount) by returns.store.id;   # <- invalid here
    select returns.store.id, store_total;

The bare parser error named a bogus `ORDER_IDENTIFIER 'auto'` token and gave the
agent no clue. Definitions must precede the `where`/`select` block.

Tool write-validation runs through the configured backend (pest by default), so
the same friendly error reaches the agent at write time."""

from __future__ import annotations

import pytest

from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_definition_after_clause
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest
from trilogy.scripts.file_helpers.preql_validation import validate_preql_content

_IMPORTS = "import x as x;\n"

_BAD_CASES = [
    "where x.val > 1\nauto t <- sum(x.val) by x.id;\nselect x.id, t;",
    "where x.val > 1\nproperty x.id.lbl <- x.val + 1;\nselect x.id, x.lbl;",
    "where x.val > 1\nimport x as y;\nselect x.id;",
    "where x.val > 1\ndef f(m) -> sum(m);\nselect x.id, @f(x.val) as t;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD_CASES)
def test_definition_after_where_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + body)
    msg = str(exc.value)
    assert "Syntax [104]" in msg, msg
    assert "before" in msg.lower(), msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_definition_before_where_parses(backend):
    """The valid form — definitions above the where/select — must parse."""
    backend(
        _IMPORTS + "auto t <- sum(x.val) by x.id;\nwhere x.val > 1\nselect x.id, t;"
    )


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_keyword_named_field_after_where_does_not_fire(backend):
    """`where x.key = ...` is not a definition — a keyword followed by an
    operator (not a name) must not trip 104. It is still a valid query."""
    backend(_IMPORTS + "where x.key > 1\nselect x.id;")


def test_detector_finds_definition_after_where():
    text = (
        "import x as x; where x.val > 1 auto t <- sum(x.val) by x.id; select x.id, t;"
    )
    pos = text.index("auto")
    found = detect_definition_after_clause(text, pos)
    assert found is not None
    assert text[found:].startswith("auto")


def test_detector_ignores_definition_at_top_level():
    """`auto NAME` with no preceding query clause in its statement is valid."""
    text = "import x as x; auto t <- sum(x.val) by x.id; where x.val > 1 select t;"
    assert detect_definition_after_clause(text, text.index("auto")) is None


def test_detector_ignores_keyword_substring():
    """A field whose name merely contains a keyword must not match."""
    text = "import x as x; where x.import_flag = 1 select x.id;"
    # the `import` substring inside `import_flag` is not a word-boundary match
    assert detect_definition_after_clause(text, text.index("import")) is None


def test_validation_uses_pest_and_emits_104():
    """Tool write-validation runs through the configured (pest) backend and
    surfaces the friendly 104 message."""
    assert CONFIG.parser_backend == ParserBackend.PEST
    msg = validate_preql_content(
        "q.preql",
        _IMPORTS + "where x.val > 1\nauto t <- sum(x.val) by x.id;\nselect x.id, t;",
    )
    assert msg is not None
    assert "Syntax [104]" in msg, msg
