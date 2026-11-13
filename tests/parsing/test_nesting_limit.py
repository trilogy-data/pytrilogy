import subprocess
import sys

import pytest

from trilogy import Dialects, parse
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.parse_engine_v2 import clear_parse_cache, parse_syntax

SHAPES = {
    "paren": ("select ", "(", "1", ")", " as z;"),
    "bracket": ("select ", "[", "1", "]", " as z;"),
    "call": ("select ", "abs(", "1", ")", " as z;"),
    "unclosed": ("select ", "(", "1", "", " as z;"),
    "case": ("select ", "case when 1=1 then ", "1", " end", " as z;"),
    "else": ("select ", "case when 1=1 then 1 else ", "1", " end", " as z;"),
    "not": ("select 1 as z where ", "not ", "z = 1", "", ";"),
    "filter": ("select (", "z ? ", "z = 1", "", ") as y;"),
    "type": ("key x ", "list<", "int", ">", ";"),
}


def nested(shape: str, depth: int) -> str:
    head, open_, leaf, close, tail = SHAPES[shape]
    return head + open_ * depth + leaf + close * depth + tail


@pytest.mark.parametrize("shape", [s for s in SHAPES if s != "unclosed"])
def test_moderate_nesting_parses_and_compiles(shape: str) -> None:
    text = nested(shape, 30)
    parse_syntax(text)
    if shape != "filter":
        Dialects.DUCK_DB.default_executor().generate_sql(text)


@pytest.mark.parametrize("shape", list(SHAPES))
def test_deep_nesting_is_a_syntax_error(shape: str) -> None:
    with pytest.raises(InvalidSyntaxException):
        parse_syntax(nested(shape, 300))


def test_nesting_error_is_coded() -> None:
    with pytest.raises(InvalidSyntaxException, match=r"Syntax \[234\]"):
        parse(nested("paren", 300))


def test_recursion_error_becomes_syntax_error() -> None:
    clear_parse_cache()
    original = CONFIG.parser_backend
    CONFIG.parser_backend = ParserBackend.LARK
    try:
        with pytest.raises(InvalidSyntaxException, match=r"Syntax \[234\]"):
            parse_syntax(nested("paren", 3000))
    finally:
        CONFIG.parser_backend = original
        clear_parse_cache()


_PROBE = """
import sys
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.parse_engine_v2 import parse_syntax
for text in sys.stdin.read().split("\\0"):
    try:
        parse_syntax(text)
    except InvalidSyntaxException:
        pass
print("survived")
"""


def test_very_deep_nesting_does_not_abort_the_process() -> None:
    texts = "\0".join(nested(shape, 30_000) for shape in SHAPES)
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        input=texts,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "survived" in result.stdout
