"""A `def` macro's parameter list is optional. All three forms below define a
valid zero-arg macro, invoked as `@name()`. Surfaced by TPC-DS q02, where the
agent wrote `def sun_sales -> sum(...)` (a no-arg named expression) and hit an
opaque parse error.

  - `def NAME -> <expr>`   (no parens)
  - `def NAME() -> <expr>` (empty parens)
both behave identically to a zero-arg macro; `def NAME(x) -> <expr>` still takes
parameters as before."""

from __future__ import annotations

import pytest

from trilogy import Environment
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_NO_PARENS = """const x <- 5;
def doubled -> x * 2;
auto a <- @doubled() + 1;"""
_EMPTY_PARENS = """const x <- 5;
def doubled() -> x * 2;
auto a <- @doubled() + 1;"""


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("src", [_NO_PARENS, _EMPTY_PARENS])
def test_zero_arg_def_parses(backend, src):
    backend(src)


@pytest.mark.parametrize("src", [_NO_PARENS, _EMPTY_PARENS])
def test_zero_arg_macro_resolves(src):
    env = Environment()
    parse_text(src, env)
    assert str(env.concepts["a"].lineage) == "add(@doubled(),1)"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_parameterized_def_still_parses(backend):
    backend("def day_sales(d) -> sum(d + 1);")
