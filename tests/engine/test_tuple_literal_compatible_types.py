from contextlib import contextmanager

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend

_BACKENDS = [ParserBackend.PEST, ParserBackend.LARK]


@contextmanager
def _using_backend(backend: ParserBackend):
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        yield
    finally:
        CONFIG.parser_backend = prev


@pytest.mark.parametrize("backend", _BACKENDS)
def test_tuple_literal_numeric_family_mix_builds(backend):
    with _using_backend(backend):
        exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
        exec_.generate_sql("const x <- 1; where x in (1, 2.0) select x;")


def test_tuple_literal_integer_numeric_mix_builds():
    # `::` cast inside a tuple is a LARK-grammar gap orthogonal to type
    # uniformity, so this case only covers the default PEST backend.
    with _using_backend(ParserBackend.PEST):
        exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
        exec_.generate_sql("const x <- 1; where x in (1, 2::numeric) select x;")


@pytest.mark.parametrize("backend", _BACKENDS)
def test_tuple_literal_numeric_family_mix_executes(backend):
    with _using_backend(backend):
        exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
        rows = exec_.execute_text("const x <- 1; where x in (1, 2.0) select x;")[
            0
        ].fetchall()
        assert [tuple(r) for r in rows] == [(1,)]


@pytest.mark.parametrize("backend", _BACKENDS)
def test_tuple_literal_incompatible_types_clean_error(backend):
    with _using_backend(backend):
        exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
        with pytest.raises(Exception) as excinfo:
            exec_.generate_sql("const x <- 1; where x in (1, 'a') select x;")
        # LARK rejects at tuple hydration ("incompatible types"); PEST defers to
        # the per-element value-list check ("cannot compare ... element")
        msg = str(excinfo.value).lower()
        assert "incompatible" in msg or "cannot compare" in msg
