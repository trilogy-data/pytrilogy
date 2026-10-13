"""The INVALID_REFERENCE_BUG sentinel is the canonical symptom of a planned
reference with no backing source CTE. The renderer's strict-mode guard is the
single backstop that turns it into an actionable error. These lock in that the
sentinel carries its reason and the guard surfaces it."""

import pytest

from trilogy.core.models.execute import INVALID_ALIAS_PREFIX
from trilogy.dialect.base import (
    BASE_INVALID,
    INVALID_REFERENCE_STRING,
    BaseDialect,
    extract_invalid_reference_reasons,
)


def test_sentinel_embeds_reason_without_callsite():
    s = INVALID_REFERENCE_STRING("Missing source CTE for x.y")
    assert BASE_INVALID in s
    assert "Missing source CTE for x.y" in s


def test_sentinel_embeds_reason_with_callsite():
    s = INVALID_REFERENCE_STRING("Missing source reference to a.b", callsite="agg")
    assert BASE_INVALID in s
    assert "Missing source reference to a.b" in s


def test_extract_reasons_dedupes_and_preserves_order():
    sql = (
        "select "
        + INVALID_REFERENCE_STRING("Missing source CTE for x.y")
        + ", "
        + INVALID_REFERENCE_STRING("Missing subselect source for p.q", callsite="sub")
        + ", "
        + INVALID_REFERENCE_STRING("Missing source CTE for x.y")
    )
    assert extract_invalid_reference_reasons(sql) == [
        "Missing source CTE for x.y",
        "Missing subselect source for p.q",
    ]


def test_extract_reasons_empty_when_no_sentinel():
    assert extract_invalid_reference_reasons("select 1 as x from t") == []


def test_invalid_alias_renders_as_sentinel():
    rendered = BaseDialect()._format_cte_column(
        None,  # type: ignore[arg-type]
        "src",
        f"{INVALID_ALIAS_PREFIX}: Concept a.b@Grain<a.b> not found on src",
        raise_invalid=False,
    )
    assert extract_invalid_reference_reasons(rendered) == [
        "Concept a.b@Grain(a.b) not found on src"
    ]


def test_invalid_alias_raises_when_asked():
    with pytest.raises(ValueError):
        BaseDialect()._format_cte_column(
            None,  # type: ignore[arg-type]
            "src",
            f"{INVALID_ALIAS_PREFIX}: missing",
            raise_invalid=True,
        )
