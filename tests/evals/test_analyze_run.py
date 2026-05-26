"""Unit tests for the eval failure-report formatter.

These cover the bits a future change is most likely to silently break:
the caret diagram surviving in the snippet, center-truncation anchoring on
``^---``, stderr extraction, and middle-truncation of the argv display.
"""

from __future__ import annotations

from evals.common.analyze_run import _error_snippet, _format_args

RAW_PARSE_ERROR = """\
exit_code: 1
--- stdout ---
some execution info box that we don't care about
--- stderr ---
Unexpected error:  --> 2:46
  |
2 | where lineitem.shipdate <= '1998-09-02'::date;
  |                                              ^---
  |
  = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail
Location:
...shipdate <= '1998-09-02'::date ??? ; select lineitem.returnflag,
"""


def test_error_snippet_preserves_caret_diagram():
    snippet = _error_snippet(RAW_PARSE_ERROR)
    assert "^---" in snippet
    assert "2 | where lineitem.shipdate" in snippet
    assert "Location:" in snippet


def test_error_snippet_strips_stdout_box_and_marker():
    snippet = _error_snippet(RAW_PARSE_ERROR)
    assert "execution info box" not in snippet
    assert "Unexpected error:" not in snippet
    assert snippet.startswith("--> 2:46")


def test_error_snippet_short_input_returned_verbatim():
    text = "exit_code: 1\n--- stderr ---\nError: boom\n"
    assert _error_snippet(text) == "boom"


def test_error_snippet_center_truncates_around_caret():
    head = "x" * 2000
    tail = "y" * 2000
    raw = f"--- stderr ---\nUnexpected error: {head}\n  | ^---\n{tail}\n"
    snippet = _error_snippet(raw, limit=400)
    assert "^---" in snippet
    assert snippet.startswith("…")
    assert snippet.endswith("…")
    assert len(snippet) < 500


def test_error_snippet_no_anchor_falls_back_to_head_tail():
    raw = "--- stderr ---\nError: " + ("z" * 2000)
    snippet = _error_snippet(raw, limit=200)
    assert "…" in snippet
    assert snippet.startswith("z")
    assert snippet.endswith("z")


def test_format_args_passthrough_when_short():
    assert _format_args(["run", "query.preql"]) == "run query.preql"


def test_format_args_middle_truncates_preserving_head_and_tail():
    args = ["run", "-", "--stdin", "x" * 400 + "TAILTOKEN"]
    out = _format_args(args, limit=120)
    assert out.startswith("run -")
    assert out.endswith("TAILTOKEN")
    assert "…" in out
    assert len(out) <= 121
