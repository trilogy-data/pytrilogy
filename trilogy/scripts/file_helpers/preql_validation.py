"""Pre-write validation for ``.preql`` content.

Shared between the agent ``write_file`` tool and ``trilogy file write`` so
both surfaces reject the same broken content (truncated bodies, HTML-escaped
operators) before it lands on disk.
"""

from __future__ import annotations

# HTML entities seen written into .preql files by some models that escape their
# own tool output. We catch the comparison-operator ones explicitly because
# they're the failure mode we've observed; other entities are flagged generically.
HTML_ENTITY_HINTS: dict[str, str] = {
    "&lt;": "<",
    "&gt;": ">",
    "&amp;": "&",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def detect_html_escapes(content: str) -> list[str]:
    return [ent for ent in HTML_ENTITY_HINTS if ent in content]


def validate_preql_syntax(content: str) -> str | None:
    """Return a parse-error message for invalid Trilogy syntax, else None."""
    from trilogy.core.exceptions import InvalidSyntaxException
    from trilogy.parsing.v2.lark_backend import parse_lark

    try:
        parse_lark(content)
    except InvalidSyntaxException as exc:
        return str(exc)
    except Exception as exc:  # safety net — never let validation crash the write
        return f"{type(exc).__name__}: {exc}"
    return None


def _size_hint(content: str) -> str:
    """Render a length + tail snippet so the agent can spot truncation.

    The tail is the last 60 chars (newlines collapsed) — when an LLM cut its
    own output mid-write the file usually ends in a half-finished token like
    ``auto orders_per_customer``, and seeing the tail is the most direct
    signal that the body is incomplete.
    """
    char_count = len(content)
    byte_count = len(content.encode("utf-8"))
    tail = content[-60:].replace("\n", "\\n")
    return f"received {char_count} chars / {byte_count} bytes; tail: …{tail!r}"


def validate_preql_content(path: str, content: str) -> str | None:
    """Return a refusal message for invalid .preql content, or None to allow.

    Returns the same message format the agent's ``write_file`` uses so the
    refusal text stays consistent across surfaces.
    """
    if not path.endswith(".preql"):
        return None
    entities = detect_html_escapes(content)
    if entities:
        decoded = ", ".join(
            f"`{e}` (use `{HTML_ENTITY_HINTS[e]}`)" for e in entities
        )
        return (
            f"refused to write '{path}': contains HTML-escaped characters: "
            f"{decoded}. Trilogy parses raw operators — emit them literally "
            "(e.g. `<=` not `&lt;=`)."
        )
    syntax_error = validate_preql_syntax(content)
    if syntax_error:
        return (
            f"refused to write '{path}': not syntactically valid Trilogy.\n"
            f"\nParse error:\n{syntax_error}\n"
            f"\nWrite stats: {_size_hint(content)}.\n"
            "If the tail looks cut off (mid-identifier, mid-statement) your "
            "response was likely truncated by max_tokens — re-issue with the "
            "COMPLETE file body, do not resend the same bytes. Pass --force to "
            "bypass validation only when you intend a partial draft."
        )
    return None
