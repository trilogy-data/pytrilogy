from __future__ import annotations

from trilogy.core.exceptions import InvalidSyntaxException

ERROR_CODES: dict[int, str] = {
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
}

DEFAULT_ERROR_SPAN: int = 30


def inject_context_maker(pos: int, text: str, span: int = 40) -> str:
    start = max(pos - span, 0)
    end = pos + span
    before = text[start:pos].rsplit("\n", 1)[-1]
    after = text[pos:end].split("\n", 1)[0]
    rcap = ""
    if after and not after[-1].isspace() and not (end > len(text)):
        rcap = "..."
    lcap = ""
    if start > 0 and not before[0].isspace():
        lcap = "..."
    lpad = " "
    rpad = " "
    if before.endswith(" "):
        lpad = ""
    if after.startswith(" "):
        rpad = ""
    return f"{lcap}{before}{lpad}???{rpad}{after}{rcap}"


def create_syntax_error(code: int, pos: int, text: str) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        f"Syntax [{code}]: "
        + ERROR_CODES[code]
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def create_generic_syntax_error(
    message: str, pos: int, text: str
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        message
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )
