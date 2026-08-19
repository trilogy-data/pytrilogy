from typing import Literal

Language = Literal["preql", "sql"]

_COMMENT_MARKERS: dict[str, str] = {"preql": "#", "sql": "--"}


def _strip_line_comment(line: str, marker: str) -> str:
    in_squote = False
    in_dquote = False
    i = 0
    n = len(line)
    while i < n:
        if not in_squote and not in_dquote and line.startswith(marker, i):
            return line[:i]
        ch = line[i]
        if ch == "'" and not in_dquote:
            in_squote = not in_squote
        elif ch == '"' and not in_squote:
            in_dquote = not in_dquote
        i += 1
    return line


def strip_line_comments(text: str, lang: Language) -> str:
    marker = _COMMENT_MARKERS[lang]
    out: list[str] = []
    for line in text.splitlines():
        cleaned = _strip_line_comment(line, marker).rstrip()
        if cleaned:
            out.append(cleaned)
    return "\n".join(out)


def query_size(text: str, lang: Language) -> int:
    return len(strip_line_comments(text, lang))
