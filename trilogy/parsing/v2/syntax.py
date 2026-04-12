from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias, cast


@dataclass(frozen=True)
class SyntaxMeta:
    line: int | None
    column: int | None
    end_line: int | None
    end_column: int | None
    start_pos: int | None
    end_pos: int | None

    @classmethod
    def from_parser_meta(cls, meta: Any | None) -> "SyntaxMeta | None":
        if meta is None:
            return None
        return cls(
            line=getattr(meta, "line", None),
            column=getattr(meta, "column", None),
            end_line=getattr(meta, "end_line", None),
            end_column=getattr(meta, "end_column", None),
            start_pos=getattr(meta, "start_pos", None),
            end_pos=getattr(meta, "end_pos", None),
        )


@dataclass(frozen=True)
class SyntaxToken:
    name: str
    value: str
    meta: SyntaxMeta | None = None

    @property
    def type(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.value

    def lower(self) -> str:
        return self.value.lower()

    def capitalize(self) -> str:
        return self.value.capitalize()

    def strip(self) -> str:
        return self.value.strip()

    def split(self, *args: Any, **kwargs: Any) -> list[str]:
        return self.value.split(*args, **kwargs)

    def rsplit(self, *args: Any, **kwargs: Any) -> list[str]:
        return self.value.rsplit(*args, **kwargs)

    def startswith(self, *args: Any, **kwargs: Any) -> bool:
        return self.value.startswith(*args, **kwargs)

    def endswith(self, *args: Any, **kwargs: Any) -> bool:
        return self.value.endswith(*args, **kwargs)


@dataclass(frozen=True)
class SyntaxNode:
    name: str
    children: tuple["SyntaxNode | SyntaxToken", ...]
    meta: SyntaxMeta | None = None


SyntaxElement: TypeAlias = SyntaxNode | SyntaxToken


@dataclass(frozen=True)
class SyntaxDocument:
    text: str
    tree: SyntaxNode

    @property
    def forms(self) -> list[SyntaxElement]:
        return list(self.tree.children)


def syntax_from_parser(element: Any) -> SyntaxElement:
    data = getattr(element, "data", None)
    token_type = getattr(element, "type", None)
    if data is not None:
        return SyntaxNode(
            name=data,
            children=tuple(syntax_from_parser(child) for child in element.children),
            meta=SyntaxMeta.from_parser_meta(getattr(element, "meta", None)),
        )
    if token_type is not None:
        return SyntaxToken(
            name=token_type,
            value=element.value,
            meta=SyntaxMeta.from_parser_meta(element),
        )
    msg = f"Unknown syntax element {element!r}"
    raise TypeError(msg)


def syntax_document_from_parser(text: str, tree: Any) -> SyntaxDocument:
    syntax = syntax_from_parser(tree)
    return SyntaxDocument(text=text, tree=cast(SyntaxNode, syntax))
