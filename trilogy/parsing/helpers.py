from __future__ import annotations

from trilogy.core.models.author import Comment


def comment_body(comment: Comment) -> str:
    """Return comment text without the ``#`` or ``//`` prefix."""
    text = comment.text.lstrip()
    if text.startswith("//"):
        return text[2:].rstrip()
    if text.startswith("#"):
        return text[1:].rstrip()
    return text.rstrip()
