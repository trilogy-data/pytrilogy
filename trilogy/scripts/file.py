"""File command for Trilogy CLI - CRUD+ operations over pluggable storage backends."""

from __future__ import annotations

import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import click

from trilogy.scripts.display import print_error, print_info, print_success
from trilogy.scripts.file_helpers import (
    FileNotFoundError,
    FileOperationError,
    get_backend,
)

_FETCH_SCHEMES = {"http", "https", "file"}


def _fetch_url(url: str, timeout: float = 30.0) -> bytes:
    scheme = urlparse(url).scheme
    if scheme not in _FETCH_SCHEMES:
        print_error(f"Unsupported --from-url scheme: {scheme!r}.")
        raise click.exceptions.Exit(2)
    req = Request(url, headers={"User-Agent": "trilogy-cli"})
    try:
        with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - scheme gated above
            return resp.read()
    except (HTTPError, URLError, TimeoutError) as exc:
        print_error(f"Failed to fetch {url}: {exc}")
        raise click.exceptions.Exit(2) from exc


def _resolve(path: str):
    try:
        return get_backend(path)
    except FileOperationError as exc:
        print_error(str(exc))
        raise click.exceptions.Exit(2) from exc


def _fail(exc: Exception, code: int = 1) -> None:
    print_error(str(exc))
    raise click.exceptions.Exit(code) from exc


def _format_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size = int(size / 1024)
    return f"{size}B"


@click.group()
def file() -> None:
    """Create, read, update, and delete files against local or remote backends.

    The same commands work against any backend Trilogy knows about. Only the
    local filesystem ships today; future releases will add cloud storage and
    remote git model backends, so write CLI-friendly scripts (and agent loops)
    against ``trilogy file`` instead of ad-hoc shell or python plumbing.
    """


@file.command("list")
@click.argument("path", type=str, required=False, default=".")
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="Recurse into subdirectories.",
)
@click.option(
    "--long",
    "-l",
    "long_format",
    is_flag=True,
    default=False,
    help="Show size and entry kind alongside the path.",
)
def list_cmd(path: str, recursive: bool, long_format: bool) -> None:
    """List files at PATH (default: current directory)."""
    backend = _resolve(path)
    try:
        entries = backend.list(path, recursive=recursive)
    except FileNotFoundError as exc:
        _fail(exc, code=1)
    except FileOperationError as exc:
        _fail(exc, code=2)

    if not entries:
        print_info(f"No entries at {path}")
        return

    for entry in entries:
        if long_format:
            kind = "DIR " if entry.is_dir else "FILE"
            click.echo(f"{kind}  {_format_size(entry.size):>8}  {entry.path}")
        else:
            suffix = "/" if entry.is_dir else ""
            click.echo(f"{entry.path}{suffix}")


@file.command("read")
@click.argument("path", type=str)
def read_cmd(path: str) -> None:
    """Read the file at PATH and write its contents to stdout."""
    backend = _resolve(path)
    try:
        data = backend.read(path)
    except FileNotFoundError as exc:
        _fail(exc, code=1)
    except FileOperationError as exc:
        _fail(exc, code=2)

    sys.stdout.buffer.write(data)
    if data and not data.endswith(b"\n"):
        sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()


@file.command("write")
@click.argument("path", type=str)
@click.option(
    "--content",
    "-c",
    type=str,
    default=None,
    help="Inline content. If omitted, reads from stdin.",
)
@click.option(
    "--from-file",
    "from_file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    default=None,
    help="Copy bytes from a local file instead of stdin/--content.",
)
@click.option(
    "--from-url",
    "from_url",
    type=str,
    default=None,
    help="Fetch bytes from an http(s):// or file:// URL (e.g. a raw GitHub gist).",
)
@click.option(
    "--escapes",
    "-e",
    is_flag=True,
    default=False,
    help="Interpret backslash escapes (\\n, \\t, \\\\, \\xNN) in --content.",
)
@click.option(
    "--no-create",
    is_flag=True,
    default=False,
    help="Fail if the target file does not already exist (update only).",
)
@click.option(
    "--quiet", "-q", is_flag=True, default=False, help="Suppress success output."
)
def write_cmd(
    path: str,
    content: str | None,
    from_file: str | None,
    from_url: str | None,
    escapes: bool,
    no_create: bool,
    quiet: bool,
) -> None:
    """Write/overwrite the file at PATH.

    Content is taken from --content, --from-file, --from-url, or stdin (in
    that order). Use --escapes with --content to embed newlines as \\n in a
    single-line string — useful on shells without heredoc support (cmd.exe,
    some CI). --from-url accepts http(s):// and file:// URLs, e.g. a raw
    GitHub gist — the README's "copy this snippet" flow.
    """
    sources = [s for s in (content, from_file, from_url) if s is not None]
    if len(sources) > 1:
        print_error("Pass at most one of --content, --from-file, or --from-url.")
        raise click.exceptions.Exit(2)
    if escapes and content is None:
        print_error("--escapes only applies to --content.")
        raise click.exceptions.Exit(2)

    backend = _resolve(path)

    if no_create and not backend.exists(path):
        print_error(f"Refusing to create {path} (--no-create).")
        raise click.exceptions.Exit(1)

    if content is not None:
        if escapes:
            try:
                content = content.encode("utf-8").decode("unicode_escape")
            except UnicodeDecodeError as exc:
                print_error(f"Invalid escape sequence: {exc}")
                raise click.exceptions.Exit(2) from exc
        data = content.encode("utf-8")
    elif from_file is not None:
        with open(from_file, "rb") as fh:
            data = fh.read()
    elif from_url is not None:
        data = _fetch_url(from_url)
    else:
        data = sys.stdin.buffer.read()

    try:
        backend.write(path, data)
    except FileOperationError as exc:
        _fail(exc, code=2)

    if not quiet:
        print_success(f"Wrote {len(data)} byte(s) to {path}")


@file.command("delete")
@click.argument("path", type=str)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="Recursively delete directories.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Do not error if the path does not exist.",
)
def delete_cmd(path: str, recursive: bool, force: bool) -> None:
    """Delete the file or directory at PATH."""
    backend = _resolve(path)
    try:
        backend.delete(path, recursive=recursive)
    except FileNotFoundError as exc:
        if force:
            return
        _fail(exc, code=1)
    except FileOperationError as exc:
        _fail(exc, code=2)
    print_success(f"Deleted {path}")


@file.command("move")
@click.argument("src", type=str)
@click.argument("dst", type=str)
def move_cmd(src: str, dst: str) -> None:
    """Move (or rename) SRC to DST."""
    src_backend = _resolve(src)
    dst_backend = _resolve(dst)
    if type(src_backend) is not type(dst_backend):
        print_error("Cross-backend moves are not supported yet.")
        raise click.exceptions.Exit(2)
    try:
        src_backend.move(src, dst)
    except FileNotFoundError as exc:
        _fail(exc, code=1)
    except FileOperationError as exc:
        _fail(exc, code=2)
    print_success(f"Moved {src} -> {dst}")


@file.command("exists")
@click.argument("path", type=str)
def exists_cmd(path: str) -> None:
    """Exit 0 if PATH exists, 1 otherwise."""
    backend = _resolve(path)
    if backend.exists(path):
        click.echo("true")
        return
    click.echo("false")
    raise click.exceptions.Exit(1)
