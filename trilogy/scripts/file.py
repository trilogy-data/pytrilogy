"""File command for Trilogy CLI - CRUD+ operations over pluggable storage backends."""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import click

from trilogy.scripts.display import (
    emit_event,
    is_json_mode,
    print_error,
    print_info,
    print_success,
)
from trilogy.scripts.file_helpers import (
    LIST_MAX_ENTRIES,
    FileNotFoundError,
    FileOperationError,
    get_backend,
    preql_description,
)
from trilogy.scripts.file_helpers.preql_validation import validate_preql_content

_FETCH_SCHEMES = {"http", "https", "file"}


def _last_statement_sql(path: str) -> str | None:
    """Best-effort compile of a just-written ``.preql`` to the SQL of its last
    generatable statement, surfaced to agents so they can inspect the codegen
    before running. Returns None (never raises) when no engine is configured,
    the file has no output statement, or compilation fails — the write itself
    has already succeeded and been syntax-validated."""
    import contextlib
    import io

    try:
        from trilogy.scripts.common import create_executor, get_runtime_config

        resolved = Path(path).resolve()
        directory = resolved.parent
        config = get_runtime_config(directory)
        if not config.engine_dialect:
            return None
        # Swallow executor build/startup chatter (JSON events, startup prints);
        # only the caller's own event should carry the SQL.
        with contextlib.redirect_stdout(io.StringIO()):
            executor = create_executor(
                param=(),
                directory=directory,
                conn_args=(),
                edialect=config.engine_dialect,
                debug=False,
                config=config,
            )
            statements = executor.parse_file(resolved)
            if not statements:
                return None
            sql = executor.generate_sql(statements[-1])
        return sql[-1] if sql else None
    except Exception:
        return None


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
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    default=False,
    help="Show every entry (default lists only .preql files + directories).",
)
def list_cmd(path: str, recursive: bool, long_format: bool, show_all: bool) -> None:
    """List files at PATH (default: current directory).

    The Trilogy-focused default surfaces just the ``.preql`` model files and
    their leading description comments — the same one-line purpose strings
    the agent's ``list_files`` tool shows. Pass ``--all`` for every entry.
    """
    backend = _resolve(path)
    try:
        entries = backend.list(path, recursive=recursive, max_entries=LIST_MAX_ENTRIES)
    except FileNotFoundError as exc:
        _fail(exc, code=1)
    except FileOperationError as exc:
        _fail(exc, code=2)

    # The backend caps its return at LIST_MAX_ENTRIES to stay memory-safe; a
    # full cap means there may be more entries we never enumerated.
    truncated = len(entries) >= LIST_MAX_ENTRIES

    if not show_all:
        entries = [e for e in entries if e.is_dir or e.path.endswith(".preql")]

    if is_json_mode():
        emit_event(
            "entries",
            path=path,
            count=len(entries),
            truncated=truncated,
            entries=[
                {
                    "path": entry.path,
                    "size": entry.size,
                    "description": (
                        None
                        if entry.is_dir
                        else preql_description.read_preql_description(Path(entry.path))
                    ),
                }
                for entry in entries
            ],
        )
        return

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
        if not entry.is_dir:
            desc_line = preql_description.format_preql_description(Path(entry.path))
            if desc_line:
                click.echo(desc_line)

    if truncated:
        print_info(
            f"... listing capped at {LIST_MAX_ENTRIES} entries; "
            "narrow the path or drop --recursive to see more."
        )


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

    if is_json_mode():
        emit_event(
            "file",
            path=path,
            bytes=len(data),
            content=data.decode("utf-8", errors="replace"),
        )
        return

    sys.stdout.buffer.write(data)
    if data and not data.endswith(b"\n"):
        sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()


# Sentinel `flag_value` for `--content`/`-c`: distinguishes "flag absent" (None →
# stdin) from "flag given with no value" (also stdin). Lets `-c` be passed bare
# alongside stdin without click's "Option '-c' requires an argument" error, while
# `-c <value>` still binds the value normally.
_CONTENT_FROM_STDIN = "\x00__content_from_stdin__\x00"


@file.command("write")
@click.argument("path", type=str)
@click.option(
    "--content",
    "-c",
    type=str,
    default=None,
    is_flag=False,
    flag_value=_CONTENT_FROM_STDIN,
    help="Inline content. Omit the flag entirely, or pass `-c` with no value, "
    "to read from stdin instead.",
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
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Skip Trilogy syntax validation for .preql files (writes raw bytes).",
)
@click.option(
    "--quiet", "-q", is_flag=True, default=False, help="Suppress success output."
)
@click.option(
    "--show-sql",
    is_flag=True,
    default=False,
    help="For .preql targets, compile and emit the last statement's generated "
    "SQL alongside the write confirmation.",
)
def write_cmd(
    path: str,
    content: str | None,
    from_file: str | None,
    from_url: str | None,
    escapes: bool,
    no_create: bool,
    force: bool,
    quiet: bool,
    show_sql: bool,
) -> None:
    """Write/overwrite the file at PATH.

    Content is taken from --content, --from-file, --from-url, or stdin (in
    that order). Use --escapes with --content to embed newlines as \\n in a
    single-line string — useful on shells without heredoc support (cmd.exe,
    some CI). --from-url accepts http(s):// and file:// URLs, e.g. a raw
    GitHub gist — the README's "copy this snippet" flow.

    For .preql targets the content is parsed and rejected if invalid so a
    truncated or HTML-escaped body can't land silently. Pass --force to skip
    that check (e.g. partial drafts you intend to edit in place).
    """
    # A bare `-c`/`--content` (no value) means "read from stdin" — normalise it to
    # None so it flows through the same stdin path as omitting the flag entirely.
    bare_content_flag = content == _CONTENT_FROM_STDIN
    if bare_content_flag:
        content = None

    sources = [s for s in (content, from_file, from_url) if s is not None]
    if len(sources) > 1:
        print_error("Pass at most one of --content, --from-file, or --from-url.")
        raise click.exceptions.Exit(2)
    if escapes and content is None:
        # `--escapes` interprets `\n`/`\t` in an INLINE `--content` value — its
        # whole purpose is no-heredoc shells. stdin already carries real newlines,
        # so `-e` with stdin (incl. a bare `-c`) is a no-op the user didn't intend.
        print_error(
            "--escapes only applies to an inline `--content <value>`; stdin "
            "already supports real newlines, so drop `-e` when piping content."
        )
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

    if not force and path.endswith(".preql"):
        try:
            preql_text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            print_error(
                f"Refusing to write '{path}': content is not valid UTF-8 "
                f"({exc}). Pass --force to skip validation."
            )
            raise click.exceptions.Exit(1) from exc
        refusal = validate_preql_content(path, preql_text)
        if refusal:
            print_error(refusal)
            raise click.exceptions.Exit(1)

    try:
        backend.write(path, data)
    except FileOperationError as exc:
        _fail(exc, code=2)

    if quiet:
        return
    # Optionally feed the just-written query's codegen back to the caller so an
    # agent can inspect the generated SQL (e.g. spot a degenerate GROUP BY)
    # before running. Off by default — compiling adds latency and tokens.
    last_sql = (
        _last_statement_sql(path)
        if show_sql and not force and path.endswith(".preql")
        else None
    )
    if is_json_mode():
        emit_event("write", path=path, bytes=len(data), last_statement_sql=last_sql)
    else:
        print_success(f"Wrote {len(data)} byte(s) to {path}")
        if last_sql:
            print_info(f"Generated SQL (last statement):\n{last_sql}")


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
    found = backend.exists(path)
    if is_json_mode():
        emit_event("exists", path=path, exists=found)
    else:
        click.echo("true" if found else "false")
    if not found:
        raise click.exceptions.Exit(1)
