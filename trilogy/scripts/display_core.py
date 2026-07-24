"""Core display infrastructure: Rich mode, console, shared constants and print helpers."""

import io
import json
import os
import sys
import threading
from collections.abc import Callable
from typing import Any

from click import echo, style


def _needs_safe_wrapping(encoding: "str | None") -> bool:
    """UTF-8 encoders never raise on real strings, so wrapping is only
    useful for narrower codepages (cp1252, ascii, etc.)."""
    if not encoding:
        return False
    norm = encoding.lower().replace("-", "").replace("_", "")
    return norm not in ("utf8", "utf8sig")


def _make_safe_stdout() -> "io.TextIOWrapper | None":
    """Wrap stdout.buffer with errors='replace' so non-encodable characters
    (e.g. '→' on cp1252 Windows consoles) degrade to '?' instead of raising
    UnicodeEncodeError out of the error-display path. Returns None when
    wrapping isn't useful or isn't safe (utf-8 streams, captured streams
    without a binary buffer)."""
    encoding = getattr(sys.stdout, "encoding", None)
    if not _needs_safe_wrapping(encoding):
        return None
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is None:
        return None
    try:
        return io.TextIOWrapper(
            buffer,
            encoding=encoding or "utf-8",
            errors="replace",
            line_buffering=True,
            write_through=True,
        )
    except (AttributeError, ValueError):
        return None


def _make_console() -> "Console | None":
    from rich.console import Console

    safe_file = _make_safe_stdout()
    return Console(file=safe_file) if safe_file is not None else Console()


def _make_error_console() -> "Console | None":
    """Rich console pointed at stderr — used by print_error / print_warning so
    error text doesn't pollute stdout (which gets parsed by tool callers)."""
    from rich.console import Console

    return Console(stderr=True)


try:
    from rich.console import Console

    RICH_AVAILABLE = True
    console: Console | None = _make_console()
    error_console: Console | None = _make_error_console()
except ImportError:
    RICH_AVAILABLE = False
    console = None
    error_console = None

FETCH_LIMIT = 51

# Hard ceiling on rows pulled from the cursor for display. We fetch this
# many regardless of the displayed-rows cap so the renderer can report the
# real total (and middle-truncate against it). Large enough to know the
# total for any plausible analytical result; cheap because the rows never
# render — only the head/tail slice does. Hitting it triggers a loud warning
# so the caller knows their query is bigger than introspection can reach.
DISPLAY_FETCH_CEILING = 1_000_000

# Rich markup styles
STYLE_SUCCESS = "bold green"
STYLE_INFO = "bold blue"
STYLE_WARNING = "bold yellow"
STYLE_ERROR = "bold red"
STYLE_HEADER = "bold magenta"

# Table header styles
HEADER_BLUE = "bold blue"
HEADER_GREEN = "bold green"
HEADER_YELLOW = "bold yellow"

# Table column styles
COL_CYAN = "cyan"
COL_WHITE = "white"
COL_DIM = "dim"
COL_GREEN = "green"


# --- Output format ----------------------------------------------------------
# Two modes: "rich" (the default human-facing rendering) and "json" (a stream
# of pretty-printed JSON event objects, newline-separated, emitted to stdout).
# JSON mode strips all decorative formatting (panels, tables, progress bars,
# colors) so an agent consuming the CLI as a subprocess gets the same
# information with no token-wasting chrome. The env var lets the agent
# subprocess opt in transparently; the CLI ``--format`` flag overrides it.
_ENV_OUTPUT_FORMAT = "TRILOGY_OUTPUT_FORMAT"
_VALID_OUTPUT_FORMATS = ("rich", "json")


def _initial_output_format() -> str:
    raw = (os.environ.get(_ENV_OUTPUT_FORMAT) or "rich").strip().lower()
    return raw if raw in _VALID_OUTPUT_FORMATS else "rich"


OUTPUT_FORMAT = _initial_output_format()


def set_output_format(fmt: "str | None") -> None:
    """Set the active output format. ``None`` re-resolves from the
    ``TRILOGY_OUTPUT_FORMAT`` env var (the default when no CLI flag is passed),
    so each CLI invocation establishes the format fresh rather than inheriting
    whatever a prior in-process invocation left behind."""
    global OUTPUT_FORMAT
    if fmt is None:
        OUTPUT_FORMAT = _initial_output_format()
        return
    fmt = fmt.strip().lower()
    OUTPUT_FORMAT = fmt if fmt in _VALID_OUTPUT_FORMATS else "rich"


def is_json_mode() -> bool:
    return OUTPUT_FORMAT == "json"


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (list, dict))


def _is_row_table(value: Any) -> bool:
    """A 2-D table: a non-empty list whose every element is a flat list of
    scalars (a result row). These are rendered one compact row per line rather
    than exploded cell-by-cell, so a 50-row result stays readable instead of
    ballooning to hundreds of lines."""
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(e, list) and all(_is_scalar(c) for c in e) for e in value)
    )


def _pretty(obj: Any, level: int = 0) -> str:
    """Pretty-print JSON for agent consumption: objects and 1-D lists expand
    one item per line; 2-D row tables stay compact (one row per line). Falls
    back to ``str`` for non-JSON scalars (datetimes, Decimals, paths) so an
    event never fails to serialize."""
    pad, pad2 = "  " * level, "  " * (level + 1)
    if isinstance(obj, dict):
        if not obj:
            return "{}"
        items = [
            f"{pad2}{json.dumps(k)}: {_pretty(v, level + 1)}" for k, v in obj.items()
        ]
        return "{\n" + ",\n".join(items) + f"\n{pad}}}"
    if isinstance(obj, list):
        if not obj:
            return "[]"
        if _is_row_table(obj):
            # Compact separators inside each row — they're already one-per-line,
            # so dropping the ", "/": " padding trims the heaviest response
            # (result rows) with no readability cost.
            rows = [
                f"{pad2}{json.dumps(r, default=str, separators=(',', ':'))}"
                for r in obj
            ]
            return "[\n" + ",\n".join(rows) + f"\n{pad}]"
        items = [f"{pad2}{_pretty(e, level + 1)}" for e in obj]
        return "[\n" + ",\n".join(items) + f"\n{pad}]"
    return json.dumps(obj, default=str)


def emit_event(event: str, *, discriminator: str = "event", **fields: Any) -> None:
    """Write one pretty-printed JSON event object to stdout in JSON mode;
    no-op otherwise. Successive events are newline-separated, so a consumer
    reads them with a streaming decoder (``json.JSONDecoder().raw_decode`` in
    a loop), not by splitting on lines.

    ``discriminator`` names the leading type-tag key (default ``"event"``); the
    explore command passes ``"type"`` instead. ``None``-valued fields are
    dropped to keep the stream compact."""
    if not is_json_mode():
        return
    payload: dict[str, Any] = {discriminator: event}
    for key, value in fields.items():
        if value is not None:
            payload[key] = value
    echo(_pretty(payload))


class SetRichMode:
    """
    Callable class that can be used as both a function and a context manager.

    Regular usage:
        set_rich_mode(True)   # Enable Rich for output formatting for CL
        set_rich_mode(False)  # Disables Rich for output formatting

    Context manager usage:
        with set_rich_mode(True):
            # Rich output mode temporarily disabled
            pass
        # Previous state automatically restored
    """

    def __call__(self, enabled: bool) -> "RichModeContext":
        current = is_rich_available()
        prior = RichModeContext(enabled, current)
        self._set_mode(enabled)
        return prior

    def _set_mode(self, enabled: bool) -> None:
        global RICH_AVAILABLE, console, error_console

        if enabled:
            try:
                RICH_AVAILABLE = True
                console = _make_console()
                error_console = _make_error_console()
            except ImportError:
                RICH_AVAILABLE = False
                console = None
                error_console = None
        else:
            RICH_AVAILABLE = False
            console = None
            error_console = None


class RichModeContext:
    """Context manager returned by SetRichMode for 'with' statement usage."""

    def __init__(self, enabled: bool, current: bool):
        self.enabled = enabled
        self.old_rich_available = current
        self.old_console: Console | None = None
        self.old_error_console: Console | None = None

    def __enter__(self) -> "RichModeContext":
        global RICH_AVAILABLE, console, error_console

        self.old_console = console
        self.old_error_console = error_console
        # The mode was already set by __call__, so we're good
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        global RICH_AVAILABLE, console, error_console

        # Restore previous state
        RICH_AVAILABLE = self.old_rich_available
        console = self.old_console
        error_console = self.old_error_console


set_rich_mode = SetRichMode()


def is_rich_available() -> bool:
    """Check if Rich mode is currently available."""
    return RICH_AVAILABLE


def _print_styled(
    message: str, rich_style: str, click_fg: str, err: bool = False
) -> None:
    """``err=True`` routes the message to stderr — used by print_error /
    print_warning so tool-result consumers (agent subprocess capture, shell
    pipelines, CI logs) can distinguish failure text from regular output. Rich
    routes via a stderr Console; click ``echo`` accepts ``err=True`` directly."""
    try:
        if RICH_AVAILABLE and console is not None:
            # err path uses error_console (stderr); the test fixture that
            # toggles RICH_AVAILABLE on without rebuilding consoles can land
            # error_console=None — fall back to the stdout console there.
            target = error_console if (err and error_console is not None) else console
            target.print(message, style=rich_style)
        else:
            echo(style(message, fg=click_fg, bold=True), err=err)
    except UnicodeEncodeError:
        # Belt-and-suspenders: if a stream still can't encode the message,
        # fall back to ASCII so an unencodable char in an error message
        # (e.g. '→' on cp1252) never masks the original error.
        safe = message.encode("ascii", "replace").decode("ascii")
        echo(safe, err=err)


def _emit_or_style(
    level: str, message: str, rich_style: str, click_fg: str, err: bool = False
) -> None:
    """In JSON mode emit the message as a ``{event: level, message}`` line;
    otherwise fall back to the styled rich/click rendering. Leading/trailing
    blank lines used for rich spacing are stripped from the JSON message."""
    if is_json_mode():
        emit_event(level, message=message.strip())
        return
    _print_styled(message, rich_style, click_fg, err=err)


def print_success(message: str) -> None:
    _emit_or_style("success", message, STYLE_SUCCESS, "green")


def print_info(message: str) -> None:
    _emit_or_style("info", message, STYLE_INFO, "blue")


def print_warning(message: str) -> None:
    _emit_or_style("warning", message, STYLE_WARNING, "yellow", err=True)


def print_error(message: str) -> None:
    _emit_or_style("error", message, STYLE_ERROR, "red", err=True)


def print_header(message: str) -> None:
    _emit_or_style("header", message, STYLE_HEADER, "magenta")


def format_duration(duration: Any) -> str:
    """Format a timedelta duration as a human-readable string."""
    total_seconds = duration.total_seconds()
    if total_seconds < 1:
        return f"{total_seconds*1000:.0f}ms"
    elif total_seconds < 60:
        return f"{total_seconds:.2f}s"
    else:
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}m {seconds:.2f}s"


def with_status(message: str) -> Any:
    """Context manager for showing status."""
    if is_json_mode():
        # Transient status spinners are pure chrome — drop them in JSON mode.
        return _DummyContext()
    if RICH_AVAILABLE and console is not None:
        return console.status(f"[bold green]{message}...")
    else:
        print_info(f"{message}...")
        return _DummyContext()


class _DummyContext:
    """Dummy context manager for fallback."""

    def __enter__(self) -> "_DummyContext":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FdStderrCapture:
    """Redirect OS-level fd 2 to a pipe so subprocess stderr is routed through
    the Rich console instead of tearing the progress bar display.

    Rich's redirect_stderr only patches sys.stderr (Python level); subprocesses
    write directly to fd 2 and bypass it.  This captures at the fd level.
    """

    def __init__(self, get_context: "Callable[[], str] | None" = None) -> None:
        self._orig_fd: int | None = None
        self._thread: threading.Thread | None = None
        self.get_context = get_context

    def __enter__(self) -> "_FdStderrCapture":
        if not (RICH_AVAILABLE and console is not None):
            return self
        self._orig_fd = os.dup(2)
        read_fd, write_fd = os.pipe()
        os.dup2(write_fd, 2)
        os.close(write_fd)
        self._thread = threading.Thread(
            target=self._drain, args=(read_fd,), daemon=True
        )
        self._thread.start()
        return self

    def _drain(self, read_fd: int) -> None:
        buf = b""
        while True:
            try:
                chunk = os.read(read_fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                text = line.decode("utf-8", errors="replace").rstrip()
                if text and console is not None:
                    if self.get_context:
                        ctx = self.get_context()
                        if ctx:
                            text = f"[dim]\\[{ctx}][/dim] {text}"
                    console.print(text)
        if buf and console is not None:
            text = buf.decode("utf-8", errors="replace").rstrip()
            if text:
                if self.get_context:
                    ctx = self.get_context()
                    if ctx:
                        text = f"[dim]\\[{ctx}][/dim] {text}"
                console.print(text)
        try:
            os.close(read_fd)
        except OSError:
            pass

    def __exit__(self, *args: object) -> None:
        if self._orig_fd is None:
            return
        # Restoring fd 2 closes the pipe write end → EOF on read end → thread exits
        os.dup2(self._orig_fd, 2)
        os.close(self._orig_fd)
        self._orig_fd = None
        if self._thread is not None:
            self._thread.join()
            self._thread = None
