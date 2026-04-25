"""Core display infrastructure: Rich mode, console, shared constants and print helpers."""

import io
import os
import sys
import threading
from typing import Any, Callable, Optional

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


try:
    from rich.console import Console

    RICH_AVAILABLE = True
    console: Optional[Console] = _make_console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

FETCH_LIMIT = 51

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
        global RICH_AVAILABLE, console

        if enabled:
            try:
                RICH_AVAILABLE = True
                console = _make_console()
            except ImportError:
                RICH_AVAILABLE = False
                console = None
        else:
            RICH_AVAILABLE = False
            console = None


class RichModeContext:
    """Context manager returned by SetRichMode for 'with' statement usage."""

    def __init__(self, enabled: bool, current: bool):
        self.enabled = enabled
        self.old_rich_available = current
        self.old_console: Optional["Console"] = None

    def __enter__(self) -> "RichModeContext":
        global RICH_AVAILABLE, console

        self.old_console = console
        # The mode was already set by __call__, so we're good
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        global RICH_AVAILABLE, console

        # Restore previous state
        RICH_AVAILABLE = self.old_rich_available
        console = self.old_console


set_rich_mode = SetRichMode()


def is_rich_available() -> bool:
    """Check if Rich mode is currently available."""
    return RICH_AVAILABLE


def _print_styled(message: str, rich_style: str, click_fg: str) -> None:
    try:
        if RICH_AVAILABLE and console is not None:
            console.print(message, style=rich_style)
        else:
            echo(style(message, fg=click_fg, bold=True))
    except UnicodeEncodeError:
        # Belt-and-suspenders: if a stream still can't encode the message,
        # fall back to ASCII so an unencodable char in an error message
        # (e.g. '→' on cp1252) never masks the original error.
        safe = message.encode("ascii", "replace").decode("ascii")
        echo(safe)


def print_success(message: str) -> None:
    _print_styled(message, STYLE_SUCCESS, "green")


def print_info(message: str) -> None:
    _print_styled(message, STYLE_INFO, "blue")


def print_warning(message: str) -> None:
    _print_styled(message, STYLE_WARNING, "yellow")


def print_error(message: str) -> None:
    _print_styled(message, STYLE_ERROR, "red")


def print_header(message: str) -> None:
    _print_styled(message, STYLE_HEADER, "magenta")


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
    if RICH_AVAILABLE and console is not None:
        return console.status(f"[bold green]{message}...")
    else:
        print_info(f"{message}...")
        return _DummyContext()


class _DummyContext:
    """Dummy context manager for fallback."""

    def __enter__(self) -> "_DummyContext":
        return self

    def __exit__(self, *args: Any) -> None:
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

    def __exit__(self, *args: Any) -> None:
        if self._orig_fd is None:
            return
        # Restoring fd 2 closes the pipe write end → EOF on read end → thread exits
        os.dup2(self._orig_fd, 2)
        os.close(self._orig_fd)
        self._orig_fd = None
        if self._thread is not None:
            self._thread.join()
            self._thread = None
