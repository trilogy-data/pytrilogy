from __future__ import annotations

# Distinct from EX_SOFTWARE/1 so a consumer can tell "the script's own logic
# failed" from "uv could not resolve dependencies", which exits 1 or 2.
SCRIPT_ERROR_EXIT_CODE = 65

ERROR_PREFIX = "trilogy-io-error: "


class TrilogyIOError(Exception):
    """A source function's output could not be turned into an Arrow stream."""


class ContractError(TrilogyIOError):
    """The request a caller passed on the command line was not usable."""
