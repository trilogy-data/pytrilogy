from __future__ import annotations

# Distinct from EX_SOFTWARE/1 so a consumer can tell "the script's own logic
# failed" from "uv could not resolve dependencies", which exits 1 or 2.
SCRIPT_ERROR_EXIT_CODE = 65

ERROR_PREFIX = "trilogy-io-error: "

# A source run as a VGI worker (``--vgi``) has no exit code or stderr of its
# own, so it reports both as one JSON object after this marker in the error
# DuckDB relays; the consumer rebuilds the failure from it.
VGI_FAILURE_MARKER = "trilogy-script-failure"
VGI_FLAG = "--vgi"


class TrilogyIOError(Exception):
    """A source function's output could not be turned into an Arrow stream."""


class ContractError(TrilogyIOError):
    """The request a caller passed on the command line was not usable."""
