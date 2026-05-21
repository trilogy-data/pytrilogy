"""Lazy accessor for networkx, an optional dependency (the ``cli`` extra).

Importing this module never imports networkx; the first attribute access on
``nx`` triggers the import and raises a clear, actionable error if networkx
is not installed. CLI commands that build dependency graphs import ``nx``
from here so that ``import trilogy`` and unrelated CLI commands stay free of
the networkx dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import networkx as nx
else:

    class _LazyNetworkX:
        _module = None

        def __getattr__(self, name: str):
            if _LazyNetworkX._module is None:
                try:
                    import networkx
                except ImportError as exc:
                    raise ImportError(
                        "networkx is required for this command; install it with "
                        "'pip install pytrilogy[cli]'"
                    ) from exc
                _LazyNetworkX._module = networkx
            return getattr(_LazyNetworkX._module, name)

    nx = _LazyNetworkX()
