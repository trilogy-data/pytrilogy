from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from trilogy.core.models.environment import Environment


@contextmanager
def hidden_datasources(environment: Environment, hide: Iterable[str]) -> Iterator[None]:
    """Hide datasources from planning for the scope, stamp-neutrally.

    Probes and refresh steps must plan with part of the environment invisible
    (only roots may answer an expected-side question; a refresh must not read
    through a stale upstream). Popping is the mechanism, but pop/update bump
    the dict's counters unconditionally, and those counters stamp the
    cross-statement planning caches — so a bare pop/restore evicted every
    cached baseline once per probe. Inside the scope the bumped counters plus
    the membership-bearing stamp give the hidden state its own honest cache
    identity; on exit the restore is object-identical, so the counters are
    put back and the full environment's caches survive.

    Restoring counters is only legal when content provably matches the
    entry state — the multi-generation session cache store relies on
    "content restored ⇒ stamp restored" being exact. If anything else wrote
    the dict inside the scope, the counters stay bumped and eviction happens
    honestly. Same threading caveats as the pop pattern it wraps: the hidden
    window is not safe against concurrent planners on the same environment.
    """
    datasources = environment.datasources
    pre = (datasources.mutations, datasources.content_version)
    hidden = {ds_id: datasources.pop(ds_id) for ds_id in hide if ds_id in datasources}
    post_pop = (datasources.mutations, datasources.content_version)
    try:
        yield
    finally:
        untouched = (datasources.mutations, datasources.content_version) == post_pop
        if hidden:
            datasources.update(hidden)
        if untouched:
            datasources.mutations, datasources.content_version = pre
