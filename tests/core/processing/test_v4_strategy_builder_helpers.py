"""`_concept_at` resolves a (possibly pseudonym) group-member address to the
concept the strategy builder should project: the exact `concepts` match when it
agrees, otherwise the `alias_origin_lookup` origin, otherwise whatever
`concepts` returned (a mismatched/missing entry)."""

from types import SimpleNamespace

from trilogy.core.processing.v4_helper.strategy_builder import _concept_at


def _env(concepts=None, origins=None) -> SimpleNamespace:
    return SimpleNamespace(
        concepts=concepts or {},
        alias_origin_lookup=origins or {},
    )


def test_exact_concepts_match_wins():
    canonical = SimpleNamespace(address="local.a")
    env = _env(concepts={"local.a": canonical})
    assert _concept_at(env, "local.a") is canonical


def test_origin_used_when_concepts_miss():
    origin = SimpleNamespace(address="s.a")
    env = _env(origins={"s.a": origin})
    assert _concept_at(env, "s.a") is origin


def test_origin_preferred_over_address_mismatch():
    mismatched = SimpleNamespace(address="local.a")
    origin = SimpleNamespace(address="s.a")
    env = _env(concepts={"s.a": mismatched}, origins={"s.a": origin})
    assert _concept_at(env, "s.a") is origin


def test_falls_back_to_mismatched_concept_when_no_origin():
    mismatched = SimpleNamespace(address="local.a")
    env = _env(concepts={"s.a": mismatched})
    assert _concept_at(env, "s.a") is mismatched


def test_returns_none_when_unknown():
    assert _concept_at(_env(), "missing") is None
