"""Opt-in lying-declaration validation (docs/subset_union_join_design.md):
the planner trusts SUBSET/EQUAL declarations, so `validate_domains` is the
cheap data check surfacing declarations the data violates. Checks run on a
CLEAN executor (no active merges — an active merge collapses the concepts and
makes the containment self-referential); declarations are read separately."""

from pathlib import Path

from tests.join_matrix.harness import make_engine, write_models
from tests.join_matrix.test_narrowing_matrix import _write_models as write_honest
from trilogy.core.domain_validation import (
    declared_domain_relations,
    generate_domain_checks,
    validate_domains,
)

HEAD = "import left_fact as a;\nimport right_fact as b;\n"


def _relations(workdir: Path, merge: str):
    declaring = make_engine(workdir)
    declaring.parse_text(HEAD + merge)
    return declared_domain_relations(declaring.environment)


def test_subset_violation_detected(tmp_path: Path):
    # adversarial rows: right has key 4 absent from left, so `merge b.r_key
    # into ~a.l_key` (b ⊆ a) is a lie with exactly one violating value.
    workdir = write_models(tmp_path)
    relations = _relations(workdir, "merge b.r_key into ~a.l_key;")
    assert [r.declaration for r in relations] == ["subset"]
    # DIRECTION: the declared subset side is the checked source — the lie is
    # a b-side value missing from a, never the reverse (the adversarial data
    # has one exclusive value per side, so a reversed check also counts 1;
    # only the source address distinguishes them).
    assert relations[0].source == "b.r_key"
    assert relations[0].target == "a.l_key"
    clean = make_engine(workdir)
    clean.parse_text(HEAD)
    violations = validate_domains(clean, relations)
    assert len(violations) == 1
    assert violations[0].violating_values == 1
    assert violations[0].check.source == "b.r_key"


def test_equal_violation_detected_both_directions(tmp_path: Path):
    # adversarial rows violate EQUAL in both directions (left-only 3,
    # right-only 4).
    workdir = write_models(tmp_path)
    relations = _relations(workdir, "merge a.l_key into b.r_key;")
    checks = generate_domain_checks(relations)
    assert [c.declaration for c in checks] == ["equal", "equal"]
    clean = make_engine(workdir)
    clean.parse_text(HEAD)
    violations = validate_domains(clean, relations)
    assert len(violations) == 2
    assert all(v.violating_values == 1 for v in violations)


def test_honest_declarations_pass(tmp_path: Path):
    workdir = write_honest(tmp_path)
    relations = _relations(workdir, "merge a.l_key into b.r_key;")
    clean = make_engine(workdir)
    clean.parse_text(HEAD)
    assert validate_domains(clean, relations) == []
