"""Error rendering: the message a user reads must be the message that was
authored. These exceptions carry regexes, newlines, and declared types, all of
which a stray repr() would escape into unreadability."""

import pytest

from trilogy.core.enums import Modifier
from trilogy.core.exceptions import (
    AmbiguousRelationshipResolutionException,
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
    DisconnectedConceptsException,
    ModelValidationError,
    UndefinedConceptException,
    render_datatype,
)
from trilogy.core.models.core import DataType, TraitDataType, ValidatedType

URL_DOMAIN = ValidatedType(type=DataType.STRING, pattern=r"\S+://\S+")
URL_IMAGE = TraitDataType(type=URL_DOMAIN, traits=["url_image"])


def _binding(**over) -> DatasourceColumnBindingData:
    base = {
        "address": "local.image",
        "value": "not-a-url",
        "value_type": DataType.STRING,
        "value_modifiers": [],
        "actual_type": URL_IMAGE,
        "actual_modifiers": [],
    }
    base.update(over)
    return DatasourceColumnBindingData(**base)  # type: ignore[arg-type]


class TestRenderDatatype:
    def test_authoring_syntax_not_the_dataclass_repr(self):
        assert render_datatype(URL_IMAGE) == r"string['\S+://\S+']::url_image"

    def test_a_bare_type_renders_as_its_keyword(self):
        assert render_datatype(DataType.INTEGER) == "int"

    def test_a_renderer_failure_never_masks_the_error_being_reported(self, monkeypatch):
        import trilogy.parsing.render as render_mod

        def boom(self, *args, **kwargs):
            raise RuntimeError("renderer is broken")

        monkeypatch.setattr(render_mod.Renderer, "to_string", boom)
        assert render_datatype(URL_IMAGE) == str(URL_IMAGE)


class TestColumnBindingFailure:
    def test_a_domain_violation_shows_the_domain_that_was_violated(self):
        message = _binding().format_failure()
        assert r"violates declared domain string['\S+://\S+']::url_image" in message
        assert "'not-a-url'" in message

    def test_a_type_mismatch_names_both_sides(self):
        message = _binding(value=1, value_type=DataType.INTEGER).format_failure()
        assert "has inferred type" in message
        assert "vs expected type string['\\S+://\\S+']::url_image" in message

    def test_a_null_value_is_a_type_mismatch_not_a_domain_violation(self):
        assert "has inferred type" in _binding(value=None).format_failure()

    def test_modifiers_are_appended_to_each_side(self):
        message = _binding(
            value=1,
            value_type=DataType.INTEGER,
            value_modifiers=[Modifier.NULLABLE],
            actual_type=DataType.INTEGER,
            actual_modifiers=[Modifier.PARTIAL],
        ).format_failure()
        assert "(NULLABLE)" in message and "(PARTIAL)" in message

    def test_a_nullable_only_mismatch_stays_legible(self):
        assert (
            _binding(
                value=1,
                value_type=DataType.INTEGER,
                value_modifiers=[Modifier.NULLABLE],
                actual_type=DataType.INTEGER,
            )
            .format_failure()
            .endswith("vs expected type int")
        )

    def test_is_modifier_issue_only_when_the_value_carries_an_undeclared_one(self):
        assert _binding(value_modifiers=[Modifier.NULLABLE]).is_modifier_issue()
        assert not _binding().is_modifier_issue()
        assert not _binding(
            value_modifiers=[Modifier.NULLABLE],
            actual_modifiers=[Modifier.NULLABLE],
        ).is_modifier_issue()

    def test_is_type_issue_compares_declared_against_observed(self):
        assert _binding(value_type=DataType.INTEGER).is_type_issue()
        assert not _binding(value_type=URL_IMAGE).is_type_issue()


class TestErrorMessages:
    def test_binding_errors_are_listed_one_per_line_unescaped(self):
        exc = DatasourceColumnBindingError("images", [_binding(), _binding()])
        assert str(exc).startswith("Datasource images failed validation.")
        lines = str(exc).splitlines()[1:]
        assert len(lines) == 2
        assert all(line.startswith("  value 'not-a-url'") for line in lines)
        assert r"\S+://\S+" in lines[0]

    def test_an_explicit_message_is_used_verbatim(self):
        exc = DatasourceColumnBindingError("images", [_binding()], message="custom")
        assert str(exc) == "custom"
        assert exc.dataset_address == "images" and len(exc.errors) == 1

    def test_str_carries_only_the_message(self):
        exc = ModelValidationError("regex \\S+ failed\non line 2")
        assert str(exc) == "regex \\S+ failed\non line 2"
        assert exc.children is None

    def test_children_are_retained(self):
        child = ModelValidationError("inner")
        assert ModelValidationError("outer", [child]).children == [child]

    @pytest.mark.parametrize(
        "exc",
        [
            UndefinedConceptException("no such concept: x", ["y", "z"]),
            AmbiguousRelationshipResolutionException("two ways", [{"a"}, {"b"}]),
            DisconnectedConceptsException("two islands", [["a"], ["b"]]),
        ],
    )
    def test_message_is_not_repr_wrapped(self, exc):
        assert str(exc) == exc.message
        assert "(" not in str(exc)

    def test_undefined_concept_keeps_its_suggestions(self):
        assert UndefinedConceptException("m", ["y", "z"]).suggestions == ["y", "z"]

    def test_ambiguous_resolution_keeps_its_candidate_parents(self):
        exc = AmbiguousRelationshipResolutionException("m", [{"a"}, {"b"}])
        assert exc.parents == [{"a"}, {"b"}]

    def test_disconnected_subgraphs_are_normalized_to_lists(self):
        exc = DisconnectedConceptsException("m", (("a", "b"), ("c",)))
        assert exc.subgraphs == [["a", "b"], ["c"]]
