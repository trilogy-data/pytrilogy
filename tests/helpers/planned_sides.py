import pytest

from trilogy.core.processing.nodes import merge_node


def record_planned_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> list[list[frozenset[str] | None]]:
    """The planned groups each joined source of every planned merge holds."""
    stated: list[list[frozenset[str] | None]] = []
    planned_source_pairs = merge_node._planned_source_pairs

    def record(merged, held, planned):
        if planned is not None:
            stated.append([held[key] for key in merged])
        return planned_source_pairs(merged, held, planned)

    monkeypatch.setattr(merge_node, "_planned_source_pairs", record)
    return stated


def every_side_planned(stated: list[list[frozenset[str] | None]]) -> bool:
    return bool(stated) and all(groups for sides in stated for groups in sides)
