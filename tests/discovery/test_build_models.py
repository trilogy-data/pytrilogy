from trilogy.core.enums import Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    CanonicalBuildConceptList,
    LooseBuildConceptList,
)
from trilogy.core.models.core import DataType


def test_loose_concept_list():
    # Create test concepts
    concept1 = BuildConcept(
        name="test1",
        canonical_name="test1_canonical",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    concept2 = BuildConcept(
        name="test2",
        canonical_name="test2_canonical",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=True,
        grain=BuildGrain(),
    )
    concept3 = BuildConcept(
        name="test3",
        canonical_name="test3_canonical",
        datatype=DataType.FLOAT,
        purpose=Purpose.METRIC,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )

    # Test initialization and basic properties
    lcl1 = LooseBuildConceptList([concept1, concept2])
    lcl2 = LooseBuildConceptList([concept2, concept3])
    lcl3 = LooseBuildConceptList([concept1, concept2])  # Same as lcl1

    # Test addresses property
    assert len(lcl1.addresses) == 2
    assert concept1.address in lcl1.addresses
    assert concept2.address in lcl1.addresses

    # Test sorted_addresses property
    assert lcl1.sorted_addresses == sorted([concept1.address, concept2.address])

    # Test string representation
    assert str(lcl1).startswith("lcl")

    # Test iteration
    concepts = list(lcl1)
    assert len(concepts) == 2
    assert concept1 in concepts
    assert concept2 in concepts

    # Test equality
    assert lcl1 == lcl3
    assert lcl1 != lcl2
    assert lcl1 != "not a list"

    # Test issubset
    lcl_subset = LooseBuildConceptList([concept1])
    assert lcl_subset.issubset(lcl1)
    assert not lcl1.issubset(lcl_subset)
    assert not lcl1.issubset("not a list")

    # Test __contains__
    assert concept1 in lcl1
    assert concept3 not in lcl1
    assert concept1.address in lcl1
    assert "nonexistent_address" not in lcl1
    assert "not a concept" not in lcl1

    # Test difference
    diff = lcl1.difference(lcl2)
    assert concept1.address in diff
    assert concept2.address not in diff
    assert concept3.address not in diff
    assert not lcl1.difference("not a list")

    # Test isdisjoint
    lcl_disjoint = LooseBuildConceptList([concept3])
    lcl_overlapping = LooseBuildConceptList([concept1, concept3])

    assert lcl1.isdisjoint(lcl_disjoint)
    assert not lcl1.isdisjoint(lcl_overlapping)
    assert not lcl1.isdisjoint("not a list")


def test_canonical_build_concept_list_membership():
    # Create test concepts
    concept1 = BuildConcept(
        name="test1",
        canonical_name="canonical1",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    concept2 = BuildConcept(
        name="test2",
        canonical_name="canonical2",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=True,
        grain=BuildGrain(),
    )
    concept3 = BuildConcept(
        name="test3",
        canonical_name="canonical3",
        datatype=DataType.FLOAT,
        purpose=Purpose.METRIC,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )

    # Test initialization and basic properties
    cbcl1 = CanonicalBuildConceptList([concept1, concept2])
    cbcl2 = CanonicalBuildConceptList([concept2, concept3])
    cbcl3 = CanonicalBuildConceptList([concept1, concept2])  # Same as cbcl1

    # Test addresses property (uses canonical_address)
    assert len(cbcl1.addresses) == 2
    assert concept1.canonical_address in cbcl1.addresses
    assert concept2.canonical_address in cbcl1.addresses

    # Test sorted_addresses property
    assert cbcl1.sorted_addresses == sorted(
        [concept1.canonical_address, concept2.canonical_address]
    )

    # Test string representation
    assert str(cbcl1).startswith("lcl")

    # Test iteration
    concepts = list(cbcl1)
    assert len(concepts) == 2
    assert concept1 in concepts
    assert concept2 in concepts

    # Test equality
    assert cbcl1 == cbcl3
    assert cbcl1 != cbcl2
    assert cbcl1 != "not a list"

    # Test issubset
    cbcl_subset = CanonicalBuildConceptList([concept1])
    assert cbcl_subset.issubset(cbcl1)
    assert not cbcl1.issubset(cbcl_subset)
    assert not cbcl1.issubset("not a list")

    # Test __contains__ membership
    assert concept1 in cbcl1
    assert concept3 not in cbcl1
    assert concept1.canonical_address in cbcl1  # String membership by canonical address
    assert "nonexistent_canonical" not in cbcl1
    assert 123 not in cbcl1  # Non-string, non-BuildConcept

    # Test difference
    diff = cbcl1.difference(cbcl2)
    assert concept1.canonical_address in diff
    assert concept2.canonical_address not in diff
    assert concept3.canonical_address not in diff
    assert not cbcl1.difference("not a list")

    # Test isdisjoint
    cbcl_disjoint = CanonicalBuildConceptList([concept3])
    cbcl_overlapping = CanonicalBuildConceptList([concept1, concept3])

    assert cbcl1.isdisjoint(cbcl_disjoint)
    assert not cbcl1.isdisjoint(cbcl_overlapping)
    assert not cbcl1.isdisjoint("not a list")


if __name__ == "__main__":
    test_loose_concept_list()
    test_canonical_build_concept_list_membership()
    print("All tests passed!")
