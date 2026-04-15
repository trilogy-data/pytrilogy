import copy
from typing import TYPE_CHECKING

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import ConceptSource, DatePart, FunctionType, Purpose
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Concept, Function, Grain, Metadata, TraitDataType
from trilogy.core.models.core import DataType, StructType, arg_to_datatype
from trilogy.core.models.environment import Environment

if TYPE_CHECKING:
    from trilogy.parsing.helpers import Meta


def generate_date_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    arg_tuples: list[tuple[FunctionType, TraitDataType]] = [
        (FunctionType.MONTH, TraitDataType(type=DataType.INTEGER, traits=["month"])),
        (FunctionType.YEAR, TraitDataType(type=DataType.INTEGER, traits=["year"])),
        (
            FunctionType.QUARTER,
            TraitDataType(type=DataType.INTEGER, traits=["quarter"]),
        ),
        (FunctionType.DAY, TraitDataType(type=DataType.INTEGER, traits=["day"])),
        (
            FunctionType.DAY_OF_WEEK,
            TraitDataType(type=DataType.INTEGER, traits=["day_of_week"]),
        ),
    ]
    for ftype, dtype in arg_tuples:
        fname = ftype.name.lower()
        address = concept.address + f".{fname}"
        if address in environment.concepts:
            continue
        default_type = (
            Purpose.CONSTANT
            if concept.purpose == Purpose.CONSTANT
            else Purpose.PROPERTY
        )
        function = Function(
            operator=ftype,
            arguments=[concept.reference],
            output_datatype=dtype,
            output_purpose=default_type,
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=function.output_datatype,
            purpose=default_type,
            lineage=function,
            grain=concept.grain,
            namespace=concept.namespace,
            keys=set(
                [concept.address],
            ),
            metadata=Metadata(
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        environment.add_concept(new_concept)
    for grain in [DatePart.MONTH, DatePart.YEAR]:
        address = concept.address + f".{grain.value}_start"
        if address in environment.concepts:
            continue
        function = Function(
            operator=FunctionType.DATE_TRUNCATE,
            arguments=[concept.reference, grain],
            output_datatype=DataType.DATE,
            output_purpose=default_type,
            arg_count=2,
        )
        new_concept = Concept(
            name=f"{concept.name}.{grain.value}_start",
            datatype=DataType.DATE,
            purpose=Purpose.PROPERTY,
            lineage=function,
            grain=copy.copy(concept.grain),
            namespace=concept.namespace,
            keys=set(
                [concept.address],
            ),
            metadata=Metadata(
                # description=f"Auto-derived from {base_description}. The date truncated to the {grain.value}.",
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )

        environment.add_concept(new_concept)


def generate_datetime_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    setup_tuples: list[tuple[FunctionType, DataType | TraitDataType]] = [
        (FunctionType.DATE, DataType.DATE),
        (FunctionType.HOUR, TraitDataType(type=DataType.INTEGER, traits=["hour"])),
        (FunctionType.MINUTE, TraitDataType(type=DataType.INTEGER, traits=["minute"])),
        (FunctionType.SECOND, TraitDataType(type=DataType.INTEGER, traits=["second"])),
    ]
    for ftype, datatype in setup_tuples:
        fname = ftype.name.lower()
        address = concept.address + f".{fname}"
        if address in environment.concepts:
            continue
        default_type = (
            Purpose.CONSTANT
            if concept.purpose == Purpose.CONSTANT
            else Purpose.PROPERTY
        )
        const_function = Function(
            operator=ftype,
            arguments=[concept.reference],
            output_datatype=datatype,
            output_purpose=default_type,
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=datatype,
            purpose=default_type,
            lineage=const_function,
            grain=copy.copy(concept.grain),
            namespace=concept.namespace,
            keys=set(
                [concept.address],
            ),
            metadata=Metadata(
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        if new_concept.name in environment.concepts:
            continue
        environment.add_concept(new_concept)


def generate_key_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    for ftype in [FunctionType.COUNT]:
        address = concept.address + f".{ftype.name.lower()}"
        if address in environment.concepts:
            continue
        fname = ftype.name.lower()
        default_type = Purpose.METRIC
        const_function: Function = Function(
            operator=ftype,
            output_datatype=DataType.INTEGER,
            output_purpose=default_type,
            arguments=[concept.reference],
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=DataType.INTEGER,
            purpose=default_type,
            lineage=const_function,
            grain=Grain(),
            namespace=concept.namespace,
            keys=set(),
            metadata=Metadata(
                # description=f"Auto-derived integer. The {ftype.value} of {concept.address}, {base_description}",
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        environment.add_concept(new_concept)


def remove_date_concepts(concept: Concept, environment: Environment):
    """Remove auto-generated date-related concepts for the given concept"""
    date_suffixes = ["month", "year", "quarter", "day", "day_of_week"]
    grain_suffixes = ["month_start", "year_start"]

    for suffix in date_suffixes + grain_suffixes:
        address = concept.address + f".{suffix}"
        if address in environment.concepts:
            derived_concept = environment.concepts[address]
            # Only remove if it was auto-derived from this concept
            if (
                derived_concept.metadata
                and derived_concept.metadata.concept_source
                == ConceptSource.AUTO_DERIVED
                and derived_concept.keys
                and concept.address in derived_concept.keys
            ):
                environment.remove_concept(address)


def remove_datetime_concepts(concept: Concept, environment: Environment):
    """Remove auto-generated datetime-related concepts for the given concept"""
    datetime_suffixes = ["date", "hour", "minute", "second"]

    for suffix in datetime_suffixes:
        address = concept.address + f".{suffix}"
        if address in environment.concepts:
            derived_concept = environment.concepts[address]
            # Only remove if it was auto-derived from this concept
            if (
                derived_concept.metadata
                and derived_concept.metadata.concept_source
                == ConceptSource.AUTO_DERIVED
                and derived_concept.keys
                and concept.address in derived_concept.keys
            ):
                environment.remove_concept(address)


def remove_key_concepts(concept: Concept, environment: Environment):
    """Remove auto-generated key-related concepts for the given concept"""
    key_suffixes = ["count"]

    for suffix in key_suffixes:
        address = concept.address + f".{suffix}"
        if address in environment.concepts:
            derived_concept = environment.concepts[address]
            if (
                derived_concept.metadata
                and derived_concept.metadata.concept_source
                == ConceptSource.AUTO_DERIVED
            ):
                environment.remove_concept(address)


def remove_struct_concepts(concept: Concept, environment: Environment):
    """Remove auto-generated struct field concepts for the given concept"""
    if not isinstance(concept.datatype, StructType):
        return

    target_namespace = (
        environment.namespace + "." + concept.name
        if environment.namespace and environment.namespace != DEFAULT_NAMESPACE
        else concept.name
    )

    # Get all concepts in the target namespace that were auto-derived
    concepts_to_remove = []
    for address, derived_concept in environment.concepts.items():
        if (
            derived_concept.namespace == target_namespace
            and derived_concept.metadata
            and derived_concept.metadata.concept_source == ConceptSource.AUTO_DERIVED
            and isinstance(derived_concept.lineage, Function)
            and derived_concept.lineage.operator == FunctionType.ATTR_ACCESS
            and len(derived_concept.lineage.arguments) >= 1
            and derived_concept.lineage.arguments[0] == concept.reference
        ):
            concepts_to_remove.append(address)

    for address in concepts_to_remove:
        environment.remove_concept(address)


def remove_related_concepts(concept: Concept, environment: Environment):
    """Remove all auto-generated concepts that were derived from the given concept"""

    # Remove key-related concepts
    if concept.purpose == Purpose.KEY:
        remove_key_concepts(concept, environment)

    # Remove datatype-specific concepts
    if concept.datatype == DataType.DATE:
        remove_date_concepts(concept, environment)
    elif concept.datatype == DataType.DATETIME:
        remove_date_concepts(concept, environment)
        remove_datetime_concepts(concept, environment)
    elif concept.datatype == DataType.TIMESTAMP:
        remove_date_concepts(concept, environment)
        remove_datetime_concepts(concept, environment)

    # Remove struct field concepts
    if isinstance(concept.datatype, StructType):
        remove_struct_concepts(concept, environment)


def generate_related_concepts(
    concept: Concept,
    environment: Environment,
    meta: "Meta | None" = None,
):
    """Auto populate struct field concepts on struct types."""
    if isinstance(concept.datatype, StructType):
        for key, value in concept.datatype.fields_map.items():
            auto = Concept(
                name=key,
                datatype=arg_to_datatype(value),
                purpose=Purpose.PROPERTY,
                namespace=(
                    environment.namespace + "." + concept.name
                    if environment.namespace
                    and environment.namespace != DEFAULT_NAMESPACE
                    else concept.name
                ),
                lineage=FunctionFactory(environment).create_function(
                    [concept.reference, key], FunctionType.ATTR_ACCESS
                ),
                grain=concept.grain,
                metadata=Metadata(
                    concept_source=ConceptSource.AUTO_DERIVED,
                ),
                keys=concept.keys,
            )
            environment.add_concept(auto, meta=meta)
            if isinstance(value, Concept):
                environment.merge_concept(auto, value, modifiers=[])


def enrich_environment(environment: Environment):
    """Optional helper to eagerly populate key/date/datetime derived concepts
    for all concepts in the environment."""
    for concept in list(environment.concepts.values()):
        if concept.purpose == Purpose.KEY:
            generate_key_concepts(concept, environment)
        if concept.datatype == DataType.DATE:
            generate_date_concepts(concept, environment)
        elif concept.datatype in (DataType.DATETIME, DataType.TIMESTAMP):
            generate_date_concepts(concept, environment)
            generate_datetime_concepts(concept, environment)
