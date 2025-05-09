from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import ConceptSource, DatePart, FunctionType, Purpose
from trilogy.core.functions import AttrAccess
from trilogy.core.models.author import Concept, Function, Grain, Metadata, TraitDataType
from trilogy.core.models.core import DataType, StructType, arg_to_datatype
from trilogy.core.models.environment import Environment
from trilogy.parsing.common import Meta


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
        function = Function.model_construct(
            operator=ftype,
            arguments=[concept.reference],
            output_datatype=dtype,
            output_purpose=default_type,
        )
        new_concept = Concept.model_construct(
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
        environment.add_concept(new_concept, add_derived=False)
    for grain in [DatePart.MONTH, DatePart.YEAR]:
        address = concept.address + f".{grain.value}_start"
        if address in environment.concepts:
            continue
        function = Function.model_construct(
            operator=FunctionType.DATE_TRUNCATE,
            arguments=[concept.reference, grain],
            output_datatype=DataType.DATE,
            output_purpose=default_type,
            arg_count=2,
        )
        new_concept = Concept.model_construct(
            name=f"{concept.name}.{grain.value}_start",
            datatype=DataType.DATE,
            purpose=Purpose.PROPERTY,
            lineage=function,
            grain=concept.grain.model_copy(),
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

        environment.add_concept(new_concept, add_derived=False)


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
        const_function = Function.model_construct(
            operator=ftype,
            arguments=[concept.reference],
            output_datatype=datatype,
            output_purpose=default_type,
        )
        new_concept = Concept.model_construct(
            name=f"{concept.name}.{fname}",
            datatype=datatype,
            purpose=default_type,
            lineage=const_function,
            grain=concept.grain.model_copy(),
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
        environment.add_concept(new_concept, add_derived=False)


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
        const_function: Function = Function.model_construct(
            operator=ftype,
            output_datatype=DataType.INTEGER,
            output_purpose=default_type,
            arguments=[concept.reference],
        )
        new_concept = Concept.model_construct(
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
        environment.add_concept(new_concept, add_derived=False)


def generate_related_concepts(
    concept: Concept,
    environment: Environment,
    meta: Meta | None = None,
    add_derived: bool = False,
):
    """Auto populate common derived concepts on types"""
    if concept.purpose == Purpose.KEY and add_derived:
        generate_key_concepts(concept, environment)

    # datatype types
    if concept.datatype == DataType.DATE and add_derived:
        generate_date_concepts(concept, environment)
    elif concept.datatype == DataType.DATETIME and add_derived:
        generate_date_concepts(concept, environment)
        generate_datetime_concepts(concept, environment)
    elif concept.datatype == DataType.TIMESTAMP and add_derived:
        generate_date_concepts(concept, environment)
        generate_datetime_concepts(concept, environment)

    if isinstance(concept.datatype, StructType):
        for key, value in concept.datatype.fields_map.items():
            auto = Concept.model_construct(
                name=key,
                datatype=arg_to_datatype(value),
                purpose=Purpose.PROPERTY,
                namespace=(
                    environment.namespace + "." + concept.name
                    if environment.namespace
                    and environment.namespace != DEFAULT_NAMESPACE
                    else concept.name
                ),
                lineage=AttrAccess([concept.reference, key], environment=environment),
                grain=concept.grain,
            )
            environment.add_concept(auto, meta=meta)
            if isinstance(value, Concept):
                environment.merge_concept(auto, value, modifiers=[])
