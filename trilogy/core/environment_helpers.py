from trilogy.core.models import (
    DataType,
    Concept,
    Environment,
    Function,
    Metadata,
    StructType,
)
from trilogy.core.functions import AttrAccess
from trilogy.core.enums import Purpose, FunctionType, ConceptSource
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.parsing.common import process_function_args, arg_to_datatype, Meta


def generate_date_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.description:
        base_description = concept.metadata.description
    else:
        base_description = f"a {concept.datatype.value}"
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    for ftype in [
        FunctionType.MONTH,
        FunctionType.YEAR,
        FunctionType.QUARTER,
        FunctionType.DAY,
        FunctionType.DAY_OF_WEEK,
    ]:
        fname = ftype.name.lower()
        default_type = (
            Purpose.CONSTANT
            if concept.purpose == Purpose.CONSTANT
            else Purpose.PROPERTY
        )
        const_function: Function = Function(
            operator=ftype,
            output_datatype=DataType.INTEGER,
            output_purpose=default_type,
            arguments=[concept],
        )
        namespace = (
            None if concept.namespace == DEFAULT_NAMESPACE else concept.namespace
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=DataType.INTEGER,
            purpose=default_type,
            lineage=const_function,
            grain=const_function.output_grain,
            namespace=namespace,
            keys=(concept,),
            metadata=Metadata(
                description=f"Auto-derived. Integer format. The {ftype.value} derived from {concept.name}, {base_description}",
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        if new_concept.name in environment.concepts:
            continue
        environment.add_concept(new_concept, add_derived=False)


def generate_datetime_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.description:
        base_description = concept.metadata.description
    else:
        base_description = f"a {concept.datatype.value}"
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    for ftype in [
        FunctionType.DATE,
        FunctionType.HOUR,
        FunctionType.MINUTE,
        FunctionType.SECOND,
    ]:
        fname = ftype.name.lower()
        default_type = (
            Purpose.CONSTANT
            if concept.purpose == Purpose.CONSTANT
            else Purpose.PROPERTY
        )
        const_function: Function = Function(
            operator=ftype,
            output_datatype=DataType.INTEGER,
            output_purpose=default_type,
            arguments=[concept],
        )
        namespace = (
            None if concept.namespace == DEFAULT_NAMESPACE else concept.namespace
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=DataType.INTEGER,
            purpose=default_type,
            lineage=const_function,
            grain=const_function.output_grain,
            namespace=namespace,
            keys=(concept,),
            metadata=Metadata(
                description=f"Auto-derived. Integer format. The {ftype.value} derived from {concept.name}, {base_description}",
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        if new_concept.name in environment.concepts:
            continue
        environment.add_concept(new_concept, add_derived=False)


def generate_key_concepts(concept: Concept, environment: Environment):
    if concept.metadata and concept.metadata.description:
        base_description = concept.metadata.description
    else:
        base_description = f"a {concept.datatype.value}"
    if concept.metadata and concept.metadata.line_number:
        base_line_number = concept.metadata.line_number
    else:
        base_line_number = None
    for ftype in [FunctionType.COUNT]:
        fname = ftype.name.lower()
        default_type = Purpose.METRIC
        const_function: Function = Function(
            operator=ftype,
            output_datatype=DataType.INTEGER,
            output_purpose=default_type,
            arguments=[concept],
        )
        namespace = (
            None if concept.namespace == DEFAULT_NAMESPACE else concept.namespace
        )
        new_concept = Concept(
            name=f"{concept.name}.{fname}",
            datatype=DataType.INTEGER,
            purpose=default_type,
            lineage=const_function,
            grain=const_function.output_grain,
            namespace=namespace,
            keys=(concept,),
            metadata=Metadata(
                description=f"Auto-derived. Integer format. The {ftype.value} derived from {concept.name}, {base_description}",
                line_number=base_line_number,
                concept_source=ConceptSource.AUTO_DERIVED,
            ),
        )
        if new_concept.name in environment.concepts:
            continue
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
            args = process_function_args(
                [concept, key], meta=meta, environment=environment
            )
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
                lineage=AttrAccess(args),
            )
            environment.add_concept(auto, meta=meta)
            if isinstance(value, Concept):
                environment.merge_concept(auto, value, modifiers=[])
