from preql.core.models import Concept, Environment, Function
from preql.core.enums import DataType, Purpose, FunctionType
from preql.constants import DEFAULT_NAMESPACE

def generate_date_concepts(concept:Concept, environment:Environment):
    for ftype in [FunctionType.MONTH, FunctionType.YEAR, FunctionType.QUARTER]:
        fname = ftype.name.lower()
        default_type = Purpose.CONSTANT if concept.purpose == Purpose.CONSTANT else Purpose.PROPERTY
        const_function: Function = Function(
                operator=ftype,
                output_datatype=DataType.INTEGER,
                output_purpose=default_type,
                arguments=[concept],
            )
        if concept.namespace != DEFAULT_NAMESPACE:
            new_namespace = f'{concept.namespace}.{concept.name}'
        else:
            new_namespace = concept.name
        new_concept = Concept(
                name=fname,
                datatype=DataType.INTEGER,
                purpose=default_type,
                lineage=const_function,
                grain=const_function.output_grain,
                namespace=new_namespace,
                keys = [concept]
            )
        environment.add_concept(new_concept)

def generate_datetime_concepts(concept:Concept, environment:Environment):
    for ftype in [FunctionType.HOUR, FunctionType.MINUTE,  FunctionType.SECOND]:
        fname = ftype.name.lower()
        default_type = Purpose.CONSTANT if concept.purpose == Purpose.CONSTANT else Purpose.PROPERTY
        const_function: Function = Function(
                operator=ftype,
                output_datatype=DataType.INTEGER,
                output_purpose=default_type,
                arguments=[concept],
            )
        if concept.namespace != DEFAULT_NAMESPACE:
            new_namespace = f'{concept.namespace}.{concept.name}'
        else:
            new_namespace = concept.name
        new_concept = Concept(
                name=f'{fname}',
                datatype=DataType.INTEGER,
                purpose=default_type,
                lineage=const_function,
                grain=const_function.output_grain,
                namespace=new_namespace,
                keys = [concept]
            )
        environment.add_concept(new_concept)


def generate_related_concepts(concept:Concept, environment:Environment):
    if concept.datatype == DataType.DATE:
        generate_date_concepts(concept, environment)
    elif concept.datatype == DataType.DATETIME:
        generate_date_concepts(concept, environment)
        generate_datetime_concepts(concept, environment)