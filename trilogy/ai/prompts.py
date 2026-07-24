import json

from trilogy import Environment
from trilogy.ai.constants import AGGREGATE_FUNCTIONS, FUNCTIONS, RULE_PROMPT
from trilogy.ai.models import LLMRequestOptions, LLMToolDefinition
from trilogy.ai.syntax_examples import example_headers
from trilogy.authoring import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.models.core import (
    DataTyped,
    EnumType,
    StructComponent,
    ValidatedType,
)
from trilogy.scripts.explore import build_concepts_payload


def get_trilogy_syntax_reference() -> str:
    return f"""Key Trilogy Syntax Rules:
{RULE_PROMPT}

Aggregate Functions:
{AGGREGATE_FUNCTIONS}

Functions:
{FUNCTIONS}

Valid types:
{[x.value for x in DataType]}

Some types may have additional metadata, which will help you understand them. For example, 'latitude', 'longitude' and 'currency' are all of type 'float', but have additional meaning.

A typical trilogy query only needs to import one file (the fact) and will use dot-references to pull in dimensions.

In the rare case of merging two fact domains, use a MERGE statement to merge share dimensions concepts another; ex `merge concept_a into ~concept_b`. This marks concept_a as being identical to a partial subset
of concept_b, enabling discovery to bridge the two. If a dimenion is accessible through a fact, via abc.def - use that instead of merging.".

Additional syntax examples:
These less-common patterns have complete, copy-pasteable examples. Do NOT guess
the syntax — print the full example on demand with
`trilogy agent-info syntax example <name>`:
{example_headers()}

"""


def get_trilogy_prompt(intro: str | None = None, outro: str | None = None) -> str:
    parts = []
    if intro:
        parts.append(intro)
    parts.append(get_trilogy_syntax_reference())
    if outro:
        parts.append(outro)
    return "\n\n".join(parts)


TRILOGY_CREATE_QUERY_TOOL = LLMToolDefinition(
    name="create_query",
    description="Validate a draft Trilogy query against the current environment.",
    input_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A draft Trilogy query to validate.",
            }
        },
        "required": ["query"],
        "additionalProperties": False,
    },
)


TRILOGY_QUERY_TOOL = LLMToolDefinition(
    name="submit_query",
    description="Return the final Trilogy query that answers the user's request.",
    input_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A complete valid Trilogy query ending with a semicolon.",
            },
            "reasoning": {
                "type": "string",
                "description": "Brief explanation for how the query answers the request.",
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    },
)


def create_query_request_options() -> LLMRequestOptions:
    return LLMRequestOptions(
        tools=[TRILOGY_CREATE_QUERY_TOOL, TRILOGY_QUERY_TOOL],
        require_tool=True,
    )


TRILOGY_LEAD_IN = get_trilogy_prompt(
    intro="You are a world-class expert in Trilogy, a SQL inspired language with similar syntax and a built in semantic layer. Use the following syntax description to help answer whatever questions they have. Often, they will be asking you to generate a query for them.",
    outro=(
        f"When generating a Trilogy query, use the {TRILOGY_CREATE_QUERY_TOOL.name} tool to validate draft queries. "
        f"That tool returns parser and query-processing validation feedback only; it does not execute the query. "
        f"When you are done, you must call the {TRILOGY_QUERY_TOOL.name} tool with the final query and optional reasoning. "
        "Do not wrap the query in markdown fences or triple quotes."
    ),
)


def datatype_to_field_prompt(
    datatype: (
        DataType | TraitDataType | ArrayType | StructType | MapType | NumericType | EnumType | ValidatedType | DataTyped | StructComponent | float | str
    ),
) -> str:
    if isinstance(datatype, EnumType):
        return f"enum<{', '.join(repr(v) for v in datatype.values)}>"
    if isinstance(datatype, ValidatedType):
        return str(datatype)
    if isinstance(datatype, TraitDataType):
        return f"{datatype_to_field_prompt(datatype.type)}({','.join(datatype.traits)})"
    if isinstance(datatype, ArrayType):
        return f"ARRAY<{datatype_to_field_prompt(datatype.type)}>"
    if isinstance(datatype, StructType):
        instantiated = []
        for name, field_type in datatype.field_types.items():
            if isinstance(field_type, StructComponent):
                instantiated.append(f"{datatype_to_field_prompt(field_type.type)}")
            else:
                instantiated.append(f"{name}: {datatype_to_field_prompt(field_type)}")
        fields_str = ", ".join(instantiated)
        return f"STRUCT<{fields_str}>"
    if isinstance(datatype, MapType):
        return f"MAP<{datatype_to_field_prompt(datatype.key_type)}, {datatype_to_field_prompt(datatype.value_type)}>"
    if isinstance(datatype, NumericType):
        return f"NUMERIC({datatype.precision}, {datatype.scale})>"
    if isinstance(datatype, DataTyped):
        return datatype_to_field_prompt(datatype.output_datatype)
    if isinstance(datatype, StructComponent):
        return f"{datatype.name}: {datatype_to_field_prompt(datatype.type)}"
    if isinstance(datatype, (int, float, str)):
        return f"{datatype}"
    return f"{datatype.value}"


def concepts_to_fields_prompt(environment: Environment) -> str:
    """Concise JSON concept dump for the environment, sharing the `explore`
    command's grouped formatting: local and imported namespaces in full
    declaration syntax, with role-played conformed dimensions deduped into one
    combined-key entry carrying per-role descriptions and provenance. Builtins and
    the internal env scaffolding are filtered out, mirroring `explore`'s
    defaults."""
    items = [
        (addr, concept)
        for addr, concept in environment.concepts.items()
        if not addr.startswith("__") and not addr.startswith("local._env_")
    ]
    import_descriptions = {
        alias: imp.description
        for alias, imps in environment.imports.items()
        for imp in imps
        if imp.description
    }
    return json.dumps(
        build_concepts_payload(environment, items, import_descriptions), indent=2
    )


def create_query_prompt(query: str, environment: Environment) -> str:
    fields = concepts_to_fields_prompt(environment)
    return f"""
Using these available concepts (base and derived calculations created with valid Trilogy), and any extra context you have:
{fields}
create the best valid Trilogy query to answer the following user input: "{query}". Use the {TRILOGY_CREATE_QUERY_TOOL.name} tool to validate draft queries if needed. You must finish by calling the {TRILOGY_QUERY_TOOL.name} tool with a complete Trilogy query in the query field. The final query must end with a semicolon."""
