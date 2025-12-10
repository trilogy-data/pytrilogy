import uuid
from typing import Any, Callable, Dict, Mapping, Optional

from jinja2 import Template

from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    UnnestMode,
    WindowType,
)
from trilogy.core.models.core import (
    DataType,
)
from trilogy.core.models.execute import CTE, CompiledCTE, UnionCTE
from trilogy.core.statements.execute import ProcessedQueryPersist
from trilogy.dialect.base import BaseDialect, safe_quote

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}


def transform_date_part(part: str) -> str:
    part_upper = part.upper()
    if part_upper == "DAY_OF_WEEK":
        return "DAYOFWEEK"
    return part_upper


def handle_length(args, types: list[DataType] | None = None) -> str:
    arg = args[0]
    if types and types[0].data_type == DataType.ARRAY:
        return f"ARRAY_LENGTH({arg})"
    return f"LENGTH({arg})"


FUNCTION_MAP = {
    FunctionType.COUNT: lambda x, types: f"count({x[0]})",
    FunctionType.SUM: lambda x, types: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x, types: handle_length(x, types),
    FunctionType.AVG: lambda x, types: f"avg({x[0]})",
    FunctionType.LIKE: lambda x, types: (
        f" CASE WHEN {x[0]} like {x[1]} THEN True ELSE False END"
    ),
    FunctionType.IS_NULL: lambda x, types: f"{x[0]} IS NULL",
    FunctionType.MINUTE: lambda x, types: f"EXTRACT(MINUTE from {x[0]})",
    FunctionType.SECOND: lambda x, types: f"EXTRACT(SECOND from {x[0]})",
    FunctionType.HOUR: lambda x, types: f"EXTRACT(HOUR from {x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"EXTRACT(DAYOFWEEK from {x[0]})-1",  # BigQuery's DAYOFWEEK returns 1 for Sunday
    FunctionType.DAY: lambda x, types: f"EXTRACT(DAY from {x[0]})",
    FunctionType.YEAR: lambda x, types: f"EXTRACT(YEAR from {x[0]})",
    FunctionType.MONTH: lambda x, types: f"EXTRACT(MONTH from {x[0]})",
    FunctionType.WEEK: lambda x, types: f"EXTRACT(WEEK from {x[0]})",
    FunctionType.QUARTER: lambda x, types: f"EXTRACT(QUARTER from {x[0]})",
    # math
    FunctionType.POWER: lambda x, types: f"POWER({x[0]}, {x[1]})",
    FunctionType.DIVIDE: lambda x, types: f"COALESCE(SAFE_DIVIDE({x[0]},{x[1]}),0)",
    FunctionType.DATE_ADD: lambda x, types: f"DATE_ADD({x[0]}, INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"DATE_SUB({x[0]}, INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_PART: lambda x, types: f"EXTRACT({transform_date_part(x[1])} FROM {x[0]})",
    FunctionType.MONTH_NAME: lambda x, types: f"FORMAT_DATE('%B', {x[0]})",
    FunctionType.DAY_NAME: lambda x, types: f"FORMAT_DATE('%A', {x[0]})",
    # string
    FunctionType.CONTAINS: lambda x, types: f"CONTAINS_SUBSTR({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: f"FLOOR(RAND()*{x[0]})",
    FunctionType.ARRAY_SUM: lambda x, types: f"(select sum(x) from unnest({x[0]}) as x)",
    FunctionType.ARRAY_DISTINCT: lambda x, types: f"ARRAY(SELECT DISTINCT element FROM UNNEST({x[0]}) AS element)",
    FunctionType.ARRAY_SORT: lambda x, types: f"ARRAY(SELECT element FROM UNNEST({x[0]}) AS element ORDER BY element)",
    # aggregate
    FunctionType.BOOL_AND: lambda x, types: f"LOGICAL_AND({x[0]})",
    FunctionType.BOOL_OR: lambda x, types: f"LOGICAL_OR({x[0]})",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args, types: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args, types: f"{args[0]}",
    FunctionType.AVG: lambda args, types: f"{args[0]}",
}

DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "STRING",
    DataType.INTEGER: "INT64",
    DataType.FLOAT: "FLOAT64",
    DataType.BOOL: "BOOL",
    DataType.NUMERIC: "NUMERIC",
    DataType.MAP: "MAP",
    DataType.DATE: "DATE",
    DataType.DATETIME: "DATETIME",
    DataType.TIMESTAMP: "TIMESTAMP",
}


BQ_SQL_TEMPLATE = Template(
    """{%- if output %}
{{output}}
{% endif %}{%- if ctes %}
WITH {% if recursive%}RECURSIVE{% endif %}{% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% else%}
{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{%- else -%}
SELECT
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}{% for join in joins %}
    {{ join }}{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if group_by %}GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}{% endif %}
"""
)


BQ_CREATE_TABLE_SQL_TEMPLATE = Template(
    """
CREATE {% if create_mode == "create_or_replace" %}OR REPLACE TABLE{% elif create_mode == "create_if_not_exists" %}TABLE IF NOT EXISTS{% else %}TABLE{% endif %} {{ name}} (
{%- for column in columns %}
    `{{ column.name }}` {{ type_map[column.name] }}{% if column.description %} OPTIONS(description='{{ column.description }}'){% endif %}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- if partition_by %}
PARTITION BY {{ partition_by }}
{%- endif %}
{%- if cluster_by %}
CLUSTER BY {{ cluster_by | join(', ') }}
{%- endif %}
{%- if table_description %}
OPTIONS(
    description='{{ table_description }}'
)
{%- endif %};
""".strip()
)

PARTITIONED_INSERT_TEMPLATE = Template(
    """
-- Step 1: materialize results
CREATE TEMP TABLE {{ tmp_table }} AS SELECT * FROM  {{ target_table }} limit 0;
                                       
INSERT INTO {{ tmp_table }}
    {{ final_select }}
;

-- Step 2: extract distinct partitions and generate dynamic statements
BEGIN
    DECLARE partition_values ARRAY<{{ partition_type }}>;
    DECLARE current_partition {{ partition_type }};
    DECLARE i INT64 DEFAULT 0;
    
    -- Get all distinct partition values
    SET partition_values = (
        SELECT ARRAY_AGG(DISTINCT {{ partition_key[0] }})
        FROM {{ tmp_table }}
    );
    
    -- Loop through each partition value
    WHILE i < ARRAY_LENGTH(partition_values) DO
        SET current_partition = partition_values[OFFSET(i)];
        
        -- Delete existing records for this partition
        EXECUTE IMMEDIATE FORMAT(
            'DELETE FROM {{ target_table }} WHERE {{ partition_key[0] }} = "%t"',
            current_partition
        );
        
        -- Insert new records for this partition
        EXECUTE IMMEDIATE FORMAT(
            'INSERT INTO {{ target_table }} SELECT * FROM {{ tmp_table }} WHERE {{ partition_key[0] }} = "%t"',
            current_partition
        );
        
        SET i = i + 1;
    END WHILE;
END;
"""
)

MAX_IDENTIFIER_LENGTH = 50


def parse_bigquery_table_name(
    table_name: str, schema: str | None = None
) -> tuple[str, str | None]:
    """Parse BigQuery table names supporting project.dataset.table format."""
    if "." in table_name and not schema:
        parts = table_name.split(".")
        if len(parts) == 2:
            schema = parts[0]
            table_name = parts[1]
        elif len(parts) == 3:
            # project.dataset.table format
            schema = f"{parts[0]}.{parts[1]}"
            table_name = parts[2]
    return table_name, schema


class BigqueryDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = BQ_SQL_TEMPLATE
    CREATE_TABLE_SQL_TEMPLATE = BQ_CREATE_TABLE_SQL_TEMPLATE
    UNNEST_MODE = UnnestMode.CROSS_JOIN_UNNEST
    DATATYPE_MAP = DATATYPE_MAP

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[tuple]:
        """BigQuery uses dataset instead of schema and supports project.dataset.table format."""
        table_name, schema = parse_bigquery_table_name(table_name, schema)

        column_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            '' as column_comment
        FROM `{schema}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        rows = executor.execute_raw_sql(column_query).fetchall()
        return rows

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """BigQuery doesn't enforce primary keys; rely on data-driven grain detection."""
        table_name, schema = parse_bigquery_table_name(table_name, schema)

        pk_query = f"""
        SELECT column_name
        FROM `{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
        WHERE table_name = '{table_name}'
        AND constraint_name LIKE '%PRIMARY%'
        """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]

    def render_array_unnest(
        self,
        left,
        right,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
    ):
        return f"{self.render_expr(left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {operator.value} unnest({self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)})"

    def generate_partitioned_insert(
        self,
        query: ProcessedQueryPersist,
        recursive: bool,
        compiled_ctes: list[CompiledCTE],
    ) -> str:
        tmp_table = f"tmp__{uuid.uuid4().hex}"
        final_select = compiled_ctes[-1].statement
        ctes = compiled_ctes[:-1]

        if not query.partition_by:
            raise ValueError("partition_by must be set for partitioned inserts.")

        partition_key = query.partition_by
        target_table = safe_quote(
            query.output_to.address.location, self.QUOTE_CHARACTER
        )

        # render intermediate CTEs
        ctes_sql = ""
        if ctes:
            rendered = []
            for c in ctes:
                rendered.append(f"{c.name} AS ({c.statement})")
            ctes_sql = "WITH " + ",\n".join(rendered)

        # create temp table first
        full_select_with_ctes = (
            final_select if not ctes_sql else f"{ctes_sql}\n{final_select}"
        )

        sql_script = PARTITIONED_INSERT_TEMPLATE.render(
            tmp_table=tmp_table,
            final_select=full_select_with_ctes,
            partition_key=partition_key,
            target_table=target_table,
            partition_type=self.DATATYPE_MAP[query.partition_types[0]],
        )

        return sql_script
