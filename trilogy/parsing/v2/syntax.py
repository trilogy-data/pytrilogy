from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias, cast


class SyntaxNodeKind(str, Enum):
    DOCUMENT = "document"
    BLOCK = "block"
    CONCEPT = "concept"
    SHOW_STATEMENT = "show_statement"
    SELECT_STATEMENT = "select_statement"
    SHOW_CATEGORY = "show_category"
    PARAMETER_DEFAULT = "parameter_default"
    PARAMETER_DECLARATION = "parameter_declaration"
    CONCEPT_DECLARATION = "concept_declaration"
    CONCEPT_PROPERTY_DECLARATION = "concept_property_declaration"
    CONCEPT_DERIVATION = "concept_derivation"
    CONSTANT_DERIVATION = "constant_derivation"
    CONCEPT_LITERAL = "concept_literal"
    DATA_TYPE = "data_type"
    NUMERIC_TYPE = "numeric_type"
    MAP_TYPE = "map_type"
    STRUCT_TYPE = "struct_type"
    STRUCT_COMPONENT = "struct_component"
    STRUCT_LITERAL = "struct_literal"
    LIST_TYPE = "list_type"
    ENUM_TYPE = "enum_type"
    CONCEPT_NULLABLE_MODIFIER = "concept_nullable_modifier"
    METADATA = "metadata"
    INT_LITERAL = "int_literal"
    FLOAT_LITERAL = "float_literal"
    BOOL_LITERAL = "bool_literal"
    NULL_LITERAL = "null_literal"
    STRING_LITERAL = "string_literal"
    ARRAY_LITERAL = "array_literal"
    TUPLE_LITERAL = "tuple_literal"
    MAP_LITERAL = "map_literal"
    LITERAL = "literal"
    PRODUCT_OPERATOR = "product_operator"
    SUM_OPERATOR = "sum_operator"
    COMPARISON = "comparison"
    BETWEEN_COMPARISON = "between_comparison"
    PARENTHETICAL = "parenthetical"
    PROPERTY_IDENTIFIER = "property_identifier"
    PROPERTY_IDENTIFIER_WILDCARD = "property_identifier_wildcard"
    COMPARISON_ROOT = "comparison_root"
    SUM_CHAIN = "sum_chain"
    PRODUCT_CHAIN = "product_chain"
    ATOM = "atom"
    # Phase 1: import + select + where + order + conditionals
    IMPORT_STATEMENT = "import_statement"
    SELECTIVE_IMPORT_STATEMENT = "selective_import_statement"
    SELF_IMPORT_STATEMENT = "self_import_statement"
    IMPORT_CONCEPTS = "import_concepts"
    SELECT_ITEM = "select_item"
    SELECT_TRANSFORM = "select_transform"
    SELECT_LIST = "select_list"
    SELECT_HIDE_MODIFIER = "select_hide_modifier"
    SELECT_PARTIAL_MODIFIER = "select_partial_modifier"
    WHERE = "where"
    HAVING = "having"
    CONDITIONAL = "conditional"
    CONDITION_PARENTHETICAL = "condition_parenthetical"
    ORDER_BY = "order_by"
    ORDER_LIST = "order_list"
    ORDERING = "ordering"
    LIMIT = "limit"
    SUBSELECT_COMPARISON = "subselect_comparison"
    EXPR_TUPLE = "expr_tuple"
    FROM_CLAUSE = "from_clause"
    # Phase 2: aggregates
    AGGREGATE_FUNCTIONS = "aggregate_functions"
    AGGREGATE_OVER = "aggregate_over"
    AGGREGATE_ALL = "aggregate_all"
    AGGREGATE_BY = "aggregate_by"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    ARRAY_AGG = "array_agg"
    BOOL_AND = "bool_and"
    BOOL_OR = "bool_or"
    ANY = "any"
    FGROUP = "fgroup"
    OVER_LIST = "over_list"
    # Phase 3: merge + cast
    MERGE_STATEMENT = "merge_statement"
    FCAST = "fcast"
    # Phase 4: filter + case + string/utility/math/date/geo/array/map functions
    FILTER_ITEM = "filter_item"
    FCASE = "fcase"
    FCASE_WHEN = "fcase_when"
    FCASE_ELSE = "fcase_else"
    FCASE_SIMPLE = "fcase_simple"
    FCASE_SIMPLE_WHEN = "fcase_simple_when"
    FNOT = "fnot"
    FBOOL = "fbool"
    CONCAT = "concat"
    FCOALESCE = "fcoalesce"
    FGREATEST = "fgreatest"
    FLEAST = "fleast"
    FNULLIF = "fnullif"
    FRECURSE_EDGE = "frecurse_edge"
    LEN = "len"
    UPPER = "upper"
    FLOWER = "flower"
    FSPLIT = "fsplit"
    FSTRPOS = "fstrpos"
    FSUBSTRING = "fsubstring"
    FCONTAINS = "fcontains"
    FTRIM = "ftrim"
    FLTRIM = "fltrim"
    FRTRIM = "frtrim"
    FREPLACE = "freplace"
    FREGEXP_EXTRACT = "fregexp_extract"
    FREGEXP_CONTAINS = "fregexp_contains"
    FREGEXP_REPLACE = "fregexp_replace"
    FHASH = "fhash"
    FHEX = "fhex"
    LIKE = "like"
    ILIKE = "ilike"
    FADD = "fadd"
    FSUB = "fsub"
    FMUL = "fmul"
    FDIV = "fdiv"
    FMOD = "fmod"
    FLOG = "flog"
    FROUND = "fround"
    FFLOOR = "ffloor"
    FCEIL = "fceil"
    FABS = "fabs"
    FSQRT = "fsqrt"
    FRANDOM = "frandom"
    FDATE = "fdate"
    FDATETIME = "fdatetime"
    FTIMESTAMP = "ftimestamp"
    FSECOND = "fsecond"
    FMINUTE = "fminute"
    FHOUR = "fhour"
    FDAY = "fday"
    FDAY_NAME = "fday_name"
    FDAY_OF_WEEK = "fday_of_week"
    FWEEK = "fweek"
    FMONTH = "fmonth"
    FMONTH_NAME = "fmonth_name"
    FFORMAT_TIME = "fformat_time"
    FPARSE_TIME = "fparse_time"
    FQUARTER = "fquarter"
    FYEAR = "fyear"
    FDATE_TRUNC = "fdate_trunc"
    FDATE_PART = "fdate_part"
    FDATE_ADD = "fdate_add"
    FDATE_SUB = "fdate_sub"
    FDATE_DIFF = "fdate_diff"
    FDATE_SPINE = "fdate_spine"
    FARRAY_SUM = "farray_sum"
    FARRAY_DISTINCT = "farray_distinct"
    FARRAY_TO_STRING = "farray_to_string"
    FARRAY_SORT = "farray_sort"
    FARRAY_TRANSFORM = "farray_transform"
    FARRAY_FILTER = "farray_filter"
    FGENERATE_ARRAY = "fgenerate_array"
    FMAP_KEYS = "fmap_keys"
    FMAP_VALUES = "fmap_values"
    FGEO_FROM_TEXT = "fgeo_from_text"
    FGEO_POINT = "fgeo_point"
    FGEO_DISTANCE = "fgeo_distance"
    FGEO_X = "fgeo_x"
    FGEO_Y = "fgeo_y"
    FGEO_CENTROID = "fgeo_centroid"
    FGEO_TRANSFORM = "fgeo_transform"
    FCURRENT_DATE = "fcurrent_date"
    FCURRENT_DATETIME = "fcurrent_datetime"
    FCURRENT_TIMESTAMP = "fcurrent_timestamp"
    UNNEST = "unnest"
    UNION = "union"
    SUBSELECT = "subselect"
    SUBSELECT_WHERE = "subselect_where"
    SUBSELECT_ORDER = "subselect_order"
    SUBSELECT_LIMIT = "subselect_limit"
    # Phase 5: window + function definitions
    WINDOW_ITEM = "window_item"
    WINDOW_ITEM_LEGACY = "window_item_legacy"
    WINDOW_ITEM_SQL = "window_item_sql"
    WINDOW_ITEM_OVER = "window_item_over"
    WINDOW_ITEM_ORDER = "window_item_order"
    WINDOW_SQL_OVER = "window_sql_over"
    WINDOW_SQL_PARTITION = "window_sql_partition"
    WINDOW_SQL_ORDER = "window_sql_order"
    FUNCTION = "function"
    RAW_FUNCTION = "raw_function"
    TABLE_FUNCTION = "table_function"
    FUNCTION_BINDING_LIST = "function_binding_list"
    FUNCTION_BINDING_ITEM = "function_binding_item"
    FUNCTION_BINDING_TYPE = "function_binding_type"
    FUNCTION_BINDING_DEFAULT = "function_binding_default"
    CUSTOM_FUNCTION = "custom_function"
    TRANSFORM_LAMBDA = "transform_lambda"
    INDEX_ACCESS = "index_access"
    MAP_KEY_ACCESS = "map_key_access"
    ATTR_ACCESS = "attr_access"
    CHAINED_ACCESS = "chained_access"
    OVER_COMPONENT = "over_component"
    # Phase 6: rowset + multi-select + align
    ROWSET_DERIVATION_STATEMENT = "rowset_derivation_statement"
    MULTI_SELECT_STATEMENT = "multi_select_statement"
    ALIGN_CLAUSE = "align_clause"
    ALIGN_ITEM = "align_item"
    DERIVE_CLAUSE = "derive_clause"
    DERIVE_ITEM = "derive_item"
    # Phase 7: remaining statements
    DATASOURCE = "datasource"
    PERSIST_STATEMENT = "persist_statement"
    AUTO_PERSIST = "auto_persist"
    FULL_PERSIST = "full_persist"
    PERSIST_PARTITION_CLAUSE = "persist_partition_clause"
    COPY_STATEMENT = "copy_statement"
    RAWSQL_STATEMENT = "rawsql_statement"
    VALIDATE_STATEMENT = "validate_statement"
    MOCK_STATEMENT = "mock_statement"
    PUBLISH_STATEMENT = "publish_statement"
    CREATE_STATEMENT = "create_statement"
    CREATE_MODIFIER_CLAUSE = "create_modifier_clause"
    TYPE_DECLARATION = "type_declaration"
    TYPE_DROP_CLAUSE = "type_drop_clause"
    CHART_STATEMENT = "chart_statement"
    CHART_SETTING = "chart_setting"
    CHART_FIELD_SETTING = "chart_field_setting"
    CHART_BOOL_SETTING = "chart_bool_setting"
    CHART_SCALE_SETTING = "chart_scale_setting"
    PROPERTIES_DECLARATION = "properties_declaration"
    INLINE_PROPERTY = "inline_property"
    INLINE_PROPERTY_LIST = "inline_property_list"
    PROP_IDENT_LIST = "prop_ident_list"
    CONCEPT_ASSIGNMENT = "concept_assignment"
    COLUMN_ASSIGNMENT = "column_assignment"
    COLUMN_ASSIGNMENT_LIST = "column_assignment_list"
    COLUMN_LIST = "column_list"
    GRAIN_CLAUSE = "grain_clause"
    ADDRESS = "address"
    QUERY = "query"
    FILE = "file"
    DATASOURCE_COLUMN_DEF = "datasource_column_def"
    DATASOURCE_COLUMN_LIST = "datasource_column_list"
    DATASOURCE_EXTRA = "datasource_extra"
    RAW_COLUMN_ASSIGNMENT = "raw_column_assignment"
    WHOLE_GRAIN_CLAUSE = "whole_grain_clause"
    DATASOURCE_STATUS_CLAUSE = "datasource_status_clause"
    DATASOURCE_PARTITION_CLAUSE = "datasource_partition_clause"
    DATASOURCE_UPDATE_TRIGGER_CLAUSE = "datasource_update_trigger_clause"


class SyntaxTokenKind(str, Enum):
    COMMENT = "comment"
    IDENTIFIER = "identifier"
    QUOTED_IDENTIFIER = "quoted_identifier"
    STRING_CHARS = "string_chars"
    SINGLE_STRING_CHARS = "single_string_chars"
    DOUBLE_STRING_CHARS = "double_string_chars"
    MULTILINE_STRING = "multiline_string"
    PURPOSE = "purpose"
    AUTO = "auto"
    CONSTANT = "constant"
    PROPERTY = "property"
    UNIQUE = "unique"
    COMPARISON_OPERATOR = "comparison_operator"
    PLUS_OR_MINUS = "plus_or_minus"
    MULTIPLY_DIVIDE_PERCENT = "multiply_divide_percent"
    CONCEPTS = "concepts"
    DATASOURCES = "datasources"
    LINE_SEPARATOR = "line_separator"
    INT_LITERAL_PART = "int_literal_part"
    LOGICAL_AND = "logical_and"
    LOGICAL_OR = "logical_or"
    CONDITION_NOT = "condition_not"
    IMPORT_DOT = "import_dot"
    ORDERING_DIRECTION = "ordering_direction"
    NULLS_SORT = "nulls_sort"
    AS = "as"
    ASSIGN = "assign"
    DATE_PART = "date_part"
    HASH_TYPE = "hash_type"
    WINDOW_TYPE_LEGACY = "window_type_legacy"
    WINDOW_TYPE_SQL = "window_type_sql"
    SELF_IMPORT = "self_import"
    DATASOURCE_STATUS = "datasource_status"
    PUBLISH_ACTION = "publish_action"
    ORDER_IDENTIFIER = "order_identifier"
    CURRENT_DATE = "current_date"
    CURRENT_DATETIME = "current_datetime"
    CURRENT_TIMESTAMP = "current_timestamp"
    SHORTHAND_MODIFIER = "shorthand_modifier"
    WILDCARD_IDENTIFIER = "wildcard_identifier"
    DATASOURCE_PARTIAL = "datasource_partial"
    PERSIST_MODE = "persist_mode"
    FILE_PATH = "file_path"
    F_FILE_PATH = "f_file_path"
    F_QUOTED_ADDRESS = "f_quoted_address"
    CHART_TYPE = "chart_type"
    VALIDATE_SCOPE = "validate_scope"
    COPY_TYPE = "copy_type"
    DATASOURCE_ROOT = "datasource_root"
    DATASOURCE_UPDATE_TRIGGER = "datasource_update_trigger"


LARK_NODE_KIND: dict[str, SyntaxNodeKind] = {
    "start": SyntaxNodeKind.DOCUMENT,
    "block": SyntaxNodeKind.BLOCK,
    "concept": SyntaxNodeKind.CONCEPT,
    "show_statement": SyntaxNodeKind.SHOW_STATEMENT,
    "select_statement": SyntaxNodeKind.SELECT_STATEMENT,
    "show_category": SyntaxNodeKind.SHOW_CATEGORY,
    "parameter_default": SyntaxNodeKind.PARAMETER_DEFAULT,
    "parameter_declaration": SyntaxNodeKind.PARAMETER_DECLARATION,
    "concept_declaration": SyntaxNodeKind.CONCEPT_DECLARATION,
    "concept_property_declaration": SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION,
    "concept_derivation": SyntaxNodeKind.CONCEPT_DERIVATION,
    "constant_derivation": SyntaxNodeKind.CONSTANT_DERIVATION,
    "concept_lit": SyntaxNodeKind.CONCEPT_LITERAL,
    "data_type": SyntaxNodeKind.DATA_TYPE,
    "numeric_type": SyntaxNodeKind.NUMERIC_TYPE,
    "map_type": SyntaxNodeKind.MAP_TYPE,
    "struct_type": SyntaxNodeKind.STRUCT_TYPE,
    "struct_component": SyntaxNodeKind.STRUCT_COMPONENT,
    "struct_lit": SyntaxNodeKind.STRUCT_LITERAL,
    "list_type": SyntaxNodeKind.LIST_TYPE,
    "enum_type": SyntaxNodeKind.ENUM_TYPE,
    "concept_nullable_modifier": SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER,
    "metadata": SyntaxNodeKind.METADATA,
    "int_lit": SyntaxNodeKind.INT_LITERAL,
    "float_lit": SyntaxNodeKind.FLOAT_LITERAL,
    "bool_lit": SyntaxNodeKind.BOOL_LITERAL,
    "null_lit": SyntaxNodeKind.NULL_LITERAL,
    "string_lit": SyntaxNodeKind.STRING_LITERAL,
    "array_lit": SyntaxNodeKind.ARRAY_LITERAL,
    "tuple_lit": SyntaxNodeKind.TUPLE_LITERAL,
    "map_lit": SyntaxNodeKind.MAP_LITERAL,
    "literal": SyntaxNodeKind.LITERAL,
    "product_operator": SyntaxNodeKind.PRODUCT_OPERATOR,
    "sum_operator": SyntaxNodeKind.SUM_OPERATOR,
    "comparison": SyntaxNodeKind.COMPARISON,
    "between_comparison": SyntaxNodeKind.BETWEEN_COMPARISON,
    "parenthetical": SyntaxNodeKind.PARENTHETICAL,
    "prop_ident": SyntaxNodeKind.PROPERTY_IDENTIFIER,
    "prop_ident_wildcard": SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD,
    "comparison_root": SyntaxNodeKind.COMPARISON_ROOT,
    "sum_chain": SyntaxNodeKind.SUM_CHAIN,
    "product_chain": SyntaxNodeKind.PRODUCT_CHAIN,
    "atom": SyntaxNodeKind.ATOM,
    # Phase 1
    "import_statement": SyntaxNodeKind.IMPORT_STATEMENT,
    "selective_import_statement": SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT,
    "self_import_statement": SyntaxNodeKind.SELF_IMPORT_STATEMENT,
    "import_concepts": SyntaxNodeKind.IMPORT_CONCEPTS,
    "select_item": SyntaxNodeKind.SELECT_ITEM,
    "select_transform": SyntaxNodeKind.SELECT_TRANSFORM,
    "select_list": SyntaxNodeKind.SELECT_LIST,
    "select_hide_modifier": SyntaxNodeKind.SELECT_HIDE_MODIFIER,
    "select_partial_modifier": SyntaxNodeKind.SELECT_PARTIAL_MODIFIER,
    "where": SyntaxNodeKind.WHERE,
    "having": SyntaxNodeKind.HAVING,
    "conditional": SyntaxNodeKind.CONDITIONAL,
    "condition_parenthetical": SyntaxNodeKind.CONDITION_PARENTHETICAL,
    "order_by": SyntaxNodeKind.ORDER_BY,
    "order_list": SyntaxNodeKind.ORDER_LIST,
    "ordering": SyntaxNodeKind.ORDERING,
    "limit": SyntaxNodeKind.LIMIT,
    "subselect_comparison": SyntaxNodeKind.SUBSELECT_COMPARISON,
    "expr_tuple": SyntaxNodeKind.EXPR_TUPLE,
    "from_clause": SyntaxNodeKind.FROM_CLAUSE,
    # Phase 2: aggregates
    "aggregate_functions": SyntaxNodeKind.AGGREGATE_FUNCTIONS,
    "aggregate_over": SyntaxNodeKind.AGGREGATE_OVER,
    "aggregate_all": SyntaxNodeKind.AGGREGATE_ALL,
    "aggregate_by": SyntaxNodeKind.AGGREGATE_BY,
    "count": SyntaxNodeKind.COUNT,
    "count_distinct": SyntaxNodeKind.COUNT_DISTINCT,
    "sum": SyntaxNodeKind.SUM,
    "avg": SyntaxNodeKind.AVG,
    "max": SyntaxNodeKind.MAX,
    "min": SyntaxNodeKind.MIN,
    "array_agg": SyntaxNodeKind.ARRAY_AGG,
    "bool_and": SyntaxNodeKind.BOOL_AND,
    "bool_or": SyntaxNodeKind.BOOL_OR,
    "any": SyntaxNodeKind.ANY,
    "fgroup": SyntaxNodeKind.FGROUP,
    "over_list": SyntaxNodeKind.OVER_LIST,
    # Phase 3: merge + cast
    "merge_statement": SyntaxNodeKind.MERGE_STATEMENT,
    "fcast": SyntaxNodeKind.FCAST,
    # Phase 4: filter + case + functions
    "filter_item": SyntaxNodeKind.FILTER_ITEM,
    "fcase": SyntaxNodeKind.FCASE,
    "fcase_when": SyntaxNodeKind.FCASE_WHEN,
    "fcase_else": SyntaxNodeKind.FCASE_ELSE,
    "fcase_simple": SyntaxNodeKind.FCASE_SIMPLE,
    "fcase_simple_when": SyntaxNodeKind.FCASE_SIMPLE_WHEN,
    "fnot": SyntaxNodeKind.FNOT,
    "fbool": SyntaxNodeKind.FBOOL,
    "concat": SyntaxNodeKind.CONCAT,
    "fcoalesce": SyntaxNodeKind.FCOALESCE,
    "fgreatest": SyntaxNodeKind.FGREATEST,
    "fleast": SyntaxNodeKind.FLEAST,
    "fnullif": SyntaxNodeKind.FNULLIF,
    "frecurse_edge": SyntaxNodeKind.FRECURSE_EDGE,
    "len": SyntaxNodeKind.LEN,
    "upper": SyntaxNodeKind.UPPER,
    "flower": SyntaxNodeKind.FLOWER,
    "fsplit": SyntaxNodeKind.FSPLIT,
    "fstrpos": SyntaxNodeKind.FSTRPOS,
    "fsubstring": SyntaxNodeKind.FSUBSTRING,
    "fcontains": SyntaxNodeKind.FCONTAINS,
    "ftrim": SyntaxNodeKind.FTRIM,
    "fltrim": SyntaxNodeKind.FLTRIM,
    "frtrim": SyntaxNodeKind.FRTRIM,
    "freplace": SyntaxNodeKind.FREPLACE,
    "fregexp_extract": SyntaxNodeKind.FREGEXP_EXTRACT,
    "fregexp_contains": SyntaxNodeKind.FREGEXP_CONTAINS,
    "fregexp_replace": SyntaxNodeKind.FREGEXP_REPLACE,
    "fhash": SyntaxNodeKind.FHASH,
    "fhex": SyntaxNodeKind.FHEX,
    "like": SyntaxNodeKind.LIKE,
    "ilike": SyntaxNodeKind.ILIKE,
    "fadd": SyntaxNodeKind.FADD,
    "fsub": SyntaxNodeKind.FSUB,
    "fmul": SyntaxNodeKind.FMUL,
    "fdiv": SyntaxNodeKind.FDIV,
    "fmod": SyntaxNodeKind.FMOD,
    "flog": SyntaxNodeKind.FLOG,
    "fround": SyntaxNodeKind.FROUND,
    "ffloor": SyntaxNodeKind.FFLOOR,
    "fceil": SyntaxNodeKind.FCEIL,
    "fabs": SyntaxNodeKind.FABS,
    "fsqrt": SyntaxNodeKind.FSQRT,
    "frandom": SyntaxNodeKind.FRANDOM,
    "fdate": SyntaxNodeKind.FDATE,
    "fdatetime": SyntaxNodeKind.FDATETIME,
    "ftimestamp": SyntaxNodeKind.FTIMESTAMP,
    "fsecond": SyntaxNodeKind.FSECOND,
    "fminute": SyntaxNodeKind.FMINUTE,
    "fhour": SyntaxNodeKind.FHOUR,
    "fday": SyntaxNodeKind.FDAY,
    "fday_name": SyntaxNodeKind.FDAY_NAME,
    "fday_of_week": SyntaxNodeKind.FDAY_OF_WEEK,
    "fweek": SyntaxNodeKind.FWEEK,
    "fmonth": SyntaxNodeKind.FMONTH,
    "fmonth_name": SyntaxNodeKind.FMONTH_NAME,
    "fformat_time": SyntaxNodeKind.FFORMAT_TIME,
    "fparse_time": SyntaxNodeKind.FPARSE_TIME,
    "fquarter": SyntaxNodeKind.FQUARTER,
    "fyear": SyntaxNodeKind.FYEAR,
    "fdate_trunc": SyntaxNodeKind.FDATE_TRUNC,
    "fdate_part": SyntaxNodeKind.FDATE_PART,
    "fdate_add": SyntaxNodeKind.FDATE_ADD,
    "fdate_sub": SyntaxNodeKind.FDATE_SUB,
    "fdate_diff": SyntaxNodeKind.FDATE_DIFF,
    "fdate_spine": SyntaxNodeKind.FDATE_SPINE,
    "farray_sum": SyntaxNodeKind.FARRAY_SUM,
    "farray_distinct": SyntaxNodeKind.FARRAY_DISTINCT,
    "farray_to_string": SyntaxNodeKind.FARRAY_TO_STRING,
    "farray_sort": SyntaxNodeKind.FARRAY_SORT,
    "farray_transform": SyntaxNodeKind.FARRAY_TRANSFORM,
    "farray_filter": SyntaxNodeKind.FARRAY_FILTER,
    "fgenerate_array": SyntaxNodeKind.FGENERATE_ARRAY,
    "fmap_keys": SyntaxNodeKind.FMAP_KEYS,
    "fmap_values": SyntaxNodeKind.FMAP_VALUES,
    "fgeo_from_text": SyntaxNodeKind.FGEO_FROM_TEXT,
    "fgeo_point": SyntaxNodeKind.FGEO_POINT,
    "fgeo_distance": SyntaxNodeKind.FGEO_DISTANCE,
    "fgeo_x": SyntaxNodeKind.FGEO_X,
    "fgeo_y": SyntaxNodeKind.FGEO_Y,
    "fgeo_centroid": SyntaxNodeKind.FGEO_CENTROID,
    "fgeo_transform": SyntaxNodeKind.FGEO_TRANSFORM,
    "fcurrent_date": SyntaxNodeKind.FCURRENT_DATE,
    "fcurrent_datetime": SyntaxNodeKind.FCURRENT_DATETIME,
    "fcurrent_timestamp": SyntaxNodeKind.FCURRENT_TIMESTAMP,
    "unnest": SyntaxNodeKind.UNNEST,
    "union": SyntaxNodeKind.UNION,
    "subselect": SyntaxNodeKind.SUBSELECT,
    "subselect_where": SyntaxNodeKind.SUBSELECT_WHERE,
    "subselect_order": SyntaxNodeKind.SUBSELECT_ORDER,
    "subselect_limit": SyntaxNodeKind.SUBSELECT_LIMIT,
    # Phase 5: window + function definitions
    "window_item": SyntaxNodeKind.WINDOW_ITEM,
    "window_item_legacy": SyntaxNodeKind.WINDOW_ITEM_LEGACY,
    "window_item_sql": SyntaxNodeKind.WINDOW_ITEM_SQL,
    "window_item_over": SyntaxNodeKind.WINDOW_ITEM_OVER,
    "window_item_order": SyntaxNodeKind.WINDOW_ITEM_ORDER,
    "window_sql_over": SyntaxNodeKind.WINDOW_SQL_OVER,
    "window_sql_partition": SyntaxNodeKind.WINDOW_SQL_PARTITION,
    "window_sql_order": SyntaxNodeKind.WINDOW_SQL_ORDER,
    "function": SyntaxNodeKind.FUNCTION,
    "raw_function": SyntaxNodeKind.RAW_FUNCTION,
    "table_function": SyntaxNodeKind.TABLE_FUNCTION,
    "function_binding_list": SyntaxNodeKind.FUNCTION_BINDING_LIST,
    "function_binding_item": SyntaxNodeKind.FUNCTION_BINDING_ITEM,
    "function_binding_type": SyntaxNodeKind.FUNCTION_BINDING_TYPE,
    "function_binding_default": SyntaxNodeKind.FUNCTION_BINDING_DEFAULT,
    "custom_function": SyntaxNodeKind.CUSTOM_FUNCTION,
    "transform_lambda": SyntaxNodeKind.TRANSFORM_LAMBDA,
    "index_access": SyntaxNodeKind.INDEX_ACCESS,
    "map_key_access": SyntaxNodeKind.MAP_KEY_ACCESS,
    "attr_access": SyntaxNodeKind.ATTR_ACCESS,
    "chained_access": SyntaxNodeKind.CHAINED_ACCESS,
    "over_component": SyntaxNodeKind.OVER_COMPONENT,
    # Phase 6: rowset + multi-select
    "rowset_derivation_statement": SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT,
    "multi_select_statement": SyntaxNodeKind.MULTI_SELECT_STATEMENT,
    "align_clause": SyntaxNodeKind.ALIGN_CLAUSE,
    "align_item": SyntaxNodeKind.ALIGN_ITEM,
    "derive_clause": SyntaxNodeKind.DERIVE_CLAUSE,
    "derive_item": SyntaxNodeKind.DERIVE_ITEM,
    # Phase 7: remaining statements
    "inline_property": SyntaxNodeKind.INLINE_PROPERTY,
    "inline_property_list": SyntaxNodeKind.INLINE_PROPERTY_LIST,
    "prop_ident_list": SyntaxNodeKind.PROP_IDENT_LIST,
    "concept_assignment": SyntaxNodeKind.CONCEPT_ASSIGNMENT,
    "column_assignment": SyntaxNodeKind.COLUMN_ASSIGNMENT,
    "column_assignment_list": SyntaxNodeKind.COLUMN_ASSIGNMENT_LIST,
    "column_list": SyntaxNodeKind.COLUMN_LIST,
    "grain_clause": SyntaxNodeKind.GRAIN_CLAUSE,
    "address": SyntaxNodeKind.ADDRESS,
    "query": SyntaxNodeKind.QUERY,
    "file": SyntaxNodeKind.FILE,
    "datasource": SyntaxNodeKind.DATASOURCE,
    "persist_statement": SyntaxNodeKind.PERSIST_STATEMENT,
    "auto_persist": SyntaxNodeKind.AUTO_PERSIST,
    "full_persist": SyntaxNodeKind.FULL_PERSIST,
    "persist_partition_clause": SyntaxNodeKind.PERSIST_PARTITION_CLAUSE,
    "copy_statement": SyntaxNodeKind.COPY_STATEMENT,
    "rawsql_statement": SyntaxNodeKind.RAWSQL_STATEMENT,
    "validate_statement": SyntaxNodeKind.VALIDATE_STATEMENT,
    "mock_statement": SyntaxNodeKind.MOCK_STATEMENT,
    "publish_statement": SyntaxNodeKind.PUBLISH_STATEMENT,
    "create_statement": SyntaxNodeKind.CREATE_STATEMENT,
    "create_modifier_clause": SyntaxNodeKind.CREATE_MODIFIER_CLAUSE,
    "type_declaration": SyntaxNodeKind.TYPE_DECLARATION,
    "type_drop_clause": SyntaxNodeKind.TYPE_DROP_CLAUSE,
    "chart_statement": SyntaxNodeKind.CHART_STATEMENT,
    "chart_setting": SyntaxNodeKind.CHART_SETTING,
    "chart_field_setting": SyntaxNodeKind.CHART_FIELD_SETTING,
    "chart_bool_setting": SyntaxNodeKind.CHART_BOOL_SETTING,
    "chart_scale_setting": SyntaxNodeKind.CHART_SCALE_SETTING,
    "properties_declaration": SyntaxNodeKind.PROPERTIES_DECLARATION,
    "datasource_column_def": SyntaxNodeKind.DATASOURCE_COLUMN_DEF,
    "datasource_column_list": SyntaxNodeKind.DATASOURCE_COLUMN_LIST,
    "datasource_extra": SyntaxNodeKind.DATASOURCE_EXTRA,
    "raw_column_assignment": SyntaxNodeKind.RAW_COLUMN_ASSIGNMENT,
    "whole_grain_clause": SyntaxNodeKind.WHOLE_GRAIN_CLAUSE,
    "datasource_status_clause": SyntaxNodeKind.DATASOURCE_STATUS_CLAUSE,
    "datasource_partition_clause": SyntaxNodeKind.DATASOURCE_PARTITION_CLAUSE,
    "datasource_update_trigger_clause": SyntaxNodeKind.DATASOURCE_UPDATE_TRIGGER_CLAUSE,
}


LARK_TOKEN_KIND: dict[str, SyntaxTokenKind] = {
    "PARSE_COMMENT": SyntaxTokenKind.COMMENT,
    "IDENTIFIER": SyntaxTokenKind.IDENTIFIER,
    "QUOTED_IDENTIFIER": SyntaxTokenKind.QUOTED_IDENTIFIER,
    "STRING_CHARS": SyntaxTokenKind.STRING_CHARS,
    "SINGLE_STRING_CHARS": SyntaxTokenKind.SINGLE_STRING_CHARS,
    "DOUBLE_STRING_CHARS": SyntaxTokenKind.DOUBLE_STRING_CHARS,
    "MULTILINE_STRING": SyntaxTokenKind.MULTILINE_STRING,
    "PURPOSE": SyntaxTokenKind.PURPOSE,
    "AUTO": SyntaxTokenKind.AUTO,
    "CONST": SyntaxTokenKind.CONSTANT,
    "PROPERTY": SyntaxTokenKind.PROPERTY,
    "UNIQUE": SyntaxTokenKind.UNIQUE,
    "COMPARISON_OPERATOR": SyntaxTokenKind.COMPARISON_OPERATOR,
    "PLUS_OR_MINUS": SyntaxTokenKind.PLUS_OR_MINUS,
    "MULTIPLY_DIVIDE_PERCENT": SyntaxTokenKind.MULTIPLY_DIVIDE_PERCENT,
    "CONCEPTS": SyntaxTokenKind.CONCEPTS,
    "DATASOURCES": SyntaxTokenKind.DATASOURCES,
    "LINE_SEPARATOR": SyntaxTokenKind.LINE_SEPARATOR,
    "__ANON_17": SyntaxTokenKind.INT_LITERAL_PART,
    "LOGICAL_AND": SyntaxTokenKind.LOGICAL_AND,
    "LOGICAL_OR": SyntaxTokenKind.LOGICAL_OR,
    "CONDITION_NOT": SyntaxTokenKind.CONDITION_NOT,
    "IMPORT_DOT": SyntaxTokenKind.IMPORT_DOT,
    "AS": SyntaxTokenKind.AS,
    "_ASSIGN": SyntaxTokenKind.ASSIGN,
    "DATE_PART": SyntaxTokenKind.DATE_PART,
    "HASH_TYPE": SyntaxTokenKind.HASH_TYPE,
    "WINDOW_TYPE_LEGACY": SyntaxTokenKind.WINDOW_TYPE_LEGACY,
    "WINDOW_TYPE_SQL": SyntaxTokenKind.WINDOW_TYPE_SQL,
    "SELF_IMPORT": SyntaxTokenKind.SELF_IMPORT,
    "DATASOURCE_STATUS": SyntaxTokenKind.DATASOURCE_STATUS,
    "PUBLISH_ACTION": SyntaxTokenKind.PUBLISH_ACTION,
    "ORDER_IDENTIFIER": SyntaxTokenKind.ORDER_IDENTIFIER,
    "CURRENT_DATE": SyntaxTokenKind.CURRENT_DATE,
    "CURRENT_DATETIME": SyntaxTokenKind.CURRENT_DATETIME,
    "CURRENT_TIMESTAMP": SyntaxTokenKind.CURRENT_TIMESTAMP,
    "SHORTHAND_MODIFIER": SyntaxTokenKind.SHORTHAND_MODIFIER,
    "WILDCARD_IDENTIFIER": SyntaxTokenKind.WILDCARD_IDENTIFIER,
    "DATASOURCE_PARTIAL": SyntaxTokenKind.DATASOURCE_PARTIAL,
    "PERSIST_MODE": SyntaxTokenKind.PERSIST_MODE,
    "FILE_PATH": SyntaxTokenKind.FILE_PATH,
    "F_FILE_PATH": SyntaxTokenKind.F_FILE_PATH,
    "F_QUOTED_ADDRESS": SyntaxTokenKind.F_QUOTED_ADDRESS,
    "CHART_TYPE": SyntaxTokenKind.CHART_TYPE,
    "VALIDATE_SCOPE": SyntaxTokenKind.VALIDATE_SCOPE,
    "COPY_TYPE": SyntaxTokenKind.COPY_TYPE,
    "DATASOURCE_ROOT": SyntaxTokenKind.DATASOURCE_ROOT,
    "DATASOURCE_UPDATE_TRIGGER": SyntaxTokenKind.DATASOURCE_UPDATE_TRIGGER,
}


@dataclass(slots=True)
class SyntaxMeta:
    line: int | None
    column: int | None
    end_line: int | None
    end_column: int | None
    start_pos: int | None
    end_pos: int | None

    @classmethod
    def from_parser_meta(cls, meta: Any | None) -> "SyntaxMeta | None":
        if meta is None:
            return None
        return cls(
            line=getattr(meta, "line", None),
            column=getattr(meta, "column", None),
            end_line=getattr(meta, "end_line", None),
            end_column=getattr(meta, "end_column", None),
            start_pos=getattr(meta, "start_pos", None),
            end_pos=getattr(meta, "end_pos", None),
        )


@dataclass(slots=True)
class SyntaxToken:
    name: str
    value: str
    meta: SyntaxMeta | None = None
    kind: SyntaxTokenKind | None = None

    @property
    def type(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.value


@dataclass(slots=True)
class SyntaxNode:
    name: str
    children: tuple["SyntaxNode | SyntaxToken", ...]
    meta: SyntaxMeta | None = None
    kind: SyntaxNodeKind | None = None

    def child_nodes(self, kind: SyntaxNodeKind | None = None) -> list["SyntaxNode"]:
        nodes = [child for child in self.children if isinstance(child, SyntaxNode)]
        if kind is None:
            return nodes
        return [child for child in nodes if child.kind == kind]

    def child_tokens(self, kind: SyntaxTokenKind | None = None) -> list["SyntaxToken"]:
        tokens = [child for child in self.children if isinstance(child, SyntaxToken)]
        if kind is None:
            return tokens
        return [child for child in tokens if child.kind == kind]

    def only_child_node(self, kind: SyntaxNodeKind | None = None) -> "SyntaxNode":
        nodes = self.child_nodes(kind)
        if len(nodes) != 1:
            expected = kind.value if kind else "node"
            raise _syntax_error(
                self, f"Expected one child '{expected}' node, found {len(nodes)}"
            )
        return nodes[0]

    def first_child_node(self, kind: SyntaxNodeKind | None = None) -> "SyntaxNode":
        nodes = self.child_nodes(kind)
        if not nodes:
            expected = kind.value if kind else "node"
            raise _syntax_error(self, f"Expected child '{expected}' node")
        return nodes[0]

    def optional_node(self, kind: SyntaxNodeKind) -> "SyntaxNode | None":
        found = self.child_nodes(kind)
        if len(found) > 1:
            raise _syntax_error(found[1], f"Expected at most one '{kind.value}' node")
        return found[0] if found else None

    def optional_token(self, kind: SyntaxTokenKind) -> "SyntaxToken | None":
        found = self.child_tokens(kind)
        if len(found) > 1:
            raise _syntax_error(found[1], f"Expected at most one '{kind.value}' token")
        return found[0] if found else None


SyntaxElement: TypeAlias = SyntaxNode | SyntaxToken


def syntax_name(element: SyntaxElement) -> str:
    return element.kind.value if element.kind else element.name


@dataclass(frozen=True)
class SyntaxDocument:
    text: str
    tree: SyntaxNode

    @property
    def forms(self) -> list[SyntaxElement]:
        return list(self.tree.children)


def _syntax_error(syntax: SyntaxElement, message: str) -> Exception:
    from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError

    return HydrationError(HydrationDiagnostic.from_syntax(message, syntax))


def syntax_from_parser(element: Any) -> SyntaxElement:
    data = getattr(element, "data", None)
    token_type = getattr(element, "type", None)
    if data is not None:
        return SyntaxNode(
            name=data,
            children=tuple(syntax_from_parser(child) for child in element.children),
            meta=SyntaxMeta.from_parser_meta(getattr(element, "meta", None)),
            kind=LARK_NODE_KIND.get(data),
        )
    if token_type is not None:
        return SyntaxToken(
            name=token_type,
            value=element.value,
            meta=SyntaxMeta.from_parser_meta(element),
            kind=LARK_TOKEN_KIND.get(token_type),
        )
    msg = f"Unknown syntax element {element!r}"
    raise TypeError(msg)


def syntax_document_from_parser(text: str, tree: Any) -> SyntaxDocument:
    syntax = syntax_from_parser(tree)
    return SyntaxDocument(text=text, tree=cast(SyntaxNode, syntax))
