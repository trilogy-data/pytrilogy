from enum import Enum

InfiniteFunctionArgs = -1


class UnnestMode(Enum):
    DIRECT = "direct"
    CROSS_APPLY = "cross_apply"
    CROSS_JOIN = "cross_join"
    CROSS_JOIN_UNNEST = "cross_join_unnest"
    CROSS_JOIN_ALIAS = "cross_join_alias"
    PRESTO = "presto"
    SNOWFLAKE = "snowflake"


class ConceptSource(Enum):
    MANUAL = "manual"
    CTE = "cte"
    SELECT = "select"
    PERSIST_STATEMENT = "persist_statement"
    AUTO_DERIVED = "auto_derived"


class StatementType(Enum):
    QUERY = "query"


class Purpose(Enum):
    CONSTANT = "const"
    KEY = "key"
    PROPERTY = "property"
    UNIQUE_PROPERTY = "unique_property"
    METRIC = "metric"
    ROWSET = "rowset"
    AUTO = "auto"
    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value):
        if value == "constant":
            return Purpose.CONSTANT
        return super()._missing_(value)


class Derivation(Enum):
    BASIC = "basic"
    GROUP_TO = "group_to"
    WINDOW = "window"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    CONSTANT = "constant"
    UNNEST = "unnest"
    UNION = "union"
    ROOT = "root"
    ROWSET = "rowset"
    MULTISELECT = "multiselect"
    RECURSIVE = "recursive"


class Granularity(Enum):
    SINGLE_ROW = "single_row"
    MULTI_ROW = "multi_row"


class Modifier(Enum):
    PARTIAL = "Partial"
    OPTIONAL = "Optional"
    HIDDEN = "Hidden"
    NULLABLE = "Nullable"

    @classmethod
    def _missing_(cls, value):
        strval = str(value)
        if strval == "~":
            return Modifier.PARTIAL
        elif strval == "?":
            return Modifier.NULLABLE
        return super()._missing_(value=strval.capitalize())


class JoinType(Enum):
    INNER = "inner"
    LEFT_OUTER = "left outer"
    FULL = "full"
    RIGHT_OUTER = "right outer"
    CROSS = "cross"


class Ordering(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"
    ASC_NULLS_AUTO = "asc nulls auto"
    ASC_NULLS_FIRST = "asc nulls first"
    ASC_NULLS_LAST = "asc nulls last"
    DESC_NULLS_FIRST = "desc nulls first"
    DESC_NULLS_LAST = "desc nulls last"
    DESC_NULLS_AUTO = "desc nulls auto"


class WindowType(Enum):
    ROW_NUMBER = "row_number"
    RANK = "rank"
    LAG = "lag"
    LEAD = "lead"
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"


class WindowOrder(Enum):
    ASCENDING = "top"
    DESCENDING = "bottom"


class FunctionType(Enum):
    # custom
    CUSTOM = "custom"

    # structural
    UNNEST = "unnest"
    RECURSE_EDGE = "recurse_edge"

    UNION = "union"

    ALIAS = "alias"

    PARENTHETICAL = "parenthetical"

    # Generic
    CASE = "case"
    CAST = "cast"
    CONCAT = "concat"
    CONSTANT = "constant"
    TYPED_CONSTANT = "typed_constant"
    COALESCE = "coalesce"
    IS_NULL = "isnull"
    NULLIF = "nullif"
    BOOL = "bool"

    # COMPLEX
    INDEX_ACCESS = "index_access"
    MAP_ACCESS = "map_access"
    ATTR_ACCESS = "attr_access"
    STRUCT = "struct"
    ARRAY = "array"
    DATE_LITERAL = "date_literal"
    DATETIME_LITERAL = "datetime_literal"

    # ARRAY
    ARRAY_DISTINCT = "array_distinct"
    ARRAY_SUM = "array_sum"
    ARRAY_SORT = "array_sort"
    ARRAY_TRANSFORM = "array_transform"
    ARRAY_TO_STRING = "array_to_string"

    # TEXT AND MAYBE MORE
    SPLIT = "split"
    LENGTH = "len"

    # Math
    DIVIDE = "divide"
    MULTIPLY = "multiply"
    ADD = "add"
    SUBTRACT = "subtract"
    MOD = "mod"
    ROUND = "round"
    ABS = "abs"
    SQRT = "sqrt"
    RANDOM = "random"
    FLOOR = "floor"
    CEIL = "ceil"
    LOG = "log"

    # Aggregates
    ## group is not a real aggregate - it just means group by this + some other set of fields
    ## but is here as syntax is identical
    GROUP = "group"

    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
    ARRAY_AGG = "array_agg"

    # String
    LIKE = "like"
    ILIKE = "ilike"
    LOWER = "lower"
    UPPER = "upper"
    SUBSTRING = "substring"
    STRPOS = "strpos"
    CONTAINS = "contains"
    TRIM = "trim"
    REPLACE = "replace"

    # STRING REGEX
    REGEXP_CONTAINS = "regexp_contains"
    REGEXP_EXTRACT = "regexp_extract"
    REGEXP_REPLACE = "regexp_replace"

    # Dates
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"

    # time
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    DAY_OF_WEEK = "day_of_week"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

    DATE_PART = "date_part"
    DATE_TRUNCATE = "date_truncate"
    DATE_ADD = "date_add"
    DATE_SUB = "date_sub"
    DATE_DIFF = "date_diff"

    # UNIX
    UNIX_TO_TIMESTAMP = "unix_to_timestamp"

    # CONSTANTS
    CURRENT_DATE = "current_date"
    CURRENT_DATETIME = "current_datetime"


class FunctionClass(Enum):
    AGGREGATE_FUNCTIONS = [
        FunctionType.MAX,
        FunctionType.MIN,
        FunctionType.SUM,
        FunctionType.AVG,
        FunctionType.ARRAY_AGG,
        FunctionType.COUNT,
        FunctionType.COUNT_DISTINCT,
    ]
    SINGLE_ROW = [
        FunctionType.CONSTANT,
        FunctionType.CURRENT_DATE,
        FunctionType.CURRENT_DATETIME,
    ]

    ONE_TO_MANY = [FunctionType.UNNEST]

    RECURSIVE = [FunctionType.RECURSE_EDGE]


class Boolean(Enum):
    TRUE = "true"
    FALSE = "false"

    @classmethod
    def _missing_(cls, value):
        if value is True:
            return Boolean.TRUE
        elif value is False:
            return Boolean.FALSE
        strval = str(value)
        if strval.lower() != strval:
            return Boolean(strval.lower())


class BooleanOperator(Enum):
    AND = "and"
    OR = "or"

    @classmethod
    def _missing_(cls, value):
        strval = str(value)
        if strval.lower() != strval:
            return BooleanOperator(strval.lower())
        return None


class ComparisonOperator(Enum):
    LT = "<"
    GT = ">"
    EQ = "="
    IS = "is"
    IS_NOT = "is not"
    GTE = ">="
    LTE = "<="
    NE = "!="
    IN = "in"
    NOT_IN = "not in"
    # TODO: deprecate for contains?
    LIKE = "like"
    ILIKE = "ilike"
    CONTAINS = "contains"
    ELSE = "else"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if not isinstance(other, ComparisonOperator):
            return False
        return self.value == other.value

    @classmethod
    def _missing_(cls, value):
        if not isinstance(value, list) and " " in str(value):
            value = str(value).split()
        if isinstance(value, list):
            processed = [str(v).lower() for v in value]
            if processed == ["not", "in"]:
                return ComparisonOperator.NOT_IN
            if processed == ["is", "not"]:
                return ComparisonOperator.IS_NOT
            if value == ["in"]:
                return ComparisonOperator.IN
        if str(value).lower() != str(value):
            return ComparisonOperator(str(value).lower())
        return super()._missing_(str(value).lower())


class DatePart(Enum):
    MONTH = "month"
    YEAR = "year"
    WEEK = "week"
    DAY = "day"
    QUARTER = "quarter"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    DAY_OF_WEEK = "day_of_week"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value.lower() != value:
            return DatePart(value.lower())
        return super()._missing_(value)


class SourceType(Enum):
    FILTER = "filter"
    SELECT = "select"
    ABSTRACT = "abstract"
    DIRECT_SELECT = "direct_select"
    GROUP = "group"
    WINDOW = "window"
    UNNEST = "unnest"
    CONSTANT = "constant"
    ROWSET = "rowset"
    MERGE = "merge"
    BASIC = "basic"
    UNION = "union"
    RECURSIVE = "recursive"


class ShowCategory(Enum):
    DATASOURCES = "datasources"
    CONCEPTS = "concepts"


class IOType(Enum):
    CSV = "csv"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value.lower() != value:
            return IOType(value.lower())
        return super()._missing_(value)
