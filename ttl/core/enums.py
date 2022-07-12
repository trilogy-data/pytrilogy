from enum import Enum

class StatementType(Enum):
    QUERY = "query"

class Purpose(Enum):
    KEY = "key"
    PROPERTY = "property"
    METRIC = "metric"

class DataType(Enum):
    STRING = "string"
    BOOL = "bool"
    MAP = "map"
    LIST = "list"
    NUMBER = "number"
    INTEGER = "int"