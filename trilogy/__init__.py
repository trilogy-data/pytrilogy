import warnings

# Suppress pydantic warning about field shadowing property in parent class.
# This is intentional - DataTyped ABC defines output_datatype as a property,
# but concrete pydantic models override it with a field.
warnings.filterwarnings(
    "ignore",
    message='Field name "output_datatype".*shadows an attribute',
    category=UserWarning,
)

from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment, EnvironmentConfig
from trilogy.dialect.enums import Dialects
from trilogy.executor import Executor
from trilogy.parser import parse

__version__ = "0.3.166"

__all__ = [
    "parse",
    "Executor",
    "Dialects",
    "Environment",
    "EnvironmentConfig",
    "CONFIG",
]
