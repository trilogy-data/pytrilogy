from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trilogy.constants import CONFIG
    from trilogy.core.models.environment import Environment, EnvironmentConfig
    from trilogy.dialect.enums import Dialects
    from trilogy.executor import Executor
    from trilogy.parser import parse

__version__ = "0.3.314"

__all__ = [
    "CONFIG",
    "Dialects",
    "Environment",
    "EnvironmentConfig",
    "Executor",
    "parse",
]

# Lazy import for CLI performance
_LAZY_ATTRS: dict[str, str] = {
    "CONFIG": "trilogy.constants",
    "Dialects": "trilogy.dialect.enums",
    "Environment": "trilogy.core.models.environment",
    "EnvironmentConfig": "trilogy.core.models.environment",
    "Executor": "trilogy.executor",
    "parse": "trilogy.parser",
}


def __getattr__(name: str) -> Any:
    module_path = _LAZY_ATTRS.get(name)
    if module_path is None:
        raise AttributeError(f"module 'trilogy' has no attribute '{name}'")
    from importlib import import_module

    value = getattr(import_module(module_path), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*__all__, "__version__"])
