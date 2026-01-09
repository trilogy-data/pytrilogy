__version__ = "0.3.157"

__all__ = [
    "parse",
    "Executor",
    "Dialects",
    "Environment",
    "EnvironmentConfig",
    "CONFIG",
]

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "CONFIG": ("trilogy.constants", "CONFIG"),
    "Environment": ("trilogy.core.models.environment", "Environment"),
    "EnvironmentConfig": ("trilogy.core.models.environment", "EnvironmentConfig"),
    "Dialects": ("trilogy.dialect.enums", "Dialects"),
    "Executor": ("trilogy.executor", "Executor"),
    "parse": ("trilogy.parser", "parse"),
}

_initialized = False


def _init_warnings():
    global _initialized
    if _initialized:
        return
    _initialized = True
    import warnings

    # Suppress pydantic warning about field shadowing property in parent class.
    # This is intentional - DataTyped ABC defines output_datatype as a property,
    # but concrete pydantic models override it with a field.
    warnings.filterwarnings(
        "ignore",
        message='Field name "output_datatype".*shadows an attribute',
        category=UserWarning,
    )


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        _init_warnings()
        module_path, attr = _LAZY_IMPORTS[name]
        import importlib

        module = importlib.import_module(module_path)
        value = getattr(module, attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
