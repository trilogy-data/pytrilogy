"""Directory-scoped imports for per-eval modules.

Every eval dir has a ``spec.py`` and several a ``db_build.py``. Imported by bare
name they share one ``sys.modules`` slot, so in a process that loads more than
one eval (the viewer, pytest) the first one imported answers for all of them."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_sibling(anchor: str | Path, name: str) -> ModuleType:
    """Import ``<anchor's dir>/<name>.py`` under a module name unique to that
    directory. The directory is on ``sys.path`` for the import only, so the
    sibling's own bare-name imports resolve without leaking to the next eval."""
    eval_dir = Path(anchor).resolve().parent
    module_name = f"_eval_{eval_dir.name}_{name}"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached
    loader_spec = importlib.util.spec_from_file_location(
        module_name, eval_dir / f"{name}.py"
    )
    if loader_spec is None or loader_spec.loader is None:
        raise ImportError(f"No module {name!r} beside {anchor}")
    module = importlib.util.module_from_spec(loader_spec)
    sys.modules[module_name] = module
    added = str(eval_dir) not in sys.path
    if added:
        sys.path.insert(0, str(eval_dir))
    try:
        loader_spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[module_name]
        raise
    finally:
        if added:
            sys.path.remove(str(eval_dir))
    return module
