import ast
from pathlib import Path

import trilogy.dialect

DIALECT_DIR = Path(trilogy.dialect.__file__).parent


def test_no_duplicate_dict_keys_in_dialect_modules():
    # A duplicated key in a dict literal is silently dropped (last wins) — in a
    # dialect FUNCTION_MAP that flips rendering semantics with no warning.
    failures: list[str] = []
    for path in sorted(DIALECT_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            seen: dict[str, int] = {}
            for key in node.keys:
                if key is None:  # **unpacking
                    continue
                sig = ast.dump(key)
                if sig in seen:
                    failures.append(
                        f"{path.name}:{key.lineno} duplicate dict key "
                        f"{ast.unparse(key)} (first defined at line {seen[sig]})"
                    )
                else:
                    seen[sig] = key.lineno
    assert not failures, "\n".join(failures)
