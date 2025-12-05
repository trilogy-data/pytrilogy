"""Helper script to get the version from trilogy/__init__.py for maturin"""
import re
from pathlib import Path

init_file = Path(__file__).parent / "trilogy" / "__init__.py"
content = init_file.read_text()

match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
if match:
    print(match.group(1))
else:
    print("0.0.0")
