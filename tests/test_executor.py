from trilogy import Dialects
from pathlib import Path


def test_file_parsing():
    target = Path(__file__).parent / "test_env.preql"
    parsed = Dialects.DUCK_DB.default_executor().parse_file(target)
    assert len(list(parsed)) == 1
