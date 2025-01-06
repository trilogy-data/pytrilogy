from cProfile import Profile
from datetime import datetime
from pathlib import Path
from pstats import SortKey, Stats

from trilogy.core.models_environment import Environment
from trilogy.parsing.parse_engine import parse_text, parse_text_raw


def parsetest():
    env = Environment(working_path=Path(__file__).parent)
    working_path = Path(__file__).parent

    with open(working_path / "query12.preql") as f:
        text = f.read()
    start = datetime.now()
    parse_text_raw(text)
    print(datetime.now() - start)
    start = datetime.now()
    parse_text(text, env)
    print(datetime.now() - start)


# 513958/513273
# 513840/513155
# 446226/445526
if __name__ == "__main__":
    with Profile() as profile:
        parsetest()
        (Stats(profile).strip_dirs().sort_stats(SortKey.TIME).print_stats(25))
