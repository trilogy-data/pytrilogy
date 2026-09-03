import re
from pathlib import Path

from pytest import fixture

from trilogy import Dialects, Environment, Executor

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
URL_PATTERN = re.compile(r"https://storage\.googleapis\.com/[^']+\.parquet")


def localized_setup_sql(setup_sql: Path, data_dir: Path) -> str:
    """``setup.sql`` with its GCS reads pointed at pre-fetched local parquet.

    Falls back to the original text unless every file is present, so a
    checkout that never ran `.scripts/fetch_gcat_data.py` behaves exactly as
    it did before. The httpfs install goes with them: it is only there to
    read those URLs, and it is a network round trip of its own.
    """
    sql = setup_sql.read_text(encoding="utf-8")
    urls = sorted(set(URL_PATTERN.findall(sql)))
    local = {url: data_dir / url.rsplit("/", 1)[-1] for url in urls}
    if not urls or not all(p.exists() and p.stat().st_size for p in local.values()):
        return sql
    for url, path in local.items():
        sql = sql.replace(url, path.as_posix())
    return sql.replace("INSTALL httpfs;", "").replace("LOAD httpfs;", "")


@fixture(scope="session")
def gcat_env_base():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.execute_raw_sql(localized_setup_sql(ROOT / "setup.sql", DATA_DIR))
    yield base


@fixture(scope="function")
def gcat_env(gcat_env_base: Executor):
    gcat_env_base.environment = Environment(
        working_path=Path(__file__).parent,
    )

    yield gcat_env_base
