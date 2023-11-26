from logging import getLogger
from dataclasses import dataclass

logger = getLogger("preql")

DEFAULT_NAMESPACE = "local"

ENV_CACHE_NAME = ".preql_cache.json"


# TODO: support loading from environments
@dataclass
class Config:
    strict_mode: bool = True
    hash_identifiers: bool = True


CONFIG = Config()

CONFIG.strict_mode = True
