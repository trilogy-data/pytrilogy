from ttl.core.models import Concept
from typing import Dict

class BaseDialect():

    def compile_sql(self, concepts:Dict[str, Concept], statements):
        raise NotImplementedError