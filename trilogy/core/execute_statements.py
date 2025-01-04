from trilogy.core.execute_models import MaterializedDataset, ProcessedQuery
from trilogy.core.author_models import Datasource
from pydantic import BaseModel
class PersistQueryMixin(BaseModel):
    output_to: MaterializedDataset
    datasource: Datasource
    # base:Dataset



class ProcessedQueryPersist(ProcessedQuery, PersistQueryMixin):
    pass

