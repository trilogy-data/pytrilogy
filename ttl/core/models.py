from dataclasses import dataclass
from typing import List, Optional, Union, Dict, Any
from ttl.core.enums import DataType, Purpose, JoinType, Ordering


@dataclass
class Metadata:
    pass

@dataclass
class Concept:
    name:str
    datatype: DataType
    purpose:Purpose
    metadata: Optional[Metadata] = None
    lineage: Optional["Function"] = None

    @property
    def sources(self)->List["Concept"]:
        if self.lineage:
            return self.lineage.arguments
        return []

@dataclass
class ColumnAssignment:
    alias:str
    concept:Concept


@dataclass
class Statement:
    pass

'''
select
    user_id,
    about_me,
    count(post_id)->post_count
;

'''
@dataclass
class Function:
    operator: str
    arguments:List[Concept]
    output_datatype:DataType
    output_purpose: Purpose


@dataclass
class ConceptTransform:
    function:Function
    output:Concept



@dataclass
class SelectItem:
    content:Union[Concept, ConceptTransform]

    @property
    def output(self)->Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        return self.content

    @property
    def input(self)->List[Concept]:
        if isinstance(self.content, ConceptTransform):
            return self.content.function.arguments
        return [self.content]

@dataclass
class OrderItem:
    identifier:str
    order: Ordering

@dataclass
class OrderBy:
    items: List[OrderItem]


@dataclass
class Select:
    selection:List[SelectItem]
    order_by:Optional[OrderBy] = None
    limit: Optional [int] = None

    @property
    def input_components(self)->List[Concept]:
        output = set()
        output_list = []
        for item in self.selection:
            for concept in item.input:
                if concept.name in output:
                    continue
                output.add(concept.name)
                output_list.append(concept)
        return output_list

    @property
    def output_components(self)->List[Concept]:
        output = []
        for item in self.selection:
            output.append(item.output)
        return output

    @property
    def grain(self)->List[Concept]:
        return [item for item in self.output_components if item.purpose == Purpose.KEY]


'''datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (id)
    address bigquery-public-data.stackoverflow.post_history
;
'''


@dataclass
class Address:
    location:str

@dataclass
class Grain:
    components:List[Concept]



@dataclass
class Datasource:
    identifier: str
    columns: List[ColumnAssignment]
    address: Address
    grain: Optional[Grain] = None

    def __post_init__(self):
        # if a user skips defining a grain, use the defined keys
        if not self.grain:
            self.grain = Grain([v for v in self.concepts if v.purpose == Purpose.KEY])
    @property
    def concepts(self)->List[Concept]:
        return [c.concept for c in self.columns]

    def get_alias(self, concept:Concept):
        for x in self.columns:
            if x.concept == concept:
                return x.alias
        raise ValueError(f'Concept {concept.name} not found on {self.identifier}.')


@dataclass
class Comment:
    text:str


@dataclass
class CTE:
    name:str
    source:Union[Datasource, "CTE"]
    output_columns: List[Concept]
    grain: List[Concept]
    base:bool = False
    group_to_grain:bool = False

@dataclass
class CompiledCTE:
    name:str
    statement:str

@dataclass
class JoinKey:
    inner:str
    outer:str

@dataclass
class Join:
    left_cte: CTE
    right_cte: CTE
    jointype: JoinType
    joinkeys: List[JoinKey]


@dataclass
class Environment:
    concepts:Dict[str, Concept]
    datasources: Dict[str, Datasource]

#TODO: combine with CTEs
# CTE contains procesed query?
# or CTE references CTE?
@dataclass
class ProcessedQuery:
    ctes:List[CTE]
    joins: List[Join]
    grain: List[Concept]
    limit:Optional[int] = None
    order_by: Optional[OrderBy] = None
    # base:Dataset
    @property
    def output_columns(self)->List[Concept]:
        output = []
        for cte in self.ctes:
            output+= cte.output_columns
        return output

    @property
    def base(self):
        return [c for c in self.ctes if c.base == True][0]

@dataclass
class Limit:
    value:int