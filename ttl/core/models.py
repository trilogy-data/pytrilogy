from dataclasses import dataclass
from typing import List, Optional, Union, Dict
from ttl.core.enums import DataType, Purpose


@dataclass
class Metadata:
    pass

@dataclass
class Concept:
    name:str
    datatype: DataType
    purpose:Purpose
    metadata: Optional[Metadata] = None


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
class SelectTransform:
    function:Function
    output:Concept



@dataclass
class SelectItem:
    content:Union[Concept, SelectTransform]

    @property
    def output(self)->Concept:
        if isinstance(self.content, SelectTransform):
            return self.content.output
        return self.content

    @property
    def input(self)->List[Concept]:
        if isinstance(self.content, SelectTransform):
            return self.content.function.arguments
        return [self.content]


@dataclass
class Select:
    selection:List[SelectItem]

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
    components:List[str]


@dataclass
class Datasource:
    identifier: str
    columns: List[ColumnAssignment]
    address: Address
    grain: Optional[Grain] = None

    def __post_init__(self):
        if not self.grain:
            self.grain = Grain([v.name for v in self.concepts])
    @property
    def concepts(self)->List[Concept]:
        return [c.concept for c in self.columns]



@dataclass
class Comment:
    text:str


@dataclass
class Environment:
    concepts:Dict[str, Concept]
    datasources: Dict[str, Datasource]

