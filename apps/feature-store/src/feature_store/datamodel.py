from datetime import datetime
from enum import Enum
from typing import Dict, Optional, List, Tuple

from hopeit.dataobjects import dataclass, dataobject


class FeatureDataType(str, Enum):
    NUMERICAL = "n"
    CATEGORICAL = "c"


@dataobject(event_ts="ts", partition=("feature_name", "entity_key"))
@dataclass
class FeatureValue:
    feature_name: str
    entity_key: str
    ts: datetime
    n: Optional[float] = None
    c: Optional[str] = None


@dataobject
@dataclass
class FeatureDefinition:
    name: str
    entity_type: str
    data_type: FeatureDataType


@dataobject
@dataclass
class FeatureValuesBatch:
    values: List[FeatureValue]


@dataobject
@dataclass
class QueryEntity:
    entity_type: str
    feature_names: List[str]


@dataobject
@dataclass
class SeedData:
    index: int
    ts: datetime
    entity_keys: Dict[str, str]  # entity_type -> entity_key


@dataobject
@dataclass
class Query:
    fields: List[QueryEntity]
    seed_data: List[SeedData]


@dataobject
@dataclass
class Job:
    job_id: str
    query: Query


@dataobject
@dataclass
class JobPartition:
    job_id: str
    partition_key: str
    feature_name: str
    entity_type: str
    entity_key: str
    index_timestamps: List[Tuple[int, datetime]]
