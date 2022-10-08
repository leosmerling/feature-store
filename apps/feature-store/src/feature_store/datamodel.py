from datetime import datetime
from enum import Enum
from typing import Optional, List

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
