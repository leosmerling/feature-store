from datetime import datetime
from enum import Enum
from typing import Optional, List

from hopeit.dataobjects import dataclass, dataobject


class FeatureDataType(str, Enum):
    NUMERICAL = "n"
    CATEGORICAL = "c"


@dataobject
@dataclass
class FeatureValue:
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
class FeatureValues:
    feature_name: str
    entity_key: str
    values: List[FeatureValue]


@dataobject
@dataclass
class FeatureValuesRequest:
    items: List[FeatureValues]
