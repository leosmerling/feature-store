from typing import List

from hopeit.app.context import EventContext
from hopeit.app.events import Spawn
from hopeit.dataobjects import dataclass, dataobject

from feature_store.datamodel import FeatureValues

__steps__ = ["spawn_features"]


@dataobject
@dataclass
class FeatureValuesRequest:
    items: List[FeatureValues]

def spawn_features(payload: FeatureValuesRequest, context: EventContext) -> Spawn[FeatureValues]:
    for feature_values in payload.items:
        yield feature_values

