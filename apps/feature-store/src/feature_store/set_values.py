from typing import Any, List

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PostprocessHook
from hopeit.app.events import Spawn
from hopeit.dataobjects import dataclass, dataobject

from feature_store.datamodel import FeatureValue, FeatureValuesBatch

__steps__ = ["spawn_features"]

__api__ = event_api(
    summary="Feature Store: Submit Feature Values",
    payload=(FeatureValuesBatch, "Batch of `FeatureValue`s to store"),
    responses={
        200: (str, 'acknowledge'),
    }
)


async def spawn_features(payload: FeatureValuesBatch, context: EventContext) -> Spawn[FeatureValue]:
    for feature_values in payload.values:
        yield feature_values


async def __postprocess__(payload: Any, context: EventContext, response: PostprocessHook) -> str:
    return "Feature values submitted."
