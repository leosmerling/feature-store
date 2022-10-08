from typing import Any, List

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PostprocessHook
from hopeit.app.events import Spawn
from hopeit.dataobjects import dataclass, dataobject

from feature_store.datamodel import FeatureValues, FeatureValuesRequest

__steps__ = ["spawn_features"]

__api__ = event_api(
    summary="Feature Store: Submit Feature Values",
    payload=(FeatureValuesRequest, "provide `id` and `user` to create Something"),
    responses={
        200: (str, 'acknowledge'),
    }
)


async def spawn_features(payload: FeatureValuesRequest, context: EventContext) -> Spawn[FeatureValues]:
    for feature_values in payload.items:
        yield feature_values


async def __postprocess__(payload: Any, context: EventContext, response: PostprocessHook) -> str:
    return "Feature values submitted."
